using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public class MessageProcessingLoop :
        MessageLoop
    {
        string table;
        long startingRow;
        Func<CancellationToken, Task<DbConnection>>? connectionBuilder;
        Func<CancellationToken, Task<DbTransaction>>? transactionBuilder;
        Func<DbTransaction, IncomingMessage, CancellationToken, Task>? transactionCallback;
        Func<DbConnection, IncomingMessage, CancellationToken, Task>? connectionCallback;
        Func<DbTransaction, long, CancellationToken, Task>? transactionPersistRowVersion;
        Func<DbConnection, long, CancellationToken, Task>? connectionPersistRowVersion;
        int batchSize;

        public MessageProcessingLoop(
            string table,
            long startingRow,
            Func<CancellationToken, Task<DbTransaction>> transactionBuilder,
            Func<DbTransaction, IncomingMessage, CancellationToken, Task> callback,
            Action<Exception> errorCallback,
            Func<DbTransaction, long, CancellationToken, Task> persistRowVersion,
            int batchSize = 10,
            TimeSpan? delay = null) :
            base(errorCallback, delay)
        {
            Guard.AgainstNullOrEmpty(table, nameof(table));
            Guard.AgainstNegativeAndZero(startingRow, nameof(startingRow));
            Guard.AgainstNull(transactionBuilder, nameof(transactionBuilder));
            Guard.AgainstNull(persistRowVersion, nameof(persistRowVersion));
            Guard.AgainstNull(callback, nameof(callback));
            Guard.AgainstNegativeAndZero(batchSize, nameof(batchSize));
            this.table = table;
            this.startingRow = startingRow;
            this.transactionBuilder = transactionBuilder.WrapFunc(nameof(transactionBuilder));
            transactionCallback = callback.WrapFunc(nameof(transactionCallback));
            transactionPersistRowVersion = persistRowVersion;
            this.batchSize = batchSize;
        }

        public MessageProcessingLoop(
            string table,
            long startingRow,
            Func<CancellationToken, Task<DbConnection>> connectionBuilder,
            Func<DbConnection, IncomingMessage, CancellationToken, Task> callback,
            Action<Exception> errorCallback,
            Func<DbConnection, long, CancellationToken, Task> persistRowVersion,
            int batchSize = 10,
            TimeSpan? delay = null) :
            base(errorCallback, delay)
        {
            Guard.AgainstNullOrEmpty(table, nameof(table));
            Guard.AgainstNegativeAndZero(startingRow, nameof(startingRow));
            Guard.AgainstNull(connectionBuilder, nameof(connectionBuilder));
            Guard.AgainstNull(persistRowVersion, nameof(persistRowVersion));
            Guard.AgainstNull(callback, nameof(callback));
            Guard.AgainstNegativeAndZero(batchSize, nameof(batchSize));
            connectionCallback = callback.WrapFunc(nameof(connectionCallback));
            this.table = table;
            this.startingRow = startingRow;
            this.connectionBuilder = connectionBuilder.WrapFunc(nameof(connectionBuilder));
            connectionPersistRowVersion = persistRowVersion;
            this.batchSize = batchSize;
        }

        protected override async Task RunBatch(CancellationToken cancellation)
        {
            DbConnection? connection = null;
            if (connectionBuilder != null)
            {
                await using (connection = await connectionBuilder(cancellation))
                {
                    var reader = new QueueManager(table, connection);
                    await RunBatch(
                            reader,
                            messageFunc: message => connectionCallback!(connection, message, cancellation),
                            persistFunc: () => connectionPersistRowVersion!(connection, startingRow, cancellation),
                            cancellation);
                }

                return;
            }

            DbTransaction? transaction = null;
            try
            {
                transaction = await transactionBuilder!(cancellation);
                connection = transaction.Connection;
                var reader = new QueueManager(table, transaction);
                try
                {
                    await RunBatch(
                            reader,
                            messageFunc: message => transactionCallback!(transaction, message, cancellation),
                            persistFunc: () => transactionPersistRowVersion!(transaction, startingRow, cancellation),
                            cancellation);
                    await transaction.CommitAsync(cancellation);
                }
                catch
                {
                    await transaction.RollbackAsync(cancellation);
                    throw;
                }
            }
            finally
            {
                if (transaction != null)
                {
                    await transaction.DisposeAsync();
                }

                if (connection != null)
                {
                    await connection.DisposeAsync();
                }
            }
        }

        async Task RunBatch(QueueManager reader, Func<IncomingMessage, Task> messageFunc, Func<Task> persistFunc, CancellationToken cancellation)
        {
            while (true)
            {
                var result = await reader.Read(batchSize, startingRow, messageFunc, cancellation);
                if (result.Count == 0)
                {
                    break;
                }

                startingRow = result.LastRowVersion.GetValueOrDefault(0) + 1;
                await persistFunc();
                if (result.Count < batchSize)
                {
                    break;
                }
            }
        }
    }
}