using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public class MessageConsumingLoop :
        MessageLoop
    {
        string table;
        Func<CancellationToken, Task<DbConnection>>? connectionBuilder;
        Func<CancellationToken, Task<DbTransaction>>? transactionBuilder;
        Func<DbTransaction, IncomingMessage, CancellationToken, Task>? transactionCallback;
        Func<DbConnection, IncomingMessage, CancellationToken, Task>? connectionCallback;
        int batchSize;

        public MessageConsumingLoop(
            string table,
            Func<CancellationToken, Task<DbTransaction>> transactionBuilder,
            Func<DbTransaction, IncomingMessage, CancellationToken, Task> callback,
            Action<Exception> errorCallback,
            int batchSize = 10,
            TimeSpan? delay = null) :
            base(errorCallback, delay)
        {
            Guard.AgainstNullOrEmpty(table, nameof(table));
            Guard.AgainstNull(transactionBuilder, nameof(transactionBuilder));
            Guard.AgainstNull(callback, nameof(callback));
            Guard.AgainstNegativeAndZero(batchSize, nameof(batchSize));
            this.table = table;
            transactionCallback = callback.WrapFunc(nameof(transactionCallback));
            this.transactionBuilder = transactionBuilder.WrapFunc(nameof(transactionBuilder));
            this.batchSize = batchSize;
        }

        public MessageConsumingLoop(
            string table,
            Func<CancellationToken, Task<DbConnection>> connectionBuilder,
            Func<DbConnection, IncomingMessage, CancellationToken, Task> callback,
            Action<Exception> errorCallback,
            int batchSize = 10,
            TimeSpan? delay = null) :
            base(errorCallback, delay)
        {
            Guard.AgainstNullOrEmpty(table, nameof(table));
            Guard.AgainstNull(connectionBuilder, nameof(connectionBuilder));
            Guard.AgainstNegativeAndZero(batchSize, nameof(batchSize));
            Guard.AgainstNull(callback, nameof(callback));
            connectionCallback = callback.WrapFunc(nameof(connectionCallback));
            this.table = table;
            this.connectionBuilder = connectionBuilder.WrapFunc(nameof(this.connectionBuilder));
            this.batchSize = batchSize;
        }

        protected override async Task RunBatch(CancellationToken cancellation)
        {
            DbConnection? connection = null;
            if (connectionBuilder != null)
            {
                await using (connection = await connectionBuilder(cancellation))
                {
                    var consumer = new QueueManager(table, connection);
                    await RunBatch(consumer, message => connectionCallback!(connection, message, cancellation), cancellation);
                }

                return;
            }
            DbTransaction? transaction = null;
            try
            {
                transaction = await transactionBuilder!(cancellation);
                connection = transaction.Connection;
                var consumer = new QueueManager(table, transaction);
                try
                {
                    await RunBatch(consumer, message => transactionCallback!(transaction, message, cancellation), cancellation);
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

        async Task RunBatch(QueueManager consumer, Func<IncomingMessage, Task> action, CancellationToken cancellation)
        {
            while (true)
            {
                var result = await consumer.Consume(batchSize, action, cancellation);
                if (result.Count < batchSize)
                {
                    break;
                }
            }
        }
    }
}