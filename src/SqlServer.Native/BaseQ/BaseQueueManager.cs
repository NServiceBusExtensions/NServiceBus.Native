using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public abstract partial class BaseQueueManager<TIncoming, TOutgoing>
        where TIncoming : class, IIncomingMessage
    {
        protected Table Table;
        protected DbConnection Connection;
        protected DbTransaction? Transaction;

        protected BaseQueueManager(Table table, DbConnection connection)
        {
            Guard.AgainstNull(table, nameof(table));
            Guard.AgainstNull(connection, nameof(connection));
            Table = table;
            Connection = connection;
        }

        protected BaseQueueManager(Table table, DbTransaction transaction)
        {
            Guard.AgainstNull(table, nameof(table));
            Guard.AgainstNull(transaction, nameof(transaction));
            Guard.AgainstNull(transaction.Connection, "transaction.Connection");
            Table = table;
            Transaction = transaction;
            Connection = transaction.Connection!;
        }

        async Task<IncomingResult> ReadMultiple(DbCommand command, Func<TIncoming, Task> func, CancellationToken cancellation)
        {
            var count = 0;
            long? lastRowVersion = null;
            await using var reader = await command.RunSequentialReader(cancellation);
            while (await reader.ReadAsync(cancellation))
            {
                count++;
                cancellation.ThrowIfCancellationRequested();
                await using var message = ReadMessage(reader);
                lastRowVersion = message.RowVersion;
                await func(message);
            }
            return new IncomingResult
            {
                Count = count,
                LastRowVersion = lastRowVersion
            };
        }
    }
}