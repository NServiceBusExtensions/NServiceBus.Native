﻿using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public partial class Receiver
    {
        public virtual Task<IncomingResult> Receive(string connection, int size, Action<IncomingMessage> action, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(action, nameof(action));
            return Receive(connection, size, action.ToTaskFunc(), cancellation);
        }

        public virtual async Task<IncomingResult> Receive(string connection, int size, Func<IncomingMessage, Task> action, CancellationToken cancellation = default)
        {
            Guard.AgainstNullOrEmpty(connection, nameof(connection));
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNull(action, nameof(action));
            using (var sqlConnection = new SqlConnection(connection))
            {
                await sqlConnection.OpenAsync(cancellation).ConfigureAwait(false);
                return await InnerReceive(sqlConnection, null, size, action, cancellation).ConfigureAwait(false);
            }
        }

        public virtual Task<IncomingResult> Receive(SqlConnection connection, int size, Action<IncomingMessage> action, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(action, nameof(action));
            return Receive(connection, size, action.ToTaskFunc(), cancellation);
        }

        public virtual Task<IncomingResult> Receive(SqlConnection connection, int size, Func<IncomingMessage, Task> action, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(connection, nameof(connection));
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNull(action, nameof(action));
            return InnerReceive(connection, null, size, action, cancellation);
        }

        public virtual Task<IncomingResult> Receive(SqlTransaction transaction, int size, Action<IncomingMessage> action, CancellationToken cancellation = default)
        {
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNull(action, nameof(action));
            return Receive(transaction, size, action.ToTaskFunc(), cancellation);
        }

        public virtual Task<IncomingResult> Receive(SqlTransaction transaction, int size, Func<IncomingMessage, Task> action, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(transaction, nameof(transaction));
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNull(action, nameof(action));
            return InnerReceive(transaction.Connection, transaction, size, action, cancellation);
        }

        async Task<IncomingResult> InnerReceive(SqlConnection connection, SqlTransaction transaction, int size, Func<IncomingMessage, Task> action, CancellationToken cancellation)
        {
            var count = 0;
            long? lastRowVersion = null;
            using (var command = BuildCommand(connection, transaction, size))
            using (var reader = await command.ExecuteSequentialReader(cancellation).ConfigureAwait(false))
            {
                while (reader.Read())
                {
                    count++;
                    cancellation.ThrowIfCancellationRequested();
                    var message = await reader.ReadMessage(cancellation).ConfigureAwait(false);
                    lastRowVersion = message.RowVersion;
                    await action(message).ConfigureAwait(false);
                }
            }

            return new IncomingResult
            {
                Count = count,
                LastRowVersion = lastRowVersion
            };
        }
    }
}