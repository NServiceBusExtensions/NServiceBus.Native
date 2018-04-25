﻿using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlServer.Native
{
    public partial class DelayedSender
    {
        public virtual async Task<long> Send(string connection, OutgoingDelayedMessage message, CancellationToken cancellation = default)
        {
            Guard.AgainstNullOrEmpty(connection, nameof(connection));
            Guard.AgainstNull(message, nameof(message));
            using (var sqlConnection = new SqlConnection(connection))
            {
                await sqlConnection.OpenAsync(cancellation).ConfigureAwait(false);
                return await InnerSend(sqlConnection, null, message, cancellation).ConfigureAwait(false);
            }
        }

        public virtual Task<long> Send(SqlConnection connection, OutgoingDelayedMessage message, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(connection, nameof(connection));
            Guard.AgainstNull(message, nameof(message));
            return InnerSend(connection, null, message, cancellation);
        }

        public virtual Task<long> Send(SqlTransaction transaction, OutgoingDelayedMessage message, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(message, nameof(message));
            Guard.AgainstNull(transaction, nameof(transaction));
            return InnerSend(transaction.Connection, transaction, message, cancellation);
        }

        async Task<long> InnerSend(SqlConnection connection, SqlTransaction transaction, OutgoingDelayedMessage message, CancellationToken cancellation)
        {
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                var parameters = command.Parameters;
                command.CommandText = string.Format(Sql, table);
                parameters.Add("Due", SqlDbType.DateTime).Value = message.Due;
                parameters.Add("Headers", SqlDbType.NVarChar).Value = message.Headers;
                parameters.Add("Body", SqlDbType.VarBinary).SetValueOrDbNull(message.Body);

                var rowVersion = await command.ExecuteScalarAsync(cancellation).ConfigureAwait(false);
                return (long) rowVersion;
            }
        }
    }
}