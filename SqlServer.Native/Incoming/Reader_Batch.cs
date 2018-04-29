﻿using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public partial class Reader
    {
        public virtual Task<IncomingResult> ReadBytes(string connection, int size, long startRowVersion, Action<IncomingBytesMessage> action, CancellationToken cancellation = default)
        {
            return ReadBytes(connection, size, startRowVersion, action.ToTaskFunc(), cancellation);
        }

        public virtual async Task<IncomingResult> ReadBytes(string connection, int size, long startRowVersion, Func<IncomingBytesMessage, Task> func, CancellationToken cancellation = default)
        {
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNegativeAndZero(startRowVersion, nameof(startRowVersion));
            Guard.AgainstNullOrEmpty(connection, nameof(connection));
            Guard.AgainstNull(func, nameof(func));
            using (var sqlConnection = new SqlConnection(connection))
            {
                await sqlConnection.OpenAsync(cancellation).ConfigureAwait(false);
                return await InnerReadBytes(sqlConnection, size, startRowVersion, func, cancellation)
                    .ConfigureAwait(false);
            }
        }

        public virtual Task<IncomingResult> ReadBytes(SqlConnection connection, int size, long startRowVersion, Action<IncomingBytesMessage> action, CancellationToken cancellation = default)
        {
            return ReadBytes(connection, size, startRowVersion, action.ToTaskFunc(), cancellation);
        }

        public virtual Task<IncomingResult> ReadBytes(SqlConnection connection, int size, long startRowVersion, Func<IncomingBytesMessage, Task> func, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(connection, nameof(connection));
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNegativeAndZero(startRowVersion, nameof(startRowVersion));
            Guard.AgainstNull(func, nameof(func));
            return InnerReadBytes(connection, size, startRowVersion, func, cancellation);
        }

        public virtual Task<IncomingResult> ReadStream(string connection, int size, long startRowVersion, Action<IncomingStreamMessage> action, CancellationToken cancellation = default)
        {
            return ReadStream(connection, size, startRowVersion, action.ToTaskFunc(), cancellation);
        }

        public virtual async Task<IncomingResult> ReadStream(string connection, int size, long startRowVersion, Func<IncomingStreamMessage, Task> func, CancellationToken cancellation = default)
        {
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNegativeAndZero(startRowVersion, nameof(startRowVersion));
            Guard.AgainstNullOrEmpty(connection, nameof(connection));
            Guard.AgainstNull(func, nameof(func));
            using (var sqlConnection = new SqlConnection(connection))
            {
                await sqlConnection.OpenAsync(cancellation).ConfigureAwait(false);
                return await InnerReadStream(sqlConnection, size, startRowVersion, func, cancellation)
                    .ConfigureAwait(false);
            }
        }

        public virtual Task<IncomingResult> ReadStream(SqlConnection connection, int size, long startRowVersion, Action<IncomingStreamMessage> action, CancellationToken cancellation = default)
        {
            return ReadStream(connection, size, startRowVersion, action.ToTaskFunc(), cancellation);
        }

        public virtual Task<IncomingResult> ReadStream(SqlConnection connection, int size, long startRowVersion, Func<IncomingStreamMessage, Task> func, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(connection, nameof(connection));
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNegativeAndZero(startRowVersion, nameof(startRowVersion));
            Guard.AgainstNull(func, nameof(func));
            return InnerReadStream(connection, size, startRowVersion, func, cancellation);
        }

        async Task<IncomingResult> InnerReadBytes(SqlConnection connection, int size, long startRowVersion, Func<IncomingBytesMessage, Task> func, CancellationToken cancellation)
        {
            using (var command = BuildCommand(connection, size, startRowVersion))
            {
                return await command.ReadMultiple(func, cancellation, reader => reader.ReadBytesMessage());
            }
        }

        async Task<IncomingResult> InnerReadStream(SqlConnection connection, int size, long startRowVersion, Func<IncomingStreamMessage, Task> func, CancellationToken cancellation)
        {
            using (var command = BuildCommand(connection, size, startRowVersion))
            {
                return await command.ReadMultiple(func, cancellation, reader => reader.ReadStreamMessage());
            }
        }
    }
}