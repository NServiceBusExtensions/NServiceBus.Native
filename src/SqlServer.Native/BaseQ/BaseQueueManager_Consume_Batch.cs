using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    /// <summary>
    /// Base class for <see cref="DelayedQueueManager"/> and <see cref="QueueManager"/>.
    /// Not to be used as an extension point.
    /// </summary>
    public abstract partial class BaseQueueManager<TIncoming, TOutgoing>
        where TIncoming : class, IIncomingMessage
    {
        protected abstract DbCommand BuildConsumeCommand(int batchSize);

        protected abstract TIncoming ReadMessage(DbDataReader dataReader, params IAsyncDisposable[] cleanups);
        public virtual Task<IncomingResult> Consume(int size, Action<TIncoming> action, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(action, nameof(action));
            return Consume(size, action.ToTaskFunc(), cancellation);
        }

        public virtual async Task<IncomingResult> Consume(int size, Func<TIncoming, Task> func, CancellationToken cancellation = default)
        {
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNull(func, nameof(func));
            await using var command = BuildConsumeCommand(size);
            return await ReadMultiple(command, func, cancellation);
        }
    }
}