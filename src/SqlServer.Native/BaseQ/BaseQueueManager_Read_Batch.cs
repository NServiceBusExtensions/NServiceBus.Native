using System;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public abstract partial class BaseQueueManager<TIncoming, TOutgoing>
        where TIncoming : class, IIncomingMessage
    {
        public virtual Task<IncomingResult> Read(int size, long startRowVersion, Action<TIncoming> action, CancellationToken cancellation = default)
        {
            return Read(size, startRowVersion, action.ToTaskFunc(), cancellation);
        }

        public virtual async Task<IncomingResult> Read(int size, long startRowVersion, Func<TIncoming, Task> func, CancellationToken cancellation = default)
        {
            Guard.AgainstNegativeAndZero(size, nameof(size));
            Guard.AgainstNegativeAndZero(startRowVersion, nameof(startRowVersion));
            Guard.AgainstNull(func, nameof(func));
            await using var command = BuildReadCommand(size, startRowVersion);
            return await ReadMultiple(command, func, cancellation);
        }
    }
}