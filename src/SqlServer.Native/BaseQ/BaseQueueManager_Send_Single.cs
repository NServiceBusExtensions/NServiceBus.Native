﻿using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public abstract partial class BaseQueueManager<TIncoming, TOutgoing>
        where TIncoming : class, IIncomingMessage
    {
        public virtual Task<long> Send(TOutgoing message, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(message, nameof(message));
            return InnerSend(message, cancellation);
        }

        async Task<long> InnerSend(TOutgoing message, CancellationToken cancellation)
        {
            using var command = CreateSendCommand();
            PopulateSendCommand(command, message);
            var rowVersion = await command.RunScalar(cancellation);
            if (rowVersion == null)
            {
                return 0;
            }

            return (long) rowVersion;
        }

        protected abstract DbCommand CreateSendCommand();
        protected abstract void PopulateSendCommand(DbCommand command, TOutgoing message);
    }
}