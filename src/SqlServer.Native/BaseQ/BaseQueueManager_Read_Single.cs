using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public abstract partial class BaseQueueManager<TIncoming, TOutgoing>
        where TIncoming : class, IIncomingMessage
    {
        protected abstract DbCommand BuildReadCommand(int batchSize, long startRowVersion);

        public virtual async Task<TIncoming?> Read(long rowVersion, CancellationToken cancellation = default)
        {
            Guard.AgainstNegativeAndZero(rowVersion, nameof(rowVersion));
            var shouldCleanup = false;
            DbDataReader? reader = null;
            try
            {
                await using var command = BuildReadCommand(1, rowVersion);
                reader = await command.RunSingleRowReader(cancellation);
                if (!await reader.ReadAsync(cancellation))
                {
                    shouldCleanup = true;
                    return default;
                }

                return ReadMessage(reader, reader);
            }
            catch
            {
                shouldCleanup = true;
                throw;
            }
            finally
            {
                if (shouldCleanup && reader != null)
                {
                    await reader.DisposeAsync();
                }
            }
        }
    }
}