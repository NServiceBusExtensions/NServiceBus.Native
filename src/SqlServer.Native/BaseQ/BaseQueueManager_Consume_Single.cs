using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public abstract partial class BaseQueueManager<TIncoming, TOutgoing>
        where TIncoming : class, IIncomingMessage
    {
        public virtual async Task<TIncoming?> Consume(CancellationToken cancellation = default)
        {
            var shouldCleanup = false;
            DbDataReader? reader = null;
            try
            {
                await using var command = BuildConsumeCommand(1);
                reader = await command.RunSingleRowReader(cancellation);
                if (!await reader.ReadAsync(cancellation))
                {
                    await reader.DisposeAsync();
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