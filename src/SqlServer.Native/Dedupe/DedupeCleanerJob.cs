using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

#if (SqlServerDedupe)
namespace NServiceBus.Transport.SqlServerDeduplication
#else
namespace NServiceBus.Transport.SqlServerNative
#endif
{
    public class DedupeCleanerJob
    {
        Table table;
        Func<CancellationToken, Task<DbConnection>> connectionBuilder;
        Action<Exception> criticalError;
        TimeSpan expireWindow;
        TimeSpan frequencyToRunCleanup;
        DedupeCleaner? cleaner;

        /// <summary>
        /// Initializes a new instance of <see cref="DedupeCleanerJob"/>.
        /// </summary>
        /// <param name="criticalError">Called when failed to clean expired records after 10 consecutive unsuccessful attempts. The most likely cause of this is connectivity issues with the database.</param>
        /// <param name="table">The sql <see cref="Table"/> to perform cleanup on.</param>
        public DedupeCleanerJob(
            Table table,
            Func<CancellationToken, Task<DbConnection>> connectionBuilder,
            Action<Exception> criticalError,
            TimeSpan? expireWindow = null,
            TimeSpan? frequencyToRunCleanup = null)
        {
            Guard.AgainstNull(table, nameof(table));
            Guard.AgainstNull(criticalError, nameof(criticalError));
            Guard.AgainstNull(connectionBuilder, nameof(connectionBuilder));
            Guard.AgainstNegativeAndZero(expireWindow, nameof(expireWindow));
            Guard.AgainstNegativeAndZero(frequencyToRunCleanup, nameof(frequencyToRunCleanup));
            this.expireWindow = expireWindow.GetValueOrDefault(TimeSpan.FromDays(1));
            this.frequencyToRunCleanup = frequencyToRunCleanup.GetValueOrDefault(TimeSpan.FromHours(1));
            this.table = table;
            this.connectionBuilder = connectionBuilder;
            this.criticalError = criticalError;
        }

        /// <summary>
        /// Begins the cleanup process. This will run in the background until <see cref="Stop"/> is called.
        /// </summary>
        public virtual void Start()
        {
            cleaner = new DedupeCleaner(async cancellation =>
                {
                    await using var connection = await connectionBuilder(cancellation);
                    var dedupeCleaner = new DedupeManager(connection, table);
                    var expiry = DateTime.UtcNow.Subtract(expireWindow);
                    await dedupeCleaner.CleanupItemsOlderThan(expiry, cancellation);
                },
                criticalError: criticalError,
                frequencyToRunCleanup: frequencyToRunCleanup,
                timer: new AsyncTimer());
            cleaner.Start();
        }

        public virtual Task Stop()
        {
            if (cleaner == null)
            {
                return Task.CompletedTask;
            }

            return cleaner.Stop();
        }
    }
}