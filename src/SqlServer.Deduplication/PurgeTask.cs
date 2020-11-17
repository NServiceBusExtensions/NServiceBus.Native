using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Features;
using NServiceBus.Transport.SqlServerDeduplication;

class PurgeTask :
    FeatureStartupTask
{
    Table table;
    Func<CancellationToken, Task<DbConnection>> connectionBuilder;

    public PurgeTask(Table table, Func<CancellationToken, Task<DbConnection>> connectionBuilder)
    {
        this.table = table;
        this.connectionBuilder = connectionBuilder;
    }

    protected override async Task OnStart(IMessageSession session)
    {
        await using var connection = await connectionBuilder(CancellationToken.None);
        var dedupeManager = new DedupeManager(connection, table);
        await dedupeManager.PurgeItems();
    }

    protected override Task OnStop(IMessageSession session)
    {
        return Task.CompletedTask;
    }
}