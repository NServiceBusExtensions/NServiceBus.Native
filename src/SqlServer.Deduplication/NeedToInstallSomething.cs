using System.Threading;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Installation;
using NServiceBus.Settings;
using NServiceBus.Transport.SqlServerDeduplication;

class NeedToInstallSomething :
    INeedToInstallSomething
{
    DedupeSettings? settings;

    public NeedToInstallSomething(ReadOnlySettings settings)
    {
        this.settings = settings.GetOrDefault<DedupeSettings>();
    }

    public async Task Install(string identity)
    {
        if (settings == null || settings.InstallerDisabled)
        {
            return;
        }

        await using var connection = await settings.ConnectionBuilder(CancellationToken.None);
        var manager = new DedupeManager(connection, settings.Table);
        await manager.Create();
    }
}