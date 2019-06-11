using System.Data.SqlClient;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Features;
using NServiceBus.Transport.SqlServerNative;

static class EndpointCreator
{
    public static async Task<EndpointConfiguration> Create(string endpointName)
    {
        using (var connection = Connection.OpenConnection())
        {
            return await Create(endpointName, connection);
        }
    }

    public static async Task<EndpointConfiguration> Create(string endpointName, SqlConnection connection)
    {
        var manager = new QueueManager(endpointName, connection);
        await manager.Create();

        var configuration = new EndpointConfiguration(endpointName);
        var transport = configuration.UseTransport<SqlServerTransport>();
        transport.ConnectionString(connection.ConnectionString);
        configuration.DisableFeature<TimeoutManager>();
        configuration.PurgeOnStartup(true);
        configuration.UsePersistence<LearningPersistence>();
        configuration.UseSerialization<NewtonsoftSerializer>();
        configuration.DisableFeature<MessageDrivenSubscriptions>();
        return configuration;
    }
}