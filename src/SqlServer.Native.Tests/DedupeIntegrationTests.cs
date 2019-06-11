using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Attachments.Sql;
using NServiceBus.Features;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;
using DedupeOutcome = NServiceBus.Transport.SqlServerDeduplication.DedupeOutcome;
using DedupeResult = NServiceBus.Transport.SqlServerDeduplication.DedupeResult;

public class DedupeIntegrationTests :
    TestBase
{
    static CountdownEvent countdown = new CountdownEvent(2);

    [Fact]
    public async Task Integration()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var dedupeManager = new DedupeManager(connection, "Deduplication");
            await dedupeManager.Create();
            var endpoint = await StartEndpoint(connection.ConnectionString);
            var messageId = Guid.NewGuid();
            var result = await SendMessage(messageId, endpoint, "context1");
            Assert.Equal("context1", result.Context);
            Assert.Equal(DedupeOutcome.Sent, result.DedupeOutcome);
            result = await SendMessage(messageId, endpoint, "context2");
            Assert.Equal("context1", result.Context);
            Assert.Equal(DedupeOutcome.Deduplicated, result.DedupeOutcome);
            if (!countdown.Wait(TimeSpan.FromSeconds(20)))
            {
                throw new Exception("Expected dedup");
            }

            await endpoint.Stop();
        }
    }

    static async Task<DedupeResult> SendMessage(Guid messageId, IEndpointInstance endpoint, string context)
    {
        var sendOptions = new SendOptions();
        sendOptions.RouteToThisEndpoint();
        var sendWithDedupe = await endpoint.SendWithDedupe(messageId, new MyMessage(), sendOptions,context);
        if (sendWithDedupe.DedupeOutcome == DedupeOutcome.Deduplicated)
        {
            countdown.Signal();
        }
        return sendWithDedupe;
    }

    static Task<IEndpointInstance> StartEndpoint(string connection)
    {
        var configuration = new EndpointConfiguration(nameof(DedupeIntegrationTests));
        configuration.UsePersistence<LearningPersistence>();
        configuration.EnableInstallers();
        configuration.EnableDedupe(connection);
        configuration.PurgeOnStartup(true);
        configuration.UseSerialization<NewtonsoftSerializer>();
        configuration.DisableFeature<TimeoutManager>();
        configuration.DisableFeature<MessageDrivenSubscriptions>();

        var attachments = configuration.EnableAttachments(connection, TimeToKeep.Default);
        attachments.UseTransportConnectivity();

        var transport = configuration.UseTransport<SqlServerTransport>();
        transport.ConnectionString(connection);
        return Endpoint.Start(configuration);
    }

    class Handler : IHandleMessages<MyMessage>
    {
        public Task Handle(MyMessage message, IMessageHandlerContext context)
        {
            countdown.Signal();
            return Task.CompletedTask;
        }
    }

    public DedupeIntegrationTests(ITestOutputHelper output) :
        base(output)
    {
    }

    class MyMessage : IMessage
    {
        public string Property { get; set; }
    }
}