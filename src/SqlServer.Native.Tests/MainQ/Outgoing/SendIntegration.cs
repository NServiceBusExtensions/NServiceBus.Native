using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;
using Headers = NServiceBus.Transport.SqlServerNative.Headers;

public class SendIntegration :
    TestBase
{
    static ManualResetEvent resetEvent;

    [Fact]
    public async Task Run()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            resetEvent = new ManualResetEvent(false);
            var configuration = await EndpointCreator.Create("IntegrationSend", connection);
            var endpoint = await Endpoint.Start(configuration);
            await SendStartMessage(connection);
            resetEvent.WaitOne();
            await endpoint.Stop();
        }
    }

    static Task SendStartMessage(SqlConnection connection)
    {
        var sender = new QueueManager("IntegrationSend", connection);
        var headers = new Dictionary<string, string>
        {
            {"NServiceBus.EnclosedMessageTypes", typeof(SendMessage).FullName}
        };

        var message = new OutgoingMessage(Guid.NewGuid(), DateTime.Now.AddDays(1), Headers.Serialize(headers), Encoding.UTF8.GetBytes("{}"));
        return sender.Send(message);
    }

    class SendHandler : IHandleMessages<SendMessage>
    {
        public Task Handle(SendMessage message, IMessageHandlerContext context)
        {
            resetEvent.Set();
            return Task.CompletedTask;
        }
    }

    class SendMessage : IMessage
    {
    }

    public SendIntegration(ITestOutputHelper output) : base(output)
    {
    }
}