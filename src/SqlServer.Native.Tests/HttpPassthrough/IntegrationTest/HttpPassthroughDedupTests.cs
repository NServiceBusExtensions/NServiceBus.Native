using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using NServiceBus;
using NServiceBus.SqlServer.HttpPassthrough;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class HttpPassthroughDedupTests :
    TestBase
{
    static int count;
    [Fact]
    public async Task Integration()
    {
        using (var connection = Connection.OpenConnection())
        {
            var manager = new DedupeManager(connection, "Deduplication");
            await manager.Create();
        }

        var endpoint = await StartEndpoint();

        var hostBuilder = new WebHostBuilder();
        hostBuilder.UseStartup<Startup>();
        using (var server = new TestServer(hostBuilder))
        using (var client = server.CreateClient())
        {
            client.DefaultRequestHeaders.Referrer = new Uri("http://TheReferrer");
            var clientFormSender = new ClientFormSender(client);
            var guid = Guid.NewGuid();
            var first = await SendAsync(clientFormSender, guid);
            Assert.Equal(202, first);
            var second = await SendAsync(clientFormSender, guid);
            Assert.Equal(208, second);
        }

        Thread.Sleep(3000);

        await endpoint.Stop();
        Assert.Equal(1, count);
    }

    static async Task<int> SendAsync(ClientFormSender clientFormSender, Guid guid)
    {
        var send = await clientFormSender.Send(
            route: "/SendMessage",
            message: "{}",
            typeName: "DedupMessage",
            destination: nameof(HttpPassthroughDedupTests),
            messageId: guid);
        return send.httpStatus;
    }

    static async Task<IEndpointInstance> StartEndpoint()
    {
        var configuration = await EndpointCreator.Create(nameof(HttpPassthroughDedupTests));
        return await Endpoint.Start(configuration);
    }

    class Handler : IHandleMessages<DedupMessage>
    {
        public Task Handle(DedupMessage message, IMessageHandlerContext context)
        {
            Interlocked.Increment(ref count);
            return Task.CompletedTask;
        }
    }

    public HttpPassthroughDedupTests(ITestOutputHelper output) : base(output)
    {
    }
}
class DedupMessage : IMessage
{
}