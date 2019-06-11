using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using My.Namespace;
using NServiceBus;
using NServiceBus.Attachments.Sql;
using NServiceBus.SqlServer.HttpPassthrough;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class HttpPassthroughIntegrationTests : TestBase
{
    static ManualResetEvent resetEvent;

    [Fact]
    public async Task Integration()
    {
        using (var connection = Connection.OpenConnection())
        {
            var manager = new DedupeManager(connection, "Deduplication");
            await manager.Create();
            await Installer.CreateTable(connection, "MessageAttachments");
        }

        resetEvent = new ManualResetEvent(false);
        var endpoint = await StartEndpoint();

        await SubmitMultipartForm();

        if (!resetEvent.WaitOne(TimeSpan.FromSeconds(2)))
        {
            throw new Exception("OutgoingMessage not received");
        }

        await endpoint.Stop();
    }

    static async Task SubmitMultipartForm()
    {
        var hostBuilder = new WebHostBuilder();
        hostBuilder.UseStartup<Startup>();
        using (var server = new TestServer(hostBuilder))
        using (var client = server.CreateClient())
        {
            client.DefaultRequestHeaders.Referrer = new Uri("http://TheReferrer");
            var message = "{\"Property\": \"Value\"}";
            var clientFormSender = new ClientFormSender(client);
            await clientFormSender.Send(
                route: "/SendMessage",
                message: message,
                typeName: "MyMessage",
                typeNamespace: "My.Namespace",
                destination: nameof(HttpPassthroughIntegrationTests),
                attachments: new Dictionary<string, byte[]>
                {
                    {"fooFile", Encoding.UTF8.GetBytes("foo")},
                    {"default", Encoding.UTF8.GetBytes("bar")}
                });
        }
    }

    static async Task<IEndpointInstance> StartEndpoint()
    {
        var configuration = await EndpointCreator.Create(nameof(HttpPassthroughIntegrationTests));
        var attachments = configuration.EnableAttachments(Connection.ConnectionString, TimeToKeep.Default);
        attachments.UseTransportConnectivity();
        return await Endpoint.Start(configuration);
    }

    class Handler : IHandleMessages<MyMessage>
    {
        public async Task Handle(MyMessage message, IMessageHandlerContext context)
        {
            var incomingAttachment = context.Attachments();
            Assert.NotNull(await incomingAttachment.GetBytes("fooFile"));
            Assert.NotNull(await incomingAttachment.GetBytes());
            Assert.Equal("Value", message.Property);
            resetEvent.Set();
        }
    }

    public HttpPassthroughIntegrationTests(ITestOutputHelper output) :
        base(output)
    {
    }
}