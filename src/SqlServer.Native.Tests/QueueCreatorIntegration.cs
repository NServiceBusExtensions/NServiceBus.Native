﻿using System.Threading;
using System.Threading.Tasks;
using NServiceBus;
using Xunit;

public class QueueCreatorIntegration
{
    static ManualResetEvent resetEvent;

    static QueueCreatorIntegration()
    {
        DbSetup.Setup();
    }

    [Fact]
    public async Task Run()
    {
        resetEvent = new ManualResetEvent(false);
        var configuration = await EndpointCreator.Create("IntegrationSend");
        var endpoint = await Endpoint.Start(configuration);
        await SendStartMessage(endpoint);
        resetEvent.WaitOne();
        await endpoint.Stop();
    }

    static Task SendStartMessage(IEndpointInstance endpoint)
    {
        var sendOptions = new SendOptions();
        sendOptions.RouteToThisEndpoint();
        return endpoint.Send(new SendMessage(), sendOptions);
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
}