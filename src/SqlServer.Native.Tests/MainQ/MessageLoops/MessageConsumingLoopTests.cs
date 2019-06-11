using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class MessageConsumingLoopTests : TestBase
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    string table = "MessageConsumingLoopTests";

    [Fact]
    public async Task Should_not_throw_when_run_over_end()
    {
        var manager = new QueueManager(table, SqlConnection);
        await manager.Drop();
        await manager.Create();
        await SendMessages();

        Exception exception = null;
        using (var loop = new MessageConsumingLoop(
            table: table,
            connectionBuilder: Connection.OpenAsyncConnection,
            callback: (connection, message, cancellation) => Task.CompletedTask,
            errorCallback: innerException => { exception = innerException;}
            ))
        {
            loop.Start();
            Thread.Sleep(1000);
        }

        Assert.Null(exception);
    }
    [Fact]
    public async Task Should_get_correct_count()
    {
        var resetEvent = new ManualResetEvent(false);
        await SqlConnection.DropTable(null, table);
        var manager = new QueueManager(table, SqlConnection);
        await manager.Create();
        await SendMessages();

        var count = 0;

        Task Callback(SqlConnection connection, IncomingMessage message, CancellationToken cancellation)
        {
            count++;
            if (count == 5)
            {
                resetEvent.Set();
            }

            return Task.CompletedTask;
        }

        using (var loop = new MessageConsumingLoop(
            table: table,
            connectionBuilder: Connection.OpenAsyncConnection,
            callback: Callback,
            errorCallback: exception => { }))
        {
            loop.Start();
            resetEvent.WaitOne(TimeSpan.FromSeconds(30));
        }

        Assert.Equal(5, count);
    }

    Task SendMessages()
    {
        var sender = new QueueManager(table, SqlConnection);

        return sender.Send(new List<OutgoingMessage>
            {
                BuildMessage("00000000-0000-0000-0000-000000000001"),
                BuildMessage("00000000-0000-0000-0000-000000000002"),
                BuildMessage("00000000-0000-0000-0000-000000000003"),
                BuildMessage("00000000-0000-0000-0000-000000000004"),
                BuildMessage("00000000-0000-0000-0000-000000000005")
            });
    }

    static OutgoingMessage BuildMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), dateTime, "headers", Encoding.UTF8.GetBytes("{}"));
    }

    public MessageConsumingLoopTests(ITestOutputHelper output) :
        base(output)
    {
    }
}