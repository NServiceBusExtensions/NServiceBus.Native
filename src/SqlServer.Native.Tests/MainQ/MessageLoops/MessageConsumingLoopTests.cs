using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class MessageConsumingLoopTests :
    TestBase
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    string table = "MessageConsumingLoopTests";

    [Fact]
    public async Task Should_not_throw_when_run_over_end()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await SendMessages(manager);

            Exception exception = null;
            using (var loop = new MessageConsumingLoop(
                table: table,
                connectionBuilder: token => database.OpenConnection(),
                callback: (connection2, message, cancellation) => Task.CompletedTask,
                errorCallback: innerException => { exception = innerException; }
            ))
            {
                loop.Start();
                Thread.Sleep(1000);
            }

            Assert.Null(exception);
        }
    }

    [Fact]
    public async Task Should_get_correct_count()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            var resetEvent = new ManualResetEvent(false);
            await SendMessages(manager);

            var count = 0;

            Task Callback(SqlConnection connection2, IncomingMessage message, CancellationToken cancellation)
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
                connectionBuilder: token => database.OpenConnection(),
                callback: Callback,
                errorCallback: exception => { }))
            {
                loop.Start();
                resetEvent.WaitOne(TimeSpan.FromSeconds(30));
            }

            Assert.Equal(5, count);
        }
    }

    static Task SendMessages(QueueManager sender)
    {
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