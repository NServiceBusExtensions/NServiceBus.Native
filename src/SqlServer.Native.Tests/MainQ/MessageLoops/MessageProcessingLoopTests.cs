using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class MessageProcessingLoopTests :
    TestBase
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    string table = "MessageProcessingLoopTests";

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
            using (var loop = new MessageProcessingLoop(
                table: table,
                startingRow: 1,
                connectionBuilder: token => database.OpenConnection(),
                callback: (sqlConnection, message, cancellation) => Task.CompletedTask,
                errorCallback: innerException => { exception = innerException; },
                persistRowVersion: (sqlConnection, currentRowVersion, token) => Task.CompletedTask
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
            var resetEvent = new ManualResetEvent(false);
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await SendMessages(manager);

            var count = 0;

            Task Callback(SqlConnection sqlConnection, IncomingMessage incomingBytesMessage, CancellationToken arg3)
            {
                count++;
                if (count == 5)
                {
                    resetEvent.Set();
                }

                return Task.CompletedTask;
            }

            using (var loop = new MessageProcessingLoop(
                table: table,
                startingRow: 1,
                connectionBuilder: token => database.OpenConnection(),
                callback: Callback,
                errorCallback: exception => { },
                persistRowVersion: (sqlConnection, currentRowVersion, token) => Task.CompletedTask))
            {
                loop.Start();
                resetEvent.WaitOne(TimeSpan.FromSeconds(30));
            }

            Assert.Equal(5, count);
        }
    }

    [Fact]
    public async Task Should_get_correct_next_row_version()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var resetEvent = new ManualResetEvent(false);
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await SendMessages(manager);

            long rowVersion = 0;

            Task PersistRowVersion(SqlConnection sqlConnection, long currentRowVersion, CancellationToken arg3)
            {
                rowVersion = currentRowVersion;
                if (rowVersion == 6)
                {
                    resetEvent.Set();
                }

                return Task.CompletedTask;
            }

            using (var loop = new MessageProcessingLoop(
                table: table,
                startingRow: 1,
                connectionBuilder: token => database.OpenConnection(),
                callback: (collection, message, cancellation) => Task.CompletedTask,
                errorCallback: exception => { },
                persistRowVersion: PersistRowVersion))
            {
                loop.Start();
                resetEvent.WaitOne(TimeSpan.FromSeconds(30));
            }

            Assert.Equal(6, rowVersion);
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

    public MessageProcessingLoopTests(ITestOutputHelper output) :
        base(output)
    {
    }
}