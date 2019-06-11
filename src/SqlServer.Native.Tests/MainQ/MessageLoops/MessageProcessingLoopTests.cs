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
        await SqlConnection.DropTable(null, table);
        var manager = new QueueManager(table, SqlConnection);
        await manager.Create();
        await SendMessages(manager);

        Exception exception = null;
        using (var loop = new MessageProcessingLoop(
            table: table,
            startingRow: 1,
            connectionBuilder: Connection.OpenAsyncConnection,
            callback: (connection, message, cancellation) => Task.CompletedTask,
            errorCallback: innerException => { exception = innerException; },
            persistRowVersion: (connection, currentRowVersion, token) => Task.CompletedTask
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
        await SendMessages(manager);

        var count = 0;

        Task Callback(SqlConnection connection, IncomingMessage incomingBytesMessage, CancellationToken arg3)
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
            connectionBuilder: Connection.OpenAsyncConnection,
            callback: Callback,
            errorCallback: exception => { },
            persistRowVersion: (connection, currentRowVersion, token) => Task.CompletedTask))
        {
            loop.Start();
            resetEvent.WaitOne(TimeSpan.FromSeconds(30));
        }

        Assert.Equal(5, count);
    }

    [Fact]
    public async Task Should_get_correct_next_row_version()
    {
        var resetEvent = new ManualResetEvent(false);
        await SqlConnection.DropTable(null, table);
        var manager = new QueueManager(table, SqlConnection);
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
            connectionBuilder: Connection.OpenAsyncConnection,
            callback: (collection, message, cancellation) => Task.CompletedTask,
            errorCallback: exception => { },
            persistRowVersion: PersistRowVersion))
        {
            loop.Start();
            resetEvent.WaitOne(TimeSpan.FromSeconds(30));
        }

        Assert.Equal(6, rowVersion);
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