 using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class DelayedSenderTests :
    TestBase
{
    static string table = "DelayedSenderTests";
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    [Fact]
    public async Task Single_bytes()
    {
        var localDb = await LocalDb();
        using (var connection = await localDb.OpenConnection())
        {
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();
            var message = BuildBytesMessage();
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadDelayedData(table, connection));
        }
    }

    [Fact]
    public async Task Single_bytes_nulls()
    {
        var localDb = await LocalDb();
        using (var connection = await localDb.OpenConnection())
        {
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();
            var message = BuildBytesNullMessage();
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadDelayedData(table, connection));
        }
    }

    [Fact]
    public async Task Single_stream()
    {
        var localDb = await LocalDb();
        using (var connection = await localDb.OpenConnection())
        {
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();
            var message = BuildStreamMessage();
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadDelayedData(table, connection));
        }
    }

    [Fact]
    public async Task Single_stream_nulls()
    {
        var localDb = await LocalDb();
        using (var connection = await localDb.OpenConnection())
        {
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();

            var message = BuildBytesNullMessage();
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadDelayedData(table, connection));
        }
    }

    [Fact]
    public async Task Batch()
    {
        var messages = new List<OutgoingDelayedMessage>
        {
            BuildBytesMessage(),
            BuildStreamMessage()
        };
        var localDb = await LocalDb();
        using (var connection = await localDb.OpenConnection())
        {
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();

            await manager.Send(messages);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadDelayedData(table, connection));
        }
    }

    [Fact]
    public async Task Batch_nulls()
    {
        var messages = new List<OutgoingDelayedMessage>
        {
            BuildBytesNullMessage(),
            BuildStreamNullMessage()
        };
        var localDb = await LocalDb();
        using (var connection = await localDb.OpenConnection())
        {
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();
            await manager.Send(messages);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadDelayedData(table, connection));
        }
    }

    static OutgoingDelayedMessage BuildBytesMessage()
    {
        return new OutgoingDelayedMessage(dateTime, "headers", Encoding.UTF8.GetBytes("{}"));
    }

    static OutgoingDelayedMessage BuildStreamMessage()
    {
        var stream = new MemoryStream(Encoding.UTF8.GetBytes("{}"));
        return new OutgoingDelayedMessage(dateTime, "headers", stream);
    }

    static OutgoingDelayedMessage BuildBytesNullMessage()
    {
        return new OutgoingDelayedMessage(dateTime, null, bodyBytes: null);
    }

    static OutgoingDelayedMessage BuildStreamNullMessage()
    {
        return new OutgoingDelayedMessage(dateTime, null, bodyStream: null);
    }

    public DelayedSenderTests(ITestOutputHelper output) :
        base(output)
    {
    }
}