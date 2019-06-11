using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class SendTests :
    TestBase
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    string table = "SendTests";

    [Fact]
    public async Task Single_bytes()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            var message = BuildBytesMessage("00000000-0000-0000-0000-000000000001");
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Single_with_transaction()
    {
        var database = await LocalDb();
        var message = BuildBytesMessage("00000000-0000-0000-0000-000000000001");
        using (var connection = await database.OpenConnection())
        {
            using (var transaction = connection.BeginTransaction())
            {
                var manager = new QueueManager(table, transaction);
                await manager.Create();
                await manager.Send(message);
                transaction.Commit();
            }

            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Single_bytes_nulls()
    {
        var database = await LocalDb();
        var message = BuildBytesNullMessage("00000000-0000-0000-0000-000000000001");
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Single_stream()
    {
        var database = await LocalDb();
        var message = BuildStreamMessage("00000000-0000-0000-0000-000000000001");
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Single_stream_nulls()
    {
        var database = await LocalDb();
        var message = BuildStreamMessage("00000000-0000-0000-0000-000000000001");
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Batch()
    {
        var database = await LocalDb();
        var messages = new List<OutgoingMessage>
        {
            BuildBytesMessage("00000000-0000-0000-0000-000000000001"),
            BuildStreamMessage("00000000-0000-0000-0000-000000000002")
        };
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await manager.Send(messages);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Batch_nulls()
    {
        var database = await LocalDb();
        var messages = new List<OutgoingMessage>
        {
            BuildBytesNullMessage("00000000-0000-0000-0000-000000000001"),
            BuildStreamNullMessage("00000000-0000-0000-0000-000000000002")
        };
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await manager.Send(messages);

            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    static OutgoingMessage BuildBytesMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), dateTime, "headers", Encoding.UTF8.GetBytes("{}"));
    }

    static OutgoingMessage BuildStreamMessage(string guid)
    {
        var stream = new MemoryStream(Encoding.UTF8.GetBytes("{}"));
        return new OutgoingMessage(new Guid(guid), dateTime, "headers", stream);
    }

    static OutgoingMessage BuildStreamNullMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), bodyStream: null);
    }

    static OutgoingMessage BuildBytesNullMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), bodyBytes: null);
    }

    public SendTests(ITestOutputHelper output) :
        base(output)
    {
    }
}