using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class WithDedupeTests :
    TestBase
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    string table = "WithDedupeTests";

    [Fact]
    public async Task Single()
    {
        var message = BuildBytesMessage("00000000-0000-0000-0000-000000000001");
        var database = await LocalDb();
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection, "Deduplication");
            await manager.Create();
            var dedupeManager = new DedupeManager(connection, "Deduplication");
            await dedupeManager.Create();
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Single_WithDuplicate()
    {
        var message = BuildBytesMessage("00000000-0000-0000-0000-000000000001");
        var database = await LocalDb();
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection, "Deduplication");
            await manager.Create();
            var dedupeManager = new DedupeManager(connection, "Deduplication");
            await dedupeManager.Create();
            await manager.Send(message);
            await manager.Send(message);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    //[Fact]
    //public void Single_WithPurgedDuplicate()
    //{
    //    var message = BuildBytesMessage("00000000-0000-0000-0000-000000000001");
    //    Send(message);
    //    Send(message);
    //    ObjectApprover.VerifyWithJson(SqlHelper.ReadData(table));
    //}

    [Fact]
    public async Task Batch()
    {
        var messages = new List<OutgoingMessage>
        {
            BuildBytesMessage("00000000-0000-0000-0000-000000000001"),
            BuildBytesMessage("00000000-0000-0000-0000-000000000002")
        };
        var database = await LocalDb();
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection, "Deduplication");
            await manager.Create();
            var dedupeManager = new DedupeManager(connection, "Deduplication");
            await dedupeManager.Create();
            await manager.Send(messages);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Batch_WithFirstDuplicate()
    {
        var message = BuildBytesMessage("00000000-0000-0000-0000-000000000001");
        var messages = new List<OutgoingMessage>
        {
            BuildBytesMessage("00000000-0000-0000-0000-000000000001"),
            BuildBytesMessage("00000000-0000-0000-0000-000000000002")
        };
        var database = await LocalDb();
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection, "Deduplication");
            await manager.Create();
            var dedupeManager = new DedupeManager(connection, "Deduplication");
            await dedupeManager.Create();
            await manager.Send(message);
            await manager.Send(messages);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    [Fact]
    public async Task Batch_WithSecondDuplicate()
    {
        var message = BuildBytesMessage("00000000-0000-0000-0000-000000000002");
        var messages = new List<OutgoingMessage>
        {
            BuildBytesMessage("00000000-0000-0000-0000-000000000001"),
            BuildBytesMessage("00000000-0000-0000-0000-000000000002")
        };
        var database = await LocalDb();
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection, "Deduplication");
            await manager.Create();
            var dedupeManager = new DedupeManager(connection, "Deduplication");
            await dedupeManager.Create();
            await manager.Send(message);
            await manager.Send(messages);
            ObjectApprover.VerifyWithJson(await SqlHelper.ReadData(table, connection));
        }
    }

    static OutgoingMessage BuildBytesMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), dateTime, "headers", Encoding.UTF8.GetBytes("{}"));
    }

    public WithDedupeTests(ITestOutputHelper output) :
        base(output)
    {
    }
}