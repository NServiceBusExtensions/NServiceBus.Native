using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class ReaderTests :
    TestBase
{
    string table = "ReaderTests";

    [Fact]
    public async Task Single()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await TestDataBuilder.SendData(table, connection);
            var reader = new QueueManager(table, connection);
            using (var result = reader.Read(1).Result)
            {
                ObjectApprover.VerifyWithJson(result.ToVerifyTarget());
            }
        }
    }

    [Fact]
    public async Task Single_nulls()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await TestDataBuilder.SendNullData(table, connection);
            var reader = new QueueManager(table, connection);
            using (var result = reader.Read(1).Result)
            {
                ObjectApprover.VerifyWithJson(result.ToVerifyTarget());
            }
        }
    }

    [Fact]
    public async Task Batch()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await TestDataBuilder.SendMultipleDataAsync(table, connection);

            var reader = new QueueManager(table, connection);
            var messages = new ConcurrentBag<IncomingVerifyTarget>();
            var result = reader.Read(
                    size: 3,
                    startRowVersion: 2,
                    action: message => { messages.Add(message.ToVerifyTarget()); })
                .Result;
            Assert.Equal(4, result.LastRowVersion);
            Assert.Equal(3, result.Count);
        }
    }

    [Fact]
    public async Task Batch_all()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await TestDataBuilder.SendMultipleDataAsync(table, connection);

            var reader = new QueueManager(table, connection);
            var messages = new ConcurrentBag<IncomingVerifyTarget>();
            await reader.Read(
                size: 10,
                startRowVersion: 1,
                action: message => { messages.Add(message.ToVerifyTarget()); });
            ObjectApprover.VerifyWithJson(messages.OrderBy(x => x.Id));
        }
    }

    public ReaderTests(ITestOutputHelper output) :
        base(output)
    {
    }
}