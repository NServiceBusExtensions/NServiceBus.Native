using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class ConsumerTests :
    TestBase
{
    string table = "ConsumerTests";

    [Fact]
    public async Task Single()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager(table, connection);
            await manager.Create();
            await TestDataBuilder.SendData(table, connection);
            var consumer = new QueueManager(table, connection);
            using (var result = consumer.Consume().Result)
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
            manager.Create().Await();
            await TestDataBuilder.SendNullData(table, connection);
            var consumer = new QueueManager(table, connection);
            using (var result = consumer.Consume().Result)
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
            manager.Create().Await();
            await TestDataBuilder.SendMultipleDataAsync(table, connection);

            var consumer = new QueueManager(table, connection);
            var messages = new ConcurrentBag<IncomingVerifyTarget>();
            var result = consumer.Consume(
                    size: 3,
                    action: message => { messages.Add(message.ToVerifyTarget()); })
                .Result;
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
            manager.Create().Await();
            await TestDataBuilder.SendMultipleDataAsync(table, connection);

            var consumer = new QueueManager(table, connection);
            var messages = new ConcurrentBag<IncomingVerifyTarget>();
            var result = consumer.Consume(
                    size: 10,
                    action: message => { messages.Add(message.ToVerifyTarget()); })
                .Result;
            Assert.Equal(5, result.Count);
            ObjectApprover.VerifyWithJson(messages.OrderBy(x => x.Id));
        }
    }

    public ConsumerTests(ITestOutputHelper output) :
        base(output)
    {
    }
}