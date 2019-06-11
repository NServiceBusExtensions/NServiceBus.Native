using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class DelayedConsumerTests : TestBase
{
    string table = "DelayedConsumerTests";

    [Fact]
    public async Task Single()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();
            await manager.SendData();
            using (var result = manager.Consume().Result)
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
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();
            await manager.SendNullData();
            using (var result = manager.Consume().Result)
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
            var manager = new DelayedQueueManager(table, connection);
            await manager.Create();
            await manager.SendMultipleData();

            var messages = new ConcurrentBag<IncomingDelayedVerifyTarget>();
            var result = manager.Consume(size: 3,
                    action: message => { messages.Add(message.ToVerifyTarget()); })
                .Result;
            Assert.Equal(3, result.Count);
            Assert.Equal(3, result.LastRowVersion);
            ObjectApprover.VerifyWithJson(messages.OrderBy(x => x.Due));
        }
    }

    public DelayedConsumerTests(ITestOutputHelper output) :
        base(output)
    {
    }
}