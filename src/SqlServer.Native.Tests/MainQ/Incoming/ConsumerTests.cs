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
        await TestDataBuilder.SendData(table);
        var consumer = new QueueManager(table, SqlConnection);
        using (var result = consumer.Consume().Result)
        {
            ObjectApprover.VerifyWithJson(result.ToVerifyTarget());
        }
    }

    [Fact]
    public async Task Single_nulls()
    {
        await TestDataBuilder.SendNullData(table);
        var consumer = new QueueManager(table, SqlConnection);
        using (var result = consumer.Consume().Result)
        {
            ObjectApprover.VerifyWithJson(result.ToVerifyTarget());
        }
    }

    [Fact]
    public async Task Batch()
    {
        await TestDataBuilder.SendMultipleDataAsync(table);

        var consumer = new QueueManager(table, SqlConnection);
        var messages = new ConcurrentBag<IncomingVerifyTarget>();
        var result = consumer.Consume(
                size: 3,
                action: message => { messages.Add(message.ToVerifyTarget()); })
            .Result;
        Assert.Equal(3, result.Count);
    }

    [Fact]
    public async Task Batch_all()
    {
        await TestDataBuilder.SendMultipleDataAsync(table);

        var consumer = new QueueManager(table, SqlConnection);
        var messages = new ConcurrentBag<IncomingVerifyTarget>();
        var result = consumer.Consume(
                size: 10,
                action: message => { messages.Add(message.ToVerifyTarget()); })
            .Result;
        Assert.Equal(5, result.Count);
        ObjectApprover.VerifyWithJson(messages.OrderBy(x => x.Id));
    }

    public ConsumerTests(ITestOutputHelper output) : base(output)
    {
        var manager = new QueueManager(table, SqlConnection);
        manager.Drop().Await();
        manager.Create().Await();
    }
}