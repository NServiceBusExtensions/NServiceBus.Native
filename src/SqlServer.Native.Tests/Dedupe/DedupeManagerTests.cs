using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class DedupeManagerTests : TestBase
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    [Fact]
    public async Task Should_only_clean_up_old_item()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var queueManager = new QueueManager("Target", connection, "Deduplication");
            await queueManager.Create();
            var dedupeManager = new DedupeManager(connection, "Deduplication");
            await dedupeManager.Create();

            var message1 = BuildBytesMessage("00000000-0000-0000-0000-000000000001");
            await queueManager.Send(message1);
            Thread.Sleep(1000);
            var now = DateTime.UtcNow;
            Thread.Sleep(1000);
            var message2 = BuildBytesMessage("00000000-0000-0000-0000-000000000002");
            await queueManager.Send(message2);
            var cleaner = new DedupeManager(connection, "Deduplication");
            await cleaner.CleanupItemsOlderThan(now);
            ObjectApprover.VerifyWithJson(SqlHelper.ReadDuplicateData("Deduplication", SqlConnection));
        }
    }

    static OutgoingMessage BuildBytesMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), dateTime, "headers", Encoding.UTF8.GetBytes("{}"));
    }

    public DedupeManagerTests(ITestOutputHelper output) :
        base(output)
    {
    }
}