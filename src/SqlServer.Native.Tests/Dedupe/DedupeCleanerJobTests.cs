using System;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class DedupeCleanerJobTests :
    TestBase
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    string table = "DedupeCleanerJobTests";

    [Fact]
    public async Task Should_only_clean_up_old_item()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var queueManager = new QueueManager(table, connection, "Deduplication");
            await queueManager.Create();
            var dedupeManager = new DedupeManager(connection, "Deduplication");
            await dedupeManager.Create();

            var message1 = BuildBytesMessage("00000000-0000-0000-0000-000000000001");
            await Send(message1,connection);
            Thread.Sleep(1000);
            var now = DateTime.UtcNow;
            Thread.Sleep(1000);

            var message2 = BuildBytesMessage("00000000-0000-0000-0000-000000000002");
            await Send(message2,connection);
            var expireWindow = DateTime.UtcNow - now;
            var cleaner = new DedupeCleanerJob(
                "Deduplication",
                x => database.OpenConnection(),
                exception => { },
                expireWindow,
                frequencyToRunCleanup: TimeSpan.FromMilliseconds(10));
            cleaner.Start();
            Thread.Sleep(100);
            cleaner.Stop().Await();
            ObjectApprover.VerifyWithJson(SqlHelper.ReadDuplicateData("Deduplication", connection));
        }
    }

    Task<long> Send(OutgoingMessage message, SqlConnection connection)
    {
        var sender = new QueueManager(table, connection, "Deduplication");
       return sender.Send(message);
    }

    static OutgoingMessage BuildBytesMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), dateTime, "headers", Encoding.UTF8.GetBytes("{}"));
    }

    public DedupeCleanerJobTests(ITestOutputHelper output) :
        base(output)
    {
    }
}