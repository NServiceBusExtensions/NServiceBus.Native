using System.Threading.Tasks;
using ApprovalTests;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class DelayedQueueCreationTests :
    TestBase
{
    [Fact]
    public async Task Run()
    {
        var localDb = await LocalDb();
        using (var connection = await localDb.OpenConnection())
        {
            var manager = new DelayedQueueManager("DelayedQueueCreationTests", connection);
            await manager.Create();
            var sqlScriptBuilder = new SqlScriptBuilder(
                tables: true,
                namesToInclude: "DelayedQueueCreationTests");
            Approvals.Verify(sqlScriptBuilder.BuildScript(connection));
        }
    }

    public DelayedQueueCreationTests(ITestOutputHelper output) :
        base(output)
    {
    }
}