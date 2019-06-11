using System.Threading.Tasks;
using ApprovalTests;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class MainQueueCreationTests :
    TestBase
{
    [Fact]
    public async Task Run()
    {
        var database = await LocalDb();
        using (var connection = await database.OpenConnection())
        {
            var manager = new QueueManager("MainQueueCreationTests", connection);
            await manager.Create();
            var sqlScriptBuilder = new SqlScriptBuilder(
                tables: true,
                namesToInclude: "MainQueueCreationTests");
            Approvals.Verify(sqlScriptBuilder.BuildScript(connection));
        }
    }

    public MainQueueCreationTests(ITestOutputHelper output) :
        base(output)
    {
    }
}