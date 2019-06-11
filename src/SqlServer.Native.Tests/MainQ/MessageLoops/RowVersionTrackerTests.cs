using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class RowVersionTrackerTests :
    TestBase
{
    public RowVersionTrackerTests(ITestOutputHelper output) :
        base(output)
    {
    }

    [Fact]
    public async Task Run()
    {
        var database = await LocalDb();

        using (var connection = await database.OpenConnection())
        {
            var tracker = new RowVersionTracker();
            await tracker.CreateTable(connection);
            var initial = await tracker.Get(connection);
            Assert.Equal(1, initial);
            await tracker.Save(connection, 4);
            var after = await tracker.Get(connection);
            Assert.Equal(4, after);
        }
    }
}