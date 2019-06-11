using System.Data.SqlClient;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using LocalDb;
using Xunit.Abstractions;

public class TestBase:
    XunitLoggingBase
{
    public TestBase(ITestOutputHelper output) :
        base(output)
    {
    }

    static SqlInstance instance;

    static TestBase()
    {
        instance = new SqlInstance(
            name: "SqlServerNative",
            buildTemplate: (SqlConnection connection) =>
            {
            });
    }

    public Task<SqlDatabase> LocalDb(
        string databaseSuffix = null,
        [CallerMemberName] string memberName = null)
    {
        return instance.Build(GetType().Name, databaseSuffix, memberName);
    }
}
