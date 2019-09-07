﻿using System.Data.SqlClient;
using Xunit.Abstractions;

public class TestBase:
    XunitApprovalBase
{
    public TestBase(ITestOutputHelper output) :
        base(output)
    {
        SqlConnection = Connection.OpenConnection();
    }

    public SqlConnection SqlConnection;

    public override void Dispose()
    {
        SqlConnection?.Dispose();
        base.Dispose();
    }
}
