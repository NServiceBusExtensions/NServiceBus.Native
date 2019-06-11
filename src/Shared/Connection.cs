using System;
using System.Data.SqlClient;

public static class Connection
{
    public static string ConnectionString;

    static Connection()
    {
        if (Environment.GetEnvironmentVariable("AppVeyor") == "True")
        {
            ConnectionString = @"Server=(local)\SQL2017;Database=master;User ID=sa;Password=Password12!;Max Pool Size=100;MultipleActiveResultSets=True";
            return;
        }

        ConnectionString = @"Server=.\SQLExpress;Database=NServiceBusNativeTests; Integrated Security=True;Max Pool Size=100;MultipleActiveResultSets=True";
    }

    public static SqlConnection OpenConnection()
    {
        var connection = new SqlConnection(ConnectionString);
        connection.Open();
        return connection;
    }
}