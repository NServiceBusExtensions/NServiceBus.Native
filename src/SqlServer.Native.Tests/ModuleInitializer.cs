﻿using NServiceBus.Transport.SqlServerNative;

public static class ModuleInitializer
{
    public static void Initialize()
    {
        SqlHelper.EnsureDatabaseExists(Connection.ConnectionString);
        using (var sqlConnection = Connection.OpenConnection())
        {
            var manager = new QueueManager("error", sqlConnection);
            manager.Create().Await();
        }
    }
}