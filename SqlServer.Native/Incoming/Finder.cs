﻿using System.Data;
using System.Data.SqlClient;

namespace NServiceBus.Transport.SqlServerNative
{
    public partial class Finder
    {
        string table;

        public Finder(string table)
        {
            Guard.AgainstNullOrEmpty(table, nameof(table));
            this.table = table;
        }

        SqlCommand BuildCommand(SqlConnection connection, int batchSize, long startRowVersion)
        {
            var command = connection.CreateCommand();
            command.CommandText = string.Format(FindSql, table, batchSize);
            command.Parameters.Add("RowVersion", SqlDbType.BigInt).Value = startRowVersion;
            return command;
        }

        public static readonly string FindSql = SqlHelpers.WrapInNoCount(@"
select top({1})
    Id,
    RowVersion,
    CorrelationId,
    ReplyToAddress,
    Expires,
    Headers,
    Body
from {0}
where RowVersion >= @RowVersion
order by RowVersion
");
    }
}