﻿using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public class RowVersionTracker
    {
        Func<CancellationToken, Task<SqlConnection>> connectionBuilder;

        public RowVersionTracker(string connection):
            this (token => SqlHelpers.OpenConnection(connection,token))
        {
        }

        public RowVersionTracker(Func<CancellationToken, Task<SqlConnection>> connectionBuilder)
        {
            Guard.AgainstNull(connectionBuilder, nameof(connectionBuilder));
            this.connectionBuilder = connectionBuilder;
        }

        public async Task CreateTable(CancellationToken cancellation = default)
        {
            using (var connection = await connectionBuilder(cancellation).ConfigureAwait(false))
            using (var command = connection.CreateCommand())
            {
                command.CommandText = Sql;
                await command.ExecuteNonQueryAsync(cancellation).ConfigureAwait(false);
            }
        }

        public async Task Save(long rowVersion, CancellationToken cancellation = default)
        {
            using (var connection = await connectionBuilder(cancellation).ConfigureAwait(false))
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
update RowVersionTracker
set RowVersion = @RowVersion
if @@rowcount = 0
    insert into RowVersionTracker (RowVersion)
    values (@RowVersion)
";
                command.Parameters.Add("RowVersion", SqlDbType.BigInt).Value = rowVersion;
                await command.ExecuteNonQueryAsync(cancellation).ConfigureAwait(false);
            }
        }

        public async Task<long> Get(CancellationToken cancellation = default)
        {
            using (var connection = await connectionBuilder(cancellation).ConfigureAwait(false))
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
select top (1) RowVersion
from RowVersionTracker";
                var result = await command.ExecuteScalarAsync(cancellation).ConfigureAwait(false);
                if (result == null)
                {
                    return 1;
                }

                return (long)result;
            }
        }

        static string Sql = @"
if exists (
    select *
    from sys.objects
    where object_id = object_id('RowVersionTracker')
        and type in ('U'))
return

create table RowVersionTracker (
    RowVersion bigint not null
);
";
    }
}