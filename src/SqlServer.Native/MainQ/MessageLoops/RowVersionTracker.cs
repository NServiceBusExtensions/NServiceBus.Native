using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.SqlServerNative
{
    public class RowVersionTracker
    {
        string table;

        public RowVersionTracker(string table = "RowVersionTracker")
        {
            Guard.AgainstNullOrEmpty(table, nameof(table));
            this.table = table;
        }

        public Task CreateTable(DbConnection connection, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(connection, nameof(connection));
            return CreateTable(connection, null, cancellation);
        }

        public Task CreateTable(DbTransaction transaction, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(transaction, nameof(transaction));
            return CreateTable(transaction.Connection!, transaction, cancellation);
        }

        Task CreateTable(DbConnection connection, DbTransaction? transaction, CancellationToken cancellation)
        {
            return connection.RunCommand(transaction, string.Format(Sql, table), cancellation);
        }

        public Task Save(DbConnection connection, long rowVersion, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(connection, nameof(connection));
            Guard.AgainstNegativeAndZero(rowVersion, nameof(rowVersion));
            return Save(connection, null, rowVersion, cancellation);
        }

        public Task Save(DbTransaction transaction, long rowVersion, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(transaction, nameof(transaction));
            Guard.AgainstNegativeAndZero(rowVersion, nameof(rowVersion));
            return Save(transaction.Connection!, transaction, rowVersion, cancellation);
        }

        async Task Save(DbConnection connection, DbTransaction? transaction, long rowVersion, CancellationToken cancellation)
        {
            await using var command = connection.CreateCommand(
                transaction: transaction,
                sql: $@"
update {table}
set RowVersion = @RowVersion
if @@rowcount = 0
  insert into {table} (RowVersion)
  values (@RowVersion)
");
            var parameter = command.CreateParameter();
            parameter.ParameterName = "RowVersion";
            parameter.DbType = DbType.Int64;
            parameter.Value = rowVersion;
            command.Parameters.Add(parameter);
            await command.RunNonQuery(cancellation);
        }

        public async Task<long> Get(DbConnection connection, CancellationToken cancellation = default)
        {
            Guard.AgainstNull(connection, nameof(connection));
            await using var command = connection.CreateCommand();
            command.CommandText = $@"
select top (1) RowVersion
from {table}";
            var result = await command.RunScalar(cancellation);
            if (result == null)
            {
                return 1;
            }

            return (long) result;
        }

        static string Sql = @"
if exists (
  select *
  from sys.objects
  where object_id = object_id('{0}')
    and type in ('U'))
return

create table {0} (
  RowVersion bigint not null
);
";
    }
}