using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
#if (SqlServerDedupe)
namespace NServiceBus.Transport.SqlServerDeduplication
#else
namespace NServiceBus.Transport.SqlServerNative
#endif
{
    public class DedupeManager
    {
        const string writeSqlFormat = "insert into {0} (Id, Context) values (@Id, @Context);";
        const string readSqlFormat = "select Context from {0} where Id = @Id";
        string writeSql = null!;
        string readSql = null!;

        DbConnection connection;
        Table table;
        DbTransaction? transaction;

        public DedupeManager(DbConnection connection, Table table)
        {
            Guard.AgainstNull(table, nameof(table));
            Guard.AgainstNull(connection, nameof(connection));
            this.connection = connection;
            this.table = table;
            InitSql();
        }

        public DedupeManager(DbTransaction transaction, Table table)
        {
            Guard.AgainstNull(table, nameof(table));
            Guard.AgainstNull(transaction, nameof(transaction));
            Guard.AgainstNull(transaction.Connection, "transaction.Connection");
            this.transaction = transaction;
            this.table = table;
            connection = transaction.Connection!;
            InitSql();
        }

        void InitSql()
        {
            writeSql = ConnectionHelpers.WrapInNoCount(string.Format(writeSqlFormat, table));
            readSql = ConnectionHelpers.WrapInNoCount(string.Format(readSqlFormat, table));
        }

        DbCommand BuildReadCommand(Guid messageId)
        {
            var command = connection.CreateCommand(transaction, readSql);
            var parameter = command.CreateParameter();
            parameter.ParameterName = "Id";
            parameter.Value = messageId;
            parameter.DbType = DbType.Guid;
            command.Parameters.Add(parameter);
            return command;
        }

        DbCommand BuildWriteCommand(Guid messageId, string? context)
        {
            var command = connection.CreateCommand(transaction, writeSql);
            var parameters = command.Parameters;
            var idParameter = command.CreateParameter();
            idParameter.ParameterName = "Id";
            idParameter.DbType = DbType.Guid;
            idParameter.Value = messageId;
            parameters.Add(idParameter);
            var contextParam = command.CreateParameter();
            contextParam.ParameterName = "Context";
            contextParam.DbType = DbType.String;
            if (context == null)
            {
                contextParam.Value = DBNull.Value;
            }
            else
            {
                contextParam.Value = context;
            }

            parameters.Add(contextParam);
            return command;
        }

        public async Task<string?> ReadContext(Guid messageId, CancellationToken cancellation = default)
        {
            Guard.AgainstEmpty(messageId, nameof(messageId));
            await using var command = BuildReadCommand(messageId);
            var o = await command.RunScalar(cancellation);
            if (o == DBNull.Value)
            {
                return null;
            }
            return (string?) o;
        }

        public async Task<DedupeResult> WriteDedupRecord(Guid messageId, string? context, CancellationToken cancellation = default)
        {
            Guard.AgainstEmpty(messageId, nameof(messageId));
            try
            {
                await using var command = BuildWriteCommand(messageId, context);
                await command.RunNonQuery(cancellation);
            }
            catch (DbException sqlException)
            {
                if (sqlException.IsKeyViolation())
                {
                    return await BuildDedupeResult(messageId, cancellation);
                }

                throw;
            }

            return new DedupeResult
            (
                dedupeOutcome: DedupeOutcome.Sent,
                context: context
            );
        }

        async Task<DedupeResult> BuildDedupeResult(Guid messageId, CancellationToken cancellation = default)
        {
            return new(
                dedupeOutcome: DedupeOutcome.Deduplicated,
                context: await ReadContext(messageId, cancellation)
            );
        }

        public async Task<DedupeResult> CommitWithDedupCheck(Guid messageId, string? context)
        {
            Guard.AgainstEmpty(messageId, nameof(messageId));
            if (transaction == null)
            {
                throw new Exception($"Can only be used if the {nameof(DbTransaction)} constructor is used.");
            }
            try
            {
                await transaction.CommitAsync();
            }
            catch (DbException sqlException)
            {
                if (sqlException.IsKeyViolation())
                {
                    return await BuildDedupeResult(messageId);
                }

                throw;
            }

            return new DedupeResult
            (
                dedupeOutcome: DedupeOutcome.Sent,
                context: context
            );
        }

        public virtual async Task CleanupItemsOlderThan(DateTime dateTime, CancellationToken cancellation = default)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"delete from {table} where Created < @date";
            var parameter = command.CreateParameter();
            parameter.ParameterName = "date";
            parameter.DbType = DbType.DateTime2;
            parameter.Value = dateTime;
            command.Parameters.Add(parameter);
            await command.RunNonQuery(cancellation);
        }

        public virtual async Task PurgeItems(CancellationToken cancellation = default)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"delete from {table}";
            await command.RunNonQuery(cancellation);
        }

        /// <summary>
        /// Drops a queue.
        /// </summary>
        public virtual Task Drop(CancellationToken cancellation = default)
        {
            return connection.DropTable(transaction, table, cancellation);
        }

        /// <summary>
        /// Creates a queue.
        /// </summary>
        public virtual Task Create(CancellationToken cancellation = default)
        {
            var command = string.Format(DedupeTableSql, table);
            return connection.RunCommand(transaction, command, cancellation);
        }

        /// <summary>
        /// The sql statements used to create the deduplication table.
        /// </summary>
        public static readonly string DedupeTableSql = @"
if exists (
  select *
  from sys.objects
  where object_id = object_id('{0}')
    and type in ('U'))
begin
  if col_length('{0}', 'Context') is null
  begin
    alter table {0}
    add Context nvarchar(max)
  end
  return
end
else
begin
  create table {0} (
    Id uniqueidentifier primary key,
    Created datetime2(0) not null default sysutcdatetime(),
    Context nvarchar(max),
  );
end
";
    }
}