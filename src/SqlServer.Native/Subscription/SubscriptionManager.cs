using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
namespace NServiceBus.Transport.SqlServerNative
{
    public class SubscriptionManager
    {
        DbConnection connection;
        Table table;
        DbTransaction? transaction;

        public SubscriptionManager(Table table, DbConnection connection)
        {
            Guard.AgainstNull(table, nameof(table));
            Guard.AgainstNull(connection, nameof(connection));
            this.connection = connection;
            this.table = table;
        }

        public SubscriptionManager(Table table, DbTransaction transaction)
        {
            Guard.AgainstNull(table, nameof(table));
            Guard.AgainstNull(transaction, nameof(transaction));
            Guard.AgainstNull(transaction.Connection, "transaction.Connection");
            this.transaction = transaction;
            this.table = table;
            connection = transaction.Connection!;
        }

        /// <summary>
        /// Drops the table.
        /// </summary>
        public virtual Task Drop(CancellationToken cancellation = default)
        {
            return connection.DropTable(transaction, table, cancellation);
        }

        /// <summary>
        /// Creates the table.
        /// </summary>
        public virtual Task Create(CancellationToken cancellation = default)
        {
            var command = string.Format(SubscriptionTableSql, table);
            return connection.RunCommand(transaction, command, cancellation);
        }

        /// <summary>
        /// The sql statements used to create the subscription table.
        /// </summary>
        public static readonly string SubscriptionTableSql = @"
if exists (
  select *
  from sys.objects
  where object_id = object_id('{0}')
    and type in ('U'))
return

create table {0} (
  QueueAddress nvarchar(200) not null,
  Endpoint nvarchar(200),
  Topic nvarchar(200) not null,
  primary key clustered
  (
    Endpoint,
    Topic
  )
)
";
    }
}