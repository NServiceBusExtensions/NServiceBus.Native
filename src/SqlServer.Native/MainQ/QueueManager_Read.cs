using System;
using System.Data;
using System.Data.Common;

namespace NServiceBus.Transport.SqlServerNative
{
    public partial class QueueManager
    {
        protected override DbCommand BuildReadCommand(int batchSize, long startRowVersion)
        {
            var command = Connection.CreateCommand(Transaction, string.Format(ReadSql, Table, batchSize));
            var parameter = command.CreateParameter();
            parameter.ParameterName = "RowVersion";
            parameter.DbType = DbType.Int64;
            parameter.Value = startRowVersion;
            command.Parameters.Add(parameter);
            return command;
        }

        public static readonly string ReadSql = ConnectionHelpers.WrapInNoCount(@"
select top({1})
  Id,
  RowVersion,
  Expires,
  Headers,
  datalength(Body),
  Body
from {0}
with (readpast)
where RowVersion >= @RowVersion
order by RowVersion
");

        protected override IncomingMessage ReadMessage(DbDataReader dataReader, params IAsyncDisposable[] cleanups)
        {
            var id = dataReader.GetGuid(0);
            var rowVersion = dataReader.GetInt64(1);
            var expires = dataReader.DatetimeOrNull(2);
            var headers = dataReader.GetFieldValue<string>(3);
            var length = dataReader.LongOrNull(4);
            StreamWrapper? streamWrapper;
            if (length == null)
            {
                streamWrapper = null;
            }
            else
            {
                streamWrapper = new StreamWrapper(length.Value, dataReader.GetStream(5));
            }

            return new IncomingMessage(
                id: id,
                rowVersion: rowVersion,
                expires: expires,
                headers: headers,
                body: streamWrapper,
                cleanups
            );
        }
    }
}