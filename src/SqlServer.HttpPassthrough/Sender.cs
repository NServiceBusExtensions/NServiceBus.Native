using System;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NServiceBus.Transport.SqlServerNative;
using NServiceBus.Attachments.Sql.Raw;
using NServiceBus.SqlServer.HttpPassthrough;
using Table = NServiceBus.Transport.SqlServerNative.Table;

class Sender
{
    Persister attachments;
    Func<CancellationToken, Task<DbConnection>> connectionFunc;
    HeadersBuilder headersBuilder;
    Table dedupeTable;
    ILogger logger;

    public Sender(Func<CancellationToken, Task<DbConnection>> connectionFunc, HeadersBuilder headersBuilder, Table attachmentsTable, Table dedupeTable, ILogger logger)
    {
        this.connectionFunc = connectionFunc;
        attachments = new Persister(new  NServiceBus.Attachments.Sql.Raw.Table(attachmentsTable.TableName, attachmentsTable.Schema, false));
        this.headersBuilder = headersBuilder;
        this.dedupeTable = dedupeTable;
        this.logger = logger;
    }

    public async Task<long> Send(PassthroughMessage message, Table destination, CancellationToken cancellation)
    {
        try
        {
           return await InnerSend(message, destination, cancellation);
        }
        catch (Exception exception)
        {
            throw new SendFailureException(message, exception);
        }
    }

    async Task<long> InnerSend(PassthroughMessage message, Table destination, CancellationToken cancellation)
    {
        await using var connection = await connectionFunc(cancellation);
        await using var transaction = await connection.BeginTransactionAsync(cancellation);
        var rowVersion = await SendInsideTransaction(message, destination, cancellation, transaction);
        await transaction.CommitAsync(cancellation);
        return rowVersion;
    }

    async Task<long> SendInsideTransaction(PassthroughMessage message, Table destination, CancellationToken cancellation, DbTransaction transaction)
    {
        var headersString = headersBuilder.GetHeadersString(message);
        LogSend(message);
        var outgoingMessage = new OutgoingMessage(
            message.Id,
            headers: headersString,
            bodyBytes: Encoding.UTF8.GetBytes(message.Body));
        var queueManager = new QueueManager(destination, transaction, dedupeTable);
        var attachmentExpiry = DateTime.UtcNow.AddDays(10);
        await SendAttachments(transaction, attachmentExpiry, cancellation, message);
        return await queueManager.Send(outgoingMessage, cancellation);
    }

    void LogSend(PassthroughMessage message)
    {
        if (!logger.IsEnabled(LogLevel.Information))
        {
            return;
        }

        logger.LogInformation("Send passthrough. Id:{id}, Destination:{destination}, Namespace:{namespace}, Type:{type}, Body:{body}, Attachments:{attachments},",
            message.Id,
            message.Destination,
            message.Namespace,
            message.Type,
            message.Body,
            message.Attachments.Select(x => x.FileName));
    }

    async Task SendAttachments(DbTransaction transaction, DateTime expiry, CancellationToken cancellation, PassthroughMessage message)
    {
        var connection = transaction.Connection!;
        foreach (var file in message.Attachments)
        {
            await SendAttachment(transaction, message.Id.ToString(), expiry, cancellation, file, connection);
        }
    }

    async Task SendAttachment(DbTransaction transaction, string messageId, DateTime expiry, CancellationToken cancellation, Attachment file, DbConnection connection)
    {
        await using var stream = file.Stream();
        await attachments.SaveStream(connection, transaction, messageId, file.FileName, expiry, stream, cancellation: cancellation);
    }
}