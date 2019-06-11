using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;

static class TestDataBuilder
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    public static async Task SendData(string table)
    {
        using (var connection = Connection.OpenConnection())
        {
            await SendData(table, connection);
        }
    }

    public static Task SendData(string table, SqlConnection connection)
    {
        var sender = new QueueManager(table, connection);
        var message = BuildMessage("00000000-0000-0000-0000-000000000001");
        return sender.Send(message);
    }

    public static async Task SendNullData(string table)
    {
        using (var connection = Connection.OpenConnection())
        {
            await SendNullData(table, connection);
        }
    }

    public static Task SendNullData(string table, SqlConnection connection)
    {
        var sender = new QueueManager(table, connection);

        var message = BuildNullMessage("00000000-0000-0000-0000-000000000001");
        return sender.Send(message);
    }

    public static async Task SendMultipleDataAsync(string table)
    {
        using (var connection = Connection.OpenConnection())
        {
            await SendMultipleDataAsync(table, connection);
        }
    }

    public static Task SendMultipleDataAsync(string table, SqlConnection connection)
    {
        var sender = new QueueManager(table, connection);
        return sender.Send(new List<OutgoingMessage>
        {
            BuildMessage("00000000-0000-0000-0000-000000000001"),
            BuildNullMessage("00000000-0000-0000-0000-000000000002"),
            BuildMessage("00000000-0000-0000-0000-000000000003"),
            BuildNullMessage("00000000-0000-0000-0000-000000000004"),
            BuildMessage("00000000-0000-0000-0000-000000000005")
        });
    }

    public static OutgoingMessage BuildMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), dateTime, "headers", Encoding.UTF8.GetBytes("{}"));
    }

    public static OutgoingMessage BuildNullMessage(string guid)
    {
        return new OutgoingMessage(new Guid(guid), bodyBytes: null);
    }
}