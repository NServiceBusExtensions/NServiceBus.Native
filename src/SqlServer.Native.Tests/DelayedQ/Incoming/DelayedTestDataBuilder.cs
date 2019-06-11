using System;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;

static class DelayedTestDataBuilder
{
    static DateTime dateTime = new DateTime(2000, 1, 1, 1, 1, 1, DateTimeKind.Utc);

    public static Task SendData(this DelayedQueueManager sender)
    {
        var message = BuildMessage();
        return sender.Send(message);
    }

    public static Task SendNullData(this DelayedQueueManager sender)
    {
        var message = BuildNullMessage();
        return sender.Send(message);
    }

    public static async Task SendMultipleData(this DelayedQueueManager sender)
    {
        var time = dateTime;
        await sender.Send(new OutgoingDelayedMessage(time, "headers", Encoding.UTF8.GetBytes("{}")));
        time = time.AddSeconds(1);
        await sender.Send(new OutgoingDelayedMessage(time, "{}", bodyBytes: null));
        time = time.AddSeconds(1);
        await sender.Send(new OutgoingDelayedMessage(time, "headers", Encoding.UTF8.GetBytes("{}")));
        time = time.AddSeconds(1);
        await sender.Send(new OutgoingDelayedMessage(time, "{}", bodyBytes: null));
        time = time.AddSeconds(1);
        await sender.Send(new OutgoingDelayedMessage(time, "headers", Encoding.UTF8.GetBytes("{}")));
    }

    public static OutgoingDelayedMessage BuildMessage()
    {
        return new OutgoingDelayedMessage(dateTime, "headers", Encoding.UTF8.GetBytes("{}"));
    }

    public static OutgoingDelayedMessage BuildNullMessage()
    {
        return new OutgoingDelayedMessage(dateTime, "{}", bodyBytes: null);
    }
}