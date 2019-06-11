using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using NServiceBus.SqlServer.HttpPassthrough;

[Authorize]
[Route("SendMessage")]
public class PassthroughController : ControllerBase
{
    ISqlPassthrough sender;

    public PassthroughController(ISqlPassthrough sender)
    {
        this.sender = sender;
    }

    [HttpPost]
    public async Task Post(CancellationToken cancellation)
    {
        try
        {
            await sender.Send(HttpContext, cancellation);
        }
        catch (SendFailureException exception)
        {
            exception.Data.Add("message", exception.PassthroughMessage.ToDictionary());
            exception.CaptureAndThrow();
        }
    }
}