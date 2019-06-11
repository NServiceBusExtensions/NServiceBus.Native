using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Internal;
using Microsoft.Extensions.Primitives;
using Microsoft.Net.Http.Headers;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class RequestParserTests :
    TestBase
{
    [Fact]
    public void Optional()
    {
        var request = new FakeHttpRequest
        {
            HeadersDictionary = new Dictionary<string, StringValues>
            {
                {"MessageType", "TheMessageType"},
                {"MessageId", new Guid("00000000-0000-0000-0000-000000000001").ToString()},
            },
            Body = Body(),
            Form = Form()
        };
        Verify(request);
    }
    [Fact]
    public void Simple()
    {
        var request = new FakeHttpRequest
        {
            HeadersDictionary = new Dictionary<string, StringValues>
            {
                {"Destination", "TheEndpoint"},
                {"MessageNamespace", "TheMessageNamespace"},
                {"MessageType", "TheMessageType"},
                {"MessageId", new Guid("00000000-0000-0000-0000-000000000001").ToString()},
                {HeaderNames.Referer, "TheReferer"}
            },
            Body = Body(),
            Form = Form()
        };
        Verify(request);
    }

    static MemoryStream Body()
    {
        return new MemoryStream(Encoding.UTF8.GetBytes("{}"));
    }

    static void Verify(FakeHttpRequest request)
    {
        var extract = RequestParser.Extract(request, CancellationToken.None).GetAwaiter().GetResult();
        ObjectApprover.VerifyWithJson(new
        {
            extract.Attachments.Single().FileName,
            extract.Body,
            extract.ClientUrl,
            extract.Destination,
            extract.Id,
            extract.Type
        });
    }

    static FormCollection Form()
    {
        var attachmentBytes = Encoding.UTF8.GetBytes("Attachment Text");
        return new FormCollection(
            new Dictionary<string, StringValues>
            {
                {"message", "{}"}
            },
            new FormFileCollection
            {
                new FormFile(new MemoryStream(attachmentBytes), 0, attachmentBytes.Length, "attachment", "attachment.txt")
            });
    }

    public RequestParserTests(ITestOutputHelper output) :
        base(output)
    {
    }
}