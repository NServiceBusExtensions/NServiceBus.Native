using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using NServiceBus.SqlServer.HttpPassthrough;
using ObjectApproval;
using Xunit;
using Xunit.Abstractions;

public class ClaimsAppenderTests :
    TestBase
{
    [Fact]
    public void Append()
    {
        var headers = new Dictionary<string, string>();
        var claims = BuildClaims();
        ClaimsAppender.Append(claims, headers, "prefix.");
        ObjectApprover.VerifyWithJson(headers);
    }

    [Fact]
    public void Extract()
    {
        var headers = new Dictionary<string, string>();
        var claims = BuildClaims();
        ClaimsAppender.Append(claims, headers, "prefix.");
        var result = ClaimsAppender.Extract(headers, "prefix.")
            .Select(x => new
            {
                x.Type,
                x.Value
            });
        ObjectApprover.VerifyWithJson(result);
    }

    static IEnumerable<Claim> BuildClaims()
    {
        yield return new Claim(ClaimTypes.Email, "User@foo.bar");
        yield return new Claim(ClaimTypes.NameIdentifier, "User1");
        yield return new Claim(ClaimTypes.NameIdentifier, "User2");
    }

    public ClaimsAppenderTests(ITestOutputHelper output) :
        base(output)
    {
    }
}