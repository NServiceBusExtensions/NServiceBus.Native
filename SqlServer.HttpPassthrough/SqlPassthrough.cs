﻿using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using NServiceBus.SqlServer.HttpPassthrough;
using NServiceBus.Transport.SqlServerNative;

class SqlPassthrough : ISqlPassthrough
{
    Sender sender;
    bool appendClaims;
    string claimsHeaderPrefix;
    Func<HttpContext, PassthroughMessage, Task<Table>> sendCallback;

    public SqlPassthrough(Func<HttpContext, PassthroughMessage, Task<Table>> sendCallback, Sender sender, bool appendClaims, string claimsHeaderPrefix)
    {
        this.sendCallback = sendCallback;
        this.sender = sender;
        this.appendClaims = appendClaims;
        this.claimsHeaderPrefix = claimsHeaderPrefix;
    }

    public async Task Send(HttpContext context, CancellationToken cancellation = default)
    {
        Guard.AgainstNull(context, nameof(context));
        var passThroughMessage = await RequestParser.Extract(context.Request, cancellation).ConfigureAwait(false);
        var destinationTable = await sendCallback(context, passThroughMessage).ConfigureAwait(true);
        ProcessClaims(context, passThroughMessage);
        var rowVersion = await sender.Send(passThroughMessage, destinationTable, cancellation).ConfigureAwait(true);
        var wasDedup = rowVersion == 0;
        if (wasDedup)
        {
            context.Response.StatusCode = (int) HttpStatusCode.Conflict;
        }
    }

    void ProcessClaims(HttpContext context, PassthroughMessage passThroughMessage)
    {
        if (!appendClaims)
        {
            return;
        }

        var user = context.User;
        if (user?.Claims == null)
        {
            return;
        }

        if (!user.Claims.Any())
        {
            return;
        }

        ClaimsSerializer.Append(user.Claims, passThroughMessage.ExtraHeaders, claimsHeaderPrefix);
    }
}