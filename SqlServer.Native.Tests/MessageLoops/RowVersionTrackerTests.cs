﻿using System.Threading.Tasks;
using NServiceBus.Transport.SqlServerNative;
using Xunit;
using Xunit.Abstractions;

public class RowVersionTrackerTests : TestBase
{
    public RowVersionTrackerTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Run()
    {
        await SqlHelpers.Drop(Connection.OpenConnection(), "RowVersionTracker");
        var tracker = new RowVersionTracker(Connection.OpenAsyncConnection);
        await tracker.CreateTable();
        var initial = await tracker.Get();
        Assert.Equal(1, initial);
        await tracker.Save(4);
        var after = await tracker.Get();
        Assert.Equal(4, after);
    }
}