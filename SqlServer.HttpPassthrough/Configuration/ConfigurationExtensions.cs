﻿using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace NServiceBus.SqlServer.HttpPassthrough
{
    public static class ConfigurationExtensions
    {
        public static void AddSqlHttpPassThrough(
            this IServiceCollection services,
            Func<CancellationToken, Task<SqlConnection>> connectionFunc)
        {
            AddSqlHttpPassThrough(services, new PassthroughConfiguration(connectionFunc));
        }

        public static void AddSqlHttpPassThrough(
            this IServiceCollection services,
            PassthroughConfiguration configuration)
        {
            Guard.AgainstNull(services, nameof(services));
            Guard.AgainstNull(configuration, nameof(configuration));

            var headersBuilder = new HeadersBuilder(configuration.originatingEndpoint, configuration.originatingMachine);
            var sender = new Sender(configuration.connectionFunc, headersBuilder,configuration.attachmentsTable);
            var sqlPassThrough = new SqlPassThrough(configuration.sendCallback, sender);
            services.AddSingleton<ISqlPassthrough>(sqlPassThrough);
            var dedupService = new DedupService(configuration.deduplicationTable, configuration.connectionFunc);
            services.AddSingleton<IHostedService>(dedupService);
        }

        public static void AddSqlHttpPassThroughBadRequestMiddleware(
            this IApplicationBuilder builder)
        {
            Guard.AgainstNull(builder, nameof(builder));
            builder.UseMiddleware<BadRequestMiddleware>();
        }
    }
}