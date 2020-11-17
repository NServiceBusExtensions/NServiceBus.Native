using System;
using System.Collections.Generic;
using System.Globalization;

namespace NServiceBus.Transport.SqlServerNative
{
    public static partial class Headers
    {
        static Dictionary<string, string> emptyHeaders = new();
        /// <summary>
        /// The string '{}', for when empty json headers are required.
        /// </summary>
        public readonly static string EmptyHeadersJson = "{}";

        /// <summary>
        /// An empty <see cref="IReadOnlyDictionary{TKey,TValue}"/>, for when empty headers are required.
        /// </summary>
        public static IReadOnlyDictionary<string, string> EmptyHeaders => emptyHeaders;

        /// <summary>
        /// Serialize <paramref name="instance"/> into json.
        /// </summary>
        public static string Serialize(IDictionary<string, string> instance)
        {
            return Serializer.SerializeDictionary(instance);
        }

        /// <summary>
        /// Deserialize <paramref name="json"/> into a <see cref="IDictionary{TKey,TValue}"/>.
        /// </summary>
        public static IDictionary<string, string> DeSerialize(string? json)
        {
            if (json == null)
            {
                return emptyHeaders;
            }

            return Serializer.DeSerializeDictionary(json);
        }

        /// <summary>
        /// The format used to store dates in NServiceBus headers.
        /// </summary>
        public const string WireDateTimeFormat = "yyyy-MM-dd HH:mm:ss:ffffff Z";

        /// <summary>
        /// Convert <paramref name="dateTime"/> to a <see cref="string"/> using <see cref="WireDateTimeFormat"/>.
        /// </summary>
        public static string ToWireFormattedString(DateTime dateTime)
        {
            return dateTime.ToUniversalTime()
                .ToString(WireDateTimeFormat, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Convert <paramref name="wireFormattedString"/> to a <see cref="DateTime"/> using <see cref="WireDateTimeFormat"/>.
        /// </summary>
        public static DateTime ToUtcDateTime(string wireFormattedString)
        {
            return DateTime.ParseExact(wireFormattedString, WireDateTimeFormat, CultureInfo.InvariantCulture)
                .ToUniversalTime();
        }
    }
}