using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;

static class Serializer
{
    static DataContractJsonSerializerSettings serializerSettings = new()
    {
        UseSimpleDictionaryFormat = true
    };

    public static string SerializeDictionary(IDictionary<string, string> instance)
    {
        var serializer = BuildDictionarySerializer();
        using var stream = new MemoryStream();
        serializer.WriteObject(stream, instance);
        return Encoding.UTF8.GetString(stream.ToArray()).Replace(@"\/", "/");
    }

    public static Dictionary<string, string> DeSerializeDictionary(string json)
    {
        var serializer = BuildDictionarySerializer();
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return (Dictionary<string, string>) serializer.ReadObject(stream)!;
    }
    public static List<string> DeSerializeList(string json)
    {
        var serializer = BuildListSerializer();
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return (List<string>) serializer.ReadObject(stream)!;
    }

    // Items needs to be a list since the DataContractJsonSerializer
    // attempts to walk into the state machine if an IEnumerable is used.
    public static string SerializeList(List<string> items)
    {
        var serializer = BuildListSerializer();
        using var stream = new MemoryStream();
        serializer.WriteObject(stream, items);
        return Encoding.UTF8.GetString(stream.ToArray()).Replace(@"\/","/");
    }

    static DataContractJsonSerializer BuildDictionarySerializer()
    {
        return new(typeof(Dictionary<string, string>), serializerSettings);
    }

    static DataContractJsonSerializer BuildListSerializer()
    {
        return new(typeof(List<string>), serializerSettings);
    }
}