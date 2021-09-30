using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public record StateCount(string State, int Count);

    public class ItemCounts
    {
        public static readonly string DocumentId = "2cac2e9f-af19-4aa8-a340-b7c1302dc59b";
        public static readonly string PartitionKey = "__METADATA__";

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; } = DocumentId;
        public string DocumentType { get; set; } = "Counts";
        public string State { get; set; } = PartitionKey;
        public StateCount[] Counts { get; set; } = Array.Empty<StateCount>();
    }
}
