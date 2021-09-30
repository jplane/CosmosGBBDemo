using Newtonsoft.Json;
using System;

namespace Common
{
    public class Item
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; } = string.Empty;
        public string DocumentType { get; set; } = "Item";
        public string Address { get; set; } = string.Empty;
        public string State { get; set; } = string.Empty;
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
    }
}
