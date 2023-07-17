using Newtonsoft.Json;
using System;

namespace DOES.Shared.Resources
{
    public class PlatformEngineCommand
    {
        [JsonConstructor]
        public PlatformEngineCommand(Dynamics.ResourceRetrievalType instruction, string uuid, string duration, string interval)
        {
            Instruction = instruction;
            UUID = uuid;
            Duration = duration;
            Interval = interval;
            Accepted = false;
        }

        [JsonProperty("instruction")]
        public Dynamics.ResourceRetrievalType Instruction { get; set; }
        [JsonProperty("uuid")]
        public string UUID { get; set; }
        [JsonProperty("duration")]
        public string Duration { get; set; }
        [JsonProperty("interval")]
        public string Interval { get; set; }
        [JsonProperty("accepted")]
        public bool Accepted { get; set; }

        public TimeSpan GetDuration()
        {
            return TimeSpan.Parse(Duration);
        }

        public TimeSpan GetInterval()
        {
            return TimeSpan.Parse(Interval);
        }
    }
}
