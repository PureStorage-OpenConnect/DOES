using Newtonsoft.Json;
using System;

namespace DOES.Shared.Resources
{
    [Serializable()]
    public class LinuxResource : PlatformResource
    {

        public LinuxResource(double cpuLoad, Int64 totalPhysicalMemory, Int64 usedPhysicalMemory, Int64 freePhysicalMemory, double currentClockSpeed, double sDTransfersPerSecond, 
            double sDKBRead, double sDKBWrite, double sDKBReadps, double sDKBWriteps, double nvmeTransfersPerSecond, double nvmeKBRead, double nvmeKBWrite, double nvmeKBWriteps, double nvmeKBReadps, 
            double dMTransfersPerSecond, double dMKBRead, double dMKBWrite, double dMKBWriteps, double dMKBReadps,
            DateTime collectedOn)
        {
            CPULoad = cpuLoad;
            TotalPhysicalMemory = totalPhysicalMemory;
            UsedPhysicalMemory = usedPhysicalMemory;
            FreePhysicalMemory = freePhysicalMemory;
            CurrentClockSpeed = currentClockSpeed;
            SDTransfersPerSecond = sDTransfersPerSecond;
            SDKBRead = sDKBRead;
            SDKBWrite = sDKBWrite;
            SDKBWriteps = sDKBWriteps;
            SDKBReadps = sDKBReadps;
            NVMETransfersPerSecond = nvmeTransfersPerSecond;
            NVMEKBRead = nvmeKBRead;
            NVMEKBWrite = nvmeKBWrite;
            NVMEKBWriteps = nvmeKBWriteps;
            NVMEKBReadps = nvmeKBReadps;
            DMTransfersPerSecond = dMTransfersPerSecond;
            DMKBRead = dMKBRead;
            DMKBWrite = dMKBWrite;
            DMKBWriteps = dMKBWriteps;
            DMKBReadps = dMKBReadps;
            CollectedOn = collectedOn; 

        }

        [JsonProperty("cpu_load")]
        public double CPULoad { get; set; }
        [JsonProperty("total_physical_memory")]
        public Int64 TotalPhysicalMemory { get; set; }
        [JsonProperty("used_physical_memory")]
        public Int64 UsedPhysicalMemory { get; set; }
        [JsonProperty("free_physical_memory")]
        public Int64 FreePhysicalMemory { get; set; }
        [JsonProperty("current_clock_speed")]
        public double CurrentClockSpeed { get; set; }
        [JsonProperty("sdk_transfers_ps")]
        public double SDTransfersPerSecond { get; set; }
        [JsonProperty("sdk_read")]
        public double SDKBRead { get; set; }
        [JsonProperty("sdk_write")]
        public double SDKBWrite { get; set; }
        [JsonProperty("sdk_read_ps")]
        public double SDKBReadps { get; set; }
        [JsonProperty("sdk_write_ps")]
        public double SDKBWriteps { get; set; }
        [JsonProperty("nvme_transfers_ps")]
        public double NVMETransfersPerSecond { get; set; }
        [JsonProperty("nvme_read")]
        public double NVMEKBRead { get; set; }
        [JsonProperty("nvme_write")]
        public double NVMEKBWrite { get; set; }
        [JsonProperty("nvme_read_ps")]
        public double NVMEKBReadps { get; set; }
        [JsonProperty("nvme_write_ps")]
        public double NVMEKBWriteps { get; set; }
        [JsonProperty("dmk_transfers_ps")]
        public double DMTransfersPerSecond { get; set; }
        [JsonProperty("dmk_read")]
        public double DMKBRead { get; set; }
        [JsonProperty("dmk_write")]
        public double DMKBWrite { get; set; }
        [JsonProperty("dmk_read_ps")]
        public double DMKBReadps { get; set; }
        [JsonProperty("dmk_write_ps")]
        public double DMKBWriteps { get; set; }
        [JsonProperty("collected_on")]
        public DateTime CollectedOn { get; set; }
        [JsonProperty("platform_type")]
        public Dynamics.Platform PlatformType { get { return Dynamics.Platform.Linux; } }

        public override Dynamics.Platform GetPlatformType()
        {
            return PlatformType;
        }
    }
}
