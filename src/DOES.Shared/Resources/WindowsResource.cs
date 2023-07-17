using Newtonsoft.Json;
using System;

/*****************************************************************
 * Required to make the code in StartGatherWindows easier to read
 * **************************************************************/


namespace DOES.Shared.Resources
{
    [Serializable()]
    public class WindowsResource : PlatformResource
    {
        public WindowsResource(double processorTime, double privledgedTime, double interruptTime, double dpcTime, int currClockSpeed, int clock,
            int cpudatawidth, int maxclockspeed, int totalProcessors, int numProc, Int64 totalmemory, Int64 freememory, Int64 usedmemory,
            double pageFileUsed, 
            double poolPagedBytes, double poolNonPagedBytes, double poolcachedBytes, double avgQueueLength, double readBytesDisk, double writeBytesDisk, 
            double avgReadBytes, double avgWriteBytes, double diskTime, double handleCount, double threadCount, double contextSwitch, double systemCall,
            double processorQueueLength, DateTime collectedOn)
        {
            ProcessorTime = processorTime;
            ProcessorPrivledgedTime = privledgedTime;
            ProcessorInterruptTime = interruptTime;
            ProcessorDPCTime = dpcTime;
            CurrentClockSpeed = currClockSpeed;
            ExtClock = clock;
            DataWidth = cpudatawidth;
            MaxClockSpeed = maxclockspeed;
            NumberOfLogicalProcessors = totalProcessors;
            NumberOfProcessors = numProc;
            TotalPhysicalMemory = totalmemory;
            FreePhysicalMemory = freememory;
            UsedPhysicalMemory = usedmemory;
            PageFileUsed = pageFileUsed;
            PoolPagedBytesMemory = poolPagedBytes;
            PoolNonPagedBytesMemory = poolNonPagedBytes;
            CachedBytesMemory = poolcachedBytes;
            PhysicalDiskAvgQueueLength = avgQueueLength;
            PhysicalDiskReadBytes = readBytesDisk;
            PhysicalDiskWriteBytes = writeBytesDisk;
            PhysicalDiskAvgReadBytes = avgReadBytes;
            PhysicalDiskAvgWriteBytes = avgWriteBytes;
            PhysicalDiskTime = diskTime;
            ProcessHandleCount = handleCount;
            ProcessThreadCount = threadCount;
            ProcessContextSwitchCount = contextSwitch;
            ProcessSystemCalls = systemCall;
            ProcessorQueueLength = processorQueueLength;
            CollectedOn = collectedOn;
        }

        [JsonProperty("processor_time")]
        public double ProcessorTime { get; set; }
        [JsonProperty("processor_privledged_time")]
        public double ProcessorPrivledgedTime { get; set; }
        [JsonProperty("processor_interrupt_time")]
        public double ProcessorInterruptTime { get; set; }
        [JsonProperty("processor_dpc_time")]
        public double ProcessorDPCTime { get; set; }
        [JsonProperty("current_clock_speed")]
        public int CurrentClockSpeed { get; set; }
        [JsonProperty("ext_clock")]
        public int ExtClock { get; set; }
        [JsonProperty("data_width")]
        public int DataWidth { get; set; }
        [JsonProperty("max_clock_speed")]
        public int MaxClockSpeed { get; set; }
        [JsonProperty("number_of_logical_processors")]
        public int NumberOfLogicalProcessors { get; set; }
        [JsonProperty("number_of_processors")]
        public Int64 NumberOfProcessors { get; set; }
        [JsonProperty("total_physical_memory")]
        public Int64 TotalPhysicalMemory { get; set; }
        [JsonProperty("free_physical_memory")]
        public Int64 FreePhysicalMemory { get; set; }
        [JsonProperty("used_physical_memory")]
        public Int64 UsedPhysicalMemory { get; set; }
        [JsonProperty("page_file_used")]
        public double PageFileUsed { get; set; }
        [JsonProperty("pool_paged_bytes_memory")]
        public double PoolPagedBytesMemory { get; set; }
        [JsonProperty("pool_non_paged_bytes_memory")]
        public double PoolNonPagedBytesMemory { get; set; }
        [JsonProperty("cached_bytes_memory")]
        public double CachedBytesMemory { get; set; }
        [JsonProperty("physical_disk_avg_queue_length")]
        public double PhysicalDiskAvgQueueLength { get; set; }
        [JsonProperty("physical_disk_read_bytes")]
        public double PhysicalDiskReadBytes { get; set; }
        [JsonProperty("physical_disk_write_bytes")]
        public double PhysicalDiskWriteBytes { get; set; }
        [JsonProperty("physical_disk_avg_read_bytes")]
        public double PhysicalDiskAvgReadBytes { get; set; }
        [JsonProperty("physical_disk_avg_write_bytes")]
        public double PhysicalDiskAvgWriteBytes { get; set; }
        [JsonProperty("physical_disk_time")]
        public double PhysicalDiskTime { get; set; }
        [JsonProperty("process_handle_count")]
        public double ProcessHandleCount { get; set; }
        [JsonProperty("process_thread_count")]
        public double ProcessThreadCount { get; set; }
        [JsonProperty("process_context_switch")]
        public double ProcessContextSwitchCount { get; set; }
        [JsonProperty("process_system_calls")]
        public double ProcessSystemCalls { get; set; }
        [JsonProperty("processor_queue_length")]
        public double ProcessorQueueLength { get; set; }
        [JsonProperty("collected_on")]
        public DateTime CollectedOn { get; set; }
        [JsonProperty("platform_type")]
        public Dynamics.Platform PlatformType { get { return Dynamics.Platform.Windows; } }

        public override Dynamics.Platform GetPlatformType()
        {
            return PlatformType;
        }
    }
}
