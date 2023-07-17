using System;
using System.Diagnostics;
using System.Management;
using DOES.Shared.Debug;
using DOES.Shared.Resources;

namespace DOES.PlatformEngine.Client
{
    public class GatherWindows
    {
        public static WindowsResource GatherWindowsResourceDataPoint(MessageQueue queue)
        {
            PerformanceCounter processorTimeCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total");
            PerformanceCounter privledgedTimeCounter = new PerformanceCounter("Processor", "% Privileged Time", "_Total");
            PerformanceCounter interruptTimeCounter = new PerformanceCounter("Processor", "% Interrupt Time", "_Total");
            PerformanceCounter dpcTimeCounter = new PerformanceCounter("Processor", "% DPC Time", "_Total");
            PerformanceCounter poolPagedBytesCounter = new PerformanceCounter("Memory", "Pool Paged Bytes", null);
            PerformanceCounter nonPagedBytesCounter = new PerformanceCounter("Memory", "Pool Nonpaged Bytes", null);
            PerformanceCounter cachedBytesCounter = new PerformanceCounter("Memory", "Cache Bytes", null);
            PerformanceCounter pageFileUseCounter = new PerformanceCounter("Paging File", "% Usage", "_Total");
            PerformanceCounter avgQueueLengthCounter = new PerformanceCounter("PhysicalDisk", "Avg. Disk Queue Length", "_Total");
            PerformanceCounter diskReadBytesCounter = new PerformanceCounter("PhysicalDisk", "Disk Read Bytes/sec", "_Total");
            PerformanceCounter diskWriteBytesCounter = new PerformanceCounter("PhysicalDisk", "Disk Write Bytes/sec", "_Total");
            PerformanceCounter avgDiskReadCounter = new PerformanceCounter("PhysicalDisk", "Avg. Disk sec/Read", "_Total");
            PerformanceCounter avgDiskWriteCounter = new PerformanceCounter("PhysicalDisk", "Avg. Disk sec/Write", "_Total");
            PerformanceCounter diskTimeCounter = new PerformanceCounter("PhysicalDisk", "% Disk Time", "_Total");
            PerformanceCounter handleCountCounter = new PerformanceCounter("Process", "Handle Count", "_Total");
            PerformanceCounter threadCountCounter = new PerformanceCounter("Process", "Thread Count", "_Total");
            PerformanceCounter contextSwicthCounter = new PerformanceCounter("System", "Context Switches/sec", null);
            PerformanceCounter systemCallCounter = new PerformanceCounter("System", "System Calls/sec", null);
            PerformanceCounter processorQueueLengthCounter = new PerformanceCounter("System", "Processor Queue Length", null);
            int CurrentClockSpeed = 0;
            int ExtClock = 0;
            int DataWidth = 0;
            int MaxClockSpeed = 0;
            int NumberOfLogicalProcessors = 0;
            int NumberOfProcessors = 0;
            Int64 TotalPhysicalMemory = 0;
            Int64 FreePhysicalMemory = 0;
            Int64 UsedPhysicalMemory;

            try
            {

                //CPU
                ManagementObjectSearcher CPUSearcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT CurrentClockSpeed, ExtClock, DataWidth, MaxClockSpeed  FROM Win32_Processor");
                foreach (ManagementObject queryObj in CPUSearcher.Get())
                {
                    CurrentClockSpeed = Convert.ToInt32(queryObj["CurrentClockSpeed"]);
                    ExtClock = Convert.ToInt32(queryObj["ExtClock"]);
                    DataWidth = Convert.ToInt16(queryObj["DataWidth"]);
                    MaxClockSpeed = Convert.ToInt32(queryObj["MaxClockSpeed"]);

                }

                ManagementObjectSearcher computerSystemSearcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT NumberOfLogicalProcessors, NumberOfProcessors, TotalPhysicalMemory  FROM Win32_ComputerSystem");
                foreach (ManagementObject queryObj in computerSystemSearcher.Get())
                {
                    NumberOfLogicalProcessors = Convert.ToInt32(queryObj["NumberOfLogicalProcessors"]);
                    NumberOfProcessors = Convert.ToInt32(queryObj["NumberOfProcessors"]);
                    TotalPhysicalMemory = Convert.ToInt64(queryObj["TotalPhysicalMemory"]);
                }

                ManagementObjectSearcher Searcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT FreePhysicalMemory FROM Win32_OperatingSystem");
                foreach (ManagementObject queryObj in Searcher.Get())
                {
                    FreePhysicalMemory = Convert.ToInt64(queryObj["FreePhysicalMemory"]);
                }



                Int64 freeMemory = FreePhysicalMemory * 1024;
                UsedPhysicalMemory = (TotalPhysicalMemory) - freeMemory;

                var unused = processorTimeCounter.NextValue(); // first call will always return 0
                unused = privledgedTimeCounter.NextValue();
                unused = interruptTimeCounter.NextValue();
                unused = dpcTimeCounter.NextValue();
                unused = poolPagedBytesCounter.NextValue();
                unused = nonPagedBytesCounter.NextValue();
                unused = cachedBytesCounter.NextValue();
                unused = pageFileUseCounter.NextValue();
                unused = avgQueueLengthCounter.NextValue();
                unused = diskReadBytesCounter.NextValue();
                unused = diskWriteBytesCounter.NextValue();
                unused = avgDiskReadCounter.NextValue();
                unused = avgDiskWriteCounter.NextValue();
                unused = diskTimeCounter.NextValue();
                unused = handleCountCounter.NextValue();
                unused = threadCountCounter.NextValue();
                unused = contextSwicthCounter.NextValue();
                unused = systemCallCounter.NextValue();
                unused = processorQueueLengthCounter.NextValue();
                System.Threading.Thread.Sleep(1000); // wait a second, then try again
                double processorTime = Math.Round(processorTimeCounter.NextValue(), 5);
                double privledgedTime = Math.Round(privledgedTimeCounter.NextValue(), 5);
                double interruptTime = Math.Round(interruptTimeCounter.NextValue(), 5);
                double dpcTime = Math.Round(dpcTimeCounter.NextValue(), 2);
                double poolPagedMemory = Math.Round(poolPagedBytesCounter.NextValue(), 5);
                double nonPagedMemory = Math.Round(nonPagedBytesCounter.NextValue(), 5);
                double cachedBytes = Math.Round(cachedBytesCounter.NextValue(), 5);
                double pagedFileUse = Math.Round(pageFileUseCounter.NextValue(), 5);
                double avgQueueLength = avgQueueLengthCounter.NextValue();
                double diskBytesRead = Math.Round(diskReadBytesCounter.NextValue(), 5);
                double diskBytesWrite = Math.Round(diskWriteBytesCounter.NextValue(), 5);
                double avgDiskRead = Math.Round(avgDiskReadCounter.NextValue(), 5);
                double avgDiskWrite = Math.Round(avgDiskWriteCounter.NextValue(), 5);
                double diskTime = Math.Round(diskTimeCounter.NextValue(), 5);
                double handleCount = Math.Round(handleCountCounter.NextValue(), 5);
                double threadCount = Math.Round(threadCountCounter.NextValue(), 5);
                double contextSwitch = Math.Round(contextSwicthCounter.NextValue(), 5);
                double systemCall = Math.Round(systemCallCounter.NextValue(), 5);
                double processorQueueLength = Math.Round(processorQueueLengthCounter.NextValue(), 5);

           


               DateTime collectionTime = DateTime.Now;
               return new WindowsResource(processorTime, privledgedTime, interruptTime, dpcTime, CurrentClockSpeed, ExtClock, DataWidth, MaxClockSpeed, NumberOfLogicalProcessors,
                    NumberOfProcessors, TotalPhysicalMemory, FreePhysicalMemory, UsedPhysicalMemory, pagedFileUse, poolPagedMemory, nonPagedMemory, cachedBytes, avgQueueLength, diskBytesRead, diskBytesWrite,
                    avgDiskRead, avgDiskWrite, diskTime, handleCount, threadCount, contextSwitch, systemCall, processorQueueLength, collectionTime);
            }
            catch (Exception ex)
            {
                queue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                return null;
            }
        }
    }
}
