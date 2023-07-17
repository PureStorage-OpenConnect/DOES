using DOES.Shared.Debug;
using DOES.Shared.Operations;
using DOES.Shared.Resources;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DOES.Cli
{
    public class Stop : Operation
    {
        private string _ip;
        //Logging Parameters 
        private bool _logData;
        private string _testname;
        private string _objectName;
        private string _objectCategory;
        private int _sequence;
        private bool _verboseWriter = false;
        static readonly HttpClient _client = new HttpClient();
        Analytic lpes;
        private MessageQueue _messageQueue = new MessageQueue();

        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public Stop(string ip, bool logData, string testName, string objectName, string ObjectCategory, int sequence)
        {
            _ip = ip;
            _logData = logData;
            _testname = testName;
            _objectName = objectName;
            _objectCategory = ObjectCategory;
            _sequence = sequence;
        }


        public override CancellationTokenSource TokenSource => throw new NotImplementedException();

        public override void ExecuteOperation()
        {
            ExecuteClientOperations().Wait();
        }

        private async Task ExecuteClientOperations()
        {
            string uuid = Client_UUID_Handler.Check_For_Client_UUID();
            string cliientWebURL = "http://" + _ip + ":53637" + "/DOES.Cli/api/3/platform/" + uuid;
            List<PlatformResource> resourceList = await StopAndRetrievePlatformResource(cliientWebURL);

            List<WindowsResource> windowsResources = new List<WindowsResource>();
            List<LinuxResource> linuxResources = new List<LinuxResource>();

            foreach (PlatformResource resObj in resourceList)
            {
                //can either log the data OR aggraget it and report
                switch (resObj.GetPlatformType())
                {
                    case Dynamics.Platform.Windows:
                        WindowsResource windowsResource = (WindowsResource)resObj;
                        windowsResources.Add(windowsResource);
                        break;
                    case Dynamics.Platform.Linux:
                        LinuxResource linuxResource = (LinuxResource)resObj;
                        linuxResources.Add(linuxResource);
                        break;
                }
            }

            if (_logData)
            {
                InitlialiseLogging();
            }

            if (windowsResources.Count != 0)
            {
                int CurrentClockSpeed = 0;
                int ExtClock = 0;
                int DataWidth = 0;
                int MaxClockSpeed = 0;
                int NumberOfLogicalProcessors = 0;
                Int64 NumberOfProcessors = 0;
                Int64 TotalPhysicalMemory = 0;
                Int64 FreePhysicalMemory = 0;
                Int64 UsedPhysicalMemory = 0;
                double processorTime = 0;
                double privledgedTime = 0;
                double interruptTime = 0;
                double dpcTime = 0;
                double poolPagedMemory = 0;
                double nonPagedMemory = 0;
                double cachedBytes = 0;
                double pagedFileUse = 0;
                double avgQueueLength = 0;
                double diskBytesRead = 0;
                double diskBytesWrite = 0;
                double avgDiskRead = 0;
                double avgDiskWrite = 0;
                double diskTime = 0;
                double handleCount = 0;
                double threadCount = 0;
                double contextSwitch = 0;
                double systemCall = 0;
                double processorQueueLength = 0;
                for (int i = 0; i < windowsResources.Count; i++)
                {
                    CurrentClockSpeed += windowsResources[i].CurrentClockSpeed;
                    ExtClock += windowsResources[i].ExtClock;
                    DataWidth += windowsResources[i].DataWidth;
                    MaxClockSpeed += windowsResources[i].MaxClockSpeed;
                    NumberOfLogicalProcessors += windowsResources[i].NumberOfLogicalProcessors;
                    NumberOfProcessors += windowsResources[i].NumberOfProcessors;
                    TotalPhysicalMemory += windowsResources[i].TotalPhysicalMemory;
                    FreePhysicalMemory += windowsResources[i].FreePhysicalMemory;
                    UsedPhysicalMemory += windowsResources[i].UsedPhysicalMemory;
                    processorTime += windowsResources[i].ProcessorTime;
                    privledgedTime += windowsResources[i].ProcessorPrivledgedTime;
                    interruptTime += windowsResources[i].ProcessorInterruptTime;
                    dpcTime += windowsResources[i].ProcessorDPCTime;
                    poolPagedMemory += windowsResources[i].PoolPagedBytesMemory;
                    nonPagedMemory += windowsResources[i].PoolNonPagedBytesMemory;
                    cachedBytes += windowsResources[i].CachedBytesMemory;
                    pagedFileUse += windowsResources[i].PageFileUsed;
                    avgQueueLength += windowsResources[i].PhysicalDiskAvgQueueLength;
                    diskBytesRead += windowsResources[i].PhysicalDiskAvgReadBytes;
                    diskBytesWrite += windowsResources[i].PhysicalDiskAvgWriteBytes;
                    avgDiskRead += windowsResources[i].PhysicalDiskAvgReadBytes;
                    avgDiskWrite += windowsResources[i].PhysicalDiskAvgWriteBytes;
                    diskTime += windowsResources[i].PhysicalDiskTime;
                    handleCount += windowsResources[i].ProcessHandleCount;
                    threadCount += windowsResources[i].ProcessThreadCount;
                    contextSwitch += windowsResources[i].ProcessContextSwitchCount;
                    systemCall += windowsResources[i].ProcessSystemCalls;
                    processorQueueLength += windowsResources[i].ProcessorQueueLength;
                    if (_logData)
                    {
                        lpes.LogPlatformEngineWindowsResource(windowsResources[i]);
                    }
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "--                Microsoft Windows Resource Gathering             -- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Processor Data --------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Processor Time                      : " + Math.Round((processorTime / windowsResources.Count), 2) + " %", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Processor Privledged Time           : " + Math.Round((privledgedTime / windowsResources.Count), 2) + " %", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Processor Interrupt Time            : " + Math.Round((interruptTime / windowsResources.Count), 2) + " %", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Processor DPC Time                  : " + Math.Round((dpcTime / windowsResources.Count), 2) + " %", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Clock Speed                         : " + CurrentClockSpeed / windowsResources.Count + " Mhz", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average External Clock Speed                : " + ExtClock / windowsResources.Count + " Mhz", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Data Width                          : " + DataWidth / windowsResources.Count + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Max Clock Speed Per Core            : " + MaxClockSpeed / windowsResources.Count + " Mhz", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Number of Logical Processors                : " + NumberOfLogicalProcessors / windowsResources.Count + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Number of Physical Processors               : " + NumberOfProcessors / windowsResources.Count + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------- Memory Data ----------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Total Physical Memory               : " + ReturnFormattedValues(TotalPhysicalMemory / windowsResources.Count), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Used Physical Memory                : " + ReturnFormattedValues(UsedPhysicalMemory / windowsResources.Count), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Free Physical Memory                : " + ReturnFormattedValues(FreePhysicalMemory / windowsResources.Count), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Page File Used                      : " + ReturnFormattedValues(pagedFileUse / windowsResources.Count) + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Paged Memory Pool                   : " + ReturnFormattedValues(poolPagedMemory / windowsResources.Count) + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Non-Paged Memory Pool               : " + ReturnFormattedValues(nonPagedMemory / windowsResources.Count) + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Cached Memory                       : " + ReturnFormattedValues(cachedBytes / windowsResources.Count) + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "---------------------------- Disk Data ------------------------------ ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Queue Length                        : " + Math.Round((avgQueueLength / windowsResources.Count), 2) + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Read Speed                          : " + ReturnFormattedValues(diskBytesRead / windowsResources.Count) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Write Speed                         : " + ReturnFormattedValues(diskBytesWrite / windowsResources.Count) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Read Speed                          : " + ReturnFormattedValues(avgDiskRead / windowsResources.Count) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Write Speed                         : " + ReturnFormattedValues(avgDiskWrite / windowsResources.Count) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Disk Time                           : " + Math.Round((diskTime / windowsResources.Count), 2) + " %", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Process Data ----------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Handle Count                        : " + Math.Round((handleCount / windowsResources.Count), 2) + "/sec", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Thread Count                        : " + Math.Round((threadCount / windowsResources.Count), 2) + "/sec", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Conext Switch Count                 : " + Math.Round((contextSwitch / windowsResources.Count), 2) + "/sec", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average System Calls                        : " + Math.Round((systemCall / windowsResources.Count), 2) + "/sec", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Average Processor Queue Length              : " + Math.Round((processorQueueLength / windowsResources.Count), 2), Message.MessageType.Report));
            }
            if (linuxResources.Count != 0)
            {
                double cpuLoad = 0;
                double totalMemory = 0;
                double usedMemory = 0;
                double freeMemory = 0;
                double currentClockSpeed = 0;
                double sdTPS = 0;
                double sdKBRead = 0;
                double sdKBWrite = 0;
                double sdKBReadPS = 0;
                double sdKBWritePS = 0;
                double nvmeTPS = 0;
                double nvmeKBRead = 0;
                double nvmeKBWrite = 0;
                double nvmeKBReadPS = 0;
                double nvmeKBWritePS = 0;
                double dmTPS = 0;
                double dmKBRead = 0;
                double dmKBWrite = 0;
                double dmKBReadPS = 0;
                double dmKBWritePS = 0;

                for (int i = 0; i < linuxResources.Count; i++)
                {
                    cpuLoad += linuxResources[i].CPULoad;
                    totalMemory += linuxResources[i].TotalPhysicalMemory;
                    usedMemory += linuxResources[i].UsedPhysicalMemory;
                    freeMemory += linuxResources[i].FreePhysicalMemory;
                    currentClockSpeed += linuxResources[i].CurrentClockSpeed;
                    sdTPS += linuxResources[i].SDTransfersPerSecond;
                    sdKBRead += linuxResources[i].SDKBRead;
                    sdKBWrite += linuxResources[i].SDKBWrite;
                    sdKBReadPS += linuxResources[i].SDKBReadps;
                    sdKBWritePS += linuxResources[i].SDKBWriteps;
                    nvmeTPS += linuxResources[i].NVMETransfersPerSecond;
                    nvmeKBRead += linuxResources[i].NVMEKBRead;
                    nvmeKBWrite += linuxResources[i].NVMEKBWrite;
                    nvmeKBReadPS += linuxResources[i].NVMEKBReadps;
                    nvmeKBWritePS += linuxResources[i].NVMEKBWriteps;
                    dmTPS += linuxResources[i].DMTransfersPerSecond;
                    dmKBRead += linuxResources[i].DMKBRead;
                    dmKBWrite += linuxResources[i].DMKBWrite;
                    dmKBReadPS += linuxResources[i].DMKBReadps;
                    dmKBWritePS += linuxResources[i].DMKBWriteps;
                    if (_logData)
                    {
                        lpes.LogPlatformEngineLinuxResource(linuxResources[i]);
                    }
                }

                _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "--                     Linux Resource Gathering                    -- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Processor Data --------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "CPU Load                              : " + Math.Round((cpuLoad / linuxResources.Count), 4) + " %", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Current Clock Speed Per Core          : " + Math.Round((currentClockSpeed / linuxResources.Count), 4), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------- Memory Data ----------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Total Physical Memory                 : " + ReturnFormattedValues(totalMemory / linuxResources.Count), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Used Physical Memory                  : " + ReturnFormattedValues(usedMemory / linuxResources.Count), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Free Physical Memory                  : " + ReturnFormattedValues(freeMemory / linuxResources.Count), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------ Standard Disk Data ------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Transfers per second                  : " + sdTPS / linuxResources.Count + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data Written                          : " + ReturnFormattedValues((sdKBWrite / linuxResources.Count) * 1024), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data Read                             : " + ReturnFormattedValues((sdKBRead / linuxResources.Count) * 1024), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Write Performance                     : " + ReturnFormattedValues(sdKBWritePS / linuxResources.Count * 1024) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Read Performance                      : " + ReturnFormattedValues(sdKBReadPS / linuxResources.Count * 1024) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------ NVMe Disk Data ------------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Transfers per second                  : " + nvmeTPS / linuxResources.Count + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data Written                          : " + ReturnFormattedValues((nvmeKBWrite / linuxResources.Count) * 1024), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data Read                             : " + ReturnFormattedValues((nvmeKBRead / linuxResources.Count) * 1024), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Write Performance                     : " + ReturnFormattedValues(nvmeKBWritePS / linuxResources.Count * 1024) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Read Performance                      : " + ReturnFormattedValues(nvmeKBReadPS / linuxResources.Count * 1024) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------ Device-Mapper Disk Data -------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Transfers per second                  : " + dmTPS / linuxResources.Count + " ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data Written                          : " + ReturnFormattedValues((dmKBWrite / linuxResources.Count) * 1024), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data Read                             : " + ReturnFormattedValues((dmKBRead / linuxResources.Count) * 1024), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Write Performance                     : " + ReturnFormattedValues(dmKBWritePS / linuxResources.Count * 1024) + "/sec ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Read Performance                      : " + ReturnFormattedValues(dmKBReadPS / linuxResources.Count * 1024) + "/sec ", Message.MessageType.Report));
            }
            WriteMessages();
        }

        private async Task<List<PlatformResource>> StopAndRetrievePlatformResource(string url)
        {
            List<PlatformResource> resource = new List<PlatformResource>();
            HttpResponseMessage response = null;
            try
            {
                response = await _client.GetAsync(url);
                if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, "There is a gather operation running which cannot be collected at this time ", Message.MessageType.Error));
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, "There is no running gather operation running for this client", Message.MessageType.Error));
                }
                else
                {
                    response.EnsureSuccessStatusCode();
                    try
                    {
                        if (response.Content is object && response.Content.Headers.ContentType.MediaType == "application/json")
                        {
                            var contentStream = await response.Content.ReadAsStringAsync();
                            var json = JsonConvert.DeserializeObject(contentStream);
                            try
                            {
                                List<WindowsResource> windowsResouce = JsonConvert.DeserializeObject<List<WindowsResource>>(json.ToString());
                                foreach (WindowsResource w in windowsResouce)
                                {
                                    resource.Add(w);
                                }
                            }
                            catch (Exception)
                            {
                                List<LinuxResource> linuxResource = JsonConvert.DeserializeObject<List<LinuxResource>>(json.ToString());
                                foreach (LinuxResource l in linuxResource)
                                {
                                    resource.Add(l);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                }
            }
            catch (HttpRequestException e)
            {
                if (e.Message == "Error while copying content to a stream.")
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, "There is either a locked duration gather operation or no gather operation running for this client. ", Message.MessageType.Error));
                }
                else
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, e.Message, Message.MessageType.Error));
                }
            }
            return resource;
        }

        private void InitlialiseLogging()
        {
            if (_testname != "" && _objectName != "" && _objectCategory != null)
            {
                lpes = new Analytic(_testname, _objectName, _objectCategory, _sequence, _messageQueue);
            }
            else if (_testname != "" || _objectName != "")
            {
                lpes = new Analytic(_testname, _objectName, _sequence, _messageQueue);
            }
            else
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data will not be logged due to missing test or object name", Message.MessageType.Error));
                _logData = false;
            }
        }

        private void WriteMessages()
        {
            List<Message> tickedMessages = _messageQueue.GetAllMessages();
            foreach (Message m in tickedMessages)
            {
                Tuple<Message.MessageType, string> messageToParse = m.GetFormattedMessage();
                if (messageToParse.Item1 == Message.MessageType.Error)
                {
                    if (_verboseWriter)
                    {
                        Console.WriteLine(messageToParse.Item2);
                    }
                }
                else if (messageToParse.Item1 == Message.MessageType.Report)
                {
                    if (_verboseWriter)
                    {
                        Console.WriteLine(messageToParse.Item2);
                    }
                }
                else
                {
                    if (_verboseWriter)
                    {
                        Console.WriteLine(messageToParse.Item2);
                    }
                    DebugLogger.LogMessages(DebugLogger.File.PlatformEngine, messageToParse.Item2);
                }
            }
        }


        private string ReturnFormattedValues(double dataValue)
        {
            Dynamics.StorageUnit storageUnit = Dynamics.StorageUnit.Bytes;

            if (dataValue > 1024L && dataValue < (1024 * 1024))
            {
                //convertToKB
                dataValue = Math.Round(dataValue / (1024L), 2);
                storageUnit = Dynamics.StorageUnit.Kilobytes;
            }
            else if (dataValue > (1024 * 1024) && dataValue < (1024 * 1024 * 1024))
            {
                //convert to MB
                dataValue = Math.Round(dataValue / (1024L * 1024L), 2);
                storageUnit = Dynamics.StorageUnit.Megabytes;
            }
            else if (dataValue > (1024 * 1024 * 1024) && dataValue < (1024L * 1024L * 1024L * 1024L))
            {
                //convert to GB
                dataValue = Math.Round(dataValue / (1024 * 1024 * 1024), 2);
                storageUnit = Dynamics.StorageUnit.Gigabytes;
            }
            else if (dataValue > (1024L * 1024L * 1024L * 1024L))
            {
                //convert to TB
                dataValue = Math.Round(dataValue / (1024L * 1024L * 1024L * 1024L), 2);
                storageUnit = Dynamics.StorageUnit.Terabytes;
            }
            string formattedValue = Math.Round(dataValue, 2) + " " + storageUnit;
            return formattedValue;
        }
    }
}
