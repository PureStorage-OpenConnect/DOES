﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Management.Automation;
using System.Net.Http;
using Newtonsoft.Json;
using System.Net.Http.Headers;
using DOES.Shared.Resources;
using DOES.Shared.Operations;
using DOES.Shared.Debug;

namespace DOES.PowerShell.Functions
{
    [Cmdlet(VerbsLifecycle.Start, "PlatformEngine")]
    public class StartPlatformEngineCmdLet : Cmdlet
    {
        #region Parameters 
        private string _ip = "localhost";
        [Parameter(Position = 0, Mandatory = false)]
        [Alias("IPAddress")]
        public string Hostname { get { return _ip; } set { _ip = value; } }
        private DOES.Shared.Resources.Dynamics.ResourceRetrievalType _retrievalType = Dynamics.ResourceRetrievalType.PointInTime;
        [Parameter(Position = 1, Mandatory = false)]
        public DOES.Shared.Resources.Dynamics.ResourceRetrievalType CollectionType { get { return _retrievalType; } set { _retrievalType = value; } }
        private TimeSpan duration = TimeSpan.FromMinutes(10);
        [Parameter(Position = 2, Mandatory = false)]
        public TimeSpan Duration { get { return duration; } set { duration = value; } }
        private TimeSpan interval = TimeSpan.FromSeconds(5);
        [Parameter(Position = 3, Mandatory = false)]
        public TimeSpan Interval { get { return interval; } set { interval = value; } }

        //Logging Parameters
        private bool _logData;
        [Parameter(Mandatory = false)]
        public SwitchParameter LogData
        {
            get { return _logData; }
            set { _logData = value; }
        }
        private string _testname = "";
        [Parameter(Mandatory = false)]
        public string TestName { get { return _testname; } set { _testname = value; } }
        private string _objectName = "";
        [Parameter(Mandatory = false)]
        public string ObjectName { get { return _objectName; } set { _objectName = value; } }
        private string _objectCategory = null;
        [Parameter(Mandatory = false)]
        public string ObjectCategory { get { return _objectCategory; } set { _objectCategory = value; } }
        private int _sequence = 1;
        [Parameter(Mandatory = false)]
        public int Sequence { get { return _sequence; } set { _sequence = value; } }
        #endregion

        #region Global Variables
        static readonly HttpClient _client = new HttpClient();
        Analytic lpes;
        private MessageQueue _messageQueue = new MessageQueue();
        #endregion


        #region Powershell Functions
        protected override void BeginProcessing()
        {
            base.BeginProcessing();
        }

        protected override void EndProcessing()
        {
            base.EndProcessing();
        }

        protected override void ProcessRecord()
        {
            ExecuteClientOperations();
        }

        protected override void StopProcessing()
        {
            base.StopProcessing();
        }

        #endregion

        private async void ExecuteClientOperations()
        {
            string uuid = Client_UUID_Handler.Check_For_Client_UUID();
            string cliientWebURL = "http://" + _ip + ":53637" + "/does/api/3/platform";
            try {
                if (_retrievalType == Dynamics.ResourceRetrievalType.PointInTime)
                {
                    PlatformResource resource = await GetPointInTimePlatformResource(cliientWebURL, uuid);
                    if (resource != null)
                    {
                        if (resource.GetPlatformType() == Dynamics.Platform.Windows)
                        {
                            WindowsResource windowsResource = (WindowsResource)resource;
                            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "--                Microsoft Windows Resource Gathering             -- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Processor Data --------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Processor Time                              : " + windowsResource.ProcessorTime + " %", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Processor Privledged Time                   : " + windowsResource.ProcessorPrivledgedTime + " %", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Processor Interrupt Time                    : " + windowsResource.ProcessorInterruptTime + " %", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Processor DPC Time                          : " + windowsResource.ProcessorDPCTime + " %", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Current Clock Speed                         : " + windowsResource.CurrentClockSpeed + " Mhz", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "External Clock Speed                        : " + windowsResource.ExtClock + " Mhz", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Data Width                                  : " + windowsResource.DataWidth + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Max Clock Speed Per Core                    : " + windowsResource.MaxClockSpeed + " Mhz", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Number of Logical Processors                : " + windowsResource.NumberOfLogicalProcessors + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Number of Physical Processors               : " + windowsResource.NumberOfProcessors + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------- Memory Data ----------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Total Physical Memory                       : " + ReturnFormattedValues(windowsResource.TotalPhysicalMemory), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Used Physical Memory                        : " + ReturnFormattedValues(windowsResource.UsedPhysicalMemory), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Free Physical Memory                        : " + ReturnFormattedValues(windowsResource.FreePhysicalMemory), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Page File Used                              : " + ReturnFormattedValues(windowsResource.PageFileUsed) + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Paged Memory Pool                           : " + ReturnFormattedValues(windowsResource.PoolPagedBytesMemory) + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Non-Paged Memory Pool                       : " + ReturnFormattedValues(windowsResource.PoolNonPagedBytesMemory) + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Cached Memory                               : " + ReturnFormattedValues(windowsResource.CachedBytesMemory) + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "---------------------------- Disk Data ------------------------------ ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Average Queue Length                        : " + windowsResource.PhysicalDiskAvgQueueLength + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Read Speed                                  : " + ReturnFormattedValues(windowsResource.PhysicalDiskReadBytes) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Write Speed                                 : " + ReturnFormattedValues(windowsResource.PhysicalDiskWriteBytes) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Average Read Speed                          : " + ReturnFormattedValues(windowsResource.PhysicalDiskAvgReadBytes) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Average Write Speed                         : " + ReturnFormattedValues(windowsResource.PhysicalDiskAvgWriteBytes) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Disk Time                                   : " + windowsResource.PhysicalDiskTime + " %", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Process Data ----------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Handle Count                                : " + windowsResource.ProcessHandleCount + "/sec", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Thread Count                                : " + windowsResource.ProcessThreadCount + "/sec", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Conext Switch Count                         : " + windowsResource.ProcessContextSwitchCount + "/sec", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "System Calls                                : " + windowsResource.ProcessSystemCalls + "/sec", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Processor Queue Length                      : " + windowsResource.ProcessorQueueLength, Message.MessageType.Report));
                            if (LogData)
                            {
                                InitlialiseLogging();
                                lpes.LogPlatformEngineWindowsResource(windowsResource);
                            }
                        }
                        else if (resource.GetPlatformType() == Dynamics.Platform.Linux)
                        {
                            LinuxResource linuxResource = (LinuxResource)resource;
                            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "--                     Linux Resource Gathering                    -- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Processor Data --------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "CPU Load                              : " + linuxResource.CPULoad + " %", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Current Clock Speed Per Core          : " + linuxResource.CurrentClockSpeed, Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------- Memory Data ----------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Total Physical Memory                 : " + ReturnFormattedValues(linuxResource.TotalPhysicalMemory), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Used Physical Memory                  : " + ReturnFormattedValues(linuxResource.UsedPhysicalMemory), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Free Physical Memory                  : " + ReturnFormattedValues(linuxResource.FreePhysicalMemory), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------ Standard Disk Data ------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Transfers per second                  : " + linuxResource.SDTransfersPerSecond + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Data Written                          : " + ReturnFormattedValues(linuxResource.SDKBWrite * 1024), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Data Read                             : " + ReturnFormattedValues(linuxResource.SDKBRead * 1024), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Write Performance                     : " + ReturnFormattedValues(linuxResource.SDKBWriteps * 1024) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Read Performance                      : " + ReturnFormattedValues(linuxResource.SDKBReadps * 1024) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------ NVMe Disk Data ------------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Transfers per second                  : " + linuxResource.NVMETransfersPerSecond + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Data Written                          : " + ReturnFormattedValues(linuxResource.NVMEKBWrite * 1024), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Data Read                             : " + ReturnFormattedValues(linuxResource.NVMEKBRead * 1024), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Write Performance                     : " + ReturnFormattedValues(linuxResource.NVMEKBWriteps * 1024) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Read Performance                      : " + ReturnFormattedValues(linuxResource.NVMEKBReadps * 1024) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------ Device-Mapper Disk Data -------------------- ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Transfers per second                  : " + linuxResource.DMTransfersPerSecond + " ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Data Written                          : " + ReturnFormattedValues(linuxResource.DMKBWrite * 1024), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Data Read                             : " + ReturnFormattedValues(linuxResource.DMKBRead * 1024), Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Write Performance                     : " + ReturnFormattedValues(linuxResource.DMKBWriteps * 1024) + "/sec ", Message.MessageType.Report));
                            _messageQueue.AddMessage(new Message(DateTime.Now, "Read Performance                      : " + ReturnFormattedValues(linuxResource.DMKBReadps * 1024) + "/sec ", Message.MessageType.Report));
                            if (LogData)
                            {
                                InitlialiseLogging();
                                lpes.LogPlatformEngineLinuxResource(linuxResource);
                            }
                        }
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, "PlatformEngine client did not accept the monitoring request", Message.MessageType.Error));
                    }
                }
                else if (_retrievalType == Dynamics.ResourceRetrievalType.UntilNotified)
                {
                    PlatformEngineCommand command = await StartGatheringPlatformResource(cliientWebURL, uuid);
                    if (command != null)
                    {
                        if (command.Accepted == true)
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, "PlatformEngine client has accepted the monitoring request", Message.MessageType.Info));
                        }
                        else
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, "PlatformEngine client did not accept the monitoring request", Message.MessageType.Error));
                        }
                    }
                }
                else if (_retrievalType == Dynamics.ResourceRetrievalType.Duration)
                {
                    PlatformEngineCommand command = await StartGatheringPlatformResource(cliientWebURL, uuid);
                    if (command != null)
                    {
                        if (command.Accepted == true)
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, "PlatformEngine client has accepted the monitoring request", Message.MessageType.Info));
                        }
                        else
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, "PlatformEngine client did not accept the monitoring request", Message.MessageType.Error));
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            WriteMessages();
        }

        private async Task<PlatformResource> GetPointInTimePlatformResource(string url, string uuid)
        {
            PlatformResource resource = null;
            try
            {
                PlatformEngineCommand pecommand = new PlatformEngineCommand(_retrievalType, uuid, TimeSpan.FromSeconds(0).ToString(), TimeSpan.FromSeconds(0).ToString());

                var stringPayload = await Task.Run(() => JsonConvert.SerializeObject(pecommand));
                var httpContent = new StringContent(stringPayload, Encoding.UTF8, "application/json");

                _client.DefaultRequestHeaders.Accept.Clear();
                _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                HttpResponseMessage response = await _client.PostAsync(url, httpContent);
                response.EnsureSuccessStatusCode();
                try
                {
                    if (response.Content is object && response.Content.Headers.ContentType.MediaType == "application/json")
                    {
                        var contentStream = await response.Content.ReadAsStringAsync();
                        var json = JsonConvert.DeserializeObject(contentStream);
                        try
                        {
                            resource = JsonConvert.DeserializeObject<WindowsResource>(json.ToString());
                        }
                        catch (Exception)
                        {
                            resource = JsonConvert.DeserializeObject<LinuxResource>(json.ToString());
                            //not of type resource
                        }
                    }
                }
                catch (Exception ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            catch (HttpRequestException e)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, e.Message, Message.MessageType.Error));
            }
            return resource;
        }

        private async Task<PlatformEngineCommand> StartGatheringPlatformResource(string url, string uuid)
        {
            PlatformEngineCommand command = null;
            try
            {
                PlatformEngineCommand pecommand = new PlatformEngineCommand(_retrievalType, uuid, duration.ToString(), interval.ToString());

                var stringPayload = await Task.Run(() => JsonConvert.SerializeObject(pecommand));
                var httpContent = new StringContent(stringPayload, Encoding.UTF8, "application/json");

                _client.DefaultRequestHeaders.Accept.Clear();
                _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                HttpResponseMessage response = await _client.PostAsync(url, httpContent);
                if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, "There is a gather operation running for this client", Message.MessageType.Error));
                }
                else
                {
                    response.EnsureSuccessStatusCode();
                }
                try
                {
                    if (response.Content is object && response.Content.Headers.ContentType.MediaType == "application/json")
                    {
                        var contentStream = await response.Content.ReadAsStringAsync();
                        var json = JsonConvert.DeserializeObject(contentStream);
                        // If the response has "accepted : false then either an existing operation is running or there is data to collect. 
                        command = JsonConvert.DeserializeObject<PlatformEngineCommand>(json.ToString());
                    }
                }
                catch (Exception ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            catch (HttpRequestException e)
            {
                if (e.Message == "Error while copying content to a stream.")
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, "There is a locked gather operation running for this client.", Message.MessageType.Error));
                }
                else
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, e.Message, Message.MessageType.Error));
                }
            }
            return command;
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
                LogData = false;
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
                    WriteVerbose(messageToParse.Item2);
                }
                else if (messageToParse.Item1 == Message.MessageType.Report)
                {
                    WriteVerbose(messageToParse.Item2);
                }
                else 
                {
                    WriteVerbose(messageToParse.Item2);
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
            string formattedValue = dataValue + " " + storageUnit;
            return formattedValue;
        }
    }
}
