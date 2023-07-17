using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using DOES.Shared.Debug;
using DOES.Shared.Operations;
using DOES.Shared.Resources;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;

namespace DOES.PlatformEngine.Client
{
    public class Startup
    {
        private ConcurrentBag<ResourceCollector> _concurrentCollectorList = new ConcurrentBag<ResourceCollector>();
        MessageQueue _messageQueue = new MessageQueue();
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
                app.UseDeveloperExceptionPage();
            else
                app.UseHsts();

            app.UseRouting();

            //accepts get to retrieve long running data bodies 
            // get to //does/api/3/platform/<uuid> to retirieve the resource tree listing created for that request 
            bool isUUIDInBag = false;
            bool okToGatherNewResource = true;
            app.UseEndpoints(endpoints =>
            {
                //This endpoint is for the base web server. Would be useful to document APi usage here. 
                endpoints.MapGet("/", async context =>
                {
                    await context.Response.WriteAsync("This is the D.O.E.S PlatformEngine client.");
                    WriteMessages();
                });

                endpoints.MapPost("/does/api/3/platform", async context =>
                {
                    // This endpoint is resposible for starting the resource gather operation. 
                    // Depending on the type of gather operation requested this will either respond with a resource 
                    // or a message informing if resource gathering has started
                    PlatformEngineCommand peCommand = null;
                    try
                    {
                        peCommand = await context.Request.ReadFromJsonAsync<PlatformEngineCommand>();
                    }
                    catch (Exception ex)
                    {
                            _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    switch (peCommand.Instruction)
                    {
                        case Dynamics.ResourceRetrievalType.PointInTime:
                            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == true)
                            {
                                // Then its a windows platform
                                WindowsResource resourceValue = GatherWindows.GatherWindowsResourceDataPoint(_messageQueue);
                                var json = JsonConvert.SerializeObject(resourceValue);
                                await context.Response.WriteAsJsonAsync(json);
                            }
                            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == true)
                            {
                                //Then its a linux platform
                                LinuxResource resourceValue = GatherLinux.GatherLinuxResourceDataPoint(_messageQueue);
                                var json = JsonConvert.SerializeObject(resourceValue);
                                await context.Response.WriteAsJsonAsync(json);
                            }
                            else
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, "Unsupported platform requested for point in time monitoring. Client ID : " + peCommand.UUID, Message.MessageType.Error));
                                context.Response.StatusCode = 404;
                                await context.Response.WriteAsync("Unsupported resource type");
                            }
                            break;
                        case Dynamics.ResourceRetrievalType.Duration:
                            isUUIDInBag = false;
                            okToGatherNewResource = true;
                            foreach (ResourceCollector rc in _concurrentCollectorList)
                            {
                                if (rc.Record.Command.UUID == peCommand.UUID)
                                {
                                    isUUIDInBag = true;
                                    if (!rc.OkForNewResourcegather())
                                    {
                                        okToGatherNewResource = false;
                                    }
                                    break;
                                }
                            }

                            if (!isUUIDInBag)
                            {
                                ResourceRecord durationRecords = new ResourceRecord(peCommand);
                                ResourceCollector durationCollector = new ResourceCollector(durationRecords, _messageQueue);
                                _concurrentCollectorList.Add(durationCollector);
                            }
                            else if (isUUIDInBag && okToGatherNewResource)
                            {
                                ConcurrentBag<ResourceCollector> _newConcurrentCollectorList = new ConcurrentBag<ResourceCollector>();
                                foreach (ResourceCollector rc in _concurrentCollectorList)
                                {
                                    if (rc.Record.Command.UUID != peCommand.UUID)
                                    {
                                        _newConcurrentCollectorList.Add(rc);
                                    }
                                }
                                ResourceRecord durationRecords = new ResourceRecord(peCommand);
                                ResourceCollector durationCollector = new ResourceCollector(durationRecords, _messageQueue);
                                _concurrentCollectorList = _newConcurrentCollectorList;
                                _concurrentCollectorList.Add(durationCollector);
                            }
                            foreach (ResourceCollector rc in _concurrentCollectorList)
                            {
                                if (rc.Record.Command.UUID == peCommand.UUID)
                                {
                                    // run a check to see if there are existing operations running
                                    //The data must have been collected and set to complete for this to run ok. 
                                    if (rc.OkForNewResourcegather())
                                    {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                                        Task.Run(() => rc.CollectDurationResourceData());
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                                        peCommand.Accepted = true;
                                        context.Response.StatusCode = 202;
                                    }
                                    else
                                    {
                                        peCommand.Accepted = false;
                                        context.Response.StatusCode = 409;
                                        _messageQueue.AddMessage(new Message(DateTime.Now, "Resource gathering operation already in process. Client ID : " + peCommand.UUID, Message.MessageType.Error));
                                    }
                                }
                                var jsonresponse = JsonConvert.SerializeObject(peCommand);
                                await context.Response.WriteAsJsonAsync(jsonresponse);
                                break;
                            }
                            break;
                        case Dynamics.ResourceRetrievalType.UntilNotified:
                            isUUIDInBag = false;
                            okToGatherNewResource = true;
                            foreach (ResourceCollector rc in _concurrentCollectorList)
                            {
                                if (rc.Record.Command.UUID == peCommand.UUID)
                                {
                                    isUUIDInBag = true;
                                    if (!rc.OkForNewResourcegather())
                                    {
                                        okToGatherNewResource = false;
                                    }
                                    break;
                                }
                            }

                            if (!isUUIDInBag)
                            {
                                ResourceRecord durationRecords = new ResourceRecord(peCommand);
                                ResourceCollector durationCollector = new ResourceCollector(durationRecords, _messageQueue);
                                _concurrentCollectorList.Add(durationCollector);
                            }
                            else if (isUUIDInBag && okToGatherNewResource)
                            {
                                ConcurrentBag<ResourceCollector> _newConcurrentCollectorList = new ConcurrentBag<ResourceCollector>();
                                foreach (ResourceCollector rc in _concurrentCollectorList)
                                {
                                    if (rc.Record.Command.UUID != peCommand.UUID)
                                    {
                                        _newConcurrentCollectorList.Add(rc);
                                    }
                                }
                                ResourceRecord durationRecords = new ResourceRecord(peCommand);
                                ResourceCollector durationCollector = new ResourceCollector(durationRecords, _messageQueue);
                                _concurrentCollectorList = _newConcurrentCollectorList;
                                _concurrentCollectorList.Add(durationCollector);
                            }

                            foreach (ResourceCollector rc in _concurrentCollectorList)
                            {
                                if (rc.Record.Command.UUID == peCommand.UUID)
                                {
                                    //run a check to see if there are existing operations running
                                    //The data must have been collected and set to complete for this to run ok. 
                                    if (rc.OkForNewResourcegather())
                                    {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                                        Task.Run(() => rc.StartGatheringResourceData());
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                                        peCommand.Accepted = true;
                                        context.Response.StatusCode = 202;
                                    }
                                    else
                                    {
                                        peCommand.Accepted = false;
                                        context.Response.StatusCode = 409;
                                        _messageQueue.AddMessage(new Message(DateTime.Now, "Resource gathering operation already in process. Client ID : " + peCommand.UUID , Message.MessageType.Error));
                                    }
                                    var jsonresponse = JsonConvert.SerializeObject(peCommand);
                                    await context.Response.WriteAsJsonAsync(jsonresponse);
                                    break;
                                }
                            }
                            break;
                    }
                    WriteMessages();
                });

                endpoints.MapGet("/does/api/3/platform/{uuid:guid}", async context =>
                {
                    var uuid = context.Request.RouteValues["uuid"];
                    List<PlatformResource> resourceRecordHistory = null;
                    bool gatherIncomplete = false;
                    foreach (ResourceCollector rc in _concurrentCollectorList)
                    {
                        if (rc.Record.Command.UUID == uuid.ToString())
                        {
                            if (rc.Record.Command.Instruction == Dynamics.ResourceRetrievalType.Duration)
                            {
                                if(rc.Record.GatherDurationCompleted)
                                {
                                    if(!rc.isCollected())
                                    {
                                        resourceRecordHistory = rc.RetrieveCollectionHistory();
                                        break;
                                    }
                                }
                                else
                                {
                                    gatherIncomplete = true;
                                    _messageQueue.AddMessage(new Message(DateTime.Now, "Resource gathering is still in process. Client ID : " + uuid , Message.MessageType.Error));
                                }
                            }
                            else if(rc.Record.Command.Instruction == Dynamics.ResourceRetrievalType.UntilNotified)
                            {
                                rc.StopGatheringResourceData();
                                while (rc.Record.GatherDurationCompleted != true)
                                {
                                    Thread.Sleep(1000);
                                }
                                resourceRecordHistory = rc.RetrieveCollectionHistory();
                                break;
                            }
                        }
                    }
                    if(resourceRecordHistory != null)
                    {
                        var jsonresponse = JsonConvert.SerializeObject(resourceRecordHistory, Formatting.Indented);
                        await context.Response.WriteAsJsonAsync(jsonresponse);
                    }
                    else if(gatherIncomplete)
                    {
                        context.Response.StatusCode = 409;
                        Message errorResponse = new Message(DateTime.Now, "Cannot start gathering new reources for this client due to existing data or running operation. Client ID : " + uuid, Message.MessageType.Error);
                        _messageQueue.AddMessage(errorResponse);
                        var jsonresponse = JsonConvert.SerializeObject(errorResponse);
                        await context.Response.WriteAsJsonAsync(jsonresponse);                      
                    }
                    else
                    {
                        context.Response.StatusCode = 404;
                        Message errorResponse = new Message(DateTime.Now, "No resource gathering operation in process for client " + uuid, Message.MessageType.Error);
                        _messageQueue.AddMessage(errorResponse);
                        var jsonresponse = JsonConvert.SerializeObject(errorResponse);
                        await context.Response.WriteAsJsonAsync(jsonresponse);
                    }
                    WriteMessages();
                });
            });
        }

        private void WriteMessages()
        {
            List<Message> tickedMessages = _messageQueue.GetAllMessages();
            foreach (Message m in tickedMessages)
            {
                Tuple<Message.MessageType, string> messageToParse = m.GetFormattedMessage();
                if (messageToParse.Item1 == Message.MessageType.Error)
                {
                    DebugLogger.LogMessages(DebugLogger.File.PlatformEngine, messageToParse.Item2);
                }
            }
        }
    }

    public class ResourceRecord
    {
        private List<PlatformResource> _resourceHistory = new List<PlatformResource>();
        public ResourceRecord(PlatformEngineCommand command)
        {
            Command = command;
            GatherDurationCompleted = false;
        }

        public void AddNewRecord(PlatformResource resource)
        {
            _resourceHistory.Add(resource);
        }

        public List<PlatformResource> RetriveRecordHistory()
        {
            return _resourceHistory;
        }

        public void ClearResourceHistory()
        {
            _resourceHistory.Clear();
        }

        public PlatformEngineCommand Command { get; set; }

        public bool GatherDurationCompleted { get; set; }
    }

    public class ResourceCollector
    {
        private static System.Timers.Timer resourceTimer;
        private bool collected;
        private bool running;
        MessageQueue _queue;
       
        public ResourceCollector(ResourceRecord record, MessageQueue queue)
        {
            Record = record;
            collected = true;
            running = false;
            _queue = queue;
        }

        public bool isCollected()
        {
            return collected;
        }

        public List<PlatformResource> RetrieveCollectionHistory()
        {
            collected = true;
            return Record.RetriveRecordHistory();
        }

        public bool OkForNewResourcegather()
        {
            if(running == false && collected == true)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public void CollectDurationResourceData()
        {
            DateTime collectorStart = DateTime.Now;
            StartGatheringResourceData();
            DateTime collectorEnd = collectorStart.Add(Record.Command.GetDuration());
            while(DateTime.Now < collectorEnd)
            {
                Thread.Sleep(Convert.ToInt32(Record.Command.GetDuration().TotalMilliseconds));
            }
            StopGatheringResourceData();
            Record.GatherDurationCompleted = true;
        }

        public void StartGatheringResourceData()
        {
            //In milliseconds
            resourceTimer = new System.Timers.Timer(Record.Command.GetInterval().TotalMilliseconds);
            resourceTimer.Elapsed += OnTimedEventGatherResource;
            resourceTimer.Enabled = true;
            running = true;
            collected = false;
        }

        private void OnTimedEventGatherResource(Object source, ElapsedEventArgs e)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == true)
            {
                Record.AddNewRecord(GatherWindows.GatherWindowsResourceDataPoint(_queue));

            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == true)
            {
                Record.AddNewRecord(GatherLinux.GatherLinuxResourceDataPoint(_queue));
            }
        }

        public void StopGatheringResourceData()
        {
            resourceTimer.Enabled = false;
            resourceTimer.Dispose();
            Record.GatherDurationCompleted = true;
            running = false;
        }

        public ResourceRecord Record { get; set; }
    }
}
