using DOES.Shared.Debug;
using DOES.DataEngine.Resources;
using System;
using System.Collections.Concurrent;
using System.Threading;
using Newtonsoft.Json;

namespace DOES.DataEngine.FileOperations
{
    /// <summary>
    /// This class handles the production of webpage data from and to files on a local filesystem. 
    /// Webpage data is consumed from this class for usage in data engine operations. 
    /// </summary>
    public class OilPump : IDisposable
    {
        private BlockingCollection<WebPage> _concurrentWebPageList = new BlockingCollection<WebPage>(new ConcurrentQueue<WebPage>(), 4);
        private ExportHandler _exporter;
        private ImportHandler _importer;
        private DataVendor _database;
        private CancellationToken _ct;
        private bool isDisposed;

        /// <summary>
        /// Instantiates an oil pump bonded to a specific database. 
        /// </summary>
        public OilPump(DataVendor database)
        {
            _database = database;
            isDisposed = false;
        }

        /// <summary>
        /// Instantiates an oil pump bonded to a specific database. 
        /// A cancellation token is included to cascade the stop operation
        /// </summary>
        public OilPump(DataVendor database, CancellationToken token)
        {
            _database = database;
            _ct = token;
            isDisposed = false;
        }

        /// <summary>
        /// Adds a webpage as a reserve for later use. 
        /// </summary>
        public void IncreaseOilReserve(WebPage page)
        {
            if (!isDisposed)
            {
                _concurrentWebPageList.Add(page);
            }
        }

        /// <summary>
        /// Returns a web page object for use. 
        /// </summary>
        public WebPage DecreaseOilReserve()
        {
            if (isDisposed)
            {
                return null;
            }
            else
            {
                try
                {
                    DateTime start = DateTime.Now;
                    while (_concurrentWebPageList.Count == 0)
                    {
                        DateTime checkDuration = DateTime.Now;
                        TimeSpan elapsed = (checkDuration - start);
                        if (elapsed.TotalSeconds > 60 && elapsed.TotalSeconds < _database.GetMechanic().FileAccessTimeout)
                        {
                            _database.DebugQueue().AddMessage(new Message(DateTime.Now, "Read from disk has waited " +
                                elapsed.TotalSeconds + " seconds for web page data", Message.MessageType.Warning));
                        }
                        if (elapsed.TotalSeconds > _database.GetMechanic().FileAccessTimeout)
                        {
                            _database.DebugQueue().AddMessage(new Message(DateTime.Now,
                                "No more files to read webpage data from or timeout expired", Message.MessageType.Error));
                            throw new Exception("No More Files to read from , file pump failure");
                        }
                        else
                        {
                            Thread.Sleep(10000);
                        }
                    }
                    return _concurrentWebPageList.Take();
                }
                catch (Exception ex)
                {
                    _database.DebugQueue().AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    SetPumpComplete();
                    return null;
                }
            }
        }

        /// <summary>
        /// Checks if the list of webpages is at a limit or if more can be added. 
        /// </summary>
        private bool PumpSaturated()
        {
            if (_concurrentWebPageList.BoundedCapacity == _concurrentWebPageList.Count)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Processes the movement of web page data to data files. 
        /// </summary>
        public ExportHandler ProcessExport(ExportHandler handler, CancellationToken token)
        {
            _exporter = handler;
            while (!token.IsCancellationRequested && !_concurrentWebPageList.IsCompleted)
            {
                WebPage page = null;
                try
                {
                    page = _concurrentWebPageList.Take(token);
                }
                catch (InvalidOperationException)
                {
                    SetPumpComplete();
                    _database.DebugQueue().AddMessage(new Message(DateTime.Now, "No more Records to read webpage data from or timeout expired", Message.MessageType.Error)); 
                }

                if (page != null)
                {
                    //track written pages here 
                    WriteToFiles(page);
                }
            }
            return _exporter;
        }

        /// <summary>
        /// Processes to the movement of web page data into the queue for usage elsewhere. 
        /// </summary>
        public ImportHandler ProcessImport(ImportHandler handler)
        {
            _importer = handler;
            ReadFromFile();
            return _importer;
        }
        /// <summary>
        /// Checks if the queue has been set to complete or not. 
        /// </summary>
        public bool GetPumpState()
        {
            if (!isDisposed)
            {
                return _concurrentWebPageList.IsAddingCompleted;
            }
            else
            {
                return true;
            }
        }
        /// <summary>
        /// Set the pump to complete so that no more webpages will be added to the queue. 
        /// </summary>
        public void SetPumpComplete()
        {
            if (!isDisposed)
            {
                _concurrentWebPageList.CompleteAdding();
            }
        }

        /// <summary>
        /// Checks how many webpages are cur rently in the queue. 
        /// </summary>
        public Int32 GetPumpCapcity()
        {
            return _concurrentWebPageList.Count;
        }

        /// <summary>
        /// Writes the webpages to a file until that file reaches a specified limit. 
        /// </summary>
        private void WriteToFiles(WebPage page)
        {
            if (_exporter.CurrentFile_FirstPageID == 0)
            {
                _exporter.InitializeWriter();
                _database.DebugQueue().AddMessage(new Message(DateTime.Now, "Version " + _database.GetMechanic().CurrentFileVersion.ToString()  +
                    " file being created : " +  _exporter.GetCurrentFileName, Message.MessageType.Info));
                string fileVersionJson = @"{ ""FileVersion"": """ + _database.GetMechanic().CurrentFileVersion + @"""}";
                _exporter.WriteJsonToFile(fileVersionJson);
            }
            _exporter.CurrentFile_PageIDCount += 1;
            page.PageID = _exporter.CurrentFile_PageIDCount;
            string webPageJson = JsonConvert.SerializeObject(page);
            _exporter.WriteJsonToFile(webPageJson);

            if (_exporter.CheckGZipStream() >= 104857600)
            {
                _exporter.CurrentFileID += 1;
                _exporter.CurrentFile_FirstPageID = 0;
                _exporter.CloseStreamWriter();
            }
            else
            {
                _exporter.CurrentFile_FirstPageID += 1;
            }
        }

        /// <summary>
        /// Read web page data from the specified file into the queue. 
        /// </summary>
        private void ReadFromFile()
        {
            try
            {
                _importer.InitializeReader();
                int pageTracker = _importer.ImportedWebPages;
                //read the first line and check the version ID
                string versionJSON = _importer.ReadFileLine();
                var versionToParse = Newtonsoft.Json.Linq.JObject.Parse(versionJSON)["FileVersion"];
                int versionID = Convert.ToInt32(versionToParse);
                if (versionID != _database.GetMechanic().CurrentFileVersion)
                {
                    throw new Exception("Version ID file mistmatch");
                }
                //Read the file
                while (!_importer.EOF() && !_ct.IsCancellationRequested)
                {
                    string webPageJsonString = _importer.ReadFileLine();
                    WebPage page = JsonConvert.DeserializeObject<WebPage>(webPageJsonString);
                    if (page != null)
                    {
                        //the the page id is Less than or equal to the recorded imported web pages then its useless and we skip it
                        if (page.PageID > _importer.ImportedWebPages)
                        {
                            //Ensure that the concurrent queue is not full
                            while (PumpSaturated())
                            {
                                Thread.Sleep(200);
                                if (_ct.IsCancellationRequested)
                                {
                                    break;
                                }
                            }
                            if (GetPumpState() == false)
                            {
                                IncreaseOilReserve(page);
                                _importer.ImportedWebPages = page.PageID;
                                _database.UpdateImportHistory(_importer);
                            }
                        }
                    }
                }
                _database.UpdateImportHistory(_importer);
                _importer.TerminateImportOperation();
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to read beyond the end of the stream".ToLower()))
                {
                    _importer.AllImportedWebPages = true;
                    _database.UpdateImportHistory(_importer);
                }
                else
                {
                    _database.DebugQueue().AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                }
                _importer.Filename = null;
            }
        }
        /// <summary>
        /// Dispose() calls Dispose(true)
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        /// <summary>
        /// Disposes the managed objects
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (isDisposed) return;

            if (disposing)
            {
                // free managed resources
                _concurrentWebPageList.Dispose();
                if (_importer != null)
                {
                    _importer.TerminateImportOperation();
                }
                if (_exporter != null)
                {
                    _exporter.CloseStreamWriter();
                }
            }
            isDisposed = true;
        }
    }
}
