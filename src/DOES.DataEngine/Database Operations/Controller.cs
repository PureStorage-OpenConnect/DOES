using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using DOES.Shared.Debug;
using DOES.DataEngine.FileOperations;
using DOES.DataEngine.Resources;
using DOES.Shared.Resources;


namespace DOES.DataEngine.Operations
{
    /// <summary>
    /// This class is responsible for coordinating all data engine operations
    /// Contains all methods used to interact with the data objects suc as object creation , destruction , population , deletion and change.
    /// </summary>
    public class Controller : Engine, IDisposable
    {
        //Common database Parameters 
        private bool _lobSchema = false;
        private int _columnWidth;
        private DataVendor _database;

        //Other parameters
        private readonly int _numberOfThreads;
        private OilPump[] _pumpThreads;

        //Result variable lists
        private DataEngineResults[] _threadResults;
        private DataEngineResults[][] _testThreadResults;
        private DataEngineResults _operationResults;
        private DataEngineResultSet _finalResults;
        private BlockingCollection<DataEngineResults> _resultCollection = new BlockingCollection<DataEngineResults>(new ConcurrentBag<DataEngineResults>());

        /// <summary>
        /// Creates a controller object.
        /// Only interacts with the database without any specific customization.
        /// Typically used to destroy or export base web page data.
        /// </summary>
        public Controller(DataVendor database)
        {
            _database = database;
        }

        /// <summary>
        /// Creates a controller object.
        /// Interacts with the database allowing for customization of maximum column width and the number of threads to run at.
        /// </summary>
        public Controller(DataVendor database, int columnWidth, int numberOfThreads)
        {
            _database = database;
            _columnWidth = columnWidth;
            _numberOfThreads = numberOfThreads;
            _threadResults = new DataEngineResults[_numberOfThreads];
        }
        /// <summary>
        /// Creates a controller object.
        /// Interacts with the database allowing for customization of maximum column width and the number of threads to run at.
        /// Allows for report aggregation of data engine test functionality (Simple , Advanced and Complex). 
        /// </summary>
        public Controller(DataVendor database, int columnWidth, int numberOfThreads, bool testCycle)
        {
            _database = database;
            _columnWidth = columnWidth;
            _numberOfThreads = numberOfThreads;
            if (!testCycle)
            {
                _threadResults = new DataEngineResults[_numberOfThreads];
            }
            _threadResults = new DataEngineResults[_numberOfThreads];
        }

        /// <summary>
        /// Destroys objects for a specific database type through drop or truncation capabilities. 
        /// </summary>
        public override bool ClearTablesAndIndexes(Dynamics.ClearingType clearingType)
        {
            bool success = _database.DestroyObjects(clearingType);
            return success;
        }

        /// <summary>
        /// Creates custom objects for a specific database type based on the type of schema selected. 
        /// </summary>
        public override bool CreateTablesAndIndexes()
        {
            //First identify xml files to read
            //There will always be two files , one for the base tables
            //Another for the fixed data tables
            string schemaBaseFile = null;
            string schemaExtensionResource = null;
            string vendor = null;

            switch (_database.GetDatabaseType())
            {
                case Dynamics.Database.MicrosoftSQL:
                    schemaBaseFile = Dynamics.SchemaResource.MSSQL_BaseTables.ToString() + ".xml";
                    vendor = "Microsoft_SQL";
                    schemaExtensionResource = (_database.GetSchemaType()) switch
                    {
                        Dynamics.DatabaseSchema.WithIndexes => Dynamics.SchemaResource.MSSQL_TablesWithIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexes => Dynamics.SchemaResource.MSSQL_TablesWithoutIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithIndexesLOB => Dynamics.SchemaResource.MSSQL_TablesWithIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexesLOB => Dynamics.SchemaResource.MSSQL_TablesWithoutIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.MemoryOptimised => Dynamics.SchemaResource.MSSQL_InMemoryTablesWithIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexes => Dynamics.SchemaResource.MSSQL_InMemoryWithoutIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.MemoryOptimisedLOB => Dynamics.SchemaResource.MSSQL_InMemoryTablesWithIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB => Dynamics.SchemaResource.MSSQL_InMemoryWithoutIndexesLOB.ToString() + ".xml",
                        _ => Dynamics.SchemaResource.MSSQL_TablesWithoutIndexes.ToString() + ".xml",
                    };
                    break;
                case Dynamics.Database.Oracle:
                    schemaBaseFile = Dynamics.SchemaResource.OracleDB_BaseTables.ToString() + ".xml";
                    vendor = "Oracle_Database";
                    schemaExtensionResource = (_database.GetSchemaType()) switch
                    {
                        Dynamics.DatabaseSchema.WithIndexes => Dynamics.SchemaResource.OracleDB_TablesWithIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexes => Dynamics.SchemaResource.OracleDB_TablesWithoutIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithIndexesLOB => Dynamics.SchemaResource.OracleDB_TablesWithIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexesLOB => Dynamics.SchemaResource.OracleDB_TablesWithoutIndexesLOB.ToString() + ".xml",
                        _ => Dynamics.SchemaResource.OracleDB_TablesWithoutIndexes.ToString() + ".xml",
                    };
                    break;
                case Dynamics.Database.SAPHANA:
                    schemaBaseFile = Dynamics.SchemaResource.SAPHANA_BaseTables.ToString() + ".xml";
                    vendor = "SAP_HANA";
                    schemaExtensionResource = (_database.GetSchemaType()) switch
                    {
                        Dynamics.DatabaseSchema.WithIndexes => Dynamics.SchemaResource.SAPHANA_TablesWithIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexes => Dynamics.SchemaResource.SAPHANA_TablesWithoutIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithIndexesLOB => Dynamics.SchemaResource.SAPHANA_TablesWithIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexesLOB => Dynamics.SchemaResource.SAPHANA_TablesWithoutIndexesLOB.ToString() + ".xml",
                        _ => Dynamics.SchemaResource.SAPHANA_TablesWithoutIndexes.ToString() + ".xml",
                    };
                    break;
                case Dynamics.Database.MySQL:
                    schemaBaseFile = Dynamics.SchemaResource.MySQL_BaseTables.ToString() + ".xml";
                    vendor = "MySQL";
                    schemaExtensionResource = (_database.GetSchemaType()) switch
                    {
                        Dynamics.DatabaseSchema.WithIndexes => Dynamics.SchemaResource.MySQL_TablesWithIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexes => Dynamics.SchemaResource.MySQL_TablesWithoutIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithIndexesLOB => Dynamics.SchemaResource.MySQL_TablesWithIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexesLOB => Dynamics.SchemaResource.MySQL_TablesWithoutIndexesLOB.ToString() + ".xml",
                        _ => Dynamics.SchemaResource.MySQL_TablesWithoutIndexes.ToString() + ".xml",
                    };
                    break;
                case Dynamics.Database.MariaDB:
                    schemaBaseFile = Dynamics.SchemaResource.MariaDB_BaseTables.ToString() + ".xml";
                    vendor = "MariaDB";
                    schemaExtensionResource = (_database.GetSchemaType()) switch
                    {
                        Dynamics.DatabaseSchema.WithIndexes => Dynamics.SchemaResource.MariaDB_TablesWithIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexes => Dynamics.SchemaResource.MariaDB_TablesWithoutIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithIndexesLOB => Dynamics.SchemaResource.MariaDB_TablesWithIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexesLOB => Dynamics.SchemaResource.MariaDB_TablesWithoutIndexesLOB.ToString() + ".xml",
                        _ => Dynamics.SchemaResource.MariaDB_TablesWithoutIndexes.ToString() + ".xml",
                    };
                    break;
                case Dynamics.Database.PostgreSQL:
                    schemaBaseFile = Dynamics.SchemaResource.PostgreSQL_BaseTables.ToString() + ".xml";
                    vendor = "PostgreSQL";
                    schemaExtensionResource = (_database.GetSchemaType()) switch
                    {
                        Dynamics.DatabaseSchema.WithIndexes => Dynamics.SchemaResource.PostgreSQL_TablesWithIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexes => Dynamics.SchemaResource.PostgreSQL_TablesWithoutIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithIndexesLOB => Dynamics.SchemaResource.PostgreSQL_TablesWithIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexesLOB => Dynamics.SchemaResource.PostgreSQL_TablesWithoutIndexesLOB.ToString() + ".xml",
                        _ => Dynamics.SchemaResource.PostgreSQL_TablesWithoutIndexes.ToString() + ".xml",
                    };
                    break;
                case Dynamics.Database.MongoDB:
                    schemaBaseFile = Dynamics.SchemaResource.MongoDB_BaseCollections.ToString() + ".xml";
                    vendor = "MongoDB";
                    schemaExtensionResource = (_database.GetSchemaType()) switch
                    {
                        Dynamics.DatabaseSchema.WithIndexes => Dynamics.SchemaResource.MongoDB_CollectionsWithIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexes => Dynamics.SchemaResource.MongoDB_CollectionsWithIndexesLOB.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithIndexesLOB => Dynamics.SchemaResource.MongoDB_CollectionsWithoutIndexes.ToString() + ".xml",
                        Dynamics.DatabaseSchema.WithoutIndexesLOB => Dynamics.SchemaResource.MongoDB_CollectionsWithoutIndexesLOB.ToString() + ".xml",
                        _ => Dynamics.SchemaResource.MongoDB_CollectionsWithIndexes.ToString() + ".xml",
                    };
                    break;
            }

            if(_database.GetSchemaType().ToString().Contains("LOB"))
            {
                _lobSchema = true;
            }                  

            List<string> baseSchemaCreationCommands = new List<string>();
            List<string> extensionSchemaCreationCommands = new List<string>();

            baseSchemaCreationCommands = GetXMLResource(schemaBaseFile, vendor);
            extensionSchemaCreationCommands = GetXMLResource(schemaExtensionResource, vendor);
            bool status = _database.CreateObjects(baseSchemaCreationCommands, extensionSchemaCreationCommands);
            return status;
        }

        /// <summary>
        /// Deletes object members for a specific database type . 
        /// Can be triggered independently or as apart of a test scenario .
        /// If not apart of a test scenario use  a null object for webPagesToProcess
        /// </summary>
        public override void Delete(ulong RequestedBytes, CancellationTokenSource tokenSource,
            int? resultIndex, Queue<WebPage>[] webPagesToProcess)
        {
            _database.TokenSource = tokenSource;
            DateTime startOperation = DateTime.Now;
            if (resultIndex != null)
            {
                _testThreadResults[(int)resultIndex] = new DataEngineResults[_numberOfThreads];
            }
            else
            {
                Tuple<UInt64, Queue<WebPage>[]> compiledAndCheckedPages = CompileWebPagesToProcess(RequestedBytes, tokenSource.Token);
                try { if (tokenSource.Token.IsCancellationRequested) { return; } } catch (ObjectDisposedException) { return; }
                RequestedBytes = compiledAndCheckedPages.Item1;
                webPagesToProcess = compiledAndCheckedPages.Item2;
                for (int queueIndex = 0; queueIndex < webPagesToProcess.Length; queueIndex++)
                {
                    if (!webPagesToProcess[queueIndex].Any())
                    {
                        RequestedBytes = 0;
                        SetResultsStateCompleted();
                    }
                }
            }
            UInt64 requestedBytesPerThread = Convert.ToUInt64(Math.Round(Convert.ToDouble(RequestedBytes / (ulong)_numberOfThreads), 0));
            Parallel.For(0, _numberOfThreads, i =>
            {
                DateTime threadStart = DateTime.Now;
                UInt64 processedRows = 0;
                UInt64 processedBytes = 0;
                UInt64 processedTransactions = 0;
                UInt64 failedProcessedTransactions = 0;
                UInt64 requestedBytesThisThread = requestedBytesPerThread;
                if (resultIndex == null)
                {
                    _threadResults[i] = new DataEngineResults();
                }
                else
                {
                    _testThreadResults[(int)resultIndex][i] = new DataEngineResults();
                }
                while (processedBytes < requestedBytesThisThread && !tokenSource.Token.IsCancellationRequested)
                {
                    if (webPagesToProcess[i].Count != 0)
                    {
                        WebPage pageToProcess = webPagesToProcess[i].Dequeue();
                        int threadPagesDeleted = _database.DeleteWebPage(pageToProcess.PageID);

                        if (threadPagesDeleted != 0)
                        {
                            processedBytes += pageToProcess.TotalLength;
                            processedRows += (ulong)threadPagesDeleted;
                            processedTransactions += 1;
                        }
                        else
                        {
                            failedProcessedTransactions += 1;
                        }
                    }
                    else
                    {
                        requestedBytesThisThread = 0;
                    }
                    if (resultIndex == null)
                    {
                        _threadResults[i].ProcessedBytes = processedBytes;
                        _threadResults[i].ProcessedRows = processedRows;
                        _threadResults[i].ProcessedTransactions = processedTransactions;
                        _threadResults[i].FailedProcessedRows = Convert.ToUInt64(failedProcessedTransactions);
                        _threadResults[i].ResultType = Dynamics.DataEngineOperation.DeleteData;
                    }
                    else
                    {
                        _testThreadResults[(int)resultIndex][i].ProcessedBytes = processedBytes;
                        _testThreadResults[(int)resultIndex][i].ProcessedRows = processedRows;
                        _testThreadResults[(int)resultIndex][i].FailedProcessedRows = failedProcessedTransactions;
                        _testThreadResults[(int)resultIndex][i].ProcessedTransactions = processedTransactions;
                        _testThreadResults[(int)resultIndex][i].ResultType = Dynamics.DataEngineOperation.DeleteData;
                    }
                }
                DateTime threadEnd = DateTime.Now;
                if (resultIndex == null)
                {
                    _threadResults[i].OperationStart = threadStart;
                    _threadResults[i].OperationEnd = threadEnd;
                }
                else
                {
                    _testThreadResults[(int)resultIndex][i].OperationStart = threadStart;
                    _testThreadResults[(int)resultIndex][i].OperationEnd = threadEnd;
                }
            });
            if (resultIndex == null)
            {
                DateTime endOperation = DateTime.Now;
                SetFinalResults(startOperation, endOperation);

                foreach (DataEngineResults dr in _threadResults)
                {
                    _resultCollection.Add(dr);
                }
                SetResultsStateCompleted();
            }
        }

        /// <summary>
        /// Extracts core webpage aspects such as URL, HTML and header data and places them in local files.
        /// </summary>
        public override void Export(string Folder, CancellationTokenSource tokenSource)
        {
            //Create a new oil pump
            OilPump exportPump = new OilPump(_database);
            _database.TokenSource = tokenSource;
            try
            {
                Task.Run(() =>
                {
                var capturedToken = tokenSource.Token;
                _database.ReadBaseWebPageData(exportPump);
                exportPump.SetPumpComplete();
                }, tokenSource.Token);

                while (!(exportPump.GetPumpCapcity() > 0))
                {

                    if (tokenSource.Token.IsCancellationRequested) { break; }
                    Thread.Sleep(2000);
                }

                Task.Run(() =>
                {
                    var capturedToken = tokenSource.Token;
                    ExportHandler exporter = new ExportHandler(Folder, _database.GetMechanic().DataFileRootName);
                    exportPump.ProcessExport(exporter, capturedToken);
                }, tokenSource.Token);


                while (!exportPump.GetPumpState())
                {
                    Thread.Sleep(5000);
                }
            }
            catch (ObjectDisposedException) { }
            exportPump.Dispose();
        }

        /// <summary>
        /// Returns the current state of controller operations. 
        /// </summary>
        public override bool GetResultsState()
        {
            return _resultCollection.IsAddingCompleted;
        }

        /// <summary>
        /// Insert data into objects for a specific database type using data files as the data source.
        /// The number of data files available needs to match the number of threads for the controller object. 
        /// </summary>
        public override void ImportFromFile(string Folder, ulong RequestedBytes, CancellationTokenSource tokenSource, 
            int? resultIndex)
        {
            // Create a token to read into both threads. 
            // This token will be cancelled if the parent token is triggered. 
            var operationTokenSource = new CancellationTokenSource();
            var operationToken = operationTokenSource.Token;
            Task fileReadTask;
            Task databaseWriteTask;
            var tasks = new ConcurrentBag<Task>();
            _database.TokenSource = tokenSource;
            // If this is a test scenario the there will be a result index
            // Having a result index allows for the tracking of analytics. 
            if (resultIndex != null)
            {
                _testThreadResults[(int)resultIndex] = new DataEngineResults[_numberOfThreads];
            }
            // The same number of producers for file read need to be created as threads. 
            // Each file read operation is read into the database insert task using parallel.for indexes
            _pumpThreads = new OilPump[_numberOfThreads];
            for (int threadinit = 0; threadinit < _numberOfThreads; threadinit++)
            {
                _pumpThreads[threadinit] = new OilPump(_database, operationToken);
            }
            // Check if the files are valid and usable
            Queue<ImportHandler> validImportFiles = new Queue<ImportHandler>();
            var importFiles = Directory.EnumerateFiles(Folder, _database.GetMechanic().DataFileRootName + "*").OrderBy(filename => filename);
            var fileCount = (from file in importFiles select file).Count();
            DateTime startOperation = DateTime.Now;
            if ((int)fileCount >= _numberOfThreads)
            {
                BlockingCollection<string> AllDataFiles = new BlockingCollection<string>(new ConcurrentQueue<string>());
                string[] DataFileToProcess = new string[_numberOfThreads];

                foreach (string file in importFiles)
                {
                    AllDataFiles.Add(file);
                }
                //This will read from a number of data files into the respective queues
                fileReadTask = Task.Run(() =>
                {
                    try
                    {
                        while (!tokenSource.Token.IsCancellationRequested && !GetResultsState() && !operationToken.IsCancellationRequested)
                        {
                            for (int fileQueueNum = 0; fileQueueNum < DataFileToProcess.Length; fileQueueNum++)
                            {
                                if (DataFileToProcess[fileQueueNum] == null)
                                {
                                    DataFileToProcess[fileQueueNum] = AllDataFiles.Take();
                                }
                            }
                            Parallel.For(0, _numberOfThreads, i =>
                            {
                                try
                                {
                                    bool threadContininuation = true;
                                    while (threadContininuation == true)
                                    {
                                        if (DataFileToProcess[i] != null)
                                        {
                                            if (_pumpThreads[i].GetPumpState() == false)
                                            {
                                                ImportHandler import = _database.CheckFileImportHistory(DataFileToProcess[i]);
                                                if (import == null)
                                                {
                                                    threadContininuation = false;
                                                    break;
                                                }
                                                if (import.Found == false)
                                                {
                                                    bool insertResult = _database.CreateImportHistory(import);
                                                    _database.DebugQueue().AddMessage(new Message(DateTime.Now,
                                                      "Thread " + i + " is reading from " + DataFileToProcess[i], Message.MessageType.Info));
                                                    import = _pumpThreads[i].ProcessImport(import);
                                                    if (import.Filename == null || import.AllImportedWebPages == true)
                                                    {
                                                        DataFileToProcess[i] = null;
                                                    }
                                                    else
                                                    {
                                                        threadContininuation = false;
                                                    }
                                                }
                                                else if (import.Found == true && import.AllImportedWebPages == false)
                                                {
                                                    _database.DebugQueue().AddMessage(new Message(DateTime.Now,
                                                      "Thread " + i + " is reading from " + DataFileToProcess[i], Message.MessageType.Info));
                                                    import = _pumpThreads[i].ProcessImport(import);
                                                    if (import.Filename == null || import.AllImportedWebPages == true)
                                                    {
                                                        DataFileToProcess[i] = null;
                                                    }
                                                    else
                                                    {
                                                        threadContininuation = false;
                                                    }
                                                }
                                                else
                                                {
                                                    DataFileToProcess[i] = null;
                                                }
                                            }
                                            else
                                            {
                                                threadContininuation = false;
                                            }
                                        }
                                        else
                                        {
                                            DataFileToProcess[i] = AllDataFiles.Take();
                                        }
                                        if (operationToken.IsCancellationRequested)
                                        {
                                            threadContininuation = false;
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _database.DebugQueue().AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                                }
                            });
                        }
                    }
                    catch (ObjectDisposedException) { }
                }, tokenSource.Token);
                tasks.Add(fileReadTask);
            }
            else
            {
                _database.DebugQueue().AddMessage(new Message(DateTime.Now,
                   "More files are needed to run the operation. Increase the number of data files or reduce the number of threads",
                   Message.MessageType.Error));
                SetResultsStateCompleted();
            }

            //then we wait for the pumps to each have something in them 
            int activePumps = 0;
            while (activePumps < _pumpThreads.Length)
            {
                foreach (OilPump e in _pumpThreads)
                {
                    if (e.GetPumpCapcity() != 0)
                    {
                        activePumps++;
                    }
                }
            }
            //and then the database can be triggerd to start running inserts
            startOperation = DateTime.Now;
            _database.TokenSource = tokenSource;
            databaseWriteTask = Task.Run(() =>
            {
                UInt64 failedLoad = 0;
                int pageID = 0;

                UInt64 requestedBytesPerThread = Convert.ToUInt64(Math.Round(Convert.ToDouble(RequestedBytes / (ulong)_numberOfThreads), 0));
                try
                {
                    DateTime startOperation = DateTime.Now;
                    Parallel.For(0, _numberOfThreads, i =>
                    {
                        try
                        {
                            DateTime threadStart = DateTime.Now;
                            UInt64 processedRows = 0;
                            UInt64 processedBytes = 0;
                            UInt64 processedTransactions = 0;
                            UInt64 requestedBytesThisThread = requestedBytesPerThread;
                            UInt64 maxAllowedFailures = Convert.ToUInt64(RequestedBytes * (ulong)50 / 1000000.0);
                            if (resultIndex == null)
                            {
                                _threadResults[i] = new DataEngineResults();
                            }
                            else
                            {
                                _testThreadResults[(int)resultIndex][i] = new DataEngineResults();
                            }
                            while ((failedLoad < maxAllowedFailures) && (processedBytes < requestedBytesThisThread))
                            {
                                if (!tokenSource.Token.IsCancellationRequested)
                                {
                                    if (_pumpThreads[i].GetPumpState() == false)
                                    {
                                        WebPage page = _pumpThreads[i].DecreaseOilReserve();
                                        if (page == null)
                                        {
                                            break;
                                        }
                                        WebPage configuredPage;
                                        if (_lobSchema == true)
                                        {
                                            configuredPage = PrepareEncodedWebPage(pageID, page.URL, page.HTML, page.Headers);
                                        }
                                        else
                                        {
                                            configuredPage = PrepareCharacterisedWebPage(pageID, page.URL, page.HTML, page.Headers);
                                        }
                                        int rowsInserted = 0;
                                        if (_lobSchema == false)
                                        {
                                            rowsInserted = _database.InsertCharacterisedWebPage(configuredPage);
                                        }
                                        else
                                        {
                                            rowsInserted = _database.InsertEncodedWebPage(configuredPage);
                                        }
                                        if (rowsInserted != 0)
                                        {
                                            processedTransactions++;
                                            processedRows += (ulong)rowsInserted;
                                            processedBytes += Convert.ToUInt32(configuredPage.TotalLength);
                                        }
                                        else
                                        {
                                            failedLoad += 1;
                                        }
                                    }
                                    else
                                    {
                                        requestedBytesThisThread = 0;
                                    }
                                }
                                else
                                {
                                    requestedBytesThisThread = 0;
                                }
                                if (resultIndex == null)
                                {
                                    _threadResults[i].ProcessedBytes = processedBytes;
                                    _threadResults[i].ProcessedRows = processedRows;
                                    _threadResults[i].ProcessedTransactions = processedTransactions;
                                    _threadResults[i].FailedProcessedRows = Convert.ToUInt64(failedLoad);
                                    _threadResults[i].ResultType = Dynamics.DataEngineOperation.InsertData;
                                }
                                else
                                {
                                    _testThreadResults[(int)resultIndex][i].ProcessedBytes = processedBytes;
                                    _testThreadResults[(int)resultIndex][i].ProcessedRows = processedRows;
                                    _testThreadResults[(int)resultIndex][i].ProcessedTransactions = processedTransactions;
                                    _testThreadResults[(int)resultIndex][i].ResultType = Dynamics.DataEngineOperation.InsertData;
                                }
                            }
                            DateTime threadEnd = DateTime.Now;
                            if (resultIndex == null)
                            {
                                _threadResults[i].OperationStart = threadStart;
                                _threadResults[i].OperationEnd = threadEnd;
                            }
                            else
                            {
                                _testThreadResults[(int)resultIndex][i].OperationStart = threadStart;
                                _testThreadResults[(int)resultIndex][i].OperationEnd = threadEnd;
                            }
                            if (_pumpThreads[i].GetPumpState() != true)
                            {
                                _pumpThreads[i].SetPumpComplete();
                            }
                        }
                        catch (Exception ex)
                        {
                            _database.DebugQueue().AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        }
                    });
                }
                catch (Exception ex)
                {
                    _database.DebugQueue().AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    foreach (OilPump pump in _pumpThreads)
                    {
                        if (pump.GetPumpState() != true)
                        {
                            pump.SetPumpComplete();
                        }
                    }
                }
            });
            tasks.Add(databaseWriteTask);

            while (CheckForRunningPumps())
            {
                Thread.Sleep(5000);
            }
            if (resultIndex == null)
            {
                DateTime endOperation = DateTime.Now;
                SetFinalResults(startOperation, endOperation);
                foreach (DataEngineResults dr in _threadResults)
                {
                    _resultCollection.Add(dr);
                }
                SetResultsStateCompleted();
            }
            operationTokenSource.Cancel();
            try
            {
                bool alltasksRunning = true;
                while(alltasksRunning)
                {
                    int numberOfRunningTasks = tasks.Count;
                    foreach (Task t in tasks)
                    {
                        if(t.IsCompleted == true)
                        {
                            numberOfRunningTasks -= 1;
                        }
                    }
                    if(numberOfRunningTasks == 0)
                    {
                        alltasksRunning = false;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                
            }
            finally
            {
                operationTokenSource.Dispose();
            }
           // To clean up the resources being used all OilPump producers are set to null. 
            for (int threadinit = 0; threadinit < _numberOfThreads; threadinit++)
            {
                _pumpThreads[threadinit].Dispose();
            }
        }

        /// <summary>
        /// Insert data into objects for a specific database type using public wikipedia webpages as the data source.
        /// </summary>
        public override void ImportFromWeb(ulong RequestedBytes, CancellationTokenSource tokenSource, 
            int? resultIndex)
        {
            if (resultIndex != null)
            {
                _testThreadResults[(int)resultIndex] = new DataEngineResults[_numberOfThreads];
            }
            DateTime startOperation = DateTime.Now;
            int wikiRequests = 0;
            int failedLoad = 0;
            int pageID = 0;

            UInt64 requestedBytesPerThread = Convert.ToUInt64(Math.Round(Convert.ToDouble(RequestedBytes / (ulong)_numberOfThreads), 0));
            List<string> urlList = GetXMLResource("URL_List.xml", null);

            Parallel.For(0, _numberOfThreads, i =>
            {
                UInt64 processedRows = 0;
                UInt64 processedBytes = 0;
                UInt64 processedTransactions = 0;
                UInt64 requestedBytesThisThread = requestedBytesPerThread;
                int maxAllowedFailures = Convert.ToInt32(RequestedBytes * (ulong)50 / 1000000.0);
                if (resultIndex == null)
                {
                    _threadResults[i] = new DataEngineResults();
                }
                else
                {
                    _testThreadResults[(int)resultIndex][i] = new DataEngineResults();
                }
                DateTime threadStart = DateTime.Now;
                try
                {
                    _database.TokenSource = tokenSource;
                    while ((failedLoad < maxAllowedFailures) && (processedBytes < requestedBytesThisThread) && (!tokenSource.Token.IsCancellationRequested))
                    {
                        Random r = new Random();
                        wikiRequests = r.Next(1, 6);
                        string URL = null;

                        URL = urlList[wikiRequests];

                        bool pageValid = false;
                        while (pageValid == false)
                        {
                            Task<WebPage> page = GetFromWeb(URL, _database.DebugQueue());
                            Thread.Sleep(200);
                            if (string.IsNullOrWhiteSpace(page.Result.HTML))
                            {
                                failedLoad += 1;
                            }
                            else
                            {
                                WebPage configuredPage;
                                if (_lobSchema == true)
                                {
                                    configuredPage = PrepareEncodedWebPage(pageID, page.Result.URL, page.Result.HTML, page.Result.Headers);
                                }
                                else
                                {
                                    configuredPage = PrepareCharacterisedWebPage(pageID, page.Result.URL, page.Result.HTML, page.Result.Headers);
                                }
                                int rowsInserted = 0;
                                if (_lobSchema == false)
                                {
                                    rowsInserted = _database.InsertCharacterisedWebPage(configuredPage);
                                }
                                else
                                {
                                    rowsInserted = _database.InsertEncodedWebPage(configuredPage);
                                }
                                if (rowsInserted != 0)
                                {
                                    processedTransactions++;
                                    processedRows += (ulong)rowsInserted;
                                    processedBytes += Convert.ToUInt32(configuredPage.TotalLength);
                                }
                                else
                                {
                                    failedLoad += 1;
                                }
                            }
                            pageValid = true;
                        }
                        if (resultIndex == null)
                        {
                            _threadResults[i].ProcessedBytes = processedBytes;
                            _threadResults[i].ProcessedRows = processedRows;
                            _threadResults[i].ProcessedTransactions = processedTransactions;
                            _threadResults[i].FailedProcessedRows = Convert.ToUInt64(failedLoad);
                            _threadResults[i].ResultType = Dynamics.DataEngineOperation.InsertData;
                        }
                        else
                        {
                            _testThreadResults[(int)resultIndex][i].ProcessedBytes = processedBytes;
                            _testThreadResults[(int)resultIndex][i].ProcessedRows = processedRows;
                            _testThreadResults[(int)resultIndex][i].ProcessedTransactions = processedTransactions;
                            _testThreadResults[(int)resultIndex][i].ResultType = Dynamics.DataEngineOperation.InsertData;
                        }
                    }
                    DateTime threadEnd = DateTime.Now;
                    if (resultIndex == null)
                    {
                        _threadResults[i].OperationStart = threadStart;
                        _threadResults[i].OperationEnd = threadEnd;
                    }
                    else
                    {
                        _testThreadResults[(int)resultIndex][i].OperationStart = threadStart;
                        _testThreadResults[(int)resultIndex][i].OperationEnd = threadEnd;
                    }
                    foreach (DataEngineResults dr in _threadResults)
                    {
                        _resultCollection.Add(dr);
                    }
                }
                catch (ObjectDisposedException) { }
            });

            if (resultIndex == null)
            {
                SetResultsStateCompleted();
                DateTime endOperation = DateTime.Now;
                SetFinalResults(startOperation, endOperation);
            }
        }

        /// <summary>
        /// Queries object members for a specific database type using the left outer join SQL syntax. 
        /// Can be triggered independently or as apart of a test scenario .
        /// If not apart of a test scenario use  a null object for webPagesToProcess
        /// </summary>
        public override void QueryDataLeftOuterJoin(ulong RequestedBytes, CancellationTokenSource tokenSource,
            int? resultIndex, Queue<WebPage>[] webPagesToProcess)
        {
            _database.TokenSource = tokenSource;
            DateTime startOperation = DateTime.Now;
            // This checks if the results index is null. If it is null then this is a standard operation.
            // If it has an index then it is apart of the test scenarios and has the webPages numbers pre compiled. 
            if (resultIndex != null)
            {
                _testThreadResults[(int)resultIndex] = new DataEngineResults[_numberOfThreads];
            }
            else
            {
                Tuple<UInt64, Queue<WebPage>[]> compiledAndCheckedPages = CompileWebPagesToProcess(RequestedBytes, tokenSource.Token);
                try { if (tokenSource.Token.IsCancellationRequested) { return; } } catch (ObjectDisposedException) { return;  } 
                RequestedBytes = compiledAndCheckedPages.Item1;
                webPagesToProcess = compiledAndCheckedPages.Item2;
                for (int queueIndex = 0; queueIndex < webPagesToProcess.Length; queueIndex++)
                {
                    if (!webPagesToProcess[queueIndex].Any())
                    {
                        RequestedBytes = 0;
                        SetResultsStateCompleted();
                    }
                }
            }
            UInt64 requestedBytesPerThread = Convert.ToUInt64(Math.Round(Convert.ToDouble(RequestedBytes / (ulong)_numberOfThreads), 0));

            Parallel.For(0, _numberOfThreads, i =>
            {
                DateTime threadStart = DateTime.Now;
                UInt64 processedBytes = 0;
                UInt64 processedTransactions = 0;
                UInt64 failedProcessedTransactions = 0;
                UInt64 requestedBytesThisThread = requestedBytesPerThread;
                if (resultIndex == null)
                {
                    _threadResults[i] = new DataEngineResults();
                }
                else
                {
                    _testThreadResults[(int)resultIndex][i] = new DataEngineResults();
                }
                while (processedBytes < requestedBytesThisThread && !tokenSource.Token.IsCancellationRequested)
                {
                    if (webPagesToProcess[i].Count != 0)
                    {
                        WebPage pageToProcess = webPagesToProcess[i].Dequeue();
                        Tuple<UInt64, UInt64> returnedQueryResults = _database.SelectWebPageLeftOuterJoin(pageToProcess.PageID, requestedBytesPerThread);

                        if (returnedQueryResults.Item2 != 0)
                        {
                            processedBytes += returnedQueryResults.Item1;
                            processedTransactions += returnedQueryResults.Item2;
                        }
                        else
                        {
                            failedProcessedTransactions += 1;
                        }
                        if (resultIndex == null)
                        {
                            _threadResults[i].ProcessedBytes = processedBytes;
                            _threadResults[i].ProcessedTransactions = processedTransactions;
                        }
                        else
                        {
                            _testThreadResults[(int)resultIndex][i].ProcessedBytes = processedBytes;
                            _testThreadResults[(int)resultIndex][i].ProcessedTransactions = processedTransactions;
                        }
                    }
                    else
                    {
                        requestedBytesThisThread = 0;
                    }
                    if (resultIndex == null)
                    {
                        _threadResults[i] = new DataEngineResults(processedBytes, 0, failedProcessedTransactions, processedTransactions,
                        Dynamics.DataEngineOperation.QueryData);
                    }
                    else
                    {
                        _testThreadResults[(int)resultIndex][i].ProcessedBytes = processedBytes;
                        _testThreadResults[(int)resultIndex][i].FailedProcessedRows = failedProcessedTransactions;
                        _testThreadResults[(int)resultIndex][i].ProcessedTransactions = processedTransactions;
                        _testThreadResults[(int)resultIndex][i].ResultType = Dynamics.DataEngineOperation.QueryData;
                    }
                }
                DateTime threadEnd = DateTime.Now;
                if (resultIndex == null)
                {
                    _threadResults[i].OperationStart = threadStart;
                    _threadResults[i].OperationEnd = threadEnd;
                }
                else
                {
                    _testThreadResults[(int)resultIndex][i].OperationStart = threadStart;
                    _testThreadResults[(int)resultIndex][i].OperationEnd = threadEnd;
                }
            });
            if (resultIndex == null)
            {
                DateTime endOperation = DateTime.Now;
                SetFinalResults(startOperation, endOperation);

                foreach (DataEngineResults dr in _threadResults)
                {
                    _resultCollection.Add(dr);
                }
                SetResultsStateCompleted();
            }
        }

        /// <summary>
        /// Queries object members for a specific database type using the union all SQL syntax . 
        /// Can be triggered independently or as apart of a test scenario .
        /// If not apart of a test scenario use  a null object for webPagesToProcess
        /// </summary>
        public override void QueryDataUnionAll(ulong RequestedBytes, CancellationTokenSource tokenSource,
            int? resultIndex, Queue<WebPage>[] webPagesToProcess)
        {
            _database.TokenSource = tokenSource;
            DateTime startOperation = DateTime.Now;
            if (resultIndex != null)
            {
                _testThreadResults[(int)resultIndex] = new DataEngineResults[_numberOfThreads];
            }
            else
            {
                Tuple<UInt64, Queue<WebPage>[]> compiledAndCheckedPages = CompileWebPagesToProcess(RequestedBytes, tokenSource.Token);
                try { if (tokenSource.Token.IsCancellationRequested) { return; } } catch (ObjectDisposedException) { return; }
                RequestedBytes =  compiledAndCheckedPages.Item1;
                webPagesToProcess = compiledAndCheckedPages.Item2;
                for (int queueIndex = 0; queueIndex < webPagesToProcess.Length; queueIndex++)
                {
                    if (!webPagesToProcess[queueIndex].Any())
                    {
                        RequestedBytes = 0;
                        SetResultsStateCompleted();
                    }
                }
            }
            UInt64 requestedBytesPerThread = Convert.ToUInt64(Math.Round(Convert.ToDouble(RequestedBytes / (ulong)_numberOfThreads), 0));

            Parallel.For(0, _numberOfThreads, i =>
            {
                DateTime threadStart = DateTime.Now;
                UInt64 processedBytes = 0;
                UInt64 processedTransactions = 0;
                UInt64 failedProcessedTransactions = 0;
                UInt64 requestedBytesThisThread = requestedBytesPerThread;
                if (resultIndex == null)
                {
                    _threadResults[i] = new DataEngineResults();
                }
                else
                {
                    _testThreadResults[(int)resultIndex][i] = new DataEngineResults();
                }
                while (processedBytes < requestedBytesThisThread && !tokenSource.Token.IsCancellationRequested)
                {
                    if (webPagesToProcess[i].Count != 0)
                    {
                        WebPage pageToProcess = webPagesToProcess[i].Dequeue();
                        Tuple<UInt64, UInt64> returnedQueryResults = _database.SelectWebPageUnionAll(pageToProcess.PageID, requestedBytesThisThread);

                        if (returnedQueryResults.Item2 != 0)
                        {
                            processedBytes += returnedQueryResults.Item1;
                            processedTransactions += returnedQueryResults.Item2;
                        }
                        else
                        {
                            failedProcessedTransactions += 1;
                        }
                        if (resultIndex == null)
                        {
                            _threadResults[i].ProcessedBytes = processedBytes;
                            _threadResults[i].ProcessedTransactions = processedTransactions;
                        }
                        else
                        {
                            _testThreadResults[(int)resultIndex][i].ProcessedBytes = processedBytes;
                            _testThreadResults[(int)resultIndex][i].ProcessedTransactions = processedTransactions;
                        }
                    }
                    else
                    {
                        requestedBytesThisThread = 0;
                    }
                    if (resultIndex == null)
                    {
                        _threadResults[i] = new DataEngineResults(processedBytes, 0, failedProcessedTransactions, processedTransactions,
                        Dynamics.DataEngineOperation.QueryData);
                    }
                    else
                    {
                        _testThreadResults[(int)resultIndex][i].ProcessedBytes = processedBytes;
                        _testThreadResults[(int)resultIndex][i].FailedProcessedRows = failedProcessedTransactions;
                        _testThreadResults[(int)resultIndex][i].ProcessedTransactions = processedTransactions;
                        _testThreadResults[(int)resultIndex][i].ResultType = Dynamics.DataEngineOperation.QueryData;
                    }
                }
                DateTime threadEnd = DateTime.Now;
                if (resultIndex == null)
                {
                    _threadResults[i].OperationStart = threadStart;
                    _threadResults[i].OperationEnd = threadEnd;
                }
                else
                {
                    _testThreadResults[(int)resultIndex][i].OperationStart = threadStart;
                    _testThreadResults[(int)resultIndex][i].OperationEnd = threadEnd;
                }
            });
            if (resultIndex == null)
            {
                DateTime endOperation = DateTime.Now;
                SetFinalResults(startOperation, endOperation);

                foreach (DataEngineResults dr in _threadResults)
                {
                    _resultCollection.Add(dr);
                }
                SetResultsStateCompleted();
            }
        }

        /// <summary>
        /// Returns the final results after test completion. 
        /// </summary>
        public override DataEngineResultSet ReturnFinalResults()
        {
            return _finalResults;
        }

        /// <summary>
        /// Returns the interim reults while an operation is running. 
        /// </summary>
        public override DataEngineResults ReturnInterimResults()
        {
            UInt64 totalProcessedBytes = 0;
            UInt64 totalProcessedRows = 0;
            UInt64 totalProcessedTransactions = 0;
            UInt64 totalFailedProcessedRows = 0;
            Dynamics.DataEngineOperation operation_Type = Dynamics.DataEngineOperation.Unknown;

            foreach (DataEngineResults dr in _threadResults)
            {
                if (dr != null)
                {
                    operation_Type = dr.ResultType;
                    totalProcessedBytes += dr.ProcessedBytes;
                    totalProcessedRows += dr.ProcessedRows;
                    totalProcessedTransactions += dr.ProcessedTransactions;
                    totalFailedProcessedRows += dr.FailedProcessedRows;
                }
                else
                {
                    totalProcessedBytes += 0;
                    totalProcessedRows += 0;
                    totalProcessedTransactions += 0;
                    totalFailedProcessedRows += 0;
                }
            }
            _operationResults = new DataEngineResults(totalProcessedBytes, totalProcessedRows, totalFailedProcessedRows, totalProcessedTransactions, operation_Type);
            return _operationResults;
        }

        /// <summary>
        /// Returns the interim reults while a test operation is running. 
        /// </summary>
        public override DataEngineResults ReturnInterimTestResults(Dynamics.DataEngineOperation operation)
        {
            if (_testThreadResults != null)
            {
                for (int i = 0; i < _testThreadResults.Length; i++)
                {
                    if (_testThreadResults[i] == null)
                    {
                        return null;
                    }
                }
                DataEngineResultSet drs = new DataEngineResultSet(_testThreadResults);
                return drs.ReturnAggregateTestResultSet(operation);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Returns the interim reults on a per thread basis while a test operation is running. 
        /// </summary>
        public override DataEngineResultSet ReturnInterimTestThreadResults()
        {
            DataEngineResultSet drs = null;
            drs = new DataEngineResultSet(_numberOfThreads, _testThreadResults.Length);
            for (int i = 0; i < _testThreadResults.Length; i++)
            {
                if (_testThreadResults[i] != null)
                {
                    for (int i2 = 0; i2 < _testThreadResults[i].Length; i2++)
                    {
                        UInt64 processedBytes = 0;
                        UInt64 processedRows = 0;
                        UInt64 processedTransactions = 0;
                        UInt64 failedProcessedRows = 0;
                        if (_testThreadResults[i][i2] != null)
                        {
                            processedBytes = _testThreadResults[i][i2].ProcessedBytes;
                            processedRows = _testThreadResults[i][i2].ProcessedRows;
                            processedTransactions = _testThreadResults[i][i2].ProcessedTransactions;
                            failedProcessedRows = _testThreadResults[i][i2].FailedProcessedRows;
                            drs.ResultCompilation[i][i2] = new DataEngineResults(processedBytes, processedRows, failedProcessedRows, processedTransactions, _testThreadResults[i][i2].ResultType);
                        }
                        else
                        {
                            drs.ResultCompilation[i][i2] = _testThreadResults[i][i2];
                        }
                    }
                }
            }
            return drs;
        }

        /// <summary>
        /// Returns the interim reults on a per thread basis while an operation is running. 
        /// </summary>
        public override DataEngineResultSet ReturnInterimThreadResults()
        {
            DataEngineResultSet drs = new DataEngineResultSet(_numberOfThreads);
            for (int i = 0; i < _threadResults.Length; i++)
            {
                UInt64 processedBytes = 0;
                UInt64 processedRows = 0;
                UInt64 processedTransactions = 0;
                UInt64 failedProcessedRows = 0;
                if (_threadResults[i] != null)
                {
                    processedBytes = _threadResults[i].ProcessedBytes;
                    processedRows = _threadResults[i].ProcessedRows;
                    processedTransactions = _threadResults[i].ProcessedTransactions;
                    failedProcessedRows = _threadResults[i].FailedProcessedRows;
                    drs.ResultSet[i] = new DataEngineResults(processedBytes, processedRows, failedProcessedRows, processedTransactions, _threadResults[i].ResultType);
                }
                else
                {
                    drs.ResultSet[i] = _threadResults[i];
                }
            }
            return drs;
        }

        /// <summary>
        /// At the completion of an operation or test the results state can be set to completed. 
        /// </summary>
        public override void SetResultsStateCompleted()
        {
            _resultCollection.CompleteAdding();
        }

        /// <summary>
        /// Runs a test of the Advanced Type. 
        /// Perfoms update (modify existing webpage), delete, insert and select operations on a selected group of web page objects. 
        /// </summary>
        public override void TestAdvanced(string Folder, ulong RequestedBytes, CancellationTokenSource tokenSource, double growthRate)
        {
            _database.TokenSource = tokenSource;
            //0 is update 
            //1 is delete
            //2 is insert
            //3 is select 
            _testThreadResults = new DataEngineResults[4][];
            Tuple<Random, UInt64> randomValueCheck = _database.InitializeRandom(false, RequestedBytes);
            try { if (tokenSource.Token.IsCancellationRequested) { return; } } catch (ObjectDisposedException) { return; }
            SortedList<int, long> colPageIDToLength = _database.GetPageIDList();
            Queue<WebPage>[] webPagesToDelete = new Queue<WebPage>[_numberOfThreads];
            Queue<WebPage>[] webPagesToUpdate = new Queue<WebPage>[_numberOfThreads];
            Queue<WebPage>[] webPagesToQuery = new Queue<WebPage>[_numberOfThreads];

            int numPageID = colPageIDToLength.Count;

            for (int i = 0; i < _numberOfThreads; i++)
            {
                webPagesToDelete[i] = new Queue<WebPage>();
                webPagesToUpdate[i] = new Queue<WebPage>();
                webPagesToQuery[i] = new Queue<WebPage>();
            }
            //Create a list of existing pages to process. 
            UInt64 bytesToProcess = 0;
            RequestedBytes = randomValueCheck.Item2;
            UInt64 RequestedBytesPerOperation = Convert.ToUInt64(Math.Round((double)(RequestedBytes / 4), 0));

            int arrayIndexCheckUpdate = 0;
            int arrayIndexCheckDelete = 0;
            int arrayIndexCheckQuery = 0;


            UInt64 updateUpperRange = RequestedBytesPerOperation;
            UInt64 deleteUpperRange = RequestedBytesPerOperation * 2;
            UInt64 queryUpperRange = RequestedBytesPerOperation * 3;

            while (bytesToProcess < queryUpperRange && !tokenSource.Token.IsCancellationRequested)
            {
                int indexOfPageID = randomValueCheck.Item1.Next(numPageID);
                int probability = (Convert.ToInt32((double)indexOfPageID / (double)numPageID) * 100);
                if (probability > randomValueCheck.Item1.Next(100))
                {
                    int pageID = colPageIDToLength.Keys[indexOfPageID];
                    bool pageidExists = false;
                    for (int iqcheck = 0; iqcheck < _numberOfThreads; iqcheck++)
                    {
                        var containsPageIdDelete = webPagesToDelete[iqcheck].Any(o => o.PageID == pageID);
                        var containsPageIdUpdate = webPagesToUpdate[iqcheck].Any(o => o.PageID == pageID);
                        var containsPageIdQuery = webPagesToQuery[iqcheck].Any(o => o.PageID == pageID);
                        if (containsPageIdDelete == true)
                        {
                            pageidExists = true;
                            break;
                        }
                        if (containsPageIdUpdate == true)
                        {
                            pageidExists = true;
                            break;
                        }
                        if (containsPageIdQuery == true)
                        {
                            pageidExists = true;
                            break;
                        }
                    }
                    if (!pageidExists)
                    {
                        Int64 totalLength = colPageIDToLength[pageID];
                        ComputedLengths pageLegnth = new ComputedLengths(Convert.ToUInt64(((double)colPageIDToLength[pageID])));
                        WebPage webPage = new WebPage(pageID, pageLegnth);
                        if (bytesToProcess < updateUpperRange)
                        {
                            webPagesToUpdate[arrayIndexCheckUpdate].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckUpdate >= (webPagesToUpdate.Length - 1)))
                            {
                                arrayIndexCheckUpdate++;
                            }
                            else
                            {
                                arrayIndexCheckUpdate = 0;
                            }
                        }
                        else if (bytesToProcess < deleteUpperRange && bytesToProcess > updateUpperRange)
                        {
                            webPagesToDelete[arrayIndexCheckDelete].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckDelete >= (webPagesToDelete.Length - 1)))
                            {
                                arrayIndexCheckDelete++;
                            }
                            else
                            {
                                arrayIndexCheckDelete = 0;
                            }
                        }
                        else
                        {
                            webPagesToQuery[arrayIndexCheckQuery].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckQuery >= (webPagesToQuery.Length - 1)))
                            {
                                arrayIndexCheckQuery++;
                            }
                            else
                            {
                                arrayIndexCheckQuery = 0;
                            }
                        }
                    }
                }
            }

            if (numPageID == 0)
            {
                RequestedBytes = 0;
                SetResultsStateCompleted();
            }

            double adjustedGrowthRate = Math.Round(growthRate / 100, 2);

            DateTime startOperation = DateTime.Now;
            //Run Update tasks 
            Task upsertTask = Task.Run(() =>
            {
                Update(RequestedBytesPerOperation, tokenSource, false, null, 0, webPagesToUpdate);
            }, tokenSource.Token);

            ////Run Delete tasks
            Task deleteTask = Task.Run(() =>
            {
                Delete(RequestedBytesPerOperation, tokenSource, 1, webPagesToDelete);
            }, tokenSource.Token);

            //Run Add/Import Task 
            Task insertTask = Task.Run(() =>
            {
                UInt64 insertBytes = Convert.ToUInt64(RequestedBytesPerOperation + (Math.Round((double)RequestedBytesPerOperation * adjustedGrowthRate, 0)));
                if (Folder == null)
                {
                    ImportFromWeb(insertBytes, tokenSource, 2);
                }
                else
                {
                    ImportFromFile(Folder, insertBytes, tokenSource, 2);
                }
            }, tokenSource.Token);

            Task selectTask = Task.Run(() =>
            {
                QueryDataUnionAll(RequestedBytesPerOperation, tokenSource , 3, webPagesToQuery);
            }, tokenSource.Token);

            bool allOperationsRunning = true;
            while (allOperationsRunning)
            {
                if (upsertTask.Status == TaskStatus.RanToCompletion && deleteTask.Status == TaskStatus.RanToCompletion &&
                    insertTask.Status == TaskStatus.RanToCompletion && selectTask.Status == TaskStatus.RanToCompletion)
                {
                    allOperationsRunning = false;
                }
                else if (upsertTask.Status != TaskStatus.RanToCompletion && deleteTask.Status != TaskStatus.RanToCompletion &&
                    insertTask.Status != TaskStatus.RanToCompletion && selectTask.Status != TaskStatus.RanToCompletion)
                {
                    Thread.Sleep(5000);
                }
            }
            DateTime endOperation = DateTime.Now;
            _database.VendorAdvancedOperations(_numberOfThreads);
            SetFinalTestResults(startOperation, endOperation, Dynamics.DataEngineOperation.TestAdvanced);

            for (int operationArray = 0; operationArray < _testThreadResults.Length; operationArray++)
            {
                for (int valueOperations = 0; valueOperations < _testThreadResults[operationArray].Length; valueOperations++)
                {
                    _resultCollection.Add(_testThreadResults[operationArray][valueOperations]);
                }
            }
            SetResultsStateCompleted();
        }

        /// <summary>
        /// Runs a test of the Complex Type. 
        /// Perfoms update (modify existing webpage), delete, select and update(replace existing webpages with new data) operations on a selected group of web page objects. 
        /// At completion data is re-inserted into the database to maintain object size. 
        /// </summary>
        public override void TestComplex(string Folder, ulong RequestedBytes, CancellationTokenSource tokenSource, double growthRate)
        {
            _database.TokenSource = tokenSource;
            //0 is update standard
            //1 is delete
            //2 is select 
            //3 is update advanced
            //4 is insert data after operations are complete

            _testThreadResults = new DataEngineResults[4][];
            Tuple<Random, UInt64> randomValueCheck = _database.InitializeRandom(false, RequestedBytes);
            try { if (tokenSource.Token.IsCancellationRequested) { return; } } catch (ObjectDisposedException) { return; }
            SortedList<int, long> colPageIDToLength = _database.GetPageIDList();
            Queue<WebPage>[] webPagesToDelete = new Queue<WebPage>[_numberOfThreads];
            Queue<WebPage>[] webPagesToUpdateStandard = new Queue<WebPage>[_numberOfThreads];
            Queue<WebPage>[] webPagesToUpdateAdvanced = new Queue<WebPage>[_numberOfThreads];
            Queue<WebPage>[] webPagesToQuery = new Queue<WebPage>[_numberOfThreads];

            int numPageID = colPageIDToLength.Count;

            for (int i = 0; i < _numberOfThreads; i++)
            {
                webPagesToDelete[i] = new Queue<WebPage>();
                webPagesToUpdateStandard[i] = new Queue<WebPage>();
                webPagesToUpdateAdvanced[i] = new Queue<WebPage>();
                webPagesToQuery[i] = new Queue<WebPage>();
            }

            //Create a list of existing pages to process. 
            UInt64 bytesToProcess = 0;
            RequestedBytes = randomValueCheck.Item2;
            UInt64 RequestedBytesPerOperation = Convert.ToUInt64(Math.Round((double)(RequestedBytes / 5), 0));

            int arrayIndexCheckUpdateStandard = 0;
            int arrayIndexCheckUpdateAdvanced = 0;
            int arrayIndexCheckDelete = 0;
            int arrayIndexCheckQuery = 0;


            UInt64 updateStandardUpperRange = RequestedBytesPerOperation;
            UInt64 updateAdvancedUpperRange = RequestedBytesPerOperation * 2;
            UInt64 deleteUpperRange = RequestedBytesPerOperation * 3;
            UInt64 queryUpperRange = RequestedBytesPerOperation * 4;

            while (bytesToProcess < queryUpperRange && !tokenSource.Token.IsCancellationRequested)
            {
                int indexOfPageID = randomValueCheck.Item1.Next(numPageID);
                int probability = (Convert.ToInt32((double)indexOfPageID / (double)numPageID) * 100);
                if (probability > randomValueCheck.Item1.Next(100))
                {
                    int pageID = colPageIDToLength.Keys[indexOfPageID];
                    bool pageidExists = false;
                    for (int iqcheck = 0; iqcheck < _numberOfThreads; iqcheck++)
                    {
                        var containsPageIdDelete = webPagesToDelete[iqcheck].Any(o => o.PageID == pageID);
                        var containsPageIdUpdate = webPagesToUpdateStandard[iqcheck].Any(o => o.PageID == pageID);
                        var containsPageIdUpdateReplace = webPagesToUpdateAdvanced[iqcheck].Any(o => o.PageID == pageID);
                        var containsPageIdQuery = webPagesToQuery[iqcheck].Any(o => o.PageID == pageID);
                        if (containsPageIdDelete == true)
                        {
                            pageidExists = true;
                            break;
                        }
                        if (containsPageIdUpdate == true)
                        {
                            pageidExists = true;
                            break;
                        }
                        if (containsPageIdUpdateReplace == true)
                        {
                            pageidExists = true;
                            break;
                        }
                        if (containsPageIdQuery == true)
                        {
                            pageidExists = true;
                            break;
                        }
                    }
                    if (!pageidExists)
                    {
                        Int64 totalLength = colPageIDToLength[pageID];
                        ComputedLengths pageLegnth = new ComputedLengths(Convert.ToUInt64(((double)colPageIDToLength[pageID])));
                        WebPage webPage = new WebPage(pageID, pageLegnth);
                        if (bytesToProcess < updateStandardUpperRange)
                        {
                            webPagesToUpdateStandard[arrayIndexCheckUpdateStandard].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckUpdateStandard >= (webPagesToUpdateStandard.Length - 1)))
                            {
                                arrayIndexCheckUpdateStandard++;
                            }
                            else
                            {
                                arrayIndexCheckUpdateStandard = 0;
                            }
                        }
                        else if (bytesToProcess < updateAdvancedUpperRange && bytesToProcess > updateStandardUpperRange)
                        {
                            webPagesToUpdateAdvanced[arrayIndexCheckUpdateAdvanced].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckUpdateAdvanced >= (webPagesToUpdateAdvanced.Length - 1)))
                            {
                                arrayIndexCheckUpdateAdvanced++;
                            }
                            else
                            {
                                arrayIndexCheckUpdateAdvanced = 0;
                            }
                        }
                        else if (bytesToProcess < deleteUpperRange && bytesToProcess > updateAdvancedUpperRange)
                        {
                            webPagesToDelete[arrayIndexCheckDelete].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckDelete >= (webPagesToDelete.Length - 1)))
                            {
                                arrayIndexCheckDelete++;
                            }
                            else
                            {
                                arrayIndexCheckDelete = 0;
                            }
                        }
                        else
                        {
                            webPagesToQuery[arrayIndexCheckQuery].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckQuery >= (webPagesToQuery.Length - 1)))
                            {
                                arrayIndexCheckQuery++;
                            }
                            else
                            {
                                arrayIndexCheckQuery = 0;
                            }
                        }
                    }
                }
            }

            if (numPageID == 0)
            {
                RequestedBytes = 0;
                SetResultsStateCompleted();
            }

            double adjustedGrowthRate = Math.Round(growthRate / 100, 2);
            DateTime startOperation = DateTime.Now;
            //Run standard Update tasks 
            //Run Update tasks 
            Task updateTask = Task.Run(() =>
            {
                Update(RequestedBytesPerOperation, tokenSource, false, null, 0, webPagesToUpdateStandard);
            }, tokenSource.Token);

            ////Run Delete tasks
            Task deleteTask = Task.Run(() =>
            {
                Delete(RequestedBytesPerOperation, tokenSource, 1, webPagesToDelete);
            }, tokenSource.Token);

            Task selectTask = Task.Run(() =>
            {
                QueryDataLeftOuterJoin(RequestedBytesPerOperation, tokenSource, 2, webPagesToQuery);
            }, tokenSource.Token);

            //Run Advanced Upsert task
            Task upsertTask = Task.Run(() =>
            {
                Update(RequestedBytesPerOperation, tokenSource, true, Folder, 3, webPagesToUpdateAdvanced);
            }, tokenSource.Token);

            var tokenSourceWriteLine = new CancellationTokenSource();

            Task writeTask = Task.Run(() =>
            {
                WriteInLine(tokenSourceWriteLine);
            }, tokenSource.Token);

            bool allOperationsRunning = true;
            while (allOperationsRunning)
            {
                if (updateTask.Status == TaskStatus.RanToCompletion && deleteTask.Status == TaskStatus.RanToCompletion &&
                    selectTask.Status == TaskStatus.RanToCompletion && upsertTask.Status == TaskStatus.RanToCompletion)
                {

                    // First we extend the array by overwriting its child objects 
                    DataEngineResults[][] _tempTestThreadResults = new DataEngineResults[5][];
                    _tempTestThreadResults[0] = _testThreadResults[0];
                    _tempTestThreadResults[1] = _testThreadResults[1];
                    _tempTestThreadResults[2] = _testThreadResults[2];
                    _tempTestThreadResults[3] = _testThreadResults[3];
                    _testThreadResults = new DataEngineResults[5][];
                    _testThreadResults = _tempTestThreadResults;
                    //Run Add/Import Task 
                    Task insertTask = Task.Run(() =>
                    {
                        UInt64 insertBytes = Convert.ToUInt64(RequestedBytesPerOperation + (Math.Round((double)RequestedBytesPerOperation * adjustedGrowthRate, 0)));
                        if (Folder == null)
                        {
                            ImportFromWeb(insertBytes, tokenSource, 4);
                        }
                        else
                        {
                            ImportFromFile(Folder, insertBytes, tokenSource, 4);
                        }
                    }, tokenSource.Token);

                    while (insertTask.Status != TaskStatus.RanToCompletion)
                    {
                        Thread.Sleep(5000);
                        if (insertTask.Status == TaskStatus.WaitingToRun)
                        {
                            tokenSource.Cancel();
                            break;
                        }
                    }
                    tokenSourceWriteLine.Cancel();
                    allOperationsRunning = false;
                }
                else if (updateTask.Status != TaskStatus.RanToCompletion && deleteTask.Status != TaskStatus.RanToCompletion &&
                    selectTask.Status != TaskStatus.RanToCompletion && upsertTask.Status != TaskStatus.RanToCompletion)
                {
                    Thread.Sleep(5000);
                }
            }

            DateTime endOperation = DateTime.Now;
            _database.VendorComplexOperations();
            _database.VendorAdvancedOperations(_numberOfThreads);
            _database.VendorConsistencyCheck();
            SetFinalTestResults(startOperation, endOperation, Dynamics.DataEngineOperation.TestComplex);

            for (int operationArray = 0; operationArray < _testThreadResults.Length; operationArray++)
            {
                for (int valueOperations = 0; valueOperations < _testThreadResults[operationArray].Length; valueOperations++)
                {
                    _resultCollection.Add(_testThreadResults[operationArray][valueOperations]);
                }
            }
            SetResultsStateCompleted();
        }

        /// <summary>
        /// Runs a test of the Simple Type. 
        /// Perfoms update (modify existing webpage), delete, and insert operations on a selected group of web page objects. 
        /// </summary>
        public override void TestSimple(string Folder, ulong RequestedBytes, CancellationTokenSource tokenSource, double growthRate)
        {
            _database.TokenSource = tokenSource;
            //0 is update 
            //1 is delete
            //2 is insert
            _testThreadResults = new DataEngineResults[3][];
            Tuple<Random, UInt64> randomValueCheck = _database.InitializeRandom(false, RequestedBytes);
            try { if (tokenSource.Token.IsCancellationRequested) { return; } } catch (ObjectDisposedException) { return; }
            SortedList<int, long> colPageIDToLength = _database.GetPageIDList();
            Queue<WebPage>[] WebPageToDelete = new Queue<WebPage>[_numberOfThreads];
            Queue<WebPage>[] WebPageToUpdate = new Queue<WebPage>[_numberOfThreads];

            int numPageID = colPageIDToLength.Count;

            for (int i = 0; i < _numberOfThreads; i++)
            {
                WebPageToDelete[i] = new Queue<WebPage>();
                WebPageToUpdate[i] = new Queue<WebPage>();
            }
            //Create a list of existing pages to process. 
            UInt64 bytesToProcess = 0;
            RequestedBytes = randomValueCheck.Item2;
            //Divide the amount to work on by the numnber of overall operations
            UInt64 RequestedBytesPerOperation = Convert.ToUInt64(Math.Round((double)(RequestedBytes / 3), 0));

            int arrayIndexCheckUpdate = 0;
            int arrayIndexCheckDelete = 0;


            UInt64 updateUpperRange = RequestedBytesPerOperation;
            UInt64 deleteUpperRange = RequestedBytesPerOperation * 2;

            while (bytesToProcess < deleteUpperRange && !tokenSource.Token.IsCancellationRequested)
            {
                int indexOfPageID = randomValueCheck.Item1.Next(numPageID);
                int probability = (Convert.ToInt32((double)indexOfPageID / (double)numPageID) * 100);
                if (probability > randomValueCheck.Item1.Next(100))
                {
                    int pageID = colPageIDToLength.Keys[indexOfPageID];
                    bool pageidExists = false;
                    for (int iqcheck = 0; iqcheck < _numberOfThreads; iqcheck++)
                    {
                        var containsPageIdDelete = WebPageToDelete[iqcheck].Any(o => o.PageID == pageID);
                        var containsPageIdUpdate = WebPageToUpdate[iqcheck].Any(o => o.PageID == pageID);
                        if (containsPageIdDelete == true)
                        {
                            pageidExists = true;
                            break;
                        }
                        if (containsPageIdUpdate == true)
                        {
                            pageidExists = true;
                            break;
                        }
                    }
                    if (!pageidExists)
                    {
                        Int64 totalLength = colPageIDToLength[pageID];
                        ComputedLengths pageLegnth = new ComputedLengths(Convert.ToUInt64(((double)colPageIDToLength[pageID])));
                        WebPage webPage = new WebPage(pageID, pageLegnth);
                        if (bytesToProcess < updateUpperRange)
                        {
                            WebPageToUpdate[arrayIndexCheckUpdate].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckUpdate >= (WebPageToUpdate.Length - 1)))
                            {
                                arrayIndexCheckUpdate++;
                            }
                            else
                            {
                                arrayIndexCheckUpdate = 0;
                            }
                        }
                        else if (bytesToProcess < deleteUpperRange && bytesToProcess > updateUpperRange)
                        {
                            WebPageToDelete[arrayIndexCheckDelete].Enqueue(webPage);
                            bytesToProcess += webPage.TotalLength;
                            if (!(arrayIndexCheckDelete >= (WebPageToDelete.Length - 1)))
                            {
                                arrayIndexCheckDelete++;
                            }
                            else
                            {
                                arrayIndexCheckDelete = 0;
                            }
                        }
                    }
                }
            }

            if (numPageID == 0)
            {
                RequestedBytes = 0;
                SetResultsStateCompleted();
            }

            double adjustedGrowthRate = Math.Round(growthRate / 100, 2);

            DateTime startOperation = DateTime.Now;
            //Run Update tasks 
            Task updateTask = Task.Run(() =>
            {
                Update(RequestedBytesPerOperation, tokenSource, false, null, 0, WebPageToUpdate);
            }, tokenSource.Token);

            //Run Delete tasks
            Task deleteTask = Task.Run(() =>
            {
                Delete(RequestedBytesPerOperation, tokenSource, 1, WebPageToDelete);
            }, tokenSource.Token);

            //Run Add/Import Task 
            Task insertTask = Task.Run(() =>
            {
                UInt64 insertBytes = Convert.ToUInt64(RequestedBytesPerOperation + (Math.Round((double)RequestedBytesPerOperation * adjustedGrowthRate, 0)));
                if (Folder == null)
                {
                    ImportFromWeb(insertBytes, tokenSource, 2);
                }
                else
                {
                    ImportFromFile(Folder, insertBytes, tokenSource, 2);
                }
            }, tokenSource.Token);

            bool allOperationsRunning = true;
            while (allOperationsRunning)
            {
                if (updateTask.Status == TaskStatus.RanToCompletion && deleteTask.Status == TaskStatus.RanToCompletion &&
                    insertTask.Status == TaskStatus.RanToCompletion)
                {
                    allOperationsRunning = false;
                }
                else if (updateTask.Status != TaskStatus.RanToCompletion && deleteTask.Status != TaskStatus.RanToCompletion &&
                    insertTask.Status != TaskStatus.RanToCompletion)
                {
                    Thread.Sleep(5000);
                }
            }
            DateTime endOperation = DateTime.Now;
            SetFinalTestResults(startOperation, endOperation, Dynamics.DataEngineOperation.TestSimple);

            for (int operationArray = 0; operationArray < _testThreadResults.Length; operationArray++)
            {
                for (int valueOperations = 0; valueOperations < _testThreadResults[operationArray].Length; valueOperations++)
                {
                    _resultCollection.Add(_testThreadResults[operationArray][valueOperations]);
                }
            }
            SetResultsStateCompleted();
        }

        /// <summary>
        /// Runs a test of the Simple Type. 
        /// Perfoms update (modify existing webpage), delete, and insert operations on a selected group of web page objects. 
        /// </summary>
        public override void Update(ulong RequestedBytes, CancellationTokenSource tokenSource,
            bool replaceWebPage, string folder, int? resultIndex, Queue<WebPage>[] webPagesToProcess)
        {
            _database.TokenSource = tokenSource;
            // Create a token to read into both threads. 
            // This token will be cancelled if the parent token is triggered. 
            var operationTokenSource = new CancellationTokenSource();
            var operationToken = operationTokenSource.Token;
            Task fileReadTask;
            var tasks = new ConcurrentBag<Task>();
            _database.CheckSchemaType();
            if (_database.GetSchemaType() == Dynamics.DatabaseSchema.MemoryOptimisedLOB ||
                    _database.GetSchemaType() == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB ||
                    _database.GetSchemaType() == Dynamics.DatabaseSchema.WithIndexesLOB ||
                    _database.GetSchemaType() == Dynamics.DatabaseSchema.WithoutIndexesLOB)
            {
                _lobSchema = true;
            }
            else
            {
                _lobSchema = false;
            }
            if (resultIndex != null)
            {
                _testThreadResults[(int)resultIndex] = new DataEngineResults[_numberOfThreads];
            }
            else
            {
                Tuple<UInt64, Queue<WebPage>[]> compiledAndCheckedPages = CompileWebPagesToProcess(RequestedBytes, tokenSource.Token);
                try { if (tokenSource.Token.IsCancellationRequested) { return; } } catch (ObjectDisposedException) { return; }
                RequestedBytes = compiledAndCheckedPages.Item1;
                webPagesToProcess = compiledAndCheckedPages.Item2;
                for (int queueIndex = 0; queueIndex < webPagesToProcess.Length; queueIndex++)
                {
                    if (!webPagesToProcess[queueIndex].Any())
                    {
                        RequestedBytes = 0;
                        SetResultsStateCompleted();
                    }
                }
            }

            if (replaceWebPage)
            {
                _pumpThreads = new OilPump[_numberOfThreads];
                for (int threadinit = 0; threadinit < _numberOfThreads; threadinit++)
                {
                    _pumpThreads[threadinit] = new OilPump(_database, operationToken);
                }
                Queue<ImportHandler> validImportFiles = new Queue<ImportHandler>();
                var importFiles = Directory.EnumerateFiles(folder, _database.GetMechanic().DataFileRootName + "*").OrderBy(filename => filename);
                var fileCount = (from file in importFiles select file).Count();

                if ((int)fileCount > _numberOfThreads)
                {
                    BlockingCollection<string> AllDataFiles = new BlockingCollection<string>(new ConcurrentQueue<string>());
                    string[] DataFileToProcess = new string[_numberOfThreads];

                    foreach (string file in importFiles)
                    {
                        AllDataFiles.Add(file);
                    }
                    //This will read from a number of data files into the respective queues
                    fileReadTask = Task.Run(() =>
                    {
                        while (!tokenSource.Token.IsCancellationRequested && !GetResultsState() && !operationToken.IsCancellationRequested)
                        {
                            for (int fileQueueNum = 0; fileQueueNum < DataFileToProcess.Length; fileQueueNum++)
                            {
                                if (DataFileToProcess[fileQueueNum] == null)
                                {
                                    DataFileToProcess[fileQueueNum] = AllDataFiles.Take();
                                }
                            }
                            Parallel.For(0, _numberOfThreads, i =>
                            {
                                try
                                {
                                    bool threadContininuation = true;
                                    while (threadContininuation && !operationToken.IsCancellationRequested)
                                    {
                                        if (DataFileToProcess[i] != null)
                                        {
                                            if (_pumpThreads[i].GetPumpState() == false)
                                            {
                                                ImportHandler import = _database.CheckFileImportHistory(DataFileToProcess[i]);
                                                if (import.Found == false)
                                                {
                                                    bool insertResult = _database.CreateImportHistory(import);
                                                    import = _pumpThreads[i].ProcessImport(import);
                                                    if (import.Filename == null || import.AllImportedWebPages == true)
                                                    {
                                                        DataFileToProcess[i] = null;
                                                    }
                                                    else
                                                    {
                                                        threadContininuation = false;
                                                    }
                                                }
                                                else if (import.Found == true && import.AllImportedWebPages == false)
                                                {
                                                    import = _pumpThreads[i].ProcessImport(import);
                                                    if (import.Filename == null || import.AllImportedWebPages == true)
                                                    {
                                                        DataFileToProcess[i] = null;
                                                    }
                                                    else
                                                    {
                                                        threadContininuation = false;
                                                    }
                                                }
                                                else
                                                {
                                                    DataFileToProcess[i] = null;
                                                }
                                            }
                                            else
                                            {
                                                threadContininuation = false;
                                            }
                                        }
                                        else
                                        {
                                            DataFileToProcess[i] = AllDataFiles.Take();
                                            _database.DebugQueue().AddMessage(new Message(DateTime.Now,
                                                "Thread " + i + " is reading from " + DataFileToProcess[i], Message.MessageType.Info));
                                        }
                                        if (operationToken.IsCancellationRequested)
                                        {
                                            threadContininuation = false;
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _database.DebugQueue().AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                                }
                            });
                        }
                    }, tokenSource.Token);
                    tasks.Add(fileReadTask);
                }
                else
                {
                    _database.DebugQueue().AddMessage(new Message(DateTime.Now,
                       "More files are needed to run the operation. Increase the number of data files or reduce the number of threads",
                       Message.MessageType.Error));
                    SetResultsStateCompleted();
                }

                //then we wait for the pumps to each have something in them 
                int activePumps = 0;
                while (activePumps < _pumpThreads.Length)
                {
                    foreach (OilPump e in _pumpThreads)
                    {
                        if (e.GetPumpCapcity() != 0)
                        {
                            activePumps++;
                        }
                    }
                }
            }

            UInt64 requestedBytesPerThread = Convert.ToUInt64(Math.Round(Convert.ToDouble(RequestedBytes / (ulong)_numberOfThreads), 0));
            DateTime startOperation = DateTime.Now;

            ParallelOptions po = new ParallelOptions
            {
                CancellationToken = tokenSource.Token
            };
            try
            {
                Parallel.For(0, _numberOfThreads, po, i =>
                {
                    try
                    {
                        DateTime threadStart = DateTime.Now;
                        UInt64 processedRows = 0;
                        UInt64 processedBytes = 0;
                        UInt64 processedTransactions = 0;
                        UInt64 failedProcessedTransactions = 0;
                        UInt64 requestedBytesThisThread = requestedBytesPerThread;
                        if (resultIndex == null)
                        {
                            _threadResults[i] = new DataEngineResults();
                        }
                        else
                        {
                            _testThreadResults[(int)resultIndex][i] = new DataEngineResults();
                        }
                        while (processedBytes < requestedBytesThisThread && !tokenSource.Token.IsCancellationRequested)
                        {
                            po.CancellationToken.ThrowIfCancellationRequested();
                            if (webPagesToProcess[i].Count != 0)
                            {
                                WebPage pageToProcess = webPagesToProcess[i].Dequeue();
                                int threadPagesUpdated = 0;

                                if (replaceWebPage)
                                {
                                    WebPage page = _pumpThreads[i].DecreaseOilReserve();
                                    if (page == null)
                                    {
                                        break;
                                    }
                                    WebPage configuredPage;
                                    if (_lobSchema)
                                    {
                                        configuredPage = PrepareEncodedWebPage(pageToProcess.PageID, page.URL, page.HTML, page.Headers);
                                        threadPagesUpdated = _database.UpdateEncodedWebPageInPlace(pageToProcess.PageID, configuredPage);
                                    }
                                    else
                                    {
                                        configuredPage = PrepareCharacterisedWebPage(pageToProcess.PageID, page.URL, page.HTML, page.Headers);
                                        threadPagesUpdated = _database.UpdateCharacterisedWebPageInPlace(pageToProcess.PageID, configuredPage);
                                    }
                                }
                                else
                                {
                                    if (_lobSchema)
                                    {
                                        threadPagesUpdated = _database.UpdateEncodedWebPage(pageToProcess.PageID);
                                    }
                                    else
                                    {
                                        threadPagesUpdated = _database.UpdateCharacterisedWebPage(pageToProcess.PageID);
                                    }
                                }


                                if (threadPagesUpdated != 0)
                                {
                                    processedBytes += pageToProcess.TotalLength;
                                    processedRows += (ulong)threadPagesUpdated;
                                    processedTransactions += 1;
                                }
                                else
                                {
                                    failedProcessedTransactions += 1;
                                }
                            }
                            else
                            {
                                requestedBytesThisThread = 0;
                            }
                            if (resultIndex == null)
                            {
                                _threadResults[i].ProcessedBytes = processedBytes;
                                _threadResults[i].ProcessedRows = processedRows;
                                _threadResults[i].ProcessedTransactions = processedTransactions;
                                _threadResults[i].FailedProcessedRows = Convert.ToUInt64(failedProcessedTransactions);
                                _threadResults[i].ResultType = Dynamics.DataEngineOperation.UpdateData;
                            }
                            else
                            {
                                _testThreadResults[(int)resultIndex][i].ProcessedBytes = processedBytes;
                                _testThreadResults[(int)resultIndex][i].ProcessedRows = processedRows;
                                _testThreadResults[(int)resultIndex][i].FailedProcessedRows = failedProcessedTransactions;
                                _testThreadResults[(int)resultIndex][i].ProcessedTransactions = processedTransactions;
                                _testThreadResults[(int)resultIndex][i].ResultType = Dynamics.DataEngineOperation.UpdateData;
                            }
                        }
                        DateTime threadEnd = DateTime.Now;
                        if (resultIndex == null)
                        {
                            _threadResults[i].OperationStart = threadStart;
                            _threadResults[i].OperationEnd = threadEnd;
                        }
                        else
                        {
                            _testThreadResults[(int)resultIndex][i].OperationStart = threadStart;
                            _testThreadResults[(int)resultIndex][i].OperationEnd = threadEnd;
                        }
                    }
                    catch (Exception ex)
                    {
                        _database.DebugQueue().AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                });
            }
            catch (OperationCanceledException)
            {
                if (replaceWebPage)
                {
                    operationTokenSource.Cancel();
                }
            }
            if (resultIndex == null)
            {
                DateTime endOperation = DateTime.Now;
                SetFinalResults(startOperation, endOperation);
                foreach (DataEngineResults dr in _threadResults)
                {
                    _resultCollection.Add(dr);
                }
                SetResultsStateCompleted();
            }
            if (replaceWebPage)
            {
                operationTokenSource.Cancel();
            }
            if (replaceWebPage)
            {
                try
                {
                    for (int threadinit = 0; threadinit < _numberOfThreads; threadinit++)
                    {
                        _pumpThreads[threadinit].Dispose();
                    }
                }
                catch (OperationCanceledException)
                {

                }
                finally
                {
                    operationTokenSource.Dispose();
                }
            }
        }

        /// <summary>
        /// Writes a 2 column wide entery to a database object every 100ms.
        /// </summary>
        public override void WriteInLine(CancellationTokenSource tokenSource)
        {
            while (!tokenSource.Token.IsCancellationRequested)
            {
                Parallel.For(0, _numberOfThreads, i =>
                {
                    _database.TokenSource = tokenSource;
                    _database.InsertPointInTimeWrite();
                });
                Thread.Sleep(100);
            }
        }

        #region Common In Class Usage items
        /// <summary>
        /// Parses the XML data objects to get the object creation for a specified database. 
        /// </summary>
        private List<string> GetXMLResource(string filename, string vendor)
        {
            string result = string.Empty;
            List<string> xmlListing = new List<string>();
            string resouceLocal = string.Empty;

            if (vendor != null)
            {
                resouceLocal = "DOES.DataEngine.Configuration." + vendor + "." + filename;
            }
            else
            {
                resouceLocal = "DOES.DataEngine.Configuration." + filename;
            }

            using (Stream stream = this.GetType().Assembly.
                       GetManifestResourceStream(resouceLocal))
            {
                using StreamReader sr = new StreamReader(stream);
                result = sr.ReadToEnd();
            }

            XDocument doc = XDocument.Parse(result);
            var list = doc.Root.Elements("string")
                           .Select(element => element.Value)
                           .ToList();
            foreach (string value in list)
            {
                string item = value;
                xmlListing.Add(item);
            }
            return xmlListing;
        }

        /// <summary>
        /// Retrieves a web page and its headers from the specified URI. 
        /// </summary>
        private static async Task<WebPage> GetFromWeb(string URI, MessageQueue queue)
        {
            WebPage newPage = new WebPage();
            string URL = null;
            string HTML = null;
            SortedList<string, string> Headers = new SortedList<string, string>();
            while (URL == null && HTML == null)
            {
                Uri targetURI = new Uri(URI);
                try
                {
                    HttpClientHandler handler = new HttpClientHandler()
                    {
                        AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate
                    };
                    using (var client = new HttpClient(handler))
                    {
                        var response = await client.GetAsync(URI);

                        response.EnsureSuccessStatusCode();
                        URL = response.RequestMessage.RequestUri.ToString();
                        if (URL.Length > 2048)
                        {
                            URL = null;
                            throw new Exception("URI is too long (" + URL.Length.ToString() + "");
                        }
                        System.Net.Http.Headers.HttpResponseHeaders responseHeaders = response.Headers;
                        string content = await response.Content.ReadAsStringAsync();
                        foreach (var value in responseHeaders)
                        {
                            string k = value.Key.ToString();
                            string[] vArray = (string[])value.Value;
                            string v = null;
                            foreach (string vItem in vArray)
                            {
                                v = v + vItem;
                            }
                            if (k == "Content-Length")
                            {
                                int.TryParse(v, out int contentLength);
                                if (contentLength > 800000)
                                {
                                    throw new Exception("Skipping too long Content-Length (" + contentLength.ToString("N0") + ")");
                                }

                            }
                            Headers.Add(k, v);
                        }
                        if (content.Length > 800000)
                        {
                            HTML = null;
                            throw new Exception("Skipping too long HTML-Length (" + content.Length.ToString() + ("N0") + ")");
                        }
                        HTML = content;
                        newPage.URL = URL;
                        newPage.HTML = HTML;
                        newPage.Headers = Headers;
                    }
                }
                catch (Exception ex)
                {
                    queue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    Thread.Sleep(200);
                }
            }
            return newPage;
        }

        /// <summary>
        /// Prepares a web page for use in a range of objects by using various encodings.This focuses on binary data in databases.
        /// Depending on the amount of randomization required the Mechanic object will encrypt and return a unique string for the URL and HTML. 
        /// </summary>
        private WebPage PrepareEncodedWebPage(int PageID, string URL, string HTML, SortedList<string, string> Headers)
        {
            int HREFS = CountBetweenM(HTML, @"href=""http://www.", @"""");
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            Encoding[] encodings = new[] { Encoding.Unicode, Encoding.ASCII, Encoding.GetEncoding(37), Encoding.UTF32 };

            byte[][] DataAsBytes = new byte[256 * 8][];
            byte[][] DataAsB64S = new byte[256 * 8][];

            byte[][] StatsAsBytes = new byte[8 * 2][];
            byte[][] StatsAsB64S = new byte[8 * 2][];

            if (_database.TableAmplifier != 0)
            {
                Parallel.For(0, 8, (i, state) =>
                {
                    string source = i < 4 ? URL : HTML;
                    for (int tableID = 0; tableID < _database.TableAmplifier; tableID++)
                    {
                        source = _database.GetMechanic().Encrypt(source);
                        DataAsBytes[tableID * 8 + i] = XorByteArray(encodings[i % 4].GetBytes(source), Convert.ToByte(tableID));
                        DataAsB64S[tableID * 8 + i] = Encoding.ASCII.GetBytes(Convert.ToBase64String(DataAsBytes[tableID * 8 + i]));
                    }

                    // Stats are only computed for the Tables *_X00 (so TableID * 8 = 0  and is ignored)
                    StatsAsBytes[i * 2] = ByteStatistics(DataAsBytes[i]);
                    StatsAsB64S[i * 2] = Encoding.ASCII.GetBytes(Convert.ToBase64String(StatsAsBytes[i * 2]));

                    StatsAsBytes[i * 2 + 1] = ByteStatistics(DataAsB64S[i]);
                    StatsAsB64S[i * 2 + 1] = Encoding.ASCII.GetBytes(Convert.ToBase64String(StatsAsBytes[i * 2 + 1]));
                }
                );
            }

            int hashURL = URL.GetHashCode();
            byte[] HTMLBinary = Encoding.ASCII.GetBytes(HTML.ToCharArray(), 0, Math.Min(HTML.Length, _columnWidth));
            byte[] HashHTML = SHA1.Create().ComputeHash(HTMLBinary);
            ComputedLengths cl = ComputeLengths(Convert.ToUInt64(HTMLBinary.Length + HashHTML.Length),
            Headers, StatsAsBytes, StatsAsB64S, DataAsBytes, DataAsB64S);
            WebPage preparedWebPage = new WebPage(PageID, _database.TableAmplifier, URL, HTML, Headers, HREFS, hashURL, HTMLBinary, HashHTML, DataAsBytes, DataAsB64S, StatsAsBytes, StatsAsB64S, cl);
            return preparedWebPage;
        }

        /// <summary>
        /// Prepares a web page for use in a range of objects.This focuses on characterised data in databases. 
        /// Depending on the amount of randomization required the Mechanic object will encrypt and return a unique string for the URL and HTML. 
        /// </summary>
        private WebPage PrepareCharacterisedWebPage(int PageID, string URL, string HTML, SortedList<string, string> Headers)
        {
            int HREFS = CountBetweenM(HTML, @"href=""http://www.", @"""");

            byte[][] DataAsBytes = new byte[256 * 8][];
            byte[][] DataAsB64S = new byte[256 * 8][];

            byte[][] StatsAsBytes = new byte[8 * 2][];
            byte[][] StatsAsB64S = new byte[8 * 2][];

            ParallelOptions po = new ParallelOptions();
            po.MaxDegreeOfParallelism = 1;
            if (_database.TableAmplifier != 0)
            {
                Parallel.For(0, 8, po,  (i, state) =>
                {
                    string source = i < 4 ? URL : HTML;
                    for (int tableID = 0; tableID < _database.TableAmplifier; tableID++)
                    {
                        source = _database.GetMechanic().Encrypt(source);
                        DataAsBytes[tableID * 8 + i] = XorByteArray(Encoding.ASCII.GetBytes(source), Convert.ToByte(tableID));
                        DataAsB64S[tableID * 8 + i] = Encoding.ASCII.GetBytes(Convert.ToBase64String(DataAsBytes[tableID * 8 + i]));
                    }

                    // Stats are only computed for the Tables *_X00 (so TableID * 8 = 0  and is ignored)
                    StatsAsBytes[i * 2] = ByteStatistics(DataAsBytes[i]);
                    StatsAsB64S[i * 2] = Encoding.ASCII.GetBytes(Convert.ToBase64String(StatsAsBytes[i * 2]));

                    StatsAsBytes[i * 2 + 1] = ByteStatistics(DataAsB64S[i]);
                    StatsAsB64S[i * 2 + 1] = Encoding.ASCII.GetBytes(Convert.ToBase64String(StatsAsBytes[i * 2 + 1]));
                }
                );
            }
            int hashURL = URL.GetHashCode();
            byte[] HTMLBinary = Encoding.ASCII.GetBytes(HTML.ToCharArray(), 0, Math.Min(HTML.Length, _columnWidth));
            byte[] HashHTML = SHA1.Create().ComputeHash(HTMLBinary);
            ComputedLengths cl = ComputeLengths(Convert.ToUInt64(HTMLBinary.Length + HashHTML.Length),
                Headers, StatsAsBytes, StatsAsB64S, DataAsBytes, DataAsB64S);
            WebPage preparedWebPage = new WebPage(PageID, _database.TableAmplifier, URL, HTML, Headers, HREFS, hashURL, 
                HTMLBinary, HashHTML, DataAsBytes, DataAsB64S, StatsAsBytes, StatsAsB64S, cl);

            return preparedWebPage;
        }

        /// <summary>
        /// Counts the length of the string and returns the value  
        /// </summary>
        private static int CountBetweenM(string InString, string FromString, string ToString)
        {
            int i = 0;
            int j = 0;
            int CountBetweenM = 0;
            while (true)
            {
                i = InString.IndexOf(FromString, j);
                if (i == -1)
                {
                    return CountBetweenM;
                }
                int SearchFrom = i + FromString.Length;
                if (SearchFrom >= InString.Length)
                {
                    return CountBetweenM;
                }
                j = InString.IndexOf(ToString, SearchFrom);
                if (j == -1)
                {
                    return CountBetweenM;
                }
                j += ToString.Length;
                CountBetweenM += 1;
            }
        }

        /// <summary>
        /// Xors a byte array to return a different byte array. 
        /// </summary>
        private static byte[] XorByteArray(byte[] MyArray, byte XorWith)
        {
            if (XorWith == 0)
            {
                return MyArray;
            }

            byte[] XoredArray = new byte[MyArray.GetUpperBound(0) + 1];
            for (int i = 0; i <= MyArray.GetUpperBound(0); i++)
            {
                XoredArray[i] = (byte)(MyArray[i] ^ XorWith);
            }
            return XoredArray;
        }

        /// <summary>
        /// Returns the statistics established from a byte array for further data analysis for each webpage object. 
        /// </summary>
        private static byte[] ByteStatistics(byte[] Data)
        {
            // Count the number of transitions from byte value n to next byte value m
            int[] Counters = new int[256 * 256];
            for (int i = 0; i <= Data.Length - 2; i++)
                Counters[Data[i] * 256 + Data[i + 1]] += 1;

            // Count the number of bytes required to store the statistics if we ignore all Counters() with 0
            int StatsCount = 0;
            for (int i = 0; i <= Counters.GetUpperBound(0); i++)
            {
                if (Counters[i] != 0)
                    StatsCount += 1; // Ignoring all Values = 0 
            }

            // Copy each non-zero values to the final byte array
            byte[] BS = new byte[(StatsCount * 6)];
            int Index = -1;
            int j = 0;
            for (int i = 0; i <= 255; i++)
            {
                for (int ii = 0; ii <= 255; ii++)
                {
                    Index += 1; // Index = i * 256 + ii
                    if (Counters[Index] != 0)
                    {
                        BS[j] = System.Convert.ToByte(i); // From Byte Value (1 byte)
                        BS[j + 1] = System.Convert.ToByte(ii); // To Byte Value (1 byte)
                        BitConverter.GetBytes(Counters[Index]).CopyTo(BS, j + 2); // Count (4 bytes)
                        j += 6; // 6 bytes per record
                    }
                }
            }
            return BS;
        }

        /// <summary>
        /// Establishes the size of a web page before it is inserted into the relevant data object.  
        /// </summary>
        private ComputedLengths ComputeLengths(UInt64 htmlLength, SortedList<string, string> Headers, byte[][] StatsAsBytes, 
            byte[][] StatsAsB64S, byte[][] DataAsBytes, byte[][] DataAsB64S)
        {
            UInt64 totalLength;
            UInt64 headersLength = 0;
            UInt64 statsLength = 0;
            UInt64 WebPageRowLength;
            UInt64 WebPageEncodingLength = 0;

            foreach (KeyValuePair<string, string> kvp in Headers)
            {
                headersLength += Convert.ToUInt64(kvp.Key.Length + kvp.Value.Length);
            }

            headersLength *= 2;
            headersLength += 28;

            if (_database.TableAmplifier != 0)
            {
                for (int i = 0; i <= StatsAsBytes.GetUpperBound(0); i++)
                {
                    statsLength += Convert.ToUInt64(StatsAsBytes[i].Length);
                    statsLength += Convert.ToUInt64(StatsAsB64S[i].Length);
                    statsLength += 32;
                }
                for (int i = 0; i <= 3; i++)
                {
                    WebPageEncodingLength += 4 + 8 + 5 * 4;
                    WebPageEncodingLength += Convert.ToUInt64(DataAsBytes[i].Length);
                    WebPageEncodingLength += Convert.ToUInt64(DataAsBytes[i + 4].Length);
                    WebPageEncodingLength += Convert.ToUInt64(DataAsB64S[i].Length);
                    WebPageEncodingLength += Convert.ToUInt64(DataAsB64S[i + 4].Length);
                }
                WebPageRowLength = Convert.ToUInt64(4 + (2 * 8) + (3 * 4) + 8 + (2 * 4) + 20 + DataAsBytes[1].Length);
                WebPageEncodingLength = Convert.ToUInt64(WebPageEncodingLength) * (Convert.ToUInt64(_database.TableAmplifier + 1));
            }
            else
            {
                WebPageRowLength = Convert.ToUInt64(4 + (2 * 8) + (3 * 4) + 8 + (2 * 4) + 20 + 0 + 0);
                WebPageEncodingLength = Convert.ToUInt64(WebPageEncodingLength) * (Convert.ToUInt64(_database.TableAmplifier + 1));
            }

            totalLength = headersLength + statsLength + WebPageRowLength + WebPageEncodingLength;

            ComputedLengths cl = new ComputedLengths(statsLength, headersLength, totalLength);
            return cl;
        }

        private Tuple<UInt64,Queue<WebPage>[]> CompileWebPagesToProcess(UInt64 RequestedBytes, CancellationToken token)
        {
            Tuple<Random, UInt64> randomValueCheck = _database.InitializeRandom(false, RequestedBytes);
            SortedList<int, long> colPageIDToLength = _database.GetPageIDList();
            List<int> queuedWebPages = new List<int>();

            Queue<WebPage>[] webPagesToProcess = new Queue<WebPage>[_numberOfThreads];
            int numPageID = colPageIDToLength.Count;

            for (int i = 0; i < webPagesToProcess.Length; i++)
            {
                webPagesToProcess[i] = new Queue<WebPage>();
            }
            UInt64 bytesToProcess = 0;
            RequestedBytes = randomValueCheck.Item2;
            int arrayIndexCheck = 0;
            while (bytesToProcess < RequestedBytes && !token.IsCancellationRequested)
            {
                //For some reason it only ever reaches half of the amount of pointer values
                if(((queuedWebPages.Count * 2) + 4) == colPageIDToLength.Count)
                {
                    RequestedBytes = bytesToProcess;
                }
                int indexOfPageID = randomValueCheck.Item1.Next(numPageID);
                int probability = (Convert.ToInt32((double)indexOfPageID / (double)numPageID) * 100);
                if (probability > randomValueCheck.Item1.Next(100))
                {
                    int pageID = colPageIDToLength.Keys[indexOfPageID];
                    bool pageidExists = false;
                    for (int iqcheck = 0; iqcheck < webPagesToProcess.Length; iqcheck++)
                    {
                        var containsPageId = webPagesToProcess[iqcheck].Any(o => o.PageID == pageID);
                        if (containsPageId == true)
                        {
                            pageidExists = true;
                            break;
                        }
                    }
                    if (!pageidExists)
                    {
                        queuedWebPages.Add(pageID);
                        Int64 totalLength = colPageIDToLength[pageID];
                        ComputedLengths pageLegnth = new ComputedLengths(Convert.ToUInt64(totalLength));
                        //if any of the queues contain the web page with the same id it must not be entered
                        WebPage webPage = new WebPage(pageID, pageLegnth);
                        webPagesToProcess[arrayIndexCheck].Enqueue(webPage);
                        bytesToProcess += webPage.TotalLength;
                        //this rotates the web pages around so that each thread has its own queue 
                        if (!(arrayIndexCheck >= (webPagesToProcess.Length - 1)))
                        {
                            arrayIndexCheck++;
                        }
                        else
                        {
                            arrayIndexCheck = 0;
                        }
                    }

                }
            }
            return new Tuple<UInt64, Queue<WebPage>[]>(RequestedBytes,webPagesToProcess);
        }

        /// <summary>
        /// If importing from a file this will check that each OilPump webpage producer object contains at least 1 webpages before continuing. 
        /// </summary>
        private bool CheckForRunningPumps()
        {
            //if any one thread engine is in a "running" state then all thread engines are in a running state 
            //should return true is any one engine is in a running state 
            bool pumpsRunning = true;
            bool[] allPumpState = new bool[_numberOfThreads];
            int numberOfActivePumps = 0;
            for (int i = 0; i < _pumpThreads.Length; i++)
            {
                allPumpState[i] = _pumpThreads[i].GetPumpState();
                if (allPumpState[i] == false)
                {
                    numberOfActivePumps++;
                }
            }
            if (numberOfActivePumps == 0)
            {
                pumpsRunning = false;
            }
            return pumpsRunning;
        }

        /// <summary>
        /// Sets the final results value after an operation has completed. 
        /// </summary>
        private void SetFinalResults(DateTime operationStart, DateTime operationEnd)
        {
            _finalResults = new DataEngineResultSet(_threadResults, operationStart, operationEnd);
        }

        /// <summary>
        /// Sets the final results value after a test operation has completed. 
        /// </summary>
        public override void SetFinalTestResults(DateTime operationStart, DateTime operationEnd, Dynamics.DataEngineOperation testOperation)
        {
            _finalResults = new DataEngineResultSet(_testThreadResults, operationStart, operationEnd, testOperation);
        }

        /// <summary>
        /// Disposes the object after implementation. 
        /// </summary>
        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
