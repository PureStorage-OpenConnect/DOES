using DOES.Shared.Debug;
using DOES.Shared.Operations;
using DOES.Shared.Resources;
using DOES.DataEngine.Operations;
using DOES.DataEngine.Resources;
using ShellProgressBar;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DOES.Cli
{
    public class Add : Operation
    {
        Task _task;
        CancellationToken _token;
        CancellationTokenSource _tokenSource;
        MessageQueue _messageQueue = new MessageQueue();
        Analytic _ldes;
        Queue<InterimStat> _stats = new Queue<InterimStat>();
        Queue<InterimThreadStat> _threadStats = new Queue<InterimThreadStat>();
        DataVendor _driver = null;
        Controller _controller = null;
        private string _ip;
        private Dynamics.Database _dbType;
        private string _dbname;
        private UInt64 _amountToAdd;
        private Dynamics.StorageUnit _unit;
        private string _user;
        private string _password;
        private int _numberOfThreads;
        private Dynamics.DatabaseSchema _schema;
        private int _tableAmplifier;
        private int _columnWidth;
        private string _folderPath;
        private double _randomPercentage;
        private int _portNumber;
        //MS SQL only parameter set 
        private string _instanceName;
        //Oracle only parameter set 
        private bool _useOracleSID;
        //MySQL only parameter set
        private Dynamics.MySQLStorageEngine _mysqlEngine;
        private string _ndbtablespace;
        //MariaDB only Parameter Set 
        private Dynamics.MariaDBStorageEngine _mariaDBEngine;
        //SAP HANA only parameter set 
        private string _instanceNumber;
        private int _percentColumnTables;
        private int _percentPagedTables;
        private int _percentageWarmExtensionNode;
        private string _extensionNodeGroupName;
        private int _partitions;
        //MongoDB only parameter set 
        private Dynamics.MongoDBDeployment _mongoDBDeployment;
        //Logging Parameters 
        private bool _logData;
        private string _testname;
        private string _objectName;
        private string _objectCategory;
        private int _sequence;

        private bool _verboseWriter = false;

        public override bool VerboseWriter { get {return _verboseWriter; } set {_verboseWriter = value; } }

        public override CancellationTokenSource TokenSource { get {return _tokenSource; } }

        public Add(string IP, Dynamics.Database dbType, string databaseName, UInt64 amountToAdd, Dynamics.StorageUnit unit, string user, string password, bool useOracleSID,int numberOfThreads, 
            Dynamics.DatabaseSchema schema, int tableAmplifier, int columnWidth, string folderPath, double randomPercentage, string instanceName, string instanceNumber, 
            int percentageColumntables, int percentagePagedTables, int percentageWarmExtensionNode, string extensionNodeGroupName, int partitions, Dynamics.MySQLStorageEngine mysqlEngine, 
            Dynamics.MariaDBStorageEngine mariadbEngine, string ndbtablespace, Dynamics.MongoDBDeployment mongoDBDeployment, int portnumber, bool logData, string testName, string objectName, string ObjectCategory, int sequence)
        {
            _ip = IP;
            _dbType = dbType;
            _dbname = databaseName;
            _amountToAdd = amountToAdd;
            _unit = unit;
            _user = user;
            _password = password;
            _useOracleSID = useOracleSID;
            _numberOfThreads = numberOfThreads;
            _schema = schema;
            _tableAmplifier = tableAmplifier;
            _columnWidth = columnWidth;
            _folderPath = folderPath;
            _randomPercentage = randomPercentage;
            _instanceName = instanceName;
            _instanceNumber = instanceNumber;
            _percentColumnTables = percentageColumntables;
            _percentPagedTables = percentagePagedTables;
            _percentageWarmExtensionNode = percentageWarmExtensionNode;
            _extensionNodeGroupName = extensionNodeGroupName;
            _partitions = partitions;
            _mysqlEngine = mysqlEngine;
            _mariaDBEngine = mariadbEngine;
            _ndbtablespace = ndbtablespace;
            _mongoDBDeployment = mongoDBDeployment;
            _portNumber = portnumber;
            _logData = logData;
            _testname = testName;
            _objectName = objectName;
            _objectCategory = ObjectCategory;
            _sequence = sequence;
            _tokenSource = new CancellationTokenSource();
            _token = _tokenSource.Token;
        }

        public override void ExecuteOperation()
        {
            Dynamics.DataEngineOperation operation = Dynamics.DataEngineOperation.InsertData;          
            Mechanic serviceOperator = new Mechanic(_randomPercentage);


            UInt64 amountToAddBytes = serviceOperator.ReturnDataUnitsAsBytes(_amountToAdd, _unit);
            DataEngineResults lastResults = new DataEngineResults(Dynamics.DataEngineOperation.InsertData);
            DataEngineResultSet lastResultSet = new DataEngineResultSet(_numberOfThreads);

            if (_dbType == Dynamics.Database.MicrosoftSQL)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 1433;
                }
                _driver = new MicrosoftSQL(_ip, _dbname, _instanceName, _user, _password, _portNumber, _tableAmplifier, _schema, serviceOperator, _messageQueue);

            }
            else if (_dbType == Dynamics.Database.SAPHANA)
            {
                try
                {
                    if (_schema == Dynamics.DatabaseSchema.MemoryOptimised || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexes ||
                        _schema == Dynamics.DatabaseSchema.MemoryOptimisedLOB || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB)
                    {
                        throw new InvalidOperationException();
                    }
                    else
                    {
                        if (_portNumber == 0)
                        {
                            _portNumber = 13;
                        }
                        _driver = new SAPHANA(_ip, _dbname, _user, _password, _instanceNumber, _tableAmplifier,
                       _percentColumnTables, _percentPagedTables, _partitions, _percentageWarmExtensionNode, _extensionNodeGroupName,
                       _numberOfThreads, _portNumber, _schema, _messageQueue, serviceOperator);
                    }
                }
                catch (InvalidOperationException)
                {
                    Console.WriteLine(_schema.ToString() + " is not a supported Schema type for SAP HANA");
                }
            }
            else if (_dbType == Dynamics.Database.Oracle)
            {
                try
                {
                    if (_schema == Dynamics.DatabaseSchema.MemoryOptimised || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexes ||
                        _schema == Dynamics.DatabaseSchema.MemoryOptimisedLOB || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB)
                    {
                        throw new InvalidOperationException();
                    }
                    else
                    {
                        if (_portNumber == 0)
                        {
                            _portNumber = 1521;
                        }
                        _driver = new OracleDB(_ip, _dbname, _user, _password, _useOracleSID, _tableAmplifier, _portNumber, _schema, serviceOperator, _messageQueue);
                    }
                }
                catch (InvalidOperationException)
                {
                    Console.WriteLine(_schema.ToString() + " is not a supported Schema type for Oracle");
                }
            }
            else if (_dbType == Dynamics.Database.MySQL)
            {
                try
                {
                    if (_schema == Dynamics.DatabaseSchema.MemoryOptimised || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexes ||
                        _schema == Dynamics.DatabaseSchema.MemoryOptimisedLOB || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB)
                    {
                        throw new InvalidOperationException();
                    }
                    else
                    {
                        if (_portNumber == 0)
                        {
                            _portNumber = 3306;
                        }
                        _driver = new MySQLDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, _schema, serviceOperator, _mysqlEngine, _ndbtablespace, _messageQueue);
                    }
                }
                catch (InvalidOperationException)
                {
                    Console.WriteLine(_schema.ToString() + " is not a supported Schema type for MySQL");
                }
            }
            else if (_dbType == Dynamics.Database.MariaDB)
            {
                try
                {
                    if (_schema == Dynamics.DatabaseSchema.MemoryOptimised || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexes ||
                        _schema == Dynamics.DatabaseSchema.MemoryOptimisedLOB || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB)
                    {
                        throw new InvalidOperationException();
                    }
                    else
                    {
                        if (_portNumber == 0)
                        {
                            _portNumber = 3306;
                        }
                        _driver = new MariaDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, _schema, serviceOperator, _mariaDBEngine, _messageQueue);
                    }
                }
                catch (InvalidOperationException)
                {
                    Console.WriteLine(_schema.ToString() + " is not a supported Schema type for MariaDB");
                }
            }
            else if (_dbType == Dynamics.Database.PostgreSQL)
            {
                try
                {
                    if (_schema == Dynamics.DatabaseSchema.MemoryOptimised || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexes ||
                        _schema == Dynamics.DatabaseSchema.MemoryOptimisedLOB || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB)
                    {
                        throw new InvalidOperationException();
                    }
                    else
                    {
                        if (_portNumber == 0)
                        {
                            _portNumber = 5432;
                        }
                        _driver = new PostgreSQLDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, _schema, serviceOperator, _messageQueue);
                    }
                }
                catch (InvalidOperationException)
                {
                    Console.WriteLine(_schema.ToString() + " is not a supported Schema type for PostgreSQL");
                }
            }
            else if (_dbType == Dynamics.Database.MongoDB)
            {
                try
                {
                    if (_schema == Dynamics.DatabaseSchema.MemoryOptimised || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexes ||
                        _schema == Dynamics.DatabaseSchema.MemoryOptimisedLOB || _schema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB)
                    {
                        throw new InvalidOperationException();
                    }
                    else
                    {
                        if (_portNumber == 0)
                        {
                            _portNumber = 27017;
                        }
                        _driver = new MongoDBOper(_ip, _dbname, _user, _password, _portNumber, _tableAmplifier, _schema, _mongoDBDeployment, serviceOperator, _messageQueue);
                    }
                }
                catch (InvalidOperationException)
                {
                    Console.WriteLine(_schema.ToString() + " is not a supported Schema type for MongoDB");
                }
            }

            try
            {
                if (_driver != null)
                {
                    _controller = new Controller(_driver, _columnWidth, _numberOfThreads);

                    string response = serviceOperator.HandleTableCreateResponse(_schema, _controller.CreateTablesAndIndexes());
                    _messageQueue.AddMessage(new Message(DateTime.Now, response, Message.MessageType.Info));
                    if (_folderPath != null)
                    {
                        //import using folder
                        DateTime operationStart = DateTime.Now;
                        _task = Task.Run(() =>
                        {
                            var capturedToken = _token;
                            _controller.ImportFromFile(_folderPath, amountToAddBytes, _tokenSource, null);
                        }, _token);
                    }
                    else
                    {
                        //import using web 
                        DateTime operationStart = DateTime.Now;
                        _task = Task.Run(() =>
                        {
                            var capturedToken = _token;
                            _controller.ImportFromWeb(amountToAddBytes, _tokenSource, null);
                        }, _token);
                    }
                }

                string dbtypeIdentifier = null;
                if (_dbType == Dynamics.Database.MicrosoftSQL)
                {
                    dbtypeIdentifier = "Microsoft SQL Server";
                }
                else if (_dbType == Dynamics.Database.SAPHANA)
                {
                    dbtypeIdentifier = "SAP HANA";
                }
                else if (_dbType == Dynamics.Database.Oracle)
                {
                    dbtypeIdentifier = "Oracle";
                }
                else if (_dbType == Dynamics.Database.MySQL)
                {
                    dbtypeIdentifier = "MySQL";
                }

                var options = new ProgressBarOptions
                {
                    ProgressCharacter = '─',
                    ProgressBarOnBottom = true
                };

                string topPbarMessage = "Overall progress for DOES.DataEngine import operation                Amount processed : 0 Bytes of 0 Bytes";

                using (var progressBar = new ProgressBar(10000, topPbarMessage, options))
                {
                    var dataRateChild = progressBar.Spawn(10000, "Monitor data rate for data insert operations for " + dbtypeIdentifier + " Database : " + _dbname + " Data rate performance : 0 MB/s ");
                    var rowRateChild = progressBar.Spawn(10000, "Monitor row insertion rate for data insert operations row rate performance 0 Rows/s");
                    var transactionRateChild = progressBar.Spawn(10000, "Monitor transaction insertion rate for data insert operations transaction performance 0 Transactions/s");

                    DateTime start = DateTime.Now;
                    DateTime end;

                    while (!_controller.GetResultsState() && !_token.IsCancellationRequested)
                    {
                        Thread.Sleep(1000);
                        DataEngineResults tempResults = _controller.ReturnInterimResults();
                        end = DateTime.Now;
                        TimeSpan duration = (end - start);
                        lastResults = UpdateProgressBar(tempResults, duration, amountToAddBytes, lastResults,
                            progressBar, dataRateChild, rowRateChild, transactionRateChild);
                        if (_logData)
                        {
                            DataEngineResultSet tempThreadResults = _controller.ReturnInterimThreadResults();
                            lastResultSet = AddDataEngineInterimThreadResults(tempThreadResults, duration, lastResultSet);
                        }
                        start = DateTime.Now;
                        WriteMessages(_ldes, operation);
                    }
                }
            

                if (!_token.IsCancellationRequested)
                {
                    DataEngineResultSet results = _controller.ReturnFinalResults();
                    if (_logData)
                    {
                        InitlialiseLogging();
                    }
                    WriteFinalReport(results, _ldes);
                }
                WriteMessages(_ldes, operation);
            }
            catch (OperationCanceledException)
            {

            }
            finally
            {
                _tokenSource.Dispose();
            }
        }


        #region Other

        private void InitlialiseLogging()
        {
            if (_testname != "" && _objectName != "" && _objectCategory != null)
            {
                _ldes = new Analytic(_testname, _objectName, _objectCategory, _sequence, _messageQueue);
                while (_stats.Count != 0)
                {
                    _ldes.LogDataEngineStatsInterim(_stats.Dequeue());
                }
                while (_threadStats.Count != 0)
                {
                    _ldes.LogDataEngineStatsThreadsInterim(_threadStats.Dequeue());
                }
            }
            else if (_testname != "" || _objectName != "")
            {
                _ldes = new Analytic(_testname, _objectName, _sequence, _messageQueue);
                while (_stats.Count != 0)
                {
                    _ldes.LogDataEngineStatsInterim(_stats.Dequeue());
                }
                while (_threadStats.Count != 0)
                {
                    _ldes.LogDataEngineStatsThreadsInterim(_threadStats.Dequeue());
                }
            }
            else
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data will not be logged due to missing test or object name", Message.MessageType.Error));
                _logData = false;
            }
        }

        private DataEngineResults UpdateProgressBar(DataEngineResults results, TimeSpan span, UInt64 RequestedBytes, DataEngineResults oldResults,
            ProgressBar pbar, ChildProgressBar dataRateBar, ChildProgressBar rowRateBar, ChildProgressBar transactionRateBar)
        {
            try
            {
                UInt64 interimProcessedBytes = results.ProcessedBytes - oldResults.ProcessedBytes;
                UInt64 interimProcessedRows = results.ProcessedRows - oldResults.ProcessedRows;
                UInt64 interimProcessedTransactions = results.ProcessedTransactions - oldResults.ProcessedTransactions;

                double processedBytesDBL = Convert.ToDouble(results.ProcessedBytes);
                double requestedBytesDBL = Convert.ToDouble(RequestedBytes);
                string storageUnitProcess = "Bytes";

                if (processedBytesDBL > 1024L && processedBytesDBL < (1024 * 1024))
                {
                    //convertToKB
                    processedBytesDBL = Math.Round(processedBytesDBL / (1024L), 2);
                    requestedBytesDBL = Math.Round(requestedBytesDBL / (1024L), 2);
                    storageUnitProcess = "KiloBytes";
                }
                else if (processedBytesDBL > (1024 * 1024) && processedBytesDBL < (1024 * 1024 * 1024))
                {
                    //convert to MB
                    processedBytesDBL = Math.Round(processedBytesDBL / (1024L * 1024L), 2);
                    requestedBytesDBL = Math.Round(requestedBytesDBL / (1024L * 1024L), 2);
                    storageUnitProcess = "Megabytes";
                }
                else if (processedBytesDBL > (1024 * 1024 * 1024) && processedBytesDBL < (1024L * 1024L * 1024L * 1024L))
                {
                    //convert to GB
                    processedBytesDBL = Math.Round(processedBytesDBL / (1024 * 1024 * 1024), 2);
                    requestedBytesDBL = Math.Round(requestedBytesDBL / (1024L * 1024L * 1024L), 2);
                    storageUnitProcess = "Gigabytes";
                }
                else if (processedBytesDBL > (1024L * 1024L * 1024L * 1024L))
                {
                    //convert to TB
                    processedBytesDBL = Math.Round(processedBytesDBL / (1024L * 1024L * 1024L * 1024L), 2);
                    requestedBytesDBL = Math.Round(requestedBytesDBL / (1024L * 1024L * 1024L * 1024L), 2);
                    storageUnitProcess = "Terabytes";
                }


                double interimProcessedBytesDBL = Convert.ToDouble(interimProcessedBytes);
                string storageUnitDR = " B/s";

                if (interimProcessedBytesDBL > 1024L && interimProcessedBytesDBL < (1024 * 1024))
                {
                    //convertToKB
                    interimProcessedBytesDBL = Math.Round(interimProcessedBytesDBL / (1024L), 2);
                    storageUnitDR = " KB/s";
                }
                else if (interimProcessedBytesDBL > (1024 * 1024) && interimProcessedBytesDBL < (1024 * 1024 * 1024))
                {
                    //convert to MB
                    interimProcessedBytesDBL = Math.Round(interimProcessedBytesDBL / (1024L * 1024L), 2);
                    storageUnitDR = " MB/s";
                }
                else if (interimProcessedBytesDBL > (1024 * 1024 * 1024) && interimProcessedBytesDBL < (1024L * 1024L * 1024L * 1024L))
                {
                    //convert to GB
                    interimProcessedBytesDBL = Math.Round(interimProcessedBytesDBL / (1024 * 1024 * 1024), 2);
                    storageUnitDR = " GB/s";
                }
                else if (interimProcessedBytesDBL > (1024L * 1024L * 1024L * 1024L))
                {
                    //convert to TB
                    interimProcessedBytesDBL = Math.Round(interimProcessedBytesDBL / (1024L * 1024L * 1024L * 1024L), 2);
                    storageUnitDR = " TB/s";
                }

                Double dataRateperformanceMBs = Math.Round((interimProcessedBytes) / (1024 * 1024) / span.TotalSeconds, 2);
                Double dataRateperformance = Math.Round(interimProcessedBytesDBL / span.TotalSeconds, 2);
                Double rowRatePerformance = Math.Round(interimProcessedRows / span.TotalSeconds, 2);
                Double transactionalPerformance = Math.Round(interimProcessedTransactions / span.TotalSeconds, 2);

                //% Progress
                int percentageComplete = 0;
                if (results.ProcessedBytes < RequestedBytes)
                {
                    percentageComplete = (int)(Math.Round(((double)results.ProcessedBytes / (double)RequestedBytes) * 10000, 0));
                }
                else
                {
                    percentageComplete = 10000;
                }
                pbar.Tick(percentageComplete, "Amount processed: " + processedBytesDBL + " " + storageUnitProcess +
                    " of " + requestedBytesDBL + " " + storageUnitProcess);

                dataRateBar.Tick(percentageComplete, "Data rate performance: " + dataRateperformance + storageUnitDR);
                rowRateBar.Tick(percentageComplete, "Row rate performance: " + rowRatePerformance + " Rows/s");
                transactionRateBar.Tick(percentageComplete, "Transactional performance  : " + transactionalPerformance + " Transactions/s");


                if (_logData)
                {
                    _stats.Enqueue(new InterimStat(results.ProcessedBytes, rowRatePerformance, transactionalPerformance, dataRateperformance, results.ResultType, DateTime.Now));
                }
            }
            catch (Exception ex) { _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error)); }
            return results;
        }

        private DataEngineResultSet AddDataEngineInterimThreadResults(DataEngineResultSet resultSet, TimeSpan span, DataEngineResultSet oldResults)
        {
            for (int i = 0; i < resultSet.ResultSet.Length; i++)
            {
                if (resultSet.ResultSet[i] != null)
                {
                    if (oldResults.ResultSet[i] == null)
                    {
                        oldResults.ResultSet[i] = new DataEngineResults(0, 0, 0, 0, resultSet.ResultSet[i].ResultType);
                    }
                    UInt64 interimProcessedBytes = resultSet.ResultSet[i].ProcessedBytes - oldResults.ResultSet[i].ProcessedBytes;
                    UInt64 interimProcessedRows = resultSet.ResultSet[i].ProcessedRows - oldResults.ResultSet[i].ProcessedRows;
                    UInt64 interimProcessedTransactions = resultSet.ResultSet[i].ProcessedTransactions - oldResults.ResultSet[i].ProcessedTransactions;
                    //MB/s Progress
                    Double dataRateperformance = Math.Round((interimProcessedBytes) / (1024 * 1024) / span.TotalSeconds, 2);
                    Double rowRatePerformance = Math.Round(interimProcessedRows / span.TotalSeconds, 2);
                    Double transactionalPerformance = Math.Round(interimProcessedTransactions / span.TotalSeconds, 2);

                    _threadStats.Enqueue(new InterimThreadStat((i + 1), resultSet.ResultSet[i].ProcessedBytes, rowRatePerformance, transactionalPerformance, dataRateperformance, resultSet.ResultSet[i].ResultType,
                        DateTime.Now));
                }
            }
            return resultSet;
        }

        private void WriteMessages(Analytic ls, Dynamics.DataEngineOperation _operation)
        {
            List<Message> tickedMessages = _messageQueue.GetAllMessages();
            string finalReportText = "";
            foreach (Message m in tickedMessages)
            {
                Tuple<Message.MessageType, string> messageToParse = m.GetFormattedMessage();
                if (messageToParse.Item1 == Message.MessageType.Report)
                {
                    finalReportText += messageToParse.Item2 + Environment.NewLine;
                    Console.WriteLine(messageToParse.Item2);
                }
                else if (messageToParse.Item1 == Message.MessageType.Command)
                {
                    if(_verboseWriter)
                    {
                        Console.WriteLine(messageToParse.Item2);
                    }
                }
                else if (messageToParse.Item1 == Message.MessageType.Error)
                {
                    Console.WriteLine(messageToParse.Item2);
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, _operation.ToString() + messageToParse.Item2);
                }
                else
                {
                    if (_verboseWriter)
                    {
                        Console.WriteLine(messageToParse.Item2);
                    }
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, _operation.ToString() + messageToParse.Item2);
                }
            }
            if (ls != null && _logData)
            {
                ls.LogDataEngineStatsFinalReport(finalReportText, _operation, DateTime.Now);
            }
        }

        private void WriteFinalReport(DataEngineResultSet results, Analytic logObject)
        {
            TimeSpan operationDuration = (results.OperationEnd - results.OperationStart);
            DataEngineResults overallResults = results.ReturnAggregateResultSet();

            double amountOfDataProcessed = overallResults.ProcessedBytes;
            Dynamics.StorageUnit storageUnit = Dynamics.StorageUnit.Bytes;

            if (amountOfDataProcessed > 1024L && amountOfDataProcessed < (1024 * 1024))
            {
                //convertToKB
                amountOfDataProcessed = Math.Round(amountOfDataProcessed / (1024L), 2);
                storageUnit = Dynamics.StorageUnit.Kilobytes;
            }
            else if (amountOfDataProcessed > (1024 * 1024) && amountOfDataProcessed < (1024 * 1024 * 1024))
            {
                //convert to MB
                amountOfDataProcessed = Math.Round(amountOfDataProcessed / (1024L * 1024L), 2);
                storageUnit = Dynamics.StorageUnit.Megabytes;
            }
            else if (amountOfDataProcessed > (1024 * 1024 * 1024) && amountOfDataProcessed < (1024L * 1024L * 1024L * 1024L))
            {
                //convert to GB
                amountOfDataProcessed = Math.Round(amountOfDataProcessed / (1024 * 1024 * 1024), 2);
                storageUnit = Dynamics.StorageUnit.Gigabytes;
            }
            else if (amountOfDataProcessed > (1024L * 1024L * 1024L * 1024L))
            {
                //convert to TB
                amountOfDataProcessed = Math.Round(amountOfDataProcessed / (1024L * 1024L * 1024L * 1024L), 2);
                storageUnit = Dynamics.StorageUnit.Terabytes;
            }


            //Overall Results
            double dataRateperformance = Math.Round((overallResults.ProcessedBytes) / (1024 * 1024) / operationDuration.TotalSeconds, 2);
            double rowRatePerformance = Math.Round(overallResults.ProcessedRows / operationDuration.TotalSeconds, 2);
            double transactionalPerformance = Math.Round(overallResults.ProcessedTransactions / operationDuration.TotalSeconds, 2);

            //Overall Thread Aggregated Results
            double dataAddedPerThread = amountOfDataProcessed / _numberOfThreads;
            double rowsAddedPerThread = overallResults.ProcessedRows / (UInt64)_numberOfThreads;
            double transactionscompletedPerThread = overallResults.ProcessedTransactions / (UInt64)_numberOfThreads;
            double averageDataRatePerThread = dataRateperformance / (UInt64)_numberOfThreads;
            double averageRowRatePerformancePerThread = rowRatePerformance / (UInt64)_numberOfThreads;
            double averageTransactionPerformancPerThread = transactionalPerformance / (UInt64)_numberOfThreads;

            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "--                      ADD OPERATION COMPLETED                    -- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "---------------------------Overall Results--------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Data added                                    : " + amountOfDataProcessed + " " + storageUnit.ToString(), Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Rows added                                    : " + overallResults.ProcessedRows, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Transactions completed                        : " + overallResults.ProcessedTransactions, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Transactions failed                           : " + overallResults.FailedProcessedRows, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Total seconds                                 : " + operationDuration.TotalSeconds, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Data rate performance                         : " + dataRateperformance + " Mb/s", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Row rate performance                          : " + rowRatePerformance + " Rows/s", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Transactional performance                     : " + transactionalPerformance + " Transactions/s", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------Overall Thread Aggregated Results----------------- ", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average data added per thread                 : " + dataAddedPerThread + " " + storageUnit.ToString(), Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average rows added per thread                 : " + rowsAddedPerThread, Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average transactions completed per thread     : " + transactionscompletedPerThread, Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average data rate performance per thread      : " + averageDataRatePerThread + " Mb/s", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average row rate performance per thread       : " + averageRowRatePerformancePerThread + " Rows/s", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average transactional performance per thread  : " + averageTransactionPerformancPerThread + " Transactions/s", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Info));

            if (_logData)
            {
                logObject.LogDataEngineStatsTotal(overallResults.ProcessedBytes, overallResults.ProcessedRows, overallResults.ProcessedTransactions, overallResults.FailedProcessedRows,
                    Convert.ToUInt64(operationDuration.TotalSeconds), _numberOfThreads, 4 + 4 * _tableAmplifier, overallResults.ResultType, DateTime.Now);
            }

            for (int i = 0; i < results.ResultSet.Length; i++)
            {
                TimeSpan threadDuration = (results.ResultSet[i].OperationEnd - results.ResultSet[i].OperationStart);
                double amountOfDataProcessedThisThread = results.ResultSet[i].ProcessedBytes;
                Dynamics.StorageUnit storageUnitThisThread = Dynamics.StorageUnit.Bytes;

                if (amountOfDataProcessed > 1024L && amountOfDataProcessed < (1024 * 1024))
                {
                    //convertToKB
                    amountOfDataProcessedThisThread = Math.Round(amountOfDataProcessed / (1024L), 2);
                    storageUnitThisThread = Dynamics.StorageUnit.Kilobytes;
                }
                else if (amountOfDataProcessed > (1024 * 1024) && amountOfDataProcessed < (1024 * 1024 * 1024))
                {
                    //convert to MB
                    amountOfDataProcessedThisThread = Math.Round(amountOfDataProcessed / (1024L * 1024L), 2);
                    storageUnitThisThread = Dynamics.StorageUnit.Megabytes;
                }
                else if (amountOfDataProcessed > (1024 * 1024 * 1024) && amountOfDataProcessed < (1024L * 1024L * 1024L * 1024L))
                {
                    //convert to GB
                    amountOfDataProcessedThisThread = Math.Round(amountOfDataProcessed / (1024 * 1024 * 1024), 2);
                    storageUnitThisThread = Dynamics.StorageUnit.Gigabytes;
                }
                else if (amountOfDataProcessed > (1024L * 1024L * 1024L * 1024L))
                {
                    //convert to TB
                    amountOfDataProcessedThisThread = Math.Round(amountOfDataProcessed / (1024L * 1024L * 1024L * 1024L), 2);
                    storageUnitThisThread = Dynamics.StorageUnit.Terabytes;
                }


                _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------Results for Thread " + (i + 1) + "------------------------ ", Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Info));
                double dataRateperformanceThisThread = Math.Round((results.ResultSet[i].ProcessedBytes) / (1024 * 1024) / threadDuration.TotalSeconds, 2);
                double rowRatePerformanceThisThread = Math.Round(results.ResultSet[i].ProcessedRows / operationDuration.TotalSeconds, 2);
                double transactionalPerformanceThisThread = Math.Round(results.ResultSet[i].ProcessedTransactions / operationDuration.TotalSeconds, 2);

                _messageQueue.AddMessage(new Message(DateTime.Now, "Data added                                    : " + amountOfDataProcessedThisThread + " " + storageUnitThisThread.ToString(), Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Rows added                                    : " + results.ResultSet[i].ProcessedRows, Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Transactions completed                        : " + results.ResultSet[i].ProcessedTransactions, Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Transactions failed                           : " + results.ResultSet[i].FailedProcessedRows, Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Total seconds                                 : " + threadDuration.TotalSeconds, Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data rate performance                         : " + dataRateperformanceThisThread + " Mb/s", Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Row rate performance                          : " + rowRatePerformanceThisThread + " Rows/s", Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Transactional performance                     : " + transactionalPerformanceThisThread + " Transactions/s", Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Info));

                if (_logData)
                {
                    logObject.LogDataEngineStatsThreads((i + 1), results.ResultSet[i].ProcessedBytes, results.ResultSet[i].ProcessedRows, results.ResultSet[i].ProcessedTransactions, results.ResultSet[i].FailedProcessedRows,
                    Convert.ToUInt64(threadDuration.TotalSeconds), results.ResultSet[i].ResultType, DateTime.Now);
                }

            }

            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "--                         END ADD REPORT                          -- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));


        }
        #endregion


    }
}
