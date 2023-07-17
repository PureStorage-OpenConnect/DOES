using DOES.Shared.Debug;
using DOES.Shared.Operations;
using DOES.Shared.Resources;
using DOES.DataEngine.Operations;
using DOES.DataEngine.Resources;
using ShellProgressBar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DOES.Cli
{
    public class Test : Operation
    {
        Task _task;
        CancellationToken _token;
        private CancellationToken _operationToken;
        CancellationTokenSource _operationTokenSource;
        CancellationTokenSource _tokenSource;
        MessageQueue _messageQueue = new MessageQueue();
        Analytic _ldes;
        Queue<InterimStat> _stats = new Queue<InterimStat>();
        Queue<InterimThreadStat> _threadStats = new Queue<InterimThreadStat>();
        DataVendor _driver = null;
        private Controller _initialController;
        private Controller _testController;
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
        private Dynamics.TestType _testType;
        private bool _deferInitialIngest;
        private int _changeRate;
        private double _growthRate;
        //MS SQL only parameter set 
        private string _instanceName;
        //Oracle only parameter set 
        private bool _useOracleSID;
        //SAP HANA only parameter set 
        private string _instanceNumber;
        private int _percentColumnTables;
        private int _percentPagedTables;
        private int _percentageWarmExtensionNode;
        private string _extensionNodeGroupName;
        private int _partitions;
        //MySQL only parameter set 
        private Dynamics.MySQLStorageEngine _mysqlEngine;
        private string _ndbtablespace;
        //MariaDB only Parameter Set 
        private Dynamics.MariaDBStorageEngine _mariaDBEngine;
        private int _portNumber;
        //MongoDB only parameter set 
        private Dynamics.MongoDBDeployment _mongoDBDeployment;
        //Logging Parameters 
        private bool _logData;
        private string _testname;
        private string _objectName;
        private string _objectCategory;
        private int _sequence;
        private bool _verboseWriter = false;

        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public override CancellationTokenSource TokenSource { get { return _tokenSource; } }

        public Test(string IP, Dynamics.Database dbType, string databaseName, UInt64 amountToAdd, Dynamics.StorageUnit unit, string user, string password, bool useOracleSID, int numberOfThreads,
            Dynamics.DatabaseSchema schema, int tableAmplifier, int columnWidth, string folderPath, double randomPercentage, Dynamics.TestType testType, bool deferInitialIngest, 
            int changeRate, double growthRate, string instanceName, string instanceNumber,
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
            _testType = testType;
            _deferInitialIngest = deferInitialIngest;
            _changeRate = changeRate;
            _growthRate = growthRate;
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
            Dynamics.DataEngineOperation operation = Dynamics.DataEngineOperation.Unknown;
            Mechanic serviceOperator = new Mechanic(_randomPercentage);

            _tokenSource = new CancellationTokenSource();
            _token = _tokenSource.Token;
            _operationTokenSource = new CancellationTokenSource();
            _operationToken = _operationTokenSource.Token;


            //note if folder excluded then run add , if included run import
            //must ensure units are converted to bytes
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
                DateTime start = DateTime.Now;
                DateTime end;
                DataEngineResultSet initialResults = null;
                if (!_deferInitialIngest)
                {
                    operation = Dynamics.DataEngineOperation.InsertData;
                    //Populate Database up to value
                    if (_driver != null)
                    {
                        _initialController = new Controller(_driver, _columnWidth, _numberOfThreads);
                        string response = serviceOperator.HandleTableCreateResponse(_schema, _initialController.CreateTablesAndIndexes());
                        _messageQueue.AddMessage(new Message(DateTime.Now, response, Message.MessageType.Info));
                        if (_folderPath != null)
                        {
                            //import using folder
                            DateTime operationStart = DateTime.Now;
                            _task = Task.Run(() =>
                            {
                                var capturedToken = _tokenSource;
                                _initialController.ImportFromFile(_folderPath, amountToAddBytes, capturedToken, null);
                            }, _token);
                        }
                        else
                        {
                            //import using web 
                            DateTime operationStart = DateTime.Now;
                            _task = Task.Run(() =>
                            {
                                var capturedToken = _tokenSource;
                                _initialController.ImportFromWeb(amountToAddBytes, capturedToken, null);
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

                    string importTopPbarMessage = "Overall progress for DOES.DataEngine import operation                Amount processed : 0 Bytes of 0 Bytes";

                    using (var progressBar = new ProgressBar(10000, importTopPbarMessage, options))
                    {
                        var dataRateChild = progressBar.Spawn(10000, "Monitor data rate for data insert operations for " + dbtypeIdentifier + " Database : " + _dbname + " Data rate performance : 0 MB/s ");
                        var rowRateChild = progressBar.Spawn(10000, "Monitor row insertion rate for data insert operations row rate performance 0 Rows/s");
                        var transactionRateChild = progressBar.Spawn(10000, "Monitor transaction insertion rate for data insert operations transaction performance 0 Transactions/s");


                        start = DateTime.Now;

                        while (!_initialController.GetResultsState() && !_token.IsCancellationRequested)
                        {
                            Thread.Sleep(1000);
                            DataEngineResults tempResults = _initialController.ReturnInterimResults();
                            end = DateTime.Now;
                            TimeSpan duration = (end - start);
                            lastResults = UpdateProgressBar(tempResults, duration, amountToAddBytes, lastResults,
                                 progressBar, dataRateChild, rowRateChild, transactionRateChild);
                            start = DateTime.Now;
                            WriteMessages(_ldes, operation);
                        }
                    }

                    start = DateTime.UtcNow;
                    if (!_token.IsCancellationRequested)
                    {
                        initialResults = _initialController.ReturnFinalResults();
                    }

                    _testController = new Controller(_driver, _columnWidth, _numberOfThreads, true);
                    if (_testType == Dynamics.TestType.Simple)
                    {
                        string testTopPbarMessage = "Overall progress for DOES.DataEngine Test operation                Amount processed : 0 Bytes of 0 Bytes";


                        operation = Dynamics.DataEngineOperation.TestSimple;
                        lastResults = new DataEngineResults(operation);

                        double changeRate = Math.Round((double)_changeRate / 100, 2);
                        UInt64 amounttoPerformOperationsOn = Convert.ToUInt64(Math.Round((Convert.ToDouble(amountToAddBytes)) * (changeRate), 0));
                        using (var progressBar = new ProgressBar(10000, testTopPbarMessage, options))
                        {
                            var dataRateChild = progressBar.Spawn(10000, "Monitor data rate for " + operation.ToString() + " for " + dbtypeIdentifier + " Database : " 
                                + dbtypeIdentifier + "Data rate performance : 0 MB/s");
                            var rowRateChild = progressBar.Spawn(10000, "Monitor row process rate for " + operation.ToString() + " row rate performance 0 Rows/s");
                            var transactionRateChild = progressBar.Spawn(10000, "Monitor transaction and query process rate for " + operation.ToString() + " transaction performance 0 Transactions/s");


                            _task = Task.Run(() =>
                            {
                                var capturedToken = _operationTokenSource;
                                _testController.TestSimple(_folderPath, amounttoPerformOperationsOn, capturedToken, _growthRate);
                            }, _operationToken);

                            while (!_testController.GetResultsState() && !_operationToken.IsCancellationRequested)
                            {
                                Thread.Sleep(2000);
                                DataEngineResults tempResults = _testController.ReturnInterimTestResults(operation);
                                end = DateTime.Now;
                                TimeSpan duration = (end - start);
                                if (tempResults != null)
                                {
                                    lastResults = UpdateProgressBarRunningTest(tempResults, duration, (amounttoPerformOperationsOn), lastResults,
                                        progressBar, dataRateChild, rowRateChild, transactionRateChild);
                                    if (_logData)
                                    {
                                        DataEngineResultSet tempThreadResults = _testController.ReturnInterimTestThreadResults();
                                        if (tempThreadResults != null) { lastResultSet = AddDataEngineInterimThreadResults(tempThreadResults, duration, lastResultSet); }
                                    }
                                    start = DateTime.Now;
                                    WriteMessages(_ldes, operation);
                                }
                            }
                        }

                        if (!_operationToken.IsCancellationRequested)
                        {
                            DataEngineResultSet results = _testController.ReturnFinalResults();
                            if (_logData)
                            {
                                InitlialiseLogging();
                            }
                            WriteFinalReport(initialResults, results, _ldes, operation);
                        }
                        //SQL
                        //Populate database up to size 
                        //Run paralell queries for insert update & delete up to a value 
                        //HANA
                        //Populate database up to size 
                        //Run paralell queries for insert update & delete up to a value 
                    }
                    else if (_testType == Dynamics.TestType.Advanced)
                    {
                        operation = Dynamics.DataEngineOperation.TestAdvanced;
                        lastResults = new DataEngineResults(Dynamics.DataEngineOperation.TestAdvanced);

                        double changeRate = Math.Round((double)_changeRate / 100, 2);
                        UInt64 amounttoPerformOperationsOn = Convert.ToUInt64(Math.Round((Convert.ToDouble(amountToAddBytes)) * (changeRate), 0));
                        string testTopPbarMessage = "Overall progress for DOES.DataEngine Test operation                Amount processed : 0 Bytes of 0 Bytes";
                        using (var progressBar = new ProgressBar(10000, testTopPbarMessage, options))
                        {
                            var dataRateChild = progressBar.Spawn(0, "Monitor data rate for " + operation.ToString() + " for " + dbtypeIdentifier + " Database : " 
                                + dbtypeIdentifier + "Data rate performance : 0 MB/s");
                            var rowRateChild = progressBar.Spawn(0, "Monitor row process rate for " + operation.ToString() + " row rate performance 0 Rows/s");
                            var transactionRateChild = progressBar.Spawn(0, "Monitor transaction and query process rate for " + operation.ToString() + " transaction performance 0 Transactions/s");

                            _task = Task.Run(() =>
                            {
                                var capturedToken = _operationTokenSource;
                                _testController.TestAdvanced(_folderPath, amounttoPerformOperationsOn, capturedToken, _growthRate);
                            }, _operationToken);

                            while (!_testController.GetResultsState() && !_operationToken.IsCancellationRequested)
                            {
                                Thread.Sleep(2000);
                                DataEngineResults tempResults = _testController.ReturnInterimTestResults(operation);
                                end = DateTime.Now;
                                TimeSpan duration = (end - start);
                                if (tempResults != null)
                                {
                                    lastResults = UpdateProgressBarRunningTest(tempResults, duration, (amounttoPerformOperationsOn), lastResults,
                                        progressBar, dataRateChild, rowRateChild, transactionRateChild);
                                    if (_logData)
                                    {
                                        DataEngineResultSet tempThreadResults = _testController.ReturnInterimTestThreadResults();
                                        if (tempThreadResults != null) { lastResultSet = AddDataEngineInterimThreadResults(tempThreadResults, duration, lastResultSet); }
                                    }
                                    start = DateTime.Now;
                                    WriteMessages(_ldes, operation);
                                    //if (tempResults.ProcessedBytes > amounttoPerformOperationsOn)
                                    //{
                                    //    dataEngineOperObj.SetRequestOverrunTrue();
                                    //}
                                }
                            }
                        }

                        if (!_operationToken.IsCancellationRequested)
                        {
                            DataEngineResultSet results = _testController.ReturnFinalResults();
                            if (_logData)
                            {
                                InitlialiseLogging();
                            }
                            WriteFinalReport(initialResults, results, _ldes, operation);
                        }
                        //SQL
                        //Populate database up to size 
                        //Run paralell queries for Insert & delete up to a value (10% of total database size)  
                        //Run a complex OLAP query up to value (10% of total database size) 
                        //Inserts and updates are partially merged with instead of insert take PageID from deleted row and update with new data//Upsert
                        //Rebuilds Indexes
                        //HANA
                        //Populate database up to size 
                        //Run paralell queries for Insert & delete up to a value (10% of total database size)  
                        //Run a complex OLAP query up to value (10% of total database size) 
                        //Inserts and updates are partially merged with instead of insert take PageID from deleted row and update with new data 
                        //Runs a Delta Merge 
                    }
                    else if (_testType == Dynamics.TestType.Complex)
                    {
                        operation = Dynamics.DataEngineOperation.TestComplex;
                        lastResults = new DataEngineResults(operation);

                        double changeRate = Math.Round((double)_changeRate / 100, 2);
                        UInt64 amounttoPerformOperationsOn = Convert.ToUInt64(Math.Round((Convert.ToDouble(amountToAddBytes)) * (changeRate), 0));
                        string testTopPbarMessage = "Overall progress for DOES.DataEngine Test operation                Amount processed : 0 Bytes of 0 Bytes";
                        using (var progressBar = new ProgressBar(10000, testTopPbarMessage, options))
                        {
                            var dataRateChild = progressBar.Spawn(0, "Monitor data rate for " + operation.ToString() + " for " + dbtypeIdentifier + " Database : " + dbtypeIdentifier + 
                                "Data rate performance : 0 MB/s");
                            var rowRateChild = progressBar.Spawn(0, "Monitor row process rate for " + operation.ToString() + " row rate performance 0 Rows/s");
                            var transactionRateChild = progressBar.Spawn(0, "Monitor transaction and query process rate for " + operation.ToString() + " transaction performance 0 Transactions/s");

                            _task = Task.Run(() =>
                    {
                        var capturedToken = _operationTokenSource;
                        _testController.TestComplex(_folderPath, amounttoPerformOperationsOn, capturedToken, _growthRate);
                    }, _operationToken);

                            while (!_testController.GetResultsState() && !_operationToken.IsCancellationRequested)
                            {
                                Thread.Sleep(1000);
                                DataEngineResults tempResults = _testController.ReturnInterimTestResults(operation);
                                end = DateTime.Now;
                                TimeSpan duration = (end - start);
                                if (tempResults != null)
                                {
                                    lastResults = UpdateProgressBarRunningTest(tempResults, duration, (amounttoPerformOperationsOn), lastResults,
                                        progressBar, dataRateChild, rowRateChild, transactionRateChild);
                                    if (_logData)
                                    {
                                        DataEngineResultSet tempThreadResults = _testController.ReturnInterimTestThreadResults();
                                        if (tempThreadResults != null) { lastResultSet = AddDataEngineInterimThreadResults(tempThreadResults, duration, lastResultSet); }
                                    }
                                    start = DateTime.Now;
                                    WriteMessages(_ldes, operation);
                                }
                            }
                        }

                        if (!_operationToken.IsCancellationRequested)
                        {
                            DataEngineResultSet results = _testController.ReturnFinalResults();
                            if (_logData)
                            {
                                InitlialiseLogging();
                            }
                            WriteFinalReport(initialResults, results, _ldes, operation);
                        }

                        //SQL
                        //Populate database up to size 
                        //Run paralell queries for insert, update & delete up to a value (10% of total database size)  
                        //Run a complex OLAP query up to value (10% of total database size) 
                        //Inserts and updates are partially merged with instead of insert take PageID from deleted row and update with new data 
                        //Write in-line data to a single table every 200-500ms (assume once the row is commited the table is treated as readonly (PointInTimeWrite)
                        //Rebuild indexes
                        //Create savepoint every 5-10% of database 
                        //Once completed do a full DBCC check 
                        //HANA
                        //Populate database up to size 
                        //Run paralell queries for insert, update & delete up to a value (10% of total database size)  
                        //Run a complex OLAP query up to value (10% of total database size) 
                        //Inserts and updates are partially merged with instead of insert take PageID from deleted row and update with new data 
                        //Write in-line data to a single table every 200-500ms (assume once the row is commited the table is treated as readonly (PointInTimeWrite)
                        //Run DeltaMerge every 5-10% of database operation completed
                        //Once completed do a consistency check as detailed in the note (https://launchpad.support.sap.com/#/notes/0002116157)
                        //Unload 10% of column tables
                    }
                    WriteMessages(_ldes, operation);
                }
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

                UInt64 interimProcessedBytes = results.ProcessedBytes - oldResults.ProcessedBytes;
                UInt64 interimProcessedRows = results.ProcessedRows - oldResults.ProcessedRows;
                UInt64 interimProcessedTransactions = results.ProcessedTransactions - oldResults.ProcessedTransactions;

                double interimProcessedBytesDBL = Convert.ToDouble(interimProcessedBytes);

                Tuple<double, Dynamics.StorageUnit> normalisedValues = ReturnConvertedUnits(interimProcessedBytesDBL);
                string storageUnitDR = "";
                if (normalisedValues.Item2 == Dynamics.StorageUnit.Bytes) { storageUnitDR = " bytes/s"; }
                else if (normalisedValues.Item2 == Dynamics.StorageUnit.Megabytes) { storageUnitDR = " MB/s"; }
                else if (normalisedValues.Item2 == Dynamics.StorageUnit.Gigabytes) { storageUnitDR = "GB/s"; }
                else if (normalisedValues.Item2 == Dynamics.StorageUnit.Terabytes) { storageUnitDR = " TB/s"; }



                Double dataRateperformance = Math.Round((normalisedValues.Item1) / span.TotalSeconds, 2);
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
                rowRateBar.Tick(percentageComplete, "Row rate performance: " + rowRatePerformance + " Rows / s");
                transactionRateBar.Tick(percentageComplete, "Transactional performance  : " + transactionalPerformance + " Query/s");


                if (_logData)
                {
                    _stats.Enqueue(new InterimStat(results.ProcessedBytes, rowRatePerformance, transactionalPerformance, dataRateperformance, results.ResultType, DateTime.Now));
                }
            }
            catch (Exception ex) { _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error)); }
            return results;
        }

        private DataEngineResults UpdateProgressBarRunningTest(DataEngineResults results, TimeSpan span, Double RequestedBytes, DataEngineResults oldResults,
            ProgressBar pbar, ChildProgressBar dataRateBar, ChildProgressBar rowRateBar, ChildProgressBar transactionRateBar)
        {
            try
            {
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

                UInt64 interimProcessedBytes = results.ProcessedBytes - oldResults.ProcessedBytes;
                UInt64 interimProcessedRows = results.ProcessedRows - oldResults.ProcessedRows;
                UInt64 interimProcessedTransactions = results.ProcessedTransactions - oldResults.ProcessedTransactions;

                double interimProcessedBytesDBL = Convert.ToDouble(interimProcessedBytes);
                Tuple<double, Dynamics.StorageUnit> normalisedValues = ReturnConvertedUnits(interimProcessedBytesDBL);
                string storageUnitDR = "";
                if (normalisedValues.Item2 == Dynamics.StorageUnit.Bytes) { storageUnitDR = " bytes/s"; }
                else if (normalisedValues.Item2 == Dynamics.StorageUnit.Megabytes) { storageUnitDR = " MB/s"; }
                else if (normalisedValues.Item2 == Dynamics.StorageUnit.Gigabytes) { storageUnitDR = " GB/s"; }
                else if (normalisedValues.Item2 == Dynamics.StorageUnit.Terabytes) { storageUnitDR = " TB/s"; }

                Double dataRateperformance = Math.Round(normalisedValues.Item1 / span.TotalSeconds, 2);
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
                rowRateBar.Tick(percentageComplete, "Row rate performance: " + rowRatePerformance + " Rows / s");
                transactionRateBar.Tick(percentageComplete, "Transactional performance  : " + transactionalPerformance + " Query/s");

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
            if (oldResults.ResultCompilation == null)
            {
                oldResults = resultSet;
            }
            for (int i = 0; i < resultSet.ResultCompilation.Length; i++)
            {
                for (int i2 = 0; i2 < resultSet.ResultCompilation[i].Length; i2++)
                {
                    if (resultSet.ResultCompilation[i][i2] != null)
                    {
                        if (i < oldResults.ResultCompilation.Length)
                        {
                            if (oldResults.ResultCompilation[i][i2] == null)
                            {
                                oldResults.ResultCompilation[i][i2] = new DataEngineResults(0, 0, 0, 0);
                            }
                            UInt64 interimProcessedBytes = resultSet.ResultCompilation[i][i2].ProcessedBytes - oldResults.ResultCompilation[i][i2].ProcessedBytes;
                            UInt64 interimProcessedRows = resultSet.ResultCompilation[i][i2].ProcessedRows - oldResults.ResultCompilation[i][i2].ProcessedRows;
                            UInt64 interimProcessedTransactions = resultSet.ResultCompilation[i][i2].ProcessedTransactions - oldResults.ResultCompilation[i][i2].ProcessedTransactions;
                            //MB/s Progress
                            Double dataRateperformance = Math.Round((interimProcessedBytes) / (1024 * 1024) / span.TotalSeconds, 2);
                            Double rowRatePerformance = Math.Round(interimProcessedRows / span.TotalSeconds, 2);
                            Double transactionalPerformance = Math.Round(interimProcessedTransactions / span.TotalSeconds, 2);


                            _threadStats.Enqueue(new InterimThreadStat(((i + 1) * (i2 + 1)), resultSet.ResultCompilation[i][i2].ProcessedBytes, rowRatePerformance, transactionalPerformance,
                                dataRateperformance, resultSet.ResultCompilation[i][i2].ResultType,
                                DateTime.Now));
                        }
                        else
                        {
                            oldResults = resultSet;
                        }
                    }
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
                    if (_verboseWriter)
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

        private Tuple<double, Dynamics.StorageUnit> ReturnConvertedUnits(double bytesToConvert)
        {
            Dynamics.StorageUnit storageUnit = Dynamics.StorageUnit.Bytes;
            if (bytesToConvert > 1024L && bytesToConvert < (1024 * 1024))
            {
                //convertToKB
                bytesToConvert = Math.Round(bytesToConvert / (1024L), 2);
                storageUnit = Dynamics.StorageUnit.Kilobytes;
            }
            else if (bytesToConvert > (1024 * 1024) && bytesToConvert < (1024 * 1024 * 1024))
            {
                //convert to MB
                bytesToConvert = Math.Round(bytesToConvert / (1024L * 1024L), 2);
                storageUnit = Dynamics.StorageUnit.Megabytes;
            }
            else if (bytesToConvert > (1024 * 1024 * 1024) && bytesToConvert < (1024L * 1024L * 1024L * 1024L))
            {
                //convert to GB
                bytesToConvert = Math.Round(bytesToConvert / (1024 * 1024 * 1024), 2);
                storageUnit = Dynamics.StorageUnit.Gigabytes;
            }
            else if (bytesToConvert > (1024L * 1024L * 1024L * 1024L))
            {
                //convert to TB
                bytesToConvert = Math.Round(bytesToConvert / (1024L * 1024L * 1024L * 1024L), 2);
                storageUnit = Dynamics.StorageUnit.Terabytes;
            }
            Tuple<double, Dynamics.StorageUnit> rolledUpValues = new Tuple<double, Dynamics.StorageUnit>(bytesToConvert, storageUnit);
            return rolledUpValues;
        }

        private void WriteFinalReport(DataEngineResultSet initialImportResults, DataEngineResultSet operationResults, Analytic logObject, Dynamics.DataEngineOperation operation)
        {
            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            if (operation == Dynamics.DataEngineOperation.TestSimple)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "--                       SIMPLE TEST COMPLETED                     -- ", Message.MessageType.Report));
            }
            else if (operation == Dynamics.DataEngineOperation.TestAdvanced)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "--                      ADVANCED TEST COMPLETED                    -- ", Message.MessageType.Report));
            }
            else if (operation == Dynamics.DataEngineOperation.TestComplex)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "--                      COMPLEX TEST COMPLETED                     -- ", Message.MessageType.Report));
            }
            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));

            if (initialImportResults != null)
            {
                TimeSpan initialImportDuration = (initialImportResults.OperationEnd - initialImportResults.OperationStart);
                DataEngineResults overallInitialImportResults = initialImportResults.ReturnAggregateResultSet();
                double amountOfDataImported = overallInitialImportResults.ProcessedBytes;
                Tuple<double, Dynamics.StorageUnit> amountOfDataImportedAdjuusted = ReturnConvertedUnits(amountOfDataImported);
                //Overall Results for intial import 
                double dataRateperformanceWrite = Math.Round((overallInitialImportResults.ProcessedBytes) / (1024 * 1024) / initialImportDuration.TotalSeconds, 2);
                double rowRatePerformanceWrite = Math.Round(overallInitialImportResults.ProcessedRows / initialImportDuration.TotalSeconds, 2);
                double transactionalPerformanceWrite = Math.Round(overallInitialImportResults.ProcessedTransactions / initialImportDuration.TotalSeconds, 2);
                _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Initial Import Results ------------------- ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Initial data processed                        : " + amountOfDataImportedAdjuusted.Item1 + " " + amountOfDataImportedAdjuusted.Item2.ToString(), Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Initial rows added                            : " + overallInitialImportResults.ProcessedRows, Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Initial number of transactions completed      : " + overallInitialImportResults.ProcessedTransactions, Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Initial number of transactions failed         : " + overallInitialImportResults.FailedProcessedRows, Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Initial ingest total seconds                  : " + initialImportDuration.TotalSeconds, Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Initial ingest data rate performance          : " + dataRateperformanceWrite + " Mb/s", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Initial ingest row rate performance           : " + rowRatePerformanceWrite + " Rows/s", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Initial ingest transactional performance      : " + transactionalPerformanceWrite + " Transactions/s", Message.MessageType.Report));
                _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
            }

            TimeSpan operationDuration = (operationResults.OperationEnd - operationResults.OperationStart);
            DataEngineResults overallOperationResults = operationResults.ReturnAggregateTestResultSet(operation);
            double amountOfDataProcessed = overallOperationResults.ProcessedBytes;
            Tuple<double, Dynamics.StorageUnit> amountOfDataProcessedAdjuusted = ReturnConvertedUnits(amountOfDataProcessed);
            //Overall Results for operation process 
            double dataRateperformance = Math.Round((overallOperationResults.ProcessedBytes) / (1024 * 1024) / operationDuration.TotalSeconds, 2);
            double rowRatePerformance = Math.Round(overallOperationResults.ProcessedRows / operationDuration.TotalSeconds, 2);
            double transactionalPerformance = Math.Round(overallOperationResults.ProcessedTransactions / operationDuration.TotalSeconds, 2);

            if (operation == Dynamics.DataEngineOperation.TestSimple)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Simple Test Results ------------------- ", Message.MessageType.Report));
            }
            else if (operation == Dynamics.DataEngineOperation.TestAdvanced)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------- Advanced Test Results ------------------- ", Message.MessageType.Report));
            }
            else if (operation == Dynamics.DataEngineOperation.TestComplex)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------------- Complex Test Results ------------------- ", Message.MessageType.Report));
            }
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Data processed                                : " + amountOfDataProcessedAdjuusted.Item1 + " " + amountOfDataProcessedAdjuusted.Item2.ToString(), Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Rows processed                                : " + overallOperationResults.ProcessedRows, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Number of transactions completed              : " + overallOperationResults.ProcessedTransactions, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Number of transactions failed                 : " + overallOperationResults.FailedProcessedRows, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Test total seconds                            : " + operationDuration.TotalSeconds, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Data rate performance                         : " + dataRateperformance + " Mb/s", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Row rate performance                          : " + rowRatePerformance + " Rows/s", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Transactional/Query performance               : " + transactionalPerformance + " Transactions/s", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));

            if (_logData)
            {
                logObject.LogDataEngineStatsTotal(overallOperationResults.ProcessedBytes, overallOperationResults.ProcessedRows, overallOperationResults.ProcessedTransactions, overallOperationResults.FailedProcessedRows,
                    Convert.ToUInt64(operationDuration.TotalSeconds), _numberOfThreads, 4 + 4 * _tableAmplifier, overallOperationResults.ResultType, DateTime.Now);
            }

            for (int i = 0; i < operationResults.ResultCompilation.Length; i++)
            {
                for (int i2 = 0; i2 < operationResults.ResultCompilation[i].Length; i2++)
                {
                    TimeSpan threadDuration = (operationResults.ResultCompilation[i][i2].OperationEnd - operationResults.ResultCompilation[i][i2].OperationStart);
                    double amountOfDataProcessedThisThread = operationResults.ResultCompilation[i][i2].ProcessedBytes;

                    if (_logData)
                    {
                        logObject.LogDataEngineStatsThreads(((i + 1) * (i2 + 1)), operationResults.ResultCompilation[i][i2].ProcessedBytes, operationResults.ResultCompilation[i][i2].ProcessedRows,
                            operationResults.ResultCompilation[i][i2].ProcessedTransactions, operationResults.ResultCompilation[i][i2].FailedProcessedRows,
                            Convert.ToUInt64(threadDuration.TotalSeconds), operationResults.ResultCompilation[i][i2].ResultType, DateTime.Now);
                    }
                }
            }

            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            if (operation == Dynamics.DataEngineOperation.TestSimple)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "--                       END SIMPLE TEST REPORT                -- ", Message.MessageType.Report));
            }
            else if (operation == Dynamics.DataEngineOperation.TestAdvanced)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "--                     END ADVANCED TEST REPORT                -- ", Message.MessageType.Report));
            }
            else if (operation == Dynamics.DataEngineOperation.TestComplex)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "--                      END COMPLEX TEST REPORT                -- ", Message.MessageType.Report));
            }
            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
        }
        #endregion
    }
}
