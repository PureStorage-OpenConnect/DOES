using System;
using System.Threading.Tasks;
using System.Management.Automation;
using System.Threading;
using System.Collections.Generic;
using DOES.Shared.Resources;
using DOES.DataEngine.Resources;
using DOES.Shared.Debug;
using DOES.DataEngine.Operations;
using DOES.Shared.Operations;

namespace DOES.PowerShell
{
    [Cmdlet(VerbsCommon.Search, "DataEngine", DefaultParameterSetName = "MS SQL")]
    public class SearchDataEngineCmdLet : Cmdlet
    {
        #region Parameters 
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "MS SQL")]
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "Oracle")]
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "MySQL")]
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "MariaDB")]
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "MongoDB")]
        public Dynamics.Database DatabaseType { get; set; }
        private string _ip = "localhost";
        [Parameter(Position = 1, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 1, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 1, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 1, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 1, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 1, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 1, Mandatory = false, ParameterSetName = "MongoDB")]
        [Alias("IPAddress")]
        public string Hostname { get { return _ip; } set { _ip = value; } }
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "MS SQL")]
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "Oracle")]
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "MySQL")]
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "MariaDB")]
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "MongoDB")]
        public string DatabaseName { get; set; }
        private string _user = null;
        [Parameter(Position = 3, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 3, Mandatory = true, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 3, Mandatory = true, ParameterSetName = "Oracle")]
        [Parameter(Position = 3, Mandatory = true, ParameterSetName = "MySQL")]
        [Parameter(Position = 3, Mandatory = true, ParameterSetName = "MariaDB")]
        [Parameter(Position = 3, Mandatory = true, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 3, Mandatory = true, ParameterSetName = "MongoDB")]
        public string UserName { get { return _user; } set { _user = value; } }
        private string _password = null;
        [Parameter(Position = 4, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "Oracle")]
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "MySQL")]
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "MariaDB")]
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "MongoDB")]
        public string Password { get { return _password; } set { _password = value; } }
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "MS SQL")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "Oracle")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "MySQL")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "MariaDB")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "MongoDB")]
        public UInt64 Amount { get; set; }
        [Parameter(Position = 6, Mandatory = true, ParameterSetName = "MS SQL")]
        [Parameter(Position = 6, Mandatory = true, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 6, Mandatory = true, ParameterSetName = "Oracle")]
        [Parameter(Position = 6, Mandatory = true, ParameterSetName = "MySQL")]
        [Parameter(Position = 6, Mandatory = true, ParameterSetName = "MariaDB")]
        [Parameter(Position = 6, Mandatory = true, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 6, Mandatory = true, ParameterSetName = "MongoDB")]
        public Dynamics.StorageUnit Unit { get; set; }
        private int _numberOfThreads = 1;
        [Parameter(Position = 7, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 7, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 7, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 7, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 7, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 7, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 7, Mandatory = false, ParameterSetName = "MongoDB")]
        public int NumberOfThreads { get { return _numberOfThreads; } set { _numberOfThreads = value; } }
        private Dynamics.DatabaseSchema _schema = Dynamics.DatabaseSchema.WithIndexesLOB;
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "MongoDB")]
        public Dynamics.DatabaseSchema SchemaType { get { return _schema; } set { _schema = value; } }
        private int _tableAmplifier = 8;
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "MongoDB")]
        public int TableAmplification { get { return _tableAmplifier; } set { _tableAmplifier = value; } }
        private Dynamics.ReadQuery _queryType = Dynamics.ReadQuery.UnionAll;
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "MongoDB")]
        public Dynamics.ReadQuery QueryType { get { return _queryType; } set { _queryType = value; } }
        private int _columnWidth = 2147483647;
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "MongoDB")]
        public int ColumnWidth { get { return _columnWidth; } set { if (value <= 2147483647) { _columnWidth = value; } } }

        //MS SQL only parameter set 
        private string _instanceName = null;
        [Parameter(Mandatory = false, ParameterSetName = "MS SQL")]
        public string NamedInstance { get { return _instanceName; } set { _instanceName = value; } }


        //Oracle only parameter set 
        private bool _useOracleSID = false;
        public bool UseOracleSID { get { return _useOracleSID; } set { _useOracleSID = value; } }

        //SAP HANA only parameter set 
        private string _instanceNumber = "00";
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public string InstanceNumber { get { return _instanceNumber; } set { _instanceNumber = value; } }
        private int _percentColumnTables = 80;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int PercentageColumns { get { return _percentColumnTables; } set { _percentColumnTables = value; } }
        private int _percentPagedTables = 0;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int PercentagePagedTables { get { return _percentPagedTables; } set { _percentPagedTables = value; } }
        private int _percentageWarmExtensionNode = 0;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int PercentageWarmExtensionNodeTables { get { return _percentageWarmExtensionNode; } set { _percentageWarmExtensionNode = value; } }
        private string _extensionNodeGroupName = "<my_warm_data>";
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public string ExtensionNodeGroupName { get { return _extensionNodeGroupName; } set { _extensionNodeGroupName = value; } }
        private int _partitions = -1;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int Partitions { get { return _partitions; } set { _partitions = value; } }

        //MySQL only parameter set 
        private Dynamics.MySQLStorageEngine _mysqlEngine = Dynamics.MySQLStorageEngine.InnoDB;
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        public Dynamics.MySQLStorageEngine StorageEngine { get { return _mysqlEngine; } set { _mysqlEngine = value; } }
        private string _ndbtablespace;
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        public string NDBTableSpace { get { return _ndbtablespace; } set { _ndbtablespace = value; } }

        //MariaDB only parameter set 
        private Dynamics.MariaDBStorageEngine _mariaDBEngine = Dynamics.MariaDBStorageEngine.InnoDB;
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        public Dynamics.MariaDBStorageEngine MariaDBStorageEngine { get { return _mariaDBEngine; } set { _mariaDBEngine = value; } }

        //MongoDB only parameter set 
        private Dynamics.MongoDBDeployment _mongoDBDeploymentType = Dynamics.MongoDBDeployment.StandAlone;
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public Dynamics.MongoDBDeployment MongoDBDeploymentType { get { return _mongoDBDeploymentType; } set { _mongoDBDeploymentType = value; } }

        private int _portNumber = 0;
        [Parameter(Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public int Port { get { return _portNumber; } set { _portNumber = value; } }

        //Logging Parameters 
        private bool _logData;
        [Parameter(Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public SwitchParameter LogData
        {
            get { return _logData; }
            set { _logData = value; }
        }
        private string _testname = "";
        [Parameter(Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public string TestName { get { return _testname; } set { _testname = value; } }
        private string _objectName = "";
        [Parameter(Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public string ObjectName { get { return _objectName; } set { _objectName = value; } }
        private string _objectCategory = null;
        [Parameter(Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public string ObjectCategory { get { return _objectCategory; } set { _objectCategory = value; } }
        private int _sequence = 1;
        [Parameter(Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public int Sequence { get { return _sequence; } set { _sequence = value; } }
        #endregion

        #region Global variables
        private Task _task;
        private CancellationToken _token;
        private CancellationTokenSource _tokenSource;
        private ProgressRecord _progressDataRate;
        private ProgressRecord _progressByteCount;
        private ProgressRecord _progressOperations;
        private MessageQueue _messageQueue = new MessageQueue();
        private Analytic _ldes;
        private Queue<InterimStat> _stats = new Queue<InterimStat>();
        private Queue<InterimThreadStat> _threadStats = new Queue<InterimThreadStat>();
        private DataVendor _driver;
        private Controller _controller;
        #endregion

        #region Powershell Functions
        protected override void BeginProcessing()
        {
            base.BeginProcessing();
        }

        protected override void ProcessRecord()
        {
            Dynamics.DataEngineOperation operation = Dynamics.DataEngineOperation.QueryData;
            Dynamics.Database _dbType = DatabaseType;
            string _IP = Hostname;
            string _dbname = DatabaseName;
            string _user = UserName;
            string _password = Password;
            UInt64 _amountToQuery = Amount;
            Dynamics.StorageUnit _unit = Unit;
            Mechanic serviceOperator = new Mechanic();

            UInt64 amountToQueryBytes = serviceOperator.ReturnDataUnitsAsBytes(_amountToQuery, _unit);
            DataEngineResults lastResults = new DataEngineResults(Dynamics.DataEngineOperation.QueryData);
            DataEngineResultSet lastResultSet = new DataEngineResultSet(_numberOfThreads);

            _tokenSource = new CancellationTokenSource();
            _token = _tokenSource.Token;

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
                catch(InvalidOperationException ex)
                {
                    ErrorRecord er = new ErrorRecord(ex, (_schema.ToString() + " is not a supported Schema type for SAP HANA"), ErrorCategory.InvalidOperation, _schema);
                    WriteError(er);
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
                catch (InvalidOperationException ex)
                {
                    ErrorRecord er = new ErrorRecord(ex, (_schema.ToString() + " is not a supported Schema type for Oracle"), ErrorCategory.InvalidOperation, _schema);
                    WriteError(er);
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
                        _driver = new MySQLDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, _schema, serviceOperator, _mysqlEngine, _ndbtablespace,_messageQueue);
                    }
                }
                catch (InvalidOperationException ex)
                {
                    ErrorRecord er = new ErrorRecord(ex, (_schema.ToString() + " is not a supported Schema type for MySQL"), ErrorCategory.InvalidOperation, _schema);
                    WriteError(er);
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
                catch (InvalidOperationException ex)
                {
                    ErrorRecord er = new ErrorRecord(ex, (_schema.ToString() + " is not a supported Schema type for MariaDB"), ErrorCategory.InvalidOperation, _schema);
                    WriteError(er);
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
                catch (InvalidOperationException ex)
                {
                    ErrorRecord er = new ErrorRecord(ex, (_schema.ToString() + " is not a supported Schema type for PostgreSQL"), ErrorCategory.InvalidOperation, _schema);
                    WriteError(er);
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
                        _driver = new MongoDBOper(_ip, _dbname, _user, _password, _portNumber, _tableAmplifier, _schema, _mongoDBDeploymentType, serviceOperator, _messageQueue);
                    }
                }
                catch (InvalidOperationException ex)
                {
                    ErrorRecord er = new ErrorRecord(ex, (_schema.ToString() + " is not a supported Schema type for MongoDB"), ErrorCategory.InvalidOperation, _schema);
                    WriteError(er);
                }
            }
            try
            {
                if (_driver != null)
                {
                    _controller = new Controller(_driver, _columnWidth, _numberOfThreads);
                    string response = serviceOperator.HandleTableCreateResponse(_schema, _controller.CreateTablesAndIndexes());
                    _messageQueue.AddMessage(new Message(DateTime.Now, response, Message.MessageType.Info));
                    DateTime operationStart = DateTime.Now;
                    _task = Task.Run(() =>
                    {
                        var capturedToken = _tokenSource;
                        if (_queryType == Dynamics.ReadQuery.LeftOuterJoin)
                        {
                            _controller.QueryDataLeftOuterJoin(amountToQueryBytes, capturedToken, null, null);
                        }
                        else if (_queryType == Dynamics.ReadQuery.UnionAll)
                        {
                            _controller.QueryDataUnionAll(amountToQueryBytes, capturedToken, null, null);
                        }
                    }, _token);
                }



                CreateProgress();

                DateTime start = DateTime.Now;
                DateTime end;

                while (!_controller.GetResultsState() && !_token.IsCancellationRequested)
                {
                    Thread.Sleep(1000);
                    DataEngineResults tempResults = _controller.ReturnInterimResults();
                    end = DateTime.Now;
                    TimeSpan duration = (end - start);
                    lastResults = UpdateProgressBar(tempResults, duration, amountToQueryBytes, lastResults);
                    if (LogData)
                    {
                        DataEngineResultSet tempThreadResults = _controller.ReturnInterimThreadResults();
                        lastResultSet = AddDataEngineInterimThreadResults(tempThreadResults, duration, lastResultSet);
                    }
                    start = DateTime.Now;
                    WriteMessages(_ldes, operation);
                }


                if (!_token.IsCancellationRequested)
                {
                    ClearProgress();
                    DataEngineResultSet results = _controller.ReturnFinalResults();
                    if (LogData)
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

        protected override void EndProcessing()
        {
            base.EndProcessing();
        }

        protected override void StopProcessing()
        {
            _tokenSource.Cancel();
            WriteMessages(null, Dynamics.DataEngineOperation.Unknown);
            base.StopProcessing();
        }
        #endregion

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
                LogData = false;
            }
        }

        private void CreateProgress()
        {

            _progressDataRate = new ProgressRecord(0,   "Data Rate          ", " Data rate performance : 0 MB/s");
            _progressOperations = new ProgressRecord(2, "Query Rate         ", " 0 Transactions/s");
            _progressByteCount = new ProgressRecord(3,  "Overall progress   ", "Amount processed : 0 Bytes of 0 Bytes");
            _progressDataRate.RecordType = ProgressRecordType.Processing;
            _progressOperations.RecordType = ProgressRecordType.Processing;
            _progressByteCount.RecordType = ProgressRecordType.Processing;
        }

        private void ClearProgress()
        {
            _progressDataRate.RecordType = ProgressRecordType.Completed;
            _progressOperations.RecordType = ProgressRecordType.Completed;
            _progressByteCount.RecordType = ProgressRecordType.Completed;
            WriteProgress(_progressDataRate);
            WriteProgress(_progressOperations);
            WriteProgress(_progressByteCount);
        }

        private DataEngineResults UpdateProgressBar(DataEngineResults results, TimeSpan span, UInt64 RequestedBytes, DataEngineResults oldResults)
        {
            try
            {
                //% Progress
                if (results.ProcessedBytes < RequestedBytes)
                {
                    int percentageComplete = (int)(Math.Round(((double)results.ProcessedBytes / (double)RequestedBytes) * 100, 0));
                    _progressByteCount.PercentComplete = percentageComplete;
                }
                else
                {
                    _progressByteCount.PercentComplete = 100;
                }

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
                Double dataRateperformance = Math.Round(interimProcessedBytesDBL / span.TotalSeconds , 2);
                Double rowRatePerformance = Math.Round(interimProcessedRows / span.TotalSeconds, 2);
                Double transactionalPerformance = Math.Round(interimProcessedTransactions / span.TotalSeconds, 2);
                _progressDataRate.StatusDescription =   "   Data rate performance        : " + dataRateperformance + storageUnitDR;
                _progressOperations.StatusDescription = "   Query performance            : " + transactionalPerformance + " Query/s";
                _progressByteCount.StatusDescription =  "   Amount processed             : " + processedBytesDBL + " " + storageUnitProcess +
                    " of " + requestedBytesDBL + " " + storageUnitProcess;


                WriteProgress(_progressDataRate);
                WriteProgress(_progressOperations);
                WriteProgress(_progressByteCount);

                if (LogData)
                {
                    _stats.Enqueue(new InterimStat(results.ProcessedBytes, rowRatePerformance, transactionalPerformance, dataRateperformanceMBs, results.ResultType, DateTime.Now));
                }
            }
            catch (PipelineStoppedException) { }
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
                        oldResults.ResultSet[i] = new DataEngineResults(0, 0, 0, 0);
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
                    WriteVerbose(messageToParse.Item2);
                }
                 else if (messageToParse.Item1 == Message.MessageType.Error)
                {
                    Console.WriteLine(messageToParse.Item2);
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, _operation.ToString() + messageToParse.Item2);
                }
                else
                {
                    WriteVerbose(messageToParse.Item2);
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, _operation.ToString() + messageToParse.Item2);
                }
            }
            if (ls != null && LogData)
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
            double transactionalPerformance = Math.Round(overallResults.ProcessedTransactions / operationDuration.TotalSeconds, 2);

            //Overall Thread Aggregated Results
            double dataAddedPerThread = amountOfDataProcessed / _numberOfThreads;
            double rowsAddedPerThread = overallResults.ProcessedRows / (UInt64)_numberOfThreads;
            double transactionscompletedPerThread = overallResults.ProcessedTransactions / (UInt64)_numberOfThreads;
            double averageDataRatePerThread = dataRateperformance / (UInt64)_numberOfThreads;
            double averageTransactionPerformancPerThread = transactionalPerformance / (UInt64)_numberOfThreads;

            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "--                      QUERY OPERATION COMPLETED                 -- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "---------------------------Overall Results--------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Data queried                                  : " + amountOfDataProcessed + " " + storageUnit.ToString(), Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Queries completed                             : " + overallResults.ProcessedTransactions, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Queries failed                                : " + overallResults.FailedProcessedRows, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Total seconds                                 : " + operationDuration.TotalSeconds, Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Data rate performance                         : " + dataRateperformance + " Mb/s", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Operational performance                       : " + transactionalPerformance + " Query/s", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "-------------------Overall Thread Aggregated Results----------------- ", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average data queried per thread               : " + dataAddedPerThread + " " + storageUnit.ToString(), Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average operations completed per thread       : " + transactionscompletedPerThread, Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average Data rate performance per thread      : " + averageDataRatePerThread + " Mb/s", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "Average operational performance per thread    : " + averageTransactionPerformancPerThread + " Query/s", Message.MessageType.Info));
            _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Info));

            if (LogData)
            {
                logObject.LogDataEngineStatsTotal(overallResults.ProcessedBytes, overallResults.ProcessedRows, overallResults.ProcessedTransactions, overallResults.FailedProcessedRows,
                    Convert.ToUInt64(operationDuration.TotalSeconds), _numberOfThreads, 4 + 4 * TableAmplification, overallResults.ResultType, DateTime.Now);
            }

            for (int i = 0; i < results.ResultSet.Length; i++)
            {
                TimeSpan threadDuration = (results.ResultSet[i].OperationEnd - results.ResultSet[i].OperationStart);
                double amountOfDataProcessedThisThread = results.ResultSet[i].ProcessedBytes;
                Dynamics.StorageUnit storageUnitThisThread = Dynamics.StorageUnit.Bytes;

                if (amountOfDataProcessedThisThread > 1024L && amountOfDataProcessedThisThread < (1024 * 1024))
                {
                    //convertToKB
                    amountOfDataProcessedThisThread = Math.Round(amountOfDataProcessedThisThread / (1024L), 2);
                    storageUnitThisThread = Dynamics.StorageUnit.Kilobytes;
                }
                else if (amountOfDataProcessedThisThread > (1024 * 1024) && amountOfDataProcessedThisThread < (1024 * 1024 * 1024))
                {
                    //convert to MB
                    amountOfDataProcessedThisThread = Math.Round(amountOfDataProcessedThisThread / (1024L * 1024L), 2);
                    storageUnitThisThread = Dynamics.StorageUnit.Megabytes;
                }
                else if (amountOfDataProcessedThisThread > (1024 * 1024 * 1024) && amountOfDataProcessedThisThread < (1024L * 1024L * 1024L * 1024L))
                {
                    //convert to GB
                    amountOfDataProcessedThisThread = Math.Round(amountOfDataProcessedThisThread / (1024 * 1024 * 1024), 2);
                    storageUnitThisThread = Dynamics.StorageUnit.Gigabytes;
                }
                else if (amountOfDataProcessedThisThread > (1024L * 1024L * 1024L * 1024L))
                {
                    //convert to TB
                    amountOfDataProcessedThisThread = Math.Round(amountOfDataProcessedThisThread / (1024L * 1024L * 1024L * 1024L), 2);
                    storageUnitThisThread = Dynamics.StorageUnit.Terabytes;
                }


                _messageQueue.AddMessage(new Message(DateTime.Now, "------------------------Results for Thread " + (i + 1) + "------------------------ ", Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Info));
                double dataRateperformanceThisThread = Math.Round((results.ResultSet[i].ProcessedBytes) / (1024 * 1024) / threadDuration.TotalSeconds, 2);
                double transactionalPerformanceThisThread = Math.Round(results.ResultSet[i].ProcessedTransactions / operationDuration.TotalSeconds, 2);

                _messageQueue.AddMessage(new Message(DateTime.Now, "Data queried                                  : " + amountOfDataProcessedThisThread + " " + storageUnitThisThread.ToString(), Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Operations completed                          : " + results.ResultSet[i].ProcessedTransactions, Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Operations failed                             : " + results.ResultSet[i].FailedProcessedRows, Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Total seconds                                 : " + threadDuration.TotalSeconds, Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Data rate performance                         : " + dataRateperformanceThisThread + " Mb/s", Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "Operational performance                       : " + transactionalPerformanceThisThread + " Query/s", Message.MessageType.Info));
                _messageQueue.AddMessage(new Message(DateTime.Now, "                                                                ", Message.MessageType.Info));

                if (LogData)
                {
                    logObject.LogDataEngineStatsThreads((i + 1), results.ResultSet[i].ProcessedBytes, results.ResultSet[i].ProcessedRows, 
                        results.ResultSet[i].ProcessedTransactions, results.ResultSet[i].FailedProcessedRows,
                    Convert.ToUInt64(threadDuration.TotalSeconds), results.ResultSet[i].ResultType, DateTime.Now);
                }
            }

            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "--                         END QUERY REPORT                        -- ", Message.MessageType.Report));
            _messageQueue.AddMessage(new Message(DateTime.Now, "--------------------------------------------------------------------- ", Message.MessageType.Report));
        }
        #endregion
    }
}
