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
    [Cmdlet(VerbsDiagnostic.Test, "DataEngine", DefaultParameterSetName = "MS SQL")]
    public class TestDataEngineCmdlet : Cmdlet
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
        private string _folderPath = null;
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 8, Mandatory = false, ParameterSetName = "MongoDB")]
        public string Folder { get { return _folderPath; } set { _folderPath = value; } }
        private Dynamics.DatabaseSchema _schema = Dynamics.DatabaseSchema.WithIndexesLOB;
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 9, Mandatory = false, ParameterSetName = "MongoDB")]
        public Dynamics.DatabaseSchema SchemaType { get { return _schema; } set { _schema = value; } }
        private int _tableAmplifier = 8;
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 10, Mandatory = false, ParameterSetName = "MongoDB")]
        public int TableAmplification { get { return _tableAmplifier; } set { _tableAmplifier = value; } }
        private int _columnWidth = 2147483647;
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 11, Mandatory = false, ParameterSetName = "MongoDB")]
        public int ColumnWidth { get { return _columnWidth; } set { if (value <= 2147483647) { _columnWidth = value; } } }
        private Dynamics.TestType _testType = Dynamics.TestType.Simple;
        [Parameter(Position = 12, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 12, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 12, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 12, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 12, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 12, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 12, Mandatory = false, ParameterSetName = "MongoDB")]
        public Dynamics.TestType Testtype { get { return _testType; } set { _testType = value; } }
        private bool _deferInitialIngest;
        [Parameter(Position = 13, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 13, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 13, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 13, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 13, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 13, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 13, Mandatory = false, ParameterSetName = "MongoDB")]
        public SwitchParameter DeferInitialWrite
        {
            get { return _deferInitialIngest; }
            set { _deferInitialIngest = value; }
        }
        private int _changeRate = 10;
        [Parameter(Position = 14, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 14, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 14, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 14, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 14, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 14, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 14, Mandatory = false, ParameterSetName = "MongoDB")]
        public int ChangeRate { get { return _changeRate; } set { _changeRate = value; } }
        private double _growthRate = 0;
        [Parameter(Position = 15, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 15, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 15, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 15, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 15, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 15, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 15, Mandatory = false, ParameterSetName = "MongoDB")]
        public double Growthrate { get { return _growthRate; } set { _growthRate = value; } }
        private double _randomPercentage = 30;
        [Parameter(Position = 16, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 16, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 16, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 16, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 16, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 16, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 16, Mandatory = false, ParameterSetName = "MongoDB")]
        public double RandomPercentage { get { return _randomPercentage; } set { _randomPercentage = value; } }

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
        public Dynamics.MySQLStorageEngine MySQLStorageEngine { get { return _mysqlEngine; } set { _mysqlEngine = value; } }
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

        #region Global Variables
        private Task _task;
        private CancellationToken _token;
        CancellationTokenSource _tokenSource;
        private CancellationToken _operationToken;
        CancellationTokenSource _operationTokenSource;
        private ProgressRecord _progressDataRate;
        private ProgressRecord _progressRows;
        private ProgressRecord _progressByteCount;
        private ProgressRecord _progressTransactions;
        private MessageQueue _messageQueue = new MessageQueue();
        private Analytic _ldes;
        private Queue<InterimStat> _stats = new Queue<InterimStat>();
        private Queue<InterimThreadStat> _threadStats = new Queue<InterimThreadStat>();
        private DataVendor _driver;
        private Controller _initialController;
        private Controller _testController;
        #endregion

        #region Powershell Functions
        protected override void BeginProcessing()
        {
            base.BeginProcessing();
        }

        protected override void ProcessRecord()
        {
            Dynamics.DataEngineOperation operation = Dynamics.DataEngineOperation.Unknown;
            Dynamics.Database _dbType = DatabaseType;
            string _dbname = DatabaseName;
            string _password = Password;
            UInt64 _amountToAdd = Amount;
            Dynamics.StorageUnit _unit = Unit;
            Mechanic serviceOperator = new Mechanic(_randomPercentage);
            string _ip = Hostname;

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
                catch (InvalidOperationException ex)
                {
                    ErrorRecord er = new ErrorRecord(ex, (_schema.ToString() + " is not a supported DatabaseSchema type for SAP HANA"), 
                        ErrorCategory.InvalidOperation, _schema);
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
                    ErrorRecord er = new ErrorRecord(ex, (_schema.ToString() + " is not a supported DatabaseSchema type for Oracle"), ErrorCategory.InvalidOperation, _schema);
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
                        _driver = new MySQLDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, _schema, serviceOperator, _mysqlEngine, _ndbtablespace, _messageQueue);
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

                    CreateProgress();

                    start = DateTime.Now;

                    while (!_initialController.GetResultsState() && !_token.IsCancellationRequested)
                    {
                        Thread.Sleep(1000);
                        DataEngineResults tempResults = _initialController.ReturnInterimResults();
                        end = DateTime.Now;
                        TimeSpan duration = (end - start);
                        lastResults = UpdateProgressBar(tempResults, duration, amountToAddBytes, lastResults);
                        start = DateTime.Now;
                        WriteMessages(_ldes, operation);
                    }
                    CreateProgress();
                    start = DateTime.UtcNow;

                    if (!_token.IsCancellationRequested)
                    {
                        ClearProgress();
                        initialResults = _initialController.ReturnFinalResults();
                    }
                }
                _testController = new Controller(_driver, _columnWidth, _numberOfThreads, true);
                if (_testType == Dynamics.TestType.Simple)
                {
                    operation = Dynamics.DataEngineOperation.TestSimple;
                    lastResults = new DataEngineResults(operation);

                    double changeRate = Math.Round((double)_changeRate / 100, 2);
                    UInt64 amounttoPerformOperationsOn = Convert.ToUInt64(Math.Round((Convert.ToDouble(amountToAddBytes)) * (changeRate), 0));
                    CreateProgress();

                    _task = Task.Run(() =>
                    {
                        var capturedToken = _operationTokenSource;
                        _testController.TestSimple(Folder, amounttoPerformOperationsOn, capturedToken, _growthRate);
                    }, _operationToken);

                    while (!_testController.GetResultsState() && !_operationToken.IsCancellationRequested)
                    {
                        Thread.Sleep(2000);
                        DataEngineResults tempResults = _testController.ReturnInterimTestResults(operation);
                        end = DateTime.Now;
                        TimeSpan duration = (end - start);
                        if (tempResults != null)
                        {
                            lastResults = UpdateProgressBarRunningTest(tempResults, duration, (amounttoPerformOperationsOn), lastResults);
                            if (LogData)
                            {
                                DataEngineResultSet tempThreadResults = _testController.ReturnInterimTestThreadResults();
                                if (tempThreadResults != null) { lastResultSet = AddDataEngineInterimThreadResults(tempThreadResults, duration, lastResultSet); }
                            }
                            start = DateTime.Now;
                            WriteMessages(_ldes, operation);
                        }
                    }

                    if (!_operationToken.IsCancellationRequested)
                    {
                        ClearProgress();
                        DataEngineResultSet results = _testController.ReturnFinalResults();
                        if (LogData)
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
                    CreateProgress();

                    _task = Task.Run(() =>
                    {
                        var capturedToken = _operationTokenSource;
                        _testController.TestAdvanced(Folder, amounttoPerformOperationsOn, capturedToken, _growthRate);
                    }, _operationToken);

                    while (!_testController.GetResultsState() && !_operationToken.IsCancellationRequested)
                    {
                        Thread.Sleep(2000);
                        DataEngineResults tempResults = _testController.ReturnInterimTestResults(operation);
                        end = DateTime.Now;
                        TimeSpan duration = (end - start);
                        if (tempResults != null)
                        {
                            lastResults = UpdateProgressBarRunningTest(tempResults, duration, (amounttoPerformOperationsOn), lastResults);
                            if (LogData)
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

                    if (!_operationToken.IsCancellationRequested)
                    {
                        ClearProgress();
                        DataEngineResultSet results = _testController.ReturnFinalResults();
                        if (LogData)
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
                    CreateProgress();

                    _task = Task.Run(() =>
                    {
                        var capturedToken = _operationTokenSource;
                        _testController.TestComplex(Folder, amounttoPerformOperationsOn, capturedToken, _growthRate);
                    }, _operationToken);

                    while (!_testController.GetResultsState() && !_operationToken.IsCancellationRequested)
                    {
                        Thread.Sleep(1000);
                        DataEngineResults tempResults = _testController.ReturnInterimTestResults(operation);
                        end = DateTime.Now;
                        TimeSpan duration = (end - start);
                        if (tempResults != null)
                        {
                            lastResults = UpdateProgressBarRunningTest(tempResults, duration, (amounttoPerformOperationsOn), lastResults);
                            if (LogData)
                            {
                                DataEngineResultSet tempThreadResults = _testController.ReturnInterimTestThreadResults();
                                if (tempThreadResults != null) { lastResultSet = AddDataEngineInterimThreadResults(tempThreadResults, duration, lastResultSet); }
                            }
                            start = DateTime.Now;
                            WriteMessages(_ldes, operation);
                        }
                    }

                    if (!_operationToken.IsCancellationRequested)
                    {
                        ClearProgress();
                        DataEngineResultSet results = _testController.ReturnFinalResults();
                        if (LogData)
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
            _operationTokenSource.Cancel();
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
            _progressDataRate = new ProgressRecord(0, "Data Rate        ", " Data rate performance : 0 MB/s");
            _progressRows = new ProgressRecord(1, "Row Rate         ", " 0 Rows/s");
            _progressTransactions = new ProgressRecord(2, "Transaction Rate ", " 0 Transactions/s");
            _progressByteCount = new ProgressRecord(3, "Overall progress ", "Amount processed : 0 Bytes of 0 Bytes");
            _progressDataRate.RecordType = ProgressRecordType.Processing;
            _progressRows.RecordType = ProgressRecordType.Processing;
            _progressTransactions.RecordType = ProgressRecordType.Processing;
            _progressByteCount.RecordType = ProgressRecordType.Processing;
        }

        private void ClearProgress()
        {
            _progressDataRate.RecordType = ProgressRecordType.Completed;
            _progressRows.RecordType = ProgressRecordType.Completed;
            _progressByteCount.RecordType = ProgressRecordType.Completed;
            _progressTransactions.RecordType = ProgressRecordType.Completed;
            WriteProgress(_progressDataRate);
            WriteProgress(_progressRows);
            WriteProgress(_progressByteCount);
            WriteProgress(_progressTransactions);
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
                else if(normalisedValues.Item2 == Dynamics.StorageUnit.Megabytes) { storageUnitDR = " MB/s"; }
                else if (normalisedValues.Item2 == Dynamics.StorageUnit.Gigabytes) { storageUnitDR =  "GB/s"; }
                else if (normalisedValues.Item2 == Dynamics.StorageUnit.Terabytes) { storageUnitDR = " TB/s"; }



                Double dataRateperformance = Math.Round((normalisedValues.Item1) /  span.TotalSeconds, 2);
                Double rowRatePerformance = Math.Round(interimProcessedRows / span.TotalSeconds, 2);
                Double transactionalPerformance = Math.Round(interimProcessedTransactions / span.TotalSeconds, 2);
                _progressDataRate.StatusDescription = "   Data rate performance        : " + dataRateperformance + storageUnitDR;
                _progressRows.StatusDescription = "   Row rate performance         : " + rowRatePerformance + " Rows/s";
                _progressTransactions.StatusDescription = "   Transactional performance    : " + transactionalPerformance + " Transactions/s";
                _progressByteCount.StatusDescription = "   Amount processed             : " + processedBytesDBL + " " + storageUnitProcess +
                    " of " + requestedBytesDBL + " " + storageUnitProcess;


                WriteProgress(_progressDataRate);
                WriteProgress(_progressRows);
                WriteProgress(_progressTransactions);
                WriteProgress(_progressByteCount);

                if (LogData)
                {
                    _stats.Enqueue(new InterimStat(results.ProcessedBytes, rowRatePerformance, transactionalPerformance, dataRateperformance, results.ResultType, DateTime.Now));
                }
            }
            catch (PipelineStoppedException) { }
            return results;
        }

        private DataEngineResults UpdateProgressBarRunningTest(DataEngineResults results, TimeSpan span, Double RequestedBytes, DataEngineResults oldResults)
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
                _progressDataRate.StatusDescription = "   Data rate performance        : " + dataRateperformance + storageUnitDR;
                _progressRows.StatusDescription = "   Row rate performance         : " + rowRatePerformance + " Rows/s";
                _progressTransactions.StatusDescription = "   Transactional performance    : " + transactionalPerformance + " Transactions/s";
                _progressByteCount.StatusDescription = "   Amount processed             : " + processedBytesDBL + " " + storageUnitProcess +
                    " of " + requestedBytesDBL + " " + storageUnitProcess;


                WriteProgress(_progressDataRate);
                WriteProgress(_progressRows);
                WriteProgress(_progressTransactions);
                WriteProgress(_progressByteCount);

                if (LogData)
                {
                    _stats.Enqueue(new InterimStat(results.ProcessedBytes, rowRatePerformance, transactionalPerformance, dataRateperformance, results.ResultType, DateTime.Now));
                }
            }
            catch (PipelineStoppedException) { }
            return results;
        }

        private DataEngineResultSet AddDataEngineInterimThreadResults(DataEngineResultSet resultSet, TimeSpan span, DataEngineResultSet oldResults)
        {
            if(oldResults.ResultCompilation == null)
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

            if (LogData)
            {
                logObject.LogDataEngineStatsTotal(overallOperationResults.ProcessedBytes, overallOperationResults.ProcessedRows, overallOperationResults.ProcessedTransactions, overallOperationResults.FailedProcessedRows,
                    Convert.ToUInt64(operationDuration.TotalSeconds), _numberOfThreads, 4 + 4 * TableAmplification, overallOperationResults.ResultType, DateTime.Now);
            }

            for (int i = 0; i < operationResults.ResultCompilation.Length; i++)
            {
                for (int i2 = 0; i2 < operationResults.ResultCompilation[i].Length; i2++)
                {
                    TimeSpan threadDuration = (operationResults.ResultCompilation[i][i2].OperationEnd - operationResults.ResultCompilation[i][i2].OperationStart);
                    double amountOfDataProcessedThisThread = operationResults.ResultCompilation[i][i2].ProcessedBytes;

                    if (LogData)
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
