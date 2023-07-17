using System;
using System.Collections.Generic;
using System.Management.Automation;
using DOES.DataEngine.Resources;
using DOES.Shared.Debug;
using DOES.DataEngine.Operations;
using DOES.Shared.Resources;
using DOES.Shared.Operations;
using System.Threading.Tasks;
using System.Threading;

namespace DOES.PowerShell
{
    [Cmdlet(VerbsData.Export, "DataEngine", DefaultParameterSetName = "MS SQL")]
    public class ExportDataEngineCmdLet : Cmdlet
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
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "MongoDB")]
        public string Password { get { return _password; } set { _password = value; } }
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "MS SQL")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "Oracle")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "MySQL")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "MariaDB")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 5, Mandatory = true, ParameterSetName = "MongoDB")]
        public string Folder { get; set; }
        private Dynamics.DatabaseSchema _schema = Dynamics.DatabaseSchema.WithIndexesLOB;
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "MongoDB")]
        public Dynamics.DatabaseSchema SchemaType { get { return _schema; } set { _schema = value; } }

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
        #endregion

        #region Global Variables
        private Task _task;
        private CancellationToken _token;
        private CancellationTokenSource _tokenSource;
        MessageQueue _messageQueue = new MessageQueue();
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
            Dynamics.Database _dbType = DatabaseType;
            string _ip = Hostname;
            string _dbname = DatabaseName;
            string _user = UserName;
            string _password = Password;
            string _folderPath = Folder;
            Mechanic serviceOperator = new Mechanic();

            _tokenSource = new CancellationTokenSource();
            _token = _tokenSource.Token;

            System.IO.Directory.CreateDirectory(_folderPath);

            if (_dbType == Dynamics.Database.MicrosoftSQL)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 1433;
                }
                _driver = new MicrosoftSQL(_ip, _dbname, _instanceName, _user, _password, _portNumber, serviceOperator, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.SAPHANA)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 13;
                }
                _driver = new SAPHANA(_ip, _dbname, _instanceNumber, _user, _password, _portNumber, serviceOperator, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.Oracle)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 1521;
                }
                _driver = new OracleDB(_ip, _dbname, _user, _password, _useOracleSID, _portNumber, serviceOperator, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.MySQL)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 3306;
                }
                _driver = new MySQLDB(_ip, _dbname, _user, _password, _portNumber, serviceOperator, _mysqlEngine,_messageQueue);
            }
            else if (_dbType == Dynamics.Database.MariaDB)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 3306;
                }
                _driver = new MariaDB(_ip, _dbname, _user, _password, _portNumber, serviceOperator, _mariaDBEngine, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.PostgreSQL)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 5432;
                }
                _driver = new PostgreSQLDB(_ip, _dbname, _user, _password, _portNumber, serviceOperator, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.MongoDB)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 27017;
                }
                _driver = new MongoDBOper(_ip, _dbname, _user, _password, _portNumber, _mongoDBDeploymentType, serviceOperator, _messageQueue);
            }
            try
            {
                if (_driver != null)
                {
                    _controller = new Controller(_driver);
                    string response = serviceOperator.HandleTableCreateResponse(_schema, _controller.CreateTablesAndIndexes());
                    _messageQueue.AddMessage(new Message(DateTime.Now, response, Message.MessageType.Info));
                    _task = Task.Run(() =>
                    {
                        var capturedToken = _tokenSource;
                        _controller.Export(_folderPath, capturedToken);
                    }, _token);


                    Thread.Sleep(5000);

                    while (_task.Status == TaskStatus.Running && !_token.IsCancellationRequested)
                    {
                        Thread.Sleep(5000);
                        WriteMessages();
                    }
                }
            }
            catch (OperationCanceledException)
            {

            }
            finally
            {
                _tokenSource.Dispose();
            }
            WriteMessages();
        }

        protected override void EndProcessing()
        {
            base.EndProcessing();
        }

        protected override void StopProcessing()
        {
            WriteMessages();
            _tokenSource.Cancel();
            base.StopProcessing();
        }

        #endregion

        #region Other
        private void WriteMessages()
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
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, "Clear " + messageToParse.Item2);
                }
                else
                {
                    WriteVerbose(messageToParse.Item2);
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, messageToParse.Item2);
                }
            }
        }
        #endregion
    }
}
