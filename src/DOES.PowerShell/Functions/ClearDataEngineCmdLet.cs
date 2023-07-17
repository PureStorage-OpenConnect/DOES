using System.Management.Automation;
using System.Collections.Generic;
using System;
using DOES.DataEngine.Resources;
using DOES.Shared.Debug;
using DOES.DataEngine.Operations;
using DOES.Shared.Resources;
using DOES.Shared.Operations;

namespace DOES.PowerShell
{
    [Cmdlet(VerbsCommon.Clear, "DataEngine", DefaultParameterSetName = "MS SQL")]
    public class ClearDataEngineCmdLet : Cmdlet
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
        private Dynamics.ClearingType _clearOperation = Dynamics.ClearingType.Drop;
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "MongoDB")]
        public Dynamics.ClearingType TableClearOperation { get { return _clearOperation; } set { _clearOperation = value; } }
        private int _tableAmplifier = 8;
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Position = 6, Mandatory = false, ParameterSetName = "MongoDB")]
        public int TableAmplification { get { return _tableAmplifier; } set { _tableAmplifier = value; } }

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
        private int _portNumber = 0;
        [Parameter(Mandatory = false, ParameterSetName = "MS SQL")]
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        [Parameter(Mandatory = false, ParameterSetName = "Oracle")]
        [Parameter(Mandatory = false, ParameterSetName = "MySQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        [Parameter(Mandatory = false, ParameterSetName = "PostgreSQL")]
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public int Port { get { return _portNumber; } set { _portNumber = value; } }

        //MongoDB only parameter set 
        private Dynamics.MongoDBDeployment _mongoDBDeploymentType = Dynamics.MongoDBDeployment.StandAlone;
        [Parameter(Mandatory = false, ParameterSetName = "MongoDB")]
        public Dynamics.MongoDBDeployment MongoDBDeploymentType { get { return _mongoDBDeploymentType; } set { _mongoDBDeploymentType = value; } }
        #endregion

        #region Global Variables
        private MessageQueue _messageQueue = new MessageQueue();
        private DataVendor _driver;
        private Controller _controller;
        private Mechanic serviceOperator = new Mechanic();

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
            Dynamics.Database _dbType = DatabaseType;
            string _dbname = DatabaseName;

            if (_dbType == Dynamics.Database.MicrosoftSQL)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 1433;
                }
                _driver = new MicrosoftSQL(_ip, _dbname, _instanceName, _user, _password, _portNumber, _tableAmplifier, serviceOperator, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.SAPHANA)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 13;
                }
                _driver = new SAPHANA(_ip, _dbname, _instanceNumber, _user, _password, _portNumber, _tableAmplifier, serviceOperator, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.Oracle)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 1521;
                }
                _driver = new OracleDB(_ip, _dbname, _user, _password, _useOracleSID, _tableAmplifier, _portNumber, serviceOperator, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.MySQL)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 3306;
                }
                _driver = new MySQLDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, serviceOperator, Dynamics.MySQLStorageEngine.InnoDB, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.MariaDB)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 3306;
                }
                _driver = new MariaDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, serviceOperator, Dynamics.MariaDBStorageEngine.InnoDB, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.PostgreSQL)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 5432;
                }
                _driver = new PostgreSQLDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, serviceOperator, _messageQueue);
            }
            else if (_dbType == Dynamics.Database.MongoDB)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 27017;
                }
                _driver = new MongoDBOper(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, _mongoDBDeploymentType, serviceOperator, _messageQueue);
            }
            if (_driver != null)
            {
                _controller = new Controller(_driver);
                HandleTableCleanResponse(_controller.ClearTablesAndIndexes(_clearOperation));
            }
            WriteMessages();
        }

        protected override void StopProcessing()
        {
            base.StopProcessing();
        }
        #endregion

        #region Other

        private void HandleTableCleanResponse(bool state)
        {
            string response = null;
            if (state)
            {
                if (_clearOperation == Dynamics.ClearingType.Drop)
                {
                    response = "Database's Tables, Constraints and Indexes dropped.";
                }
                else if (_clearOperation == Dynamics.ClearingType.Truncate)
                {
                    response = "Database's Tables truncated.";
                }
            }
            else
            {
                response = "One or more Tables not cleared.";
            }
            WriteVerbose(response);
        }

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
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine,"Clear " + messageToParse.Item2);
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

