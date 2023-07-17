using DOES.Shared.Debug;
using DOES.Shared.Operations;
using DOES.Shared.Resources;
using DOES.DataEngine.Operations;
using DOES.DataEngine.Resources;
using System;
using System.Collections.Generic;
using System.Threading;

namespace DOES.Cli
{
    public class Clear : Operation
    {
        MessageQueue _messageQueue = new MessageQueue();
        DataVendor _driver = null;
        Controller _controller = null;
        private string _ip;
        private Dynamics.Database _dbType;
        private string _dbname;
        private string _user;
        private string _password;
        private int _tableAmplifier;
        private Dynamics.ClearingType _clearOperation;
        //MS SQL only parameter set 
        private string _instanceName;
        //Oracle only parameter set 
        private bool _useOracleSID;
        //SAP HANA only parameter set 
        private string _instanceNumber;
        private int _portNumber;
        private bool _verboseWriter = false;
        //MongoDB only parameter set 
        private Dynamics.MongoDBDeployment _mongoDBDeployment;


        public Clear(string IP, Dynamics.Database dbType, string databaseName, string user, string password, bool useOracleSID, int tableAmplifier, Dynamics.ClearingType clearOperation,
            string instanceName, string instanceNumber, int portnumber, Dynamics.MongoDBDeployment mongoDBDeployment)
        {
            _ip = IP;
            _dbType = dbType;
            _dbname = databaseName;
            _user = user;
            _password = password;
            _useOracleSID = useOracleSID;
            _tableAmplifier = tableAmplifier;
            _clearOperation = clearOperation;
            _instanceName = instanceName;
            _instanceNumber = instanceNumber;
            _portNumber = portnumber;
            _mongoDBDeployment = mongoDBDeployment;
        }

        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public override CancellationTokenSource TokenSource => throw new NotImplementedException();

        public override void ExecuteOperation()
        {
            //Dynamics.DataEngineOperation operation = Dynamics.DataEngineOperation.CleanDB;
            Mechanic serviceOperator = new Mechanic();

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
                _driver = new MongoDBOper(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, _mongoDBDeployment, serviceOperator, _messageQueue);
            }
            if (_driver != null)
            {
                _controller = new Controller(_driver);
                HandleTableCleanResponse(_controller.ClearTablesAndIndexes(_clearOperation));
            }
            WriteMessages();
        }

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
                response = "One or more Tables not cleaned.";
            }
            if (_verboseWriter)
            {
                Console.WriteLine(response);
            }
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
                    if (_verboseWriter)
                    {
                        Console.WriteLine(messageToParse.Item2);
                    }
                }
                else if (messageToParse.Item1 == Message.MessageType.Error)
                {
                    Console.WriteLine(messageToParse.Item2);
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, "Clear : " + messageToParse.Item2);
                }
                else
                {
                    if (_verboseWriter)
                    {
                        Console.WriteLine(messageToParse.Item2);
                    }
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, messageToParse.Item2);
                }
            }
        }
        #endregion
    }
}
