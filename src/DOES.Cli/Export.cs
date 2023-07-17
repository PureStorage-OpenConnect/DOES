using DOES.Shared.Debug;
using DOES.Shared.Operations;
using DOES.Shared.Resources;
using DOES.DataEngine.Operations;
using DOES.DataEngine.Resources;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DOES.Cli
{
    public class Export : Operation
    {
       private Task _task;
        MessageQueue _messageQueue = new MessageQueue();
        CancellationToken _token;
        CancellationTokenSource _tokenSource;
        DataVendor _driver = null;
        Controller _controller = null;
        private string _ip;
        private Dynamics.Database _dbType;
        private string _dbname;
        private string _user;
        private string _password;
        private string _folderPath;
        private int _portNumber;
        //MS SQL only parameter set 
        private string _instanceName;
        //Oracle only parameter set 
        private bool _useOracleSID;
        //SAP HANA only parameter set 
        private string _instanceNumber;
        private Dynamics.DatabaseSchema _schema;
        //MySQL only parameter set 
        private Dynamics.MySQLStorageEngine _mysqlEngine;
        private string _ndbtablespace;
        //MariaDB only Parameter Set 
        private Dynamics.MariaDBStorageEngine _mariaDBEngine;
        private bool _verboseWriter = false;
        //MongoDB only parameter set 
        private Dynamics.MongoDBDeployment _mongoDBDeployment;

        public Export(string IP, Dynamics.Database dbType, string databaseName, string user, string password, bool useOracleSID, string folderPath, 
           string instanceName, string instanceNumber, Dynamics.DatabaseSchema schema, int portnumber, Dynamics.MySQLStorageEngine mysqlEngine,
            Dynamics.MariaDBStorageEngine mariadbEngine, string ndbtablespace, Dynamics.MongoDBDeployment mongoDBDeployment)
        {
            _ip = IP;
            _dbType = dbType;
            _dbname = databaseName;
            _user = user;
            _password = password;
            _useOracleSID = useOracleSID;
            _folderPath = folderPath;
            _instanceName = instanceName;
            _instanceNumber = instanceNumber;
            _schema = schema;
            _portNumber = portnumber;
            _mongoDBDeployment = mongoDBDeployment;
            _mysqlEngine = mysqlEngine;
            _mariaDBEngine = mariadbEngine;
            _ndbtablespace = ndbtablespace;
        }

        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public override CancellationTokenSource TokenSource { get { return _tokenSource; } }

        public override void ExecuteOperation()
        {
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
                _driver = new MySQLDB(_ip, _dbname, _user, _password, _portNumber, serviceOperator, _mysqlEngine, _messageQueue);
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
                _driver = new MongoDBOper(_ip, _dbname, _user, _password, _portNumber, _mongoDBDeployment, serviceOperator, _messageQueue);
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
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, "Export : " + messageToParse.Item2);
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
    }
}
