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
    public class Write : Operation
    {
        Task _task;
        CancellationToken _token;
        CancellationTokenSource _tokenSource;
        MessageQueue _messageQueue = new MessageQueue();
        DataVendor _driver = null;
        Controller _controller = null;
        private string _ip;
        private Dynamics.Database _dbType;
        private string _dbname;
        private string _user;
        private string _password;
        private int _numberOfThreads;
        private Dynamics.DatabaseSchema _schema;
        private int _tableAmplifier;
        private int _columnWidth;
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

        private bool _verboseWriter = false;

        public Write(string IP, Dynamics.Database dbType, string databaseName, string user, string password, bool useOracleSID, int numberOfThreads,
           Dynamics.DatabaseSchema schema, int tableAmplifier, int columnWidth, double randomPercentage, string instanceName, string instanceNumber,
           int percentageColumntables, int percentagePagedTables, int percentageWarmExtensionNode, string extensionNodeGroupName, int partitions, Dynamics.MySQLStorageEngine mysqlEngine,
           Dynamics.MariaDBStorageEngine mariadbEngine, string ndbtablespace, Dynamics.MongoDBDeployment mongoDBDeployment,  int portnumber)
        {
            _ip = IP;
            _dbType = dbType;
            _dbname = databaseName;
            _user = user;
            _password = password;
            _useOracleSID = useOracleSID;
            _numberOfThreads = numberOfThreads;
            _schema = schema;
            _tableAmplifier = tableAmplifier;
            _columnWidth = columnWidth;
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
            _tokenSource = new CancellationTokenSource();
            _token = _tokenSource.Token;
        }


        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public override CancellationTokenSource TokenSource { get { return _tokenSource; } }

        public override void ExecuteOperation()
        {
    
            Mechanic serviceOperator = new Mechanic();


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


                    _task = Task.Run(() =>
                    {
                        var capturedToken = _tokenSource;
                        _controller.WriteInLine(capturedToken);
                    }, _token);
                }

                while (!_token.IsCancellationRequested)
                {
                    Thread.Sleep(1000);
                    WriteMessages();
                }

                if (!_token.IsCancellationRequested)
                {

                    WriteMessages();
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
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, "WriteInLine :" + messageToParse.Item2);
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
