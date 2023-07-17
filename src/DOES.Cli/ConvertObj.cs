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
    public class ConvertObj : Operation
    {
        MessageQueue _messageQueue = new MessageQueue();
        private string _ip;
        private Dynamics.Database _dbType;
        private string _dbname;
        private string _user;
        private string _password;
        private int _tableAmplifier;
        //MariaDB only Parameter Set 
        private Dynamics.MariaDBStorageEngine _mariaDBEngine;
        private int _portNumber;
        private bool _verboseWriter = false;

        private int _setPercentage;
        private bool _changeObjectProperties;


        public ConvertObj(string IP, Dynamics.Database dbType, string databaseName, string user, string password, int tableAmplifier,
           Dynamics.MariaDBStorageEngine mariadbEngine, int portnumber, int setPercentage, bool changeObjectProperties)
        {
            _ip = IP;
            _dbType = dbType;
            _dbname = databaseName;
            _user = user;
            _password = password;
            _tableAmplifier = tableAmplifier;
            _mariaDBEngine = mariadbEngine;
            _portNumber = portnumber;
            _setPercentage = setPercentage;
            _changeObjectProperties = changeObjectProperties;
        }


        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public override CancellationTokenSource TokenSource => throw new NotImplementedException();

        public override void ExecuteOperation()
        {
    
            Mechanic serviceOperator = new Mechanic();


            if (_dbType == Dynamics.Database.MariaDB)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 3306;
                }

                MariaDB mariaOperator = new MariaDB(_ip, _dbname, _user, _password, _tableAmplifier, _portNumber, serviceOperator, _mariaDBEngine, _messageQueue);
                if (_changeObjectProperties)
                {
                    mariaOperator.SetMariaDBStorageEngineBaseTables(_mariaDBEngine);
                    mariaOperator.SetMariaDBStorageEngineEncodedTables(_mariaDBEngine, _setPercentage);
                }
            }
            else
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Only MariaDB Databases support Conversion Operations", Message.MessageType.Error));
            }

            WriteMessages();
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
