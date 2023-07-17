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
    public class Merge : Operation
    {
        MessageQueue _messageQueue = new MessageQueue();
        private string _ip;
        private Dynamics.Database _dbType;
        private string _dbname;
        private string _user;
        private string _password;
        private int _tableAmplifier;
        private int _unloadPercentage;
        private int _setExtensionNodePercentage;
        private string _extensionNodeGroupName;
        private int _setPagedTablesPercentage;
        private int _setColumnLoadableTablesPercentage;
        private int _percentToPreload;
        private string _instanceNumber;
        private int _percentColumnTables;
        private int _portNumber;
        private bool _verboseWriter = false;

        public Merge(string IP, Dynamics.Database dbType, string databaseName, string user, string password, int tableAmplifier, int unloadPercentage, int setExtensionNodePercentage,
            string extensionNodeGroupName, int setPagedTablesPercentage, int setColumnLoadableTablesPercentage, int percentToPreload, string instanceNumber, int percentColumnTables,
            int portnumber)
        {
            _ip = IP;
            _dbType = dbType;
            _dbname = databaseName;
            _user = user;
            _password = password;
            _tableAmplifier = tableAmplifier;
            _unloadPercentage = unloadPercentage;
            _setExtensionNodePercentage = setExtensionNodePercentage;
            _extensionNodeGroupName = extensionNodeGroupName;
            _setPagedTablesPercentage = setPagedTablesPercentage;
            _setColumnLoadableTablesPercentage = setColumnLoadableTablesPercentage;
            _percentToPreload = percentToPreload;
            _instanceNumber = instanceNumber;
            _percentColumnTables = percentColumnTables;
            _portNumber = portnumber;
        }

        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public override CancellationTokenSource TokenSource => throw new NotImplementedException();

        public override void ExecuteOperation()
        {
            Mechanic serviceOperator = new Mechanic();
            if (_dbType == Dynamics.Database.SAPHANA)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 13;
                }
                SAPHANA hanaOperator = new SAPHANA(_ip, _dbname, _instanceNumber, _user, _password, _portNumber, _tableAmplifier, _percentColumnTables, serviceOperator, _messageQueue);
                hanaOperator.DeltaMergeColumnTables();
                hanaOperator.UnloadTables(_unloadPercentage);
                hanaOperator.SetTableForExtensionNode(_setExtensionNodePercentage, _extensionNodeGroupName);
                hanaOperator.SetColumnLoadableAttribute(_setColumnLoadableTablesPercentage);
                hanaOperator.SetPagedAttributeOnTables(_setPagedTablesPercentage);
                hanaOperator.SetPreloadAttributeOnTables(_percentToPreload);
            }
            else
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Only SAP HANA Databases support Delta Merge operations", Message.MessageType.Error));
            }

            WriteMessages();
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
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, "Merge : " + messageToParse.Item2);
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
