using System;
using System.Collections.Generic;
using System.Management.Automation;
using DOES.Shared.Resources;
using DOES.DataEngine.Resources;
using DOES.Shared.Debug;
using DOES.DataEngine.Operations;
using DOES.Shared.Operations;

namespace DOES.PowerShell
{
    [Cmdlet(VerbsData.Merge, "DataEngine", DefaultParameterSetName = "SAP HANA")]
    public class MergeDataEngineCmdLet : Cmdlet
    {
        #region Parameters 
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "SAP HANA")]
        public Dynamics.Database DatabaseType { get; set; }
        private string _ip = "localhost";
        [Parameter(Position = 1, Mandatory = true, ParameterSetName = "SAP HANA")]
        [Alias("IPAddress")]
        public string Hostname { get { return _ip; } set { _ip = value; } }
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "SAP HANA")]
        public string DatabaseName { get; set; }
        private string _user = null;
        [Parameter(Position = 3, Mandatory = true, ParameterSetName = "SAP HANA")]
        public string UserName { get { return _user; } set { _user = value; } }
        private string _password = null;
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "SAP HANA")]
        public string Password { get { return _password; } set { _password = value; } }
        private int _tableAmplifier = 8;
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "SAP HANA")]
        public int TableAmplification { get { return _tableAmplifier; } set { _tableAmplifier = value; } }
        private int _unloadPercentage = 0;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int UnloadTablesPercentage { get { return _unloadPercentage; } set { _unloadPercentage = value; } }
        private int _setExtensionNodePercentage = 0;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int SetExtensionNodePercentage { get { return _setExtensionNodePercentage; } set { _setExtensionNodePercentage = value; } }
        private string _extensionNodeGroupName = "<my_warm_data>";
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public string ExtensionNodeGroupName { get { return _extensionNodeGroupName; } set { _extensionNodeGroupName = value; } }
        private int _setPagedTablesPercentage = 0;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int SetPagedtablesPercentage { get { return _setPagedTablesPercentage; } set { _setPagedTablesPercentage = value; } }
        private int _setColumnLoadableTablesPercentage = 0;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int SetColumnLoadablePercentage { get { return _setColumnLoadableTablesPercentage; } set { _setColumnLoadableTablesPercentage = value; } }
        private int _percentToPreload = 0;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int SetPercentageToPreload { get { return _percentToPreload; } set { _percentToPreload = value; } }

        //SAP HANA only parameter set 
        private string _instanceNumber = "00";
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public string InstanceNumber { get { return _instanceNumber; } set { _instanceNumber = value; } }
        private int _percentColumnTables = 70;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int PercentageColumns { get { return _percentColumnTables; } set { _percentColumnTables = value; } }
        private int _portNumber = 15;
        [Parameter(Mandatory = false, ParameterSetName = "SAP HANA")]
        public int Port { get { return _portNumber; } set { _portNumber = value; } }
        #endregion

        #region Global Variables
        private MessageQueue _messageQueue = new MessageQueue();
        #endregion

        #region Powershell Functions
        protected override void BeginProcessing()
        {
            base.BeginProcessing();
        }

        protected override void ProcessRecord()
        {
            Dynamics.Database _dbType = DatabaseType;
            string _IP = Hostname;
            string _dbname = DatabaseName;
            string _user = UserName;
            string _password = Password;
            Mechanic serviceOperator = new Mechanic();
            

            if (_dbType == Dynamics.Database.SAPHANA)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 13;
                }
                SAPHANA hanaOperator = new SAPHANA(Hostname, DatabaseName, _instanceNumber, _user, _password, _portNumber, _tableAmplifier, _percentColumnTables, serviceOperator, _messageQueue);
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

        protected override void EndProcessing()
        {
            base.EndProcessing();
        }

        protected override void StopProcessing()
        {
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
