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
    [Cmdlet(VerbsData.Convert, "DataEngine", DefaultParameterSetName = "MariaDB")]
    public class ConvertDataEngineCmdLet : Cmdlet
    {
        #region Parameters 
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = "MariaDB")]
        public Dynamics.Database DatabaseType { get; set; }
        private string _ip = "localhost";
        [Parameter(Position = 1, Mandatory = true, ParameterSetName = "MariaDB")]
        [Alias("IPAddress")]
        public string Hostname { get { return _ip; } set { _ip = value; } }
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = "MariaDB")]
        public string DatabaseName { get; set; }
        private string _user = null;
        [Parameter(Position = 3, Mandatory = true, ParameterSetName = "MariaDB")]
        public string UserName { get { return _user; } set { _user = value; } }
        private string _password = null;
        [Parameter(Position = 4, Mandatory = true, ParameterSetName = "MariaDB")]
        public string Password { get { return _password; } set { _password = value; } }
        private int _tableAmplifier = 8;
        [Parameter(Position = 5, Mandatory = false, ParameterSetName = "MariaDB")]
        public int TableAmplification { get { return _tableAmplifier; } set { _tableAmplifier = value; } }
        private int _portNumber = 0;
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        public int Port { get { return _portNumber; } set { _portNumber = value; } }
        private int _setPercentage = 0;
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        public int SetPercentage { get { return _setPercentage; } set { _setPercentage = value; } }

        private bool _changeObjectProperties;
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        public SwitchParameter ChangeObjectProperties
        {
            get { return _changeObjectProperties; }
            set { _changeObjectProperties = value; }
        }

        //MariaDB only parameter set 
        private Dynamics.MariaDBStorageEngine _mariaDBEngine = Dynamics.MariaDBStorageEngine.InnoDB;
        [Parameter(Mandatory = false, ParameterSetName = "MariaDB")]
        public Dynamics.MariaDBStorageEngine MariaDBStorageEngine { get { return _mariaDBEngine; } set { _mariaDBEngine = value; } }
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
            

            if (_dbType == Dynamics.Database.MariaDB)
            {
                if (_portNumber == 0)
                {
                    _portNumber = 3306;
                }

                MariaDB mariaOperator = new MariaDB(Hostname, _dbname, _user, _password, _tableAmplifier,_portNumber, serviceOperator, _mariaDBEngine, _messageQueue);
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
