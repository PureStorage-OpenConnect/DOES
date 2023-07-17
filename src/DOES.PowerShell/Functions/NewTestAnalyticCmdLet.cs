using System;
using System.Management.Automation;
using DOES.Shared.Debug;
using DOES.Shared.Resources;
using DOES.Shared.Operations;

namespace DOES.PowerShell
{
    [Cmdlet(VerbsCommon.New, "TestAnalytic")]
    public class NewTestAnalyticCmdLet : Cmdlet
    {
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true)]
        public string TestName { get; set; }
        [Parameter(Position = 1, Mandatory = true, ValueFromPipeline = true)]
        public string CodeRevision { get; set; }

        //Other Advanced Track and Trace Parameters 
        [Parameter(Mandatory = false, ParameterSetName = "AdvancedTestAnalytics")]
        public Dynamics.Database Solution { get; set; }
        [Parameter(Mandatory = false, ParameterSetName = "AdvancedTestAnalytics")]
        public string DataSize { get; set; }
        [Parameter(Mandatory = false, ParameterSetName = "AdvancedTestAnalytics")]
        public string ChangeRate { get; set; }
        [Parameter(Mandatory = false, ParameterSetName = "AdvancedTestAnalytics")]
        public string Attempt { get; set; }


        private MessageQueue _messageQueue = new MessageQueue();

        protected override void BeginProcessing()
        {
            base.BeginProcessing();
        }

        protected override void ProcessRecord()
        { 
            string _testName = TestName;
            string _coderevision = CodeRevision;
            Dynamics.Database _solution = Solution;
            string _dataSize = DataSize;
            string _changeRate = ChangeRate;
            string _attempt = Attempt;
            

            bool advancedLogging = false;
            
            if(_dataSize != null &&  
               _dataSize != null && _changeRate != null && _attempt != null)
            {
                advancedLogging = true;
            }
            else if (_dataSize == null || _changeRate == null || Attempt == null)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Not Enough Arguments Specified to Trigger Advanced Analytical Functions", Message.MessageType.Warning));
            }

            Analytic analytic = new Analytic(_messageQueue);

            if (advancedLogging == false)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Standard Test Object To Be Created", Message.MessageType.Info));
                DOESTest testObj = new DOESTest(_testName, _coderevision, _messageQueue);
                analytic.Test = testObj;
                analytic.LogTest();
            }
            else if(advancedLogging == true)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Advanced Test Object To Be Created", Message.MessageType.Info));
                DOESTest testObj = new DOESTest(_testName, _coderevision, _solution, _dataSize, _changeRate, _attempt, _messageQueue);
                analytic.Test = testObj;
                analytic.LogAdvancedTest();
            }
        }

        protected override void EndProcessing()
        {
            base.EndProcessing();
        }

        protected override void StopProcessing()
        {
            base.StopProcessing();
        }

    }
}
