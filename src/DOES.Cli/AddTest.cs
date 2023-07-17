using DOES.Shared.Debug;
using DOES.Shared.Operations;
using DOES.Shared.Resources;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DOES.Cli
{
    public class AddTest : Operation
    {
        private string _testName;
        private string _codeRevision;
        private Dynamics.Database _solution;
        private string _dataSize;
        private string _changeRate;
        private string _attempt;
        private MessageQueue _messageQueue = new MessageQueue();

        private bool _verboseWriter = false;

        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public AddTest(string testName, string codeRevision, Dynamics.Database solution,
            string dataSize, string changeRate, string attempt)
        {
            _testName = testName;
            _codeRevision = codeRevision;
            _solution = solution;
            _dataSize = dataSize;
            _changeRate = changeRate;
            _attempt = attempt;
        }

        public override CancellationTokenSource TokenSource => throw new NotImplementedException();

        public override void ExecuteOperation()
        {
            bool advancedLogging = false;

            if (_dataSize != null &&
               _dataSize != null && _changeRate != null && _attempt != null)
            {
                advancedLogging = true;
            }
            else if (_dataSize == null || _changeRate == null || _attempt == null)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Not Enough Arguments Specified to Trigger Advanced Analytical Functions", Message.MessageType.Warning));
            }

            Analytic analytic = new Analytic(_messageQueue);

            if (advancedLogging == false)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Standard Test Object To Be Created", Message.MessageType.Info));
                DOESTest testObj = new DOESTest(_testName, _codeRevision, _messageQueue);
                analytic.Test = testObj;
                analytic.LogTest();
            }
            else if (advancedLogging == true)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, "Advanced Test Object To Be Created", Message.MessageType.Info));
                DOESTest testObj = new DOESTest(_testName, _codeRevision, _solution, _dataSize, _changeRate, _attempt, _messageQueue);
                analytic.Test = testObj;
                analytic.LogAdvancedTest();
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
                else
                {
                    if (_verboseWriter)
                    {
                        Console.WriteLine(messageToParse.Item2);
                    }
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine,  messageToParse.Item2);
                }
            }
        }
    }
}
