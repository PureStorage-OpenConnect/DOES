using DOES.Shared.Debug;
using DOES.Shared.Operations;
using System;
using System.Collections.Generic;
using System.Threading;

namespace DOES.Cli
{
    public class AddSequence : Operation
    {
        private string _testName;
        private string _objectName;
        private int _sequence;
        private bool _logData;
        private DateTime? _dataImportStart;
        private DateTime? _dataImportEnd;
        private DateTime? _dataChangeStart;
        private DateTime? _dataChangeEnd;
        private DateTime? _coreJobStart;
        private DateTime? _coreJobEnd;
        private DateTime? _otherJobStart;
        private DateTime? _otherJobEnd;
        private DateTime? _sequenceStart;
        private DateTime? _sequenceEnd;
        private MessageQueue _messageQueue;

        private bool _verboseWriter = false;

        public override bool VerboseWriter { get { return _verboseWriter; } set { _verboseWriter = value; } }

        public AddSequence(string testName, string objectName, int sequence, bool logdata, DateTime? dataImportStart,
            DateTime? dataImportEnd, DateTime? dataChangeStart, DateTime? dataChangeEnd, DateTime? coreJobStart,
            DateTime? coreJobEnd, DateTime? otherJobStart, DateTime? otherJobEnd, DateTime? sequenceStart,
            DateTime? sequenceEnd)
        {
            _testName = testName;
            _objectName = objectName;
            _sequence = sequence;
            _logData = logdata;
            _dataImportStart = dataImportStart;
            _dataImportEnd = dataImportEnd;
            _dataChangeStart = dataChangeStart;
            _dataChangeEnd = dataChangeEnd;
            _coreJobStart = coreJobStart;
            _coreJobEnd = coreJobEnd;
            _otherJobStart = otherJobStart;
            _otherJobEnd = otherJobEnd;
            _sequenceStart = sequenceStart;
            _sequenceEnd = sequenceEnd;
        }

        public override CancellationTokenSource TokenSource => throw new NotImplementedException();

        public override void ExecuteOperation()
        {
            _messageQueue = new MessageQueue();

            if (_logData)
            {
                Analytic analytic = new Analytic(_testName, _objectName, _sequence, _messageQueue);

                Sequence seq = new Sequence(analytic, _sequence);
                string columnname = null;
                if (_dataImportStart.HasValue)
                {
                    columnname = "DataImportStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_dataImportStart), seq);
                }
                if (_dataImportEnd.HasValue)
                {
                    columnname = "DataImportEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_dataImportEnd), seq);
                }
                if (_dataChangeStart.HasValue)
                {
                    columnname = "DataChangeStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_dataChangeStart), seq);
                }
                if (_dataChangeEnd.HasValue)
                {
                    columnname = "DataChangeEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_dataChangeEnd), seq);
                }
                if (_coreJobStart.HasValue)
                {
                    columnname = "CoreJobStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_coreJobStart), seq);
                }
                if (_coreJobEnd.HasValue)
                {
                    columnname = "CoreJobEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_coreJobEnd), seq);
                }
                if (_otherJobStart.HasValue)
                {
                    columnname = "OtherJobStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_otherJobStart), seq);
                }
                if (_otherJobEnd.HasValue)
                {
                    columnname = "OtherJobEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_otherJobEnd), seq);
                }
                if (_sequenceStart.HasValue)
                {
                    columnname = "SequenceStart";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_sequenceStart), seq);
                }
                if (_sequenceEnd.HasValue)
                {
                    columnname = "SequenceEnd";
                    analytic.LogSequenceData(columnname, Convert.ToDateTime(_sequenceEnd), seq);
                }
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
                    DebugLogger.LogMessages(DebugLogger.File.DataEngine, messageToParse.Item2);
                }
            }
        }
    }
}
