using System;
using DOES.Shared.Resources;
using DOES.Shared.Debug;

namespace DOES.Shared.Operations
{
    [Serializable()]
    public class DOESTest
    {
        private Int32 _testID;
        private string _testName;
        private DateTime _startDate;
        private DateTime _lastChecked;
        private string _codeRevision;
        private Dynamics.Database _solution;
        private string _dataSize;
        private string _changeRate;
        private string _attempt;
        private MessageQueue _queue;

        public DOESTest(string name, MessageQueue queue)
        {
            _testName = name;
            _queue = queue;
        }

        public DOESTest(string name, int id,  MessageQueue queue)
        {
            _testID = id;
            _testName = name;
            _queue = queue;
        }

        public DOESTest(string name, string revision, MessageQueue queue)
        {
            _testName = name;
            _codeRevision = revision;
            _queue = queue;
        }

        public DOESTest(int id, string name, string revision, MessageQueue queue)
        {
            _testID = id;
            _testName = name;
            _codeRevision = revision;
            _queue = queue;
        }

        public DOESTest(string name, string revision, Dynamics.Database solution, string dataSize, string changeRate, string attempt, MessageQueue queue)
        {
            _testName = name;
            _codeRevision = revision;
            _solution = solution;
            _dataSize = dataSize;
            _changeRate = changeRate;
            _attempt = attempt;
            _queue = queue;
        }

        public DOESTest(int id, string name, DateTime start, DateTime last, string revision, MessageQueue queue)
        {
            _testID = id;
            _testName = name;
            _startDate = start;
            _lastChecked = last;
            _codeRevision = revision;
            _queue = queue;
        }

        public DOESTest(int id, string name, DateTime start, string revision, MessageQueue queue)
        {
            _testID = id;
            _testName = name;
            _startDate = start;
            _codeRevision = revision;
            _queue = queue;
        }

        public int TestID { get { return _testID; } set { _testID = value; } }
        public string TestName { get { return _testName; } set { _testName = value; } }
        public DateTime StartDate { get { return _startDate; } set { _startDate = value; } }
        public DateTime LastChecked { get { return _lastChecked; } set { _lastChecked = value; } }
        public Dynamics.Database Solution { get { return _solution; } set { _solution = value; } }
        public string CodeRevision { get { return _codeRevision; } set { _codeRevision = value; } }
        public string DataSize { get { return _dataSize; } set { _dataSize = value; } }
        public string ChangeRate { get { return _changeRate; } set { _changeRate = value; } }
        public string Attempt { get { return _attempt; } set { _attempt = value; } }
    }
}
