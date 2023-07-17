using System;

namespace DOES.Shared.Operations
{
    public class Sequence
    {
        Analytic _analytic;
        int _sequence;
        DateTime _dataImportStart;
        DateTime _dataImportEnd;
        DateTime _dataChangeStart;
        DateTime _dataChangeEnd;
        DateTime _coreJobStart;
        DateTime _corejobEnd;
        DateTime _otherJobStart;
        DateTime _otherJobEnd;
        DateTime _iterationStart;
        DateTime _iterationEnd;

        public Sequence(Analytic analytic, int sequence)
        {
            _analytic = analytic;
            _sequence = sequence;
        }

        public Sequence(DateTime importStart, DateTime importEnd, DateTime changeStart, DateTime changeEnd, DateTime jobStart, DateTime jobEnd, DateTime otherStart, DateTime otherEnd, 
            DateTime iterationBegin, DateTime iterationEnd)
        {
            _dataImportStart = importStart;
            _dataImportEnd = importEnd;
            _dataChangeStart = changeStart;
            _dataChangeStart = changeEnd;
            _coreJobStart = jobStart;
            _corejobEnd = jobEnd;
            _otherJobStart = otherStart;
            _otherJobEnd = otherEnd;
            _iterationStart = iterationBegin;
            _iterationEnd = iterationEnd;
        }

        public Analytic AnalyticObject { get { return _analytic; } }
        public int SequenceNumber { get { return _sequence; } set {_sequence = value; } }
        public DateTime DataImportStart { get { return _dataImportStart; } set { _dataImportStart = value; } }
        public DateTime DataImportEnd { get { return _dataImportEnd; } set { _dataImportEnd = value; } }
        public DateTime DataChangeStart { get { return _dataChangeStart; } set { _dataChangeStart = value; } }
        public DateTime DataChangeEnd { get { return _dataChangeEnd; } set { _dataChangeEnd = value; } }
        public DateTime CoreJobStart { get { return _coreJobStart; } set { _coreJobStart = value; } }
        public DateTime CoreJobEnd { get { return _corejobEnd; } set { _corejobEnd = value; } }
        public DateTime OtherJobStart { get { return _otherJobStart; } set { _otherJobStart = value; } }
        public DateTime OtherJobEnd { get { return _otherJobEnd; } set { _otherJobEnd = value; } }
        public DateTime IterationStart { get { return _iterationStart; } set { _iterationStart = value; } }
        public DateTime IterationEnd { get { return _iterationEnd; } set { _iterationEnd = value; } }

    }
}
