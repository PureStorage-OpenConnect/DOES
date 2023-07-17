using System;
using DOES.Shared.Resources;

namespace DOES.Shared.Operations
{
    [Serializable()]
    public class InterimThreadStat
    {
        public InterimThreadStat(int threadID, UInt64 dataProcessedBytes, double rowPerformance, double transactionPerformance, double dataRatePerformance,
            Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            ThreadID = threadID;
            DataProcessedBytes = dataProcessedBytes;
            RowRatePerformance = rowPerformance;
            TransactionalPerformance = transactionPerformance;
            DataRatePerformance = dataRatePerformance;
            Operation = operation;
            CollectedOn = collectedOn;
        }

        public int ThreadID { get; }
        public UInt64 DataProcessedBytes { get; }
        public double RowRatePerformance { get; }
        public double TransactionalPerformance { get; }
        public double DataRatePerformance { get; }
        public Dynamics.DataEngineOperation Operation { get; }
        public DateTime CollectedOn { get; }
    }
}
