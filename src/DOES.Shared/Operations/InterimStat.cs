using System;
using DOES.Shared.Resources;

namespace DOES.Shared.Operations
{
    [Serializable()]
    public class InterimStat 
    {
        public InterimStat(UInt64 dataProcessedBytes, double rowPerformance, double transactionPerformance, double dataRatePerformance,
            Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            DataProcessedBytes = dataProcessedBytes;
            RowRatePerformance = rowPerformance;
            TransactionalPerformance = transactionPerformance;
            DataRatePerformance = dataRatePerformance;
            Operation = operation;
            CollectedOn = collectedOn;
        }

        public UInt64 DataProcessedBytes { get; }
        public double RowRatePerformance { get; }
        public double TransactionalPerformance { get; }
        public double DataRatePerformance { get; }
        public Dynamics.DataEngineOperation Operation { get; }
        public DateTime CollectedOn { get; }
    }
}
