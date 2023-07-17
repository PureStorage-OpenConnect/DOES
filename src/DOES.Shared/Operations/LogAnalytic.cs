using DOES.Shared.Resources;
using System;

namespace DOES.Shared.Operations
{
    public abstract class LogAnalytic
    {
        public abstract bool CreateAnalyticsObjects();
        public abstract DOESTest LogTest();
        public abstract DOESTest LogAdvancedTest();
        public abstract DOESObject LogTestObject();
        public abstract void LogSequenceData(string column, DateTime value, Sequence objectiveItem);
        public abstract void LogDataEngineStatsInterim(InterimStat stat);
        public abstract void LogDataEngineStatsThreadsInterim(InterimThreadStat stat);
        public abstract void LogDataEngineStatsThreads(int threadNumber, UInt64 dataProcessedBytes, UInt64 rowsAffected, UInt64 transactionsCompleted, UInt64 transactionsFailed, UInt64 totalSeconds,
           Dynamics.DataEngineOperation operation_, DateTime collectedOn);
        public abstract void LogDataEngineStatsTotal(UInt64 dataProcessedBytes, UInt64 rowsAffected, UInt64 transactionsCompleted, UInt64 transactionsFailed, UInt64 totalSeconds, int numberOfThreads, int numberOfTables,
            Dynamics.DataEngineOperation operation,
            DateTime collectedOn);
        public abstract void LogDataEngineStatsFinalReport(string reportText, Dynamics.DataEngineOperation operation, DateTime collectedOn);
        public abstract void LogPlatformEngineWindowsResource(WindowsResource windowsData);
        public abstract void LogPlatformEngineLinuxResource(LinuxResource linuxData);
    }
}
