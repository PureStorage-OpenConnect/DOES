using DOES.Shared.Debug;
using DOES.Shared.Resources;
using System;

namespace DOES.Shared.Operations
{
    public class Analytic
    {
        private DOESTest _test;
        private DOESObject _object;
        private AnalyticsPersistence _ap;
        private MessageQueue _queue;
        private int _sequence;
        private LogAnalytic analyticsEnpoint;
        bool analyticsStatus = false;

        public DOESTest Test { get { return _test; } set { _test = value; } }
        public DOESObject TestObject { get { return _object; } set { _object = value; } }
        public AnalyticsPersistence Persistence { get { return _ap; } }
        public MessageQueue DebugQueue { get { return _queue; } }
        public int Sequence { get { return _sequence; } }

        public Analytic(MessageQueue queue)
        {
            _queue = queue;
            _ap = new AnalyticsPersistence(_queue);
        }

        public enum AnalyticType
        {
            DataEngineInterimStat,
            DataEngineInterimThreadStat,
            DataEngineThreadStatInterim,
            DataEngineStatsTotal,
            DataEngineLogStatsFinalReport
        };

        public void SetAnalyticEndpoint()
        {
            switch (_ap.PersistenceInstance.DatabaseVendor)
            {
                case Dynamics.Database.MicrosoftSQL:
                    analyticsEnpoint = new MicrosoftSQL_LogAnalytic(this);
                    break;
                case Dynamics.Database.Oracle:
                    analyticsEnpoint = new OracleDB_LogAnalytic(this);
                    break;
                case Dynamics.Database.SAPHANA:
                    analyticsEnpoint = new SAPHANA_LogAnalytic(this);
                    break;
                case Dynamics.Database.MySQL:
                    analyticsEnpoint = new MySQL_LogAnalytic(this);
                    break;
                case Dynamics.Database.MariaDB:
                    analyticsEnpoint = new MariaDB_LogAnalytic(this);
                    break;
                case Dynamics.Database.PostgreSQL:
                    analyticsEnpoint = new PostgreSQL_LogAnalytic(this);
                    break;
            }
        }

        public Analytic(string testName, string objectName, MessageQueue queue)
        {
            _queue = queue;
            _ap = new AnalyticsPersistence(_queue);
            _test = new DOESTest(testName, _queue);
            _object = new DOESObject(_test, objectName, queue);
            SetAnalyticEndpoint();
            analyticsStatus  = analyticsEnpoint.CreateAnalyticsObjects();
            if (analyticsStatus)
            { 
                analyticsEnpoint.LogTest();
                analyticsEnpoint.LogTestObject();
            }
        }

        public Analytic(string testName, string objectName, string ObjectCategory, MessageQueue queue)
        {
            _queue = queue;
            _ap = new AnalyticsPersistence(_queue);
            _test = new DOESTest(testName, _queue);
            _object = new DOESObject(_test, objectName, ObjectCategory, queue);
            SetAnalyticEndpoint();
            analyticsStatus = analyticsEnpoint.CreateAnalyticsObjects();
            if (analyticsStatus)
            {
                analyticsEnpoint.LogTestObject();
                analyticsEnpoint.LogTestObject();
            }

        }

        public Analytic(string testName, string objectName, int sequence, MessageQueue queue)
        {
            _queue = queue;
            _ap = new AnalyticsPersistence(_queue);
            _test = new DOESTest(testName, _queue);
            _object = new DOESObject(_test, objectName, queue);
            _sequence = sequence;
            SetAnalyticEndpoint();
            analyticsStatus = analyticsEnpoint.CreateAnalyticsObjects();
            if (analyticsStatus)
            {
                analyticsEnpoint.LogTest();
                analyticsEnpoint.LogTestObject();
            }
        }

        public Analytic(string testName, string objectName, string ObjectCategory, int sequence, MessageQueue queue)
        {
            _queue = queue;
            _ap = new AnalyticsPersistence(_queue);
            _test = new DOESTest(testName, _queue);
            _object = new DOESObject(_test, objectName, ObjectCategory, queue);
            _sequence = sequence;
            SetAnalyticEndpoint();
            analyticsStatus = analyticsEnpoint.CreateAnalyticsObjects();
            if (analyticsStatus)
            {
                analyticsEnpoint.LogTest();
                analyticsEnpoint.LogTestObject();
            }
        }

        public void LogTest()
        {
            SetAnalyticEndpoint();
            analyticsStatus = analyticsEnpoint.CreateAnalyticsObjects();
            if (analyticsStatus)
            {
                analyticsEnpoint.LogTest();
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log Test Analytic due to issues with database", Message.MessageType.Error));
            }
        }

        public void LogAdvancedTest()
        {
            SetAnalyticEndpoint();
            analyticsStatus = analyticsEnpoint.CreateAnalyticsObjects();
            if (analyticsStatus)
            {
                analyticsEnpoint.LogAdvancedTest();
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log Advanced Test Analytic due to issues with database", Message.MessageType.Error));
            }
        }


        public void LogDataEngineStatsInterim(InterimStat stat)
        {
            if (analyticsStatus)
            {
                analyticsEnpoint.LogDataEngineStatsInterim(stat);
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log DOES.DataEngine Analytic due to issues with database", Message.MessageType.Error));
            }
        }

        public void LogDataEngineStatsThreadsInterim(InterimThreadStat stat)
        {
            if (analyticsStatus)
            {
                analyticsEnpoint.LogDataEngineStatsThreadsInterim(stat);
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log DOES.DataEngine Analytic due to issues with database", Message.MessageType.Error));
            }
        }

        public void LogDataEngineStatsThreads(int threadNumber, UInt64 dataProcessedBytes, UInt64 rowsAffected, UInt64 transactionsCompleted, UInt64 transactionsFailed, UInt64 totalSeconds,
           Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            if (analyticsStatus)
            {
                analyticsEnpoint.LogDataEngineStatsThreads(threadNumber, dataProcessedBytes, rowsAffected, transactionsCompleted, transactionsFailed, totalSeconds, operation, collectedOn);
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log DOES.DataEngine Analytic due to issues with database", Message.MessageType.Error));
            }
        }

        public void LogDataEngineStatsTotal(UInt64 dataProcessedBytes, UInt64 rowsAffected, UInt64 transactionsCompleted, UInt64 transactionsFailed, UInt64 totalSeconds, int numberOfThreads, int numberOfTables,
            Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            if (analyticsStatus)
            {
                analyticsEnpoint.LogDataEngineStatsTotal(dataProcessedBytes, rowsAffected, transactionsCompleted, transactionsFailed, totalSeconds, numberOfThreads, numberOfTables, operation, collectedOn);
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log DOES.DataEngine Analytic due to issues with database", Message.MessageType.Error));
            }
        }

        public void LogDataEngineStatsFinalReport(string reportText, Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            if (analyticsStatus)
            {
                analyticsEnpoint.LogDataEngineStatsFinalReport(reportText, operation, collectedOn);
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log DOES.DataEngine Analytic due to issues with database", Message.MessageType.Error));
            }
        }

        public void LogPlatformEngineWindowsResource(WindowsResource windowsData)
        {
            if (analyticsStatus)
            {
                analyticsEnpoint.LogPlatformEngineWindowsResource(windowsData);
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log PlatFormEngine Analytic due to issues with database", Message.MessageType.Error));
            }
        }

        public void LogPlatformEngineLinuxResource(LinuxResource linuxData)
        {
            if (analyticsStatus)
            {
                analyticsEnpoint.LogPlatformEngineLinuxResource(linuxData);
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log PlatformEngine Analytic due to issues with database", Message.MessageType.Error));
            }

        }

        public void LogSequenceData(string column, DateTime value, Sequence objectiveItem)
        {
            if (analyticsStatus)
            {
                analyticsEnpoint.LogSequenceData(column, value, objectiveItem);
            }
            else
            {
                _queue.AddMessage(new Message(DateTime.Now, "Cannot Log Test Analytic due to issues with database", Message.MessageType.Error));
            }
        }
    }
}
