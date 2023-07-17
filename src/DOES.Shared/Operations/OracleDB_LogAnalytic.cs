using DOES.Shared.Debug;
using DOES.Shared.Resources;
using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using System.Text;
using System.Text.RegularExpressions;

namespace DOES.Shared.Operations
{
    public class OracleDB_LogAnalytic : LogAnalytic
    {
        private Analytic _analytic;

        public OracleDB_LogAnalytic(Analytic analytic)
        {
            _analytic = analytic;
        }

        public override DOESTest LogTest()
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    using (OracleCommand command = new OracleCommand())
                    {
                        string checkForExistingTestObject = "SELECT TestID FROM Tests where TestName = '" + _analytic.Test.TestName + "'";
                        command.CommandText = checkForExistingTestObject;
                        command.Connection = connection;
                        connection.Open();
                        OracleDataReader reader = command.ExecuteReader();
                        DateTime TimeUpdated = DateTime.Now;
                        if (reader.HasRows == true)
                        {
                            while (reader.Read())
                            {
                                _analytic.Test.TestID = Convert.ToInt32(reader[0]);
                            }
                            connection.Close();
                            //update start time 

                            string updateTestObject = "UPDATE Tests SET LastChecked = :NewLastChecked WHERE TestName = '" + _analytic.Test.TestName + "'";
                            using (OracleCommand updateTestEntryCommand = new OracleCommand())
                            {
                                updateTestEntryCommand.Connection = connection;
                                updateTestEntryCommand.Parameters.Add(":NewLastChecked", TimeUpdated);
                                updateTestEntryCommand.CommandText = updateTestObject;
                                connection.Open();
                                updateTestEntryCommand.ExecuteNonQuery();
                                connection.Close();
                            }
                        }
                        else
                        {
                            connection.Close();
                            if (_analytic.Test.CodeRevision == null)
                            {
                                connection.Open();
                                OracleCommand testIDCommand = new OracleCommand("SELECT Seq_TestID.NEXTVAL FROM DUAL", connection);
                                _analytic.Test.TestID = Convert.ToInt32(testIDCommand.ExecuteScalar());
                                connection.Close();
                                string newTestObject = "INSERT INTO TESTS (TestID, TestName, StartDate, LastChecked) VALUES(:TestID ,:TestName, :StartDate, :LastChecked)";
                                using (OracleCommand newtestEntryCommand = new OracleCommand())
                                {
                                    newtestEntryCommand.Parameters.Add(":TestID", _analytic.Test.TestID);
                                    newtestEntryCommand.Parameters.Add(":TestName", _analytic.Test.TestName);
                                    newtestEntryCommand.Parameters.Add(":StartDate", DateTime.Now);
                                    newtestEntryCommand.Parameters.Add(":LastChecked", DateTime.Now);
                                    newtestEntryCommand.Connection = connection;
                                    newtestEntryCommand.CommandText = newTestObject;
                                    connection.Open();
                                    newtestEntryCommand.ExecuteNonQuery();
                                    connection.Close();
                                }
                            }
                            else
                            {
                                connection.Open();
                                OracleCommand testIDCommand = new OracleCommand("SELECT Seq_TestID.NEXTVAL FROM DUAL", connection);
                                _analytic.Test.TestID = Convert.ToInt32(testIDCommand.ExecuteScalar());
                                connection.Close();
                                string newTestObject = "INSERT INTO TESTS (TestID, TestName, StartDate, CodeRevision, LastChecked) VALUES(:TestID, :TestName, :StartDate, :CodeRevision, :LastChecked)";
                                using (OracleCommand newtestEntryCommand = new OracleCommand())
                                {
                                    newtestEntryCommand.Parameters.Add(":TestID", _analytic.Test.TestID);
                                    newtestEntryCommand.Parameters.Add(":TestName", _analytic.Test.TestName);
                                    newtestEntryCommand.Parameters.Add(":StartDate", DateTime.Now);
                                    newtestEntryCommand.Parameters.Add(":CodeRevision", _analytic.Test.CodeRevision);
                                    newtestEntryCommand.Parameters.Add(":LastChecked", DateTime.Now);
                                    newtestEntryCommand.Connection = connection;
                                    newtestEntryCommand.CommandText = newTestObject;
                                    connection.Open();
                                    newtestEntryCommand.ExecuteNonQuery();
                                    connection.Close();
                                }
                            }
                        }
                    }
                }
            }
            catch (OracleException ex1)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex1.Message, Message.MessageType.Error));
                _analytic.Test.TestID = 0;
            }
            return _analytic.Test;
        }

        public override DOESTest LogAdvancedTest()
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    using (OracleCommand command = new OracleCommand())
                    {
                        string checkForExistingTestObject = "SELECT TestID FROM Tests where TestName = '" + _analytic.Test.TestName + "'";
                        command.CommandText = checkForExistingTestObject;
                        
                        command.Connection = connection;
                        connection.Open();
                        OracleDataReader reader = command.ExecuteReader();
                        DateTime TimeUpdated = DateTime.Now;
                        if (reader.HasRows == true)
                        {
                            while (reader.Read())
                            {
                                _analytic.Test.TestID = Convert.ToInt32(reader[0]);
                            }
                            connection.Close();
                            //update start time 

                            string updateTestObject = "UPDATE Tests SET LastChecked = :NewLastChecked WHERE TestName = '" + _analytic.Test.TestName + "'";
                            using (OracleCommand updateTestEntryCommand = new OracleCommand())
                            {
                                updateTestEntryCommand.Connection = connection;
                                updateTestEntryCommand.Parameters.Add(":NewLastChecked", TimeUpdated);
                                updateTestEntryCommand.CommandText = updateTestObject;
                                connection.Open();
                                updateTestEntryCommand.ExecuteNonQuery();
                                connection.Close();
                            }
                        }
                        else
                        {
                            connection.Close();
                            if (_analytic.Test.CodeRevision == null)
                            {
                                connection.Open();
                                OracleCommand testIDCommand = new OracleCommand("SELECT Seq_TestID.NEXTVAL FROM DUAL", connection);
                                _analytic.Test.TestID = Convert.ToInt32(testIDCommand.ExecuteScalar());
                                connection.Close();
                                string newTestObject = "INSERT INTO TESTS (TestID, TestName, StartDate, LastChecked, Solution, " +
                                    "DataSize, ChangeRate, Attempt) " +
                                    "VALUES(:TestID, :TestName, :StartDate, :LastChecked, :Solution, " +
                                    ":DataSize, :ChangeRate, :Attempt)";
                                using (OracleCommand newtestEntryCommand = new OracleCommand())
                                {
                                    newtestEntryCommand.Parameters.Add(":TestID", _analytic.Test.TestID);
                                    newtestEntryCommand.Parameters.Add(":TestName", _analytic.Test.TestName);
                                    newtestEntryCommand.Parameters.Add(":StartDate", DateTime.Now);
                                    newtestEntryCommand.Parameters.Add(":LastChecked", DateTime.Now);
                                    newtestEntryCommand.Parameters.Add(":Solution", _analytic.Test.Solution.ToString());
                                    newtestEntryCommand.Parameters.Add(":DataSize", _analytic.Test.DataSize);
                                    newtestEntryCommand.Parameters.Add(":ChangeRate", _analytic.Test.ChangeRate);
                                    newtestEntryCommand.Parameters.Add(":Attempt", _analytic.Test.Attempt);
                                    newtestEntryCommand.Connection = connection;
                                    newtestEntryCommand.CommandText = newTestObject;
                                    connection.Open();
                                    newtestEntryCommand.ExecuteNonQuery();
                                    connection.Close();
                                }
                            }
                            else
                            {
                                connection.Open();
                                OracleCommand testIDCommand = new OracleCommand("SELECT Seq_TestID.NEXTVAL FROM DUAL", connection);
                                _analytic.Test.TestID = Convert.ToInt32(testIDCommand.ExecuteScalar());
                                connection.Close();
                                string newTestObject = "INSERT INTO TESTS (TestID, TestName, StartDate, LastChecked, CodeRevision, Solution, " +
                                    "DataSize, ChangeRate, Attempt) " +
                                    "VALUES(:TestID, :TestName, :StartDate, :LastChecked, :CodeRevision, :Solution, " +
                                    ":DataSize, :ChangeRate, :Attempt)";
                                using (OracleCommand newtestEntryCommand = new OracleCommand())
                                {
                                    newtestEntryCommand.Parameters.Add(":TestID", _analytic.Test.TestID);
                                    newtestEntryCommand.Parameters.Add(":TestName", _analytic.Test.TestName);
                                    newtestEntryCommand.Parameters.Add(":StartDate", DateTime.Now);
                                    newtestEntryCommand.Parameters.Add(":LastChecked", DateTime.Now);
                                    newtestEntryCommand.Parameters.Add(":CodeRevision", _analytic.Test.CodeRevision);
                                    newtestEntryCommand.Parameters.Add(":Solution", _analytic.Test.Solution.ToString());
                                    newtestEntryCommand.Parameters.Add(":DataSize", _analytic.Test.DataSize);
                                    newtestEntryCommand.Parameters.Add(":ChangeRate", _analytic.Test.ChangeRate);
                                    newtestEntryCommand.Parameters.Add(":Attempt", _analytic.Test.Attempt);
                                    newtestEntryCommand.Connection = connection;
                                    newtestEntryCommand.CommandText = newTestObject;
                                    connection.Open();
                                    newtestEntryCommand.ExecuteNonQuery();
                                    connection.Close();
                                }
                            }
                        }
                    }
                }
            }
            catch (OracleException ex1)
            {

                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex1.Message, Message.MessageType.Error));
                _analytic.Test.TestID = 0;
            }
            return _analytic.Test;
        }

        public override DOESObject LogTestObject()
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    using (OracleCommand command = new OracleCommand())
                    {
                        string checkForExistingTestObject = "";
                        string newObject = "";
                        if (_analytic.TestObject.ObjectCategory == null)
                        {
                            checkForExistingTestObject = "SELECT ObjectID FROM Objects WHERE ObjectTag = '" + _analytic.TestObject.ObjectTag + "' AND TestID = " + _analytic.Test.TestID;
                            newObject = "INSERT INTO Objects (TestID, ObjectID, ObjectTag) VALUES(:TestID, :ObjectID, :ObjectTag)";
                        }
                        else if (_analytic.TestObject.ObjectCategory != null)
                        {
                            checkForExistingTestObject = "SELECT ObjectID FROM Objects WHERE ObjectTag = '" + _analytic.TestObject.ObjectTag + "' AND Category = '" + _analytic.TestObject.ObjectCategory +
                                "' AND TestID = " + _analytic.Test.TestID;
                            newObject = "INSERT INTO Objects (TestID, ObjectID,  ObjectTag, Category) VALUES(:TestID, :ObjectID, :ObjectTag, :Category)";
                        }
                        command.CommandText = checkForExistingTestObject;
                        command.Connection = connection;
                        connection.Open();
                        OracleDataReader reader = command.ExecuteReader();
                        if (reader.HasRows == true)
                        {
                            while (reader.Read())
                            {
                                _analytic.TestObject.ObjectID = Convert.ToInt32(reader[0]);
                            }
                            connection.Close();
                        }
                        else
                        {
                            connection.Close();
                            connection.Open();
                            OracleCommand objectIDCommand = new OracleCommand("SELECT Seq_ObjectID.NEXTVAL FROM DUAL", connection);
                            _analytic.TestObject.ObjectID = Convert.ToInt32(objectIDCommand.ExecuteScalar());
                            connection.Close();
                            using (OracleCommand newObjectEntryCommand = new OracleCommand())
                            {
                                newObjectEntryCommand.Parameters.Add(":TestID", _analytic.Test.TestID);
                                newObjectEntryCommand.Parameters.Add(":ObjectID", _analytic.TestObject.ObjectID);
                                newObjectEntryCommand.Parameters.Add(":ObjectTag", _analytic.TestObject.ObjectTag);
                                if (_analytic.TestObject.ObjectCategory != null) { newObjectEntryCommand.Parameters.Add(":Category", _analytic.TestObject.ObjectCategory); }
                                newObjectEntryCommand.Connection = connection;
                                newObjectEntryCommand.CommandText = newObject;
                                connection.Open();
                                newObjectEntryCommand.ExecuteNonQuery();
                                connection.Close();
                            }
                        }
                    }
                }
            }
            catch (OracleException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                _analytic.TestObject.ObjectID = 0;
            }
            return _analytic.TestObject;
        }

        public override void LogSequenceData(string column, DateTime value, Sequence objectiveItem)
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    using (OracleCommand command = new OracleCommand())
                    {
                        string checkForExistingTestObject = "SELECT * FROM SequenceStats WHERE TestID = (SELECT TestID FROM Tests WHERE TestName = '" +
                            objectiveItem.AnalyticObject.Test.TestName + "') AND Sequence = :Sequence";
                        command.CommandText = checkForExistingTestObject;
                        command.Parameters.Add(":Sequence", objectiveItem.SequenceNumber.ToString());
                        
                        command.Connection = connection;
                        connection.Open();
                        OracleDataReader reader = command.ExecuteReader();
                        if (reader.HasRows == true)
                        {
                            connection.Close();
                            //update existing records

                            string updateSequenceObject = "UPDATE SequenceStats SET " + column + " = :NewValue WHERE TestID = (SELECT TestID FROM Tests WHERE TestName = '" +
                                objectiveItem.AnalyticObject.Test.TestName + "') AND Sequence = :Sequence";
                            using (OracleCommand updateSequenceEntry = new OracleCommand())
                            {
                                updateSequenceEntry.Connection = connection;
                                updateSequenceEntry.Parameters.Add(":NewValue", value);
                                updateSequenceEntry.Parameters.Add(":Sequence", objectiveItem.SequenceNumber.ToString());
                                updateSequenceEntry.CommandText = updateSequenceObject;
                                connection.Open();
                                updateSequenceEntry.ExecuteNonQuery();
                                connection.Close();
                            }
                        }
                        else
                        {
                            connection.Close();
                            string newSequenceObject = "INSERT INTO SequenceStats (TestID, Sequence, " + column + ") VALUES ((SELECT TestID FROM Tests WHERE TestName = '" +
                                objectiveItem.AnalyticObject.Test.TestName + "'), :Sequence, :Value)";
                            using (OracleCommand newtestEntryCommand = new OracleCommand())
                            {

                                newtestEntryCommand.Parameters.Add(":Sequence", objectiveItem.SequenceNumber.ToString());
                                newtestEntryCommand.Parameters.Add(":Value", value);
                                newtestEntryCommand.Connection = connection;
                                newtestEntryCommand.CommandText = newSequenceObject;
                                connection.Open();
                                newtestEntryCommand.ExecuteNonQuery();
                                connection.Close();
                            }
                            _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, "Sequence does not exist , creating record", Message.MessageType.Info));
                        }
                    }
                }
            }
            catch (OracleException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsInterim(InterimStat stat)
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newInterimStatString = "INSERT INTO DataEngineStatsInterim (ObjectID, Sequence, DataProcessedBytes, RowPerformancePerSecond, TransactionPerformancePerSecond, " +
                        "DataRatePerformanceMbytesPerSecond, Operation, CollectedOn) VALUES (:ObjectID, :Sequence, :DataProcessedBytes, :RowPerformancePerSecond, :TransactionPerformancePerSecond, " +
                        ":DataRatePerformanceMbytesPerSecond, :Operation, :CollectedOn)";
                    using (OracleCommand command = new OracleCommand(newInterimStatString, connection))
                    {
                        command.CommandTimeout = 1200;
                        
                        command.Parameters.Add(":ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.Add(":Sequence", _analytic.Sequence);
                        command.Parameters.Add(":DataProcessedBytes", Convert.ToInt64(stat.DataProcessedBytes));
                        command.Parameters.Add(":RowPerformancePerSecond", stat.RowRatePerformance);
                        command.Parameters.Add(":TransactionPerformancePerSecond", stat.TransactionalPerformance);
                        command.Parameters.Add(":DataRatePerformanceMbytesPerSecond", stat.DataRatePerformance);
                        command.Parameters.Add(":Operation", stat.Operation.ToString());
                        command.Parameters.Add(":CollectedOn", stat.CollectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (OracleException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsThreadsInterim(InterimThreadStat stat)
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newInterimStatString = "INSERT INTO DataEngineStatsThreadsInterim (ObjectID, ThreadID, Sequence, DataProcessedBytes, RowPerformancePerSecond, TransactionPerformancePerSecond, " +
                        "DataRatePerformanceMbytesPerSecond, Operation, CollectedOn) VALUES (:ObjectID, :ThreadID, :Sequence, :DataProcessedBytes, :RowPerformancePerSecond, :TransactionPerformancePerSecond, " +
                        ":DataRatePerformanceMbytesPerSecond, :Operation, :CollectedOn)";
                    using (OracleCommand command = new OracleCommand(newInterimStatString, connection))
                    {
                        command.CommandTimeout = 1200;
                        
                        command.Parameters.Add(":ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.Add(":ThreadID", stat.ThreadID);
                        command.Parameters.Add(":Sequence", _analytic.Sequence);
                        command.Parameters.Add(":DataProcessedBytes", Convert.ToInt64(stat.DataProcessedBytes));
                        command.Parameters.Add(":RowPerformancePerSecond", stat.RowRatePerformance);
                        command.Parameters.Add(":TransactionPerformancePerSecond", stat.TransactionalPerformance);
                        command.Parameters.Add(":DataRatePerformanceMbytesPerSecond", stat.DataRatePerformance);
                        command.Parameters.Add(":Operation", stat.Operation.ToString());
                        command.Parameters.Add(":CollectedOn", stat.CollectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (OracleException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsThreads(int threadNumber, UInt64 dataProcessedBytes, UInt64 rowsAffected, UInt64 transactionsCompleted, UInt64 transactionsFailed, UInt64 totalSeconds,
            Dynamics.DataEngineOperation operation_, DateTime collectedOn)
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newThreadStatString = "INSERT INTO DataEngineStatsThreads (ObjectID, Sequence, ThreadID, DataSizeBytes, RowsAffected, TransactionsCompleted, TransactionsFailed, TotalSeconds, " +
                        "Operation, CollectedOn) VALUES (:ObjectID, :Sequence, :ThreadID, :DataSizeBytes, :RowsAffected, :TransactionsCompleted, :TransactionsFailed, :TotalSeconds, " +
                        ":Operation, :CollectedOn)";
                    using (OracleCommand command = new OracleCommand(newThreadStatString, connection))
                    {
                        command.CommandTimeout = 1200;
                        
                        command.Parameters.Add(":ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.Add(":Sequence", _analytic.Sequence);
                        command.Parameters.Add(":ThreadID", threadNumber);
                        command.Parameters.Add(":DataSizeBytes", Convert.ToInt64(dataProcessedBytes));
                        command.Parameters.Add(":RowsAffected", Convert.ToInt64(rowsAffected));
                        command.Parameters.Add(":TransactionsCompleted", Convert.ToInt64(transactionsCompleted));
                        command.Parameters.Add(":TransactionsFailed", Convert.ToInt64(transactionsFailed));
                        command.Parameters.Add(":TotalSeconds", Convert.ToInt64(totalSeconds));
                        command.Parameters.Add(":Operation", operation_.ToString());
                        command.Parameters.Add(":CollectedOn", collectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (OracleException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsTotal(UInt64 dataProcessedBytes, UInt64 rowsAffected, UInt64 transactionsCompleted, UInt64 transactionsFailed, UInt64 totalSeconds, int numberOfThreads, int numberOfTables,
            Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newTotalStatsString = "INSERT INTO DataEngineStatsTotal (ObjectID, Sequence, DataSizeBytes, RowsAffected, TransactionsCompleted, TransactionsFailed, TotalSeconds, NumberOfThreads," +
                        "NumberOfTables, Operation, CollectedOn) VALUES (:ObjectID, :Sequence, :DataSizeBytes, :RowsAffected, :TransactionsCompleted, :TransactionsFailed, :TotalSeconds, :NumberOfThreads," +
                        ":NumberOfTables, :Operation, :CollectedOn)";
                    using (OracleCommand command = new OracleCommand(newTotalStatsString, connection))
                    {
                        command.CommandTimeout = 1200;
                        
                        command.Parameters.Add(":ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.Add(":Sequence", _analytic.Sequence);
                        command.Parameters.Add(":DataSizeBytes", Convert.ToInt64(dataProcessedBytes));
                        command.Parameters.Add(":RowsAffected", Convert.ToInt64(rowsAffected));
                        command.Parameters.Add(":TransactionsCompleted", Convert.ToInt64(transactionsCompleted));
                        command.Parameters.Add(":TransactionsFailed", Convert.ToInt64(transactionsFailed));
                        command.Parameters.Add(":TotalSeconds", Convert.ToInt64(totalSeconds));
                        command.Parameters.Add(":NumberOfThreads", numberOfThreads);
                        command.Parameters.Add(":NumberOfTables", numberOfTables);
                        command.Parameters.Add(":Operation", operation.ToString());
                        command.Parameters.Add(":CollectedOn", collectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (OracleException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsFinalReport(string reportText, Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    connection.Open();
                    OracleCommand reportIDCommand = new OracleCommand("SELECT Seq_ReportID.NEXTVAL FROM DUAL", connection);
                    int reportID = Convert.ToInt32(reportIDCommand.ExecuteScalar());
                    connection.Close();
                    string newFinalReportString = "INSERT INTO DataEngineFinalReports (ReportID, ObjectID, Sequence, Report, Operation, CollectedOn) VALUES " +
                        "(:ReportID, :ObjectID, :Sequence, :Report, :Operation, :CollectedOn)";
                    using (OracleCommand command = new OracleCommand(newFinalReportString, connection))
                    {
                        command.CommandTimeout = 1200;
                        command.Parameters.Add(":ReportID", reportID);
                        command.Parameters.Add(":ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.Add(":Sequence", _analytic.Sequence);
                        command.Parameters.Add(":Report", reportText);
                        command.Parameters.Add(":Operation", operation.ToString());
                        command.Parameters.Add(":CollectedOn", collectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (OracleException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogPlatformEngineWindowsResource(WindowsResource windowsData)
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newResourceEntry = "INSERT INTO WindowsResourceData(ObjectID, Sequence, ProcessorTime, ProcessorPrivledgedTime, ProcessorInterruptTime, ProcessorDPCTime, " +
                        "CurrentClockSpeed, ExtClock, DataWidth, MaxClockSpeed, NumberOfLogicalProcessors, NumberOfProcessors, TotalPhysicalMemory, UsedPhysicalMemory, FreePhysicalMemory, " +
                        "PageFileUsed, PoolPagedBytesMemory, PoolNonPagedBytesMemory, CachedBytesMemory, PhysicalDiskAvgQueueLength, PhysicalDiskReadBytes, PhysicalDiskWriteBytes, " +
                        "PhysicalDiskAvgReadBytes, PhysicalDiskAvgWriteBytes, PhysicalDiskTime, ProcessHandleCount, ProcessThreadCount, ProcessContextSwitchCount, ProcessSystemCalls, ProcessorQueueLength, " +
                        "CollectedOn) " +
                        "VALUES (:ObjectID, :Sequence, :ProcessorTime, :ProcessorPrivledgedTime, :ProcessorInterruptTime, :ProcessorDPCTime, " +
                        ":CurrentClockSpeed, :ExtClock, :DataWidth, :MaxClockSpeed, :NumberOfLogicalProcessors, :NumberOfProcessors, :TotalPhysicalMemory, :UsedPhysicalMemory, :FreePhysicalMemory, " +
                        ":PageFileUsed, :PoolPagedBytesMemory, :PoolNonPagedBytesMemory, :CachedBytesMemory, :PhysicalDiskAvgQueueLength, :PhysicalDiskReadBytes, :PhysicalDiskWriteBytes, " +
                        ":PhysicalDiskAvgReadBytes, :PhysicalDiskAvgWriteBytes, :PhysicalDiskTime, :ProcessHandleCount, :ProcessThreadCount, :ProcessContextSwitchCount, :ProcessSystemCalls, :ProcessorQueueLength, " +
                        ":CollectedOn) ";
                    using (OracleCommand command = new OracleCommand(newResourceEntry, connection))
                    {
                        command.CommandTimeout = 600;
                        
                        command.Parameters.Add(":ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.Add(":Sequence", _analytic.Sequence);
                        command.Parameters.Add(":ProcessorTime", windowsData.ProcessorTime);
                        command.Parameters.Add(":ProcessorPrivledgedTime", windowsData.ProcessorPrivledgedTime);
                        command.Parameters.Add(":ProcessorInterruptTime", windowsData.ProcessorInterruptTime);
                        command.Parameters.Add(":ProcessorDPCTime", windowsData.ProcessorDPCTime);
                        command.Parameters.Add(":CurrentClockSpeed", windowsData.CurrentClockSpeed);
                        command.Parameters.Add(":ExtClock", windowsData.ExtClock);
                        command.Parameters.Add(":DataWidth", windowsData.DataWidth);
                        command.Parameters.Add(":MaxClockSpeed", windowsData.MaxClockSpeed);
                        command.Parameters.Add(":NumberOfLogicalProcessors", windowsData.NumberOfLogicalProcessors);
                        command.Parameters.Add(":NumberOfProcessors", windowsData.NumberOfProcessors);
                        command.Parameters.Add(":TotalPhysicalMemory", windowsData.TotalPhysicalMemory);
                        command.Parameters.Add(":UsedPhysicalMemory", windowsData.UsedPhysicalMemory);
                        command.Parameters.Add(":FreePhysicalMemory", windowsData.FreePhysicalMemory);
                        command.Parameters.Add(":PageFileUsed", windowsData.PageFileUsed);
                        command.Parameters.Add(":PoolPagedBytesMemory", windowsData.PoolPagedBytesMemory);
                        command.Parameters.Add(":PoolNonPagedBytesMemory", windowsData.PoolNonPagedBytesMemory);
                        command.Parameters.Add(":CachedBytesMemory", windowsData.CachedBytesMemory);
                        command.Parameters.Add(":PhysicalDiskAvgQueueLength", windowsData.PhysicalDiskAvgQueueLength);
                        command.Parameters.Add(":PhysicalDiskReadBytes", windowsData.PhysicalDiskReadBytes);
                        command.Parameters.Add(":PhysicalDiskWriteBytes", windowsData.PhysicalDiskWriteBytes);
                        command.Parameters.Add(":PhysicalDiskAvgReadBytes", windowsData.PhysicalDiskAvgReadBytes);
                        command.Parameters.Add(":PhysicalDiskAvgWriteBytes", windowsData.PhysicalDiskAvgWriteBytes);
                        command.Parameters.Add(":PhysicalDiskTime", windowsData.PhysicalDiskTime);
                        command.Parameters.Add(":ProcessHandleCount", windowsData.ProcessHandleCount);
                        command.Parameters.Add(":ProcessThreadCount", windowsData.ProcessThreadCount);
                        command.Parameters.Add(":ProcessContextSwitchCount", windowsData.ProcessContextSwitchCount);
                        command.Parameters.Add(":ProcessSystemCalls", windowsData.ProcessSystemCalls);
                        command.Parameters.Add(":ProcessorQueueLength", windowsData.ProcessorQueueLength);
                        command.Parameters.Add(":CollectedOn", windowsData.CollectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (OracleException error)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, error.Message, Message.MessageType.Error));
            }
        }

        public override void LogPlatformEngineLinuxResource(LinuxResource linuxData)
        {
            try
            {
                using (OracleConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newResourceEntry = "INSERT INTO LinuxResourceData(ObjectID, Sequence, CPULoad, TotalPhysicalMemory, UsedPhysicalMemory, FreePhysicalMemory, " +
                        "CurrentClockSpeed, SDTransfersPerSecond, SDkBRead, SDkBWrite, SDkBReadps, SDkBWriteps, NVMeTransfersPerSecond, NVMekBRead, NVMekBWrite, NVMekBReadps, NVMekBWriteps," +
                        "DMTransfersPerSecond, DMkBRead, DMkBWrite, DMkBReadps, DMkBWriteps, CollectedOn) " +
                        "VALUES (:ObjectID, :Sequence, :CPULoad, :TotalPhysicalMemory, :UsedPhysicalMemory, :FreePhysicalMemory, " +
                        ":CurrentClockSpeed, :SDTransfersPerSecond, :SDkBRead, :SDkBWrite, :SDkBReadps, :SDkBWriteps, :NVMeTransfersPerSecond, :NVMekBRead, :NVMekBWrite, :NVMekBReadps, :NVMekBWriteps," +
                        ":DMTransfersPerSecond, :DMkBRead, :DMkBWrite, :DMkBReadps, :DMkBWriteps, :CollectedOn) ";
                    using (OracleCommand command = new OracleCommand(newResourceEntry, connection))
                    {
                        command.CommandTimeout = 600;
                        
                        command.Parameters.Add(":ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.Add(":Sequence", _analytic.Sequence);
                        command.Parameters.Add(":CPULoad", linuxData.CPULoad);
                        command.Parameters.Add(":TotalPhysicalMemory", linuxData.TotalPhysicalMemory);
                        command.Parameters.Add(":UsedPhysicalMemory", linuxData.UsedPhysicalMemory);
                        command.Parameters.Add(":FreePhysicalMemory", linuxData.FreePhysicalMemory);
                        command.Parameters.Add(":CurrentClockSpeed", linuxData.CurrentClockSpeed);
                        command.Parameters.Add(":SDTransfersPerSecond", linuxData.SDTransfersPerSecond);
                        command.Parameters.Add(":SDkBRead", linuxData.SDKBRead);
                        command.Parameters.Add(":SDkBWrite", linuxData.SDKBWrite);
                        command.Parameters.Add(":SDkBReadps", linuxData.SDKBReadps);
                        command.Parameters.Add(":SDkBWriteps", linuxData.SDKBWriteps);
                        command.Parameters.Add(":NVMeTransfersPerSecond", linuxData.NVMETransfersPerSecond);
                        command.Parameters.Add(":NVMekBRead", linuxData.NVMEKBRead);
                        command.Parameters.Add(":NVMekBWrite", linuxData.NVMEKBWrite);
                        command.Parameters.Add(":NVMekBReadps", linuxData.NVMEKBReadps);
                        command.Parameters.Add(":NVMekBWriteps", linuxData.NVMEKBWriteps);
                        command.Parameters.Add(":DMTransfersPerSecond", linuxData.DMTransfersPerSecond);
                        command.Parameters.Add(":DMkBRead", linuxData.DMKBRead);
                        command.Parameters.Add(":DMkBWrite", linuxData.DMKBWrite);
                        command.Parameters.Add(":DMkBReadps", linuxData.DMKBReadps);
                        command.Parameters.Add(":DMkBWriteps", linuxData.DMKBWriteps);
                        command.Parameters.Add(":CollectedOn", linuxData.CollectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (OracleException error)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, error.Message, Message.MessageType.Error));
            }
        }

        public override bool CreateAnalyticsObjects()
        {
            bool sucess = true;
            OracleConnection conn = _analytic.Persistence.PersistenceInstance.GetConnection();
            try
            {
                conn.Open();
                foreach (string table in _analytic.Persistence.AnalyticsTables)
                {
                    OracleCommand command = new OracleCommand("SELECT * FROM " + table + " WHERE rownum = 1", conn);
                    command.CommandTimeout = 120;
                    command.ExecuteScalar();
                }
                conn.Close();
            }
            catch (Exception ex)
            {
                List<string> baseSchemaCreationCommands = _analytic.Persistence.GetXMLResource();
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                conn.Close();
                using (conn)
                {
                    try
                    {
                        using (OracleCommand command = new OracleCommand())
                        {
                            StringBuilder sb = new StringBuilder();
                            foreach (string s in baseSchemaCreationCommands)
                            {
                                if (s.Trim() == "")
                                {
                                    continue;
                                }
                                else if (s.StartsWith("//") == true)
                                {
                                    if (sb.Length != 0)
                                    {
                                        string SQLString = Regex.Replace(sb.ToString(), @"\n|\r|\t", "");
                                        command.CommandText = sb.ToString();
                                        
                                        command.Connection = conn;
                                        try
                                        {
                                            conn.Open();
                                            command.ExecuteNonQuery();
                                            conn.Close();
                                        }
                                        catch (OracleException sqlex)
                                        {
                                            if (!sqlex.Message.Contains("ORA - 00955: name is already used by an existing object"))
                                            {
                                                sucess = false;
                                                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, sqlex.Message, Message.MessageType.Error));
                                            }
                                            if (conn.State == System.Data.ConnectionState.Open)
                                            {
                                                conn.Close();
                                            }

                                        }
                                    }
                                    sb.Clear();
                                }
                                else
                                {
                                    if (s.StartsWith("\t"))
                                    {
                                        string truncatedString = s.Replace("\t", "");
                                        sb.Append(truncatedString);
                                    }
                                    else
                                    {
                                        sb.Append(s);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex1)
                    {
                        sucess = false;
                        _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex1.Message, Message.MessageType.Error));
                    }
                }
            }
            return sucess;
        }
    }
}
