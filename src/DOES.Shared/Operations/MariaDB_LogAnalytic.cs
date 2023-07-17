using DOES.Shared.Debug;
using DOES.Shared.Resources;
using System;
using System.Collections.Generic;
using MySqlConnector;
using System.Text;
using System.Text.RegularExpressions;

namespace DOES.Shared.Operations
{
    public class MariaDB_LogAnalytic : LogAnalytic
    {
        private Analytic _analytic;

        public MariaDB_LogAnalytic(Analytic analytic)
        {
            _analytic = analytic;
        }

        public override DOESTest LogTest()
        {
            try
            {
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        string checkForExistingTestObject = "SELECT TestID FROM Tests where TestName = '" + _analytic.Test.TestName + "'";
                        command.CommandText = checkForExistingTestObject;
                        command.Connection = connection;
                        connection.Open();
                        MySqlDataReader reader = command.ExecuteReader();
                        DateTime TimeUpdated = DateTime.Now;
                        if (reader.HasRows == true)
                        {
                            while (reader.Read())
                            {
                                _analytic.Test.TestID = Convert.ToInt32(reader[0]);
                            }
                            connection.Close();
                            //update start time 

                            string updateTestObject = "UPDATE Tests SET LastChecked = @NewLastChecked WHERE TestName = '" + _analytic.Test.TestName + "'";
                            using (MySqlCommand updateTestEntryCommand = new MySqlCommand())
                            {
                                updateTestEntryCommand.Connection = connection;
                                updateTestEntryCommand.Parameters.AddWithValue("@NewLastChecked", TimeUpdated);
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
                                string newTestObject = "INSERT INTO Tests (TestName, StartDate, LastChecked) VALUES(@TestName, @StartDate, @LastChecked);" +
                                     "SELECT LAST_INSERT_ID();";
                                using (MySqlCommand newtestEntryCommand = new MySqlCommand())
                                {
                                    newtestEntryCommand.Parameters.AddWithValue("@TestName", _analytic.Test.TestName);
                                    newtestEntryCommand.Parameters.AddWithValue("@StartDate", DateTime.Now);
                                    newtestEntryCommand.Parameters.AddWithValue("@LastChecked", DateTime.Now);
                                    newtestEntryCommand.Connection = connection;
                                    newtestEntryCommand.CommandText = newTestObject;
                                    connection.Open();
                                    _analytic.Test.TestID = Convert.ToInt32(newtestEntryCommand.ExecuteScalar());
                                    connection.Close();
                                }
                            }
                            else
                            {
                                string newTestObject = "INSERT INTO Tests (TestName, StartDate, CodeRevision, LastChecked) VALUES(@TestName, @StartDate, @CodeRevision, @LastChecked);" +
                                    "SELECT LAST_INSERT_ID();";
                                using (MySqlCommand newtestEntryCommand = new MySqlCommand())
                                {
                                    newtestEntryCommand.Parameters.AddWithValue("@TestName", _analytic.Test.TestName);
                                    newtestEntryCommand.Parameters.AddWithValue("@StartDate", DateTime.Now);
                                    newtestEntryCommand.Parameters.AddWithValue("@CodeRevision", _analytic.Test.CodeRevision);
                                    newtestEntryCommand.Parameters.AddWithValue("@LastChecked", DateTime.Now);
                                    newtestEntryCommand.Connection = connection;
                                    newtestEntryCommand.CommandText = newTestObject;
                                    connection.Open();
                                    _analytic.Test.TestID = Convert.ToInt32(newtestEntryCommand.ExecuteScalar());
                                    connection.Close();
                                }
                            }
                        }
                    }
                }
            }
            catch (MySqlException ex1)
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
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        string checkForExistingTestObject = "SELECT TestID FROM Tests where TestName = '" + _analytic.Test.TestName + "'";
                        command.CommandText = checkForExistingTestObject;
                        command.Connection = connection;
                        connection.Open();
                        MySqlDataReader reader = command.ExecuteReader();
                        DateTime TimeUpdated = DateTime.Now;
                        if (reader.HasRows == true)
                        {
                            while (reader.Read())
                            {
                                _analytic.Test.TestID = Convert.ToInt32(reader[0]);
                            }
                            connection.Close();
                            //update start time 

                            string updateTestObject = "UPDATE Tests SET LastChecked = @NewLastChecked WHERE TestName = '" + _analytic.Test.TestName + "'";
                            using (MySqlCommand updateTestEntryCommand = new MySqlCommand())
                            {
                                updateTestEntryCommand.Connection = connection;
                                updateTestEntryCommand.Parameters.AddWithValue("@NewLastChecked", TimeUpdated);
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
                                string newTestObject = "INSERT INTO Tests (TestName, StartDate, LastChecked, Solution, " +
                                    "DataSize, ChangeRate, Attempt) " +
                                    "VALUES(@TestName, @StartDate, @LastChecked, @Solution, " +
                                    "@DataSize, @ChangeRate, @Attempt);" +
                                     "SELECT LAST_INSERT_ID()";
                                using (MySqlCommand newtestEntryCommand = new MySqlCommand())
                                {
                                    newtestEntryCommand.Parameters.AddWithValue("@TestName", _analytic.Test.TestName);
                                    newtestEntryCommand.Parameters.AddWithValue("@StartDate", DateTime.Now);
                                    newtestEntryCommand.Parameters.AddWithValue("@LastChecked", DateTime.Now);
                                    newtestEntryCommand.Parameters.AddWithValue("@Solution", _analytic.Test.Solution.ToString());
                                    newtestEntryCommand.Parameters.AddWithValue("@DataSize", _analytic.Test.DataSize);
                                    newtestEntryCommand.Parameters.AddWithValue("@ChangeRate", _analytic.Test.ChangeRate);
                                    newtestEntryCommand.Parameters.AddWithValue("@Attempt", _analytic.Test.Attempt);
                                    newtestEntryCommand.Connection = connection;
                                    newtestEntryCommand.CommandText = newTestObject;
                                    connection.Open();
                                    _analytic.Test.TestID = Convert.ToInt32(newtestEntryCommand.ExecuteScalar());
                                    connection.Close();
                                }
                            }
                            else
                            {
                                string newTestObject = "INSERT INTO Tests (TestName, StartDate, LastChecked, CodeRevision, Solution, " +
                                    "DataSize, ChangeRate, Attempt) " +
                                    "VALUES(@TestName, @StartDate, @LastChecked, @CodeRevision, @Solution, " +
                                    "@DataSize, @ChangeRate, @Attempt);" +
                                    "SELECT LAST_INSERT_ID()";
                                using (MySqlCommand newtestEntryCommand = new MySqlCommand())
                                {
                                    newtestEntryCommand.Parameters.AddWithValue("@TestName", _analytic.Test.TestName);
                                    newtestEntryCommand.Parameters.AddWithValue("@StartDate", DateTime.Now);
                                    newtestEntryCommand.Parameters.AddWithValue("@LastChecked", DateTime.Now);
                                    newtestEntryCommand.Parameters.AddWithValue("@CodeRevision", _analytic.Test.CodeRevision);
                                    newtestEntryCommand.Parameters.AddWithValue("@Solution", _analytic.Test.Solution.ToString());
                                    newtestEntryCommand.Parameters.AddWithValue("@DataSize", _analytic.Test.DataSize);
                                    newtestEntryCommand.Parameters.AddWithValue("@ChangeRate", _analytic.Test.ChangeRate);
                                    newtestEntryCommand.Parameters.AddWithValue("@Attempt", _analytic.Test.Attempt);
                                    newtestEntryCommand.Connection = connection;
                                    newtestEntryCommand.CommandText = newTestObject;
                                    connection.Open();
                                    _analytic.Test.TestID = Convert.ToInt32(newtestEntryCommand.ExecuteScalar());
                                    connection.Close();
                                }
                            }
                        }
                    }
                }
            }
            catch (MySqlException ex1)
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
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        string checkForExistingTestObject = "";
                        string newObject = "";
                        if (_analytic.TestObject.ObjectCategory == null)
                        {
                            checkForExistingTestObject = "SELECT ObjectID FROM Objects WHERE ObjectTag = '" + _analytic.TestObject.ObjectTag + "' AND TestID = " + _analytic.Test.TestID;
                            newObject = "INSERT INTO Objects (TestID, ObjectTag) VALUES(@TestID, @ObjectTag);" +
                                 "SELECT LAST_INSERT_ID()";
                        }
                        else if (_analytic.TestObject.ObjectCategory != null)
                        {
                            checkForExistingTestObject = "SELECT ObjectID FROM Objects WHERE ObjectTag = '" + _analytic.TestObject.ObjectTag + "' AND Category = '" + _analytic.TestObject.ObjectCategory +
                                "' AND TestID = " + _analytic.Test.TestID;
                            newObject = "INSERT INTO Objects (TestID, ObjectTag, Category) VALUES(@TestID, @ObjectTag, @Category);" +
                                 "SELECT LAST_INSERT_ID()";
                        }
                        command.CommandText = checkForExistingTestObject;
                        command.Connection = connection;
                        connection.Open();
                        MySqlDataReader reader = command.ExecuteReader();
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
                            using (MySqlCommand newObjectEntryCommand = new MySqlCommand())
                            {
                                newObjectEntryCommand.Parameters.AddWithValue("@TestID", _analytic.Test.TestID);
                                newObjectEntryCommand.Parameters.AddWithValue("@ObjectTag", _analytic.TestObject.ObjectTag);
                                if (_analytic.TestObject.ObjectCategory != null) { newObjectEntryCommand.Parameters.AddWithValue("@Category", _analytic.TestObject.ObjectCategory); }
                                newObjectEntryCommand.Connection = connection;
                                newObjectEntryCommand.CommandText = newObject;
                                connection.Open();
                                _analytic.TestObject.ObjectID = Convert.ToInt32(newObjectEntryCommand.ExecuteScalar());
                                connection.Close();
                            }
                        }
                    }
                }
            }
            catch (MySqlException ex)
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
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        string checkForExistingTestObject = "SELECT * FROM SequenceStats WHERE TestID = (SELECT TestID FROM Tests WHERE TestName = '" +
                            objectiveItem.AnalyticObject.Test.TestName + "') AND Sequence = @Sequence";
                        command.CommandText = checkForExistingTestObject;
                        command.Parameters.AddWithValue("@Sequence", objectiveItem.SequenceNumber.ToString());
                        command.Connection = connection;
                        connection.Open();
                        MySqlDataReader reader = command.ExecuteReader();
                        if (reader.HasRows == true)
                        {
                            connection.Close();
                            //update existing records

                            string updateSequenceObject = "UPDATE SequenceStats SET " + column + " = @NewValue WHERE TestID = (SELECT TestID FROM Tests WHERE TestName = '" +
                                objectiveItem.AnalyticObject.Test.TestName + "') AND Sequence = @Sequence";
                            using (MySqlCommand updateSequenceEntry = new MySqlCommand())
                            {
                                updateSequenceEntry.Connection = connection;
                                updateSequenceEntry.Parameters.AddWithValue("@NewValue", value);
                                updateSequenceEntry.Parameters.AddWithValue("@Sequence", objectiveItem.SequenceNumber.ToString());
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
                                objectiveItem.AnalyticObject.Test.TestName + "'), @Sequence, @Value)";
                            using (MySqlCommand newtestEntryCommand = new MySqlCommand())
                            {

                                newtestEntryCommand.Parameters.AddWithValue("@Sequence", objectiveItem.SequenceNumber.ToString());
                                newtestEntryCommand.Parameters.AddWithValue("@Value", value);
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
            catch (MySqlException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsInterim(InterimStat stat)
        {
            try
            {
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newInterimStatString = "INSERT INTO DataEngineStatsInterim (ObjectID, Sequence, DataProcessedBytes, RowPerformancePerSecond, TransactionPerformancePerSecond, " +
                        "DataRatePerformanceMbytesPerSecond, Operation, CollectedOn) VALUES (@ObjectID, @Sequence, @DataProcessedBytes, @RowPerformancePerSecond, @TransactionPerformancePerSecond, " +
                        "@DataRatePerformanceMbytesPerSecond, @Operation, @CollectedOn)";
                    using (MySqlCommand command = new MySqlCommand(newInterimStatString, connection))
                    {
                        command.CommandTimeout = 1200;
                        command.Parameters.AddWithValue("@ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.AddWithValue("@Sequence", _analytic.Sequence);
                        command.Parameters.AddWithValue("@DataProcessedBytes", Convert.ToInt64(stat.DataProcessedBytes));
                        command.Parameters.AddWithValue("@RowPerformancePerSecond", stat.RowRatePerformance);
                        command.Parameters.AddWithValue("@TransactionPerformancePerSecond", stat.TransactionalPerformance);
                        command.Parameters.AddWithValue("@DataRatePerformanceMbytesPerSecond", stat.DataRatePerformance);
                        command.Parameters.AddWithValue("@Operation", stat.Operation.ToString());
                        command.Parameters.AddWithValue("@CollectedOn", stat.CollectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (MySqlException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsThreadsInterim(InterimThreadStat stat)
        {
            try
            {
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newInterimStatString = "INSERT INTO DataEngineStatsThreadsInterim (ObjectID, ThreadID, Sequence, DataProcessedBytes, RowPerformancePerSecond, TransactionPerformancePerSecond, " +
                        "DataRatePerformanceMbytesPerSecond, Operation, CollectedOn) VALUES (@ObjectID, @ThreadID, @Sequence, @DataProcessedBytes, @RowPerformancePerSecond, @TransactionPerformancePerSecond, " +
                        "@DataRatePerformanceMbytesPerSecond, @Operation, @CollectedOn)";
                    using (MySqlCommand command = new MySqlCommand(newInterimStatString, connection))
                    {
                        command.CommandTimeout = 1200;
                        command.Parameters.AddWithValue("@ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.AddWithValue("@ThreadID", stat.ThreadID);
                        command.Parameters.AddWithValue("@Sequence", _analytic.Sequence);
                        command.Parameters.AddWithValue("@DataProcessedBytes", Convert.ToInt64(stat.DataProcessedBytes));
                        command.Parameters.AddWithValue("@RowPerformancePerSecond", stat.RowRatePerformance);
                        command.Parameters.AddWithValue("@TransactionPerformancePerSecond", stat.TransactionalPerformance);
                        command.Parameters.AddWithValue("@DataRatePerformanceMbytesPerSecond", stat.DataRatePerformance);
                        command.Parameters.AddWithValue("@Operation", stat.Operation.ToString());
                        command.Parameters.AddWithValue("@CollectedOn", stat.CollectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (MySqlException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsThreads(int threadNumber, UInt64 dataProcessedBytes, UInt64 rowsAffected, UInt64 transactionsCompleted, UInt64 transactionsFailed, UInt64 totalSeconds,
            Dynamics.DataEngineOperation operation_, DateTime collectedOn)
        {
            try
            {
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newThreadStatString = "INSERT INTO DataEngineStatsThreads (ObjectID, Sequence, ThreadID, DataSizeBytes, RowsAffected, TransactionsCompleted, TransactionsFailed, TotalSeconds, " +
                        "Operation, CollectedOn) VALUES (@ObjectID, @Sequence, @ThreadID, @DataSizeBytes, @RowsAffected, @TransactionsCompleted, @TransactionsFailed, @TotalSeconds, " +
                        "@Operation, @CollectedOn)";
                    using (MySqlCommand command = new MySqlCommand(newThreadStatString, connection))
                    {
                        command.CommandTimeout = 1200;
                        command.Parameters.AddWithValue("@ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.AddWithValue("@Sequence", _analytic.Sequence);
                        command.Parameters.AddWithValue("@ThreadID", threadNumber);
                        command.Parameters.AddWithValue("@DataSizeBytes", Convert.ToInt64(dataProcessedBytes));
                        command.Parameters.AddWithValue("@RowsAffected", Convert.ToInt64(rowsAffected));
                        command.Parameters.AddWithValue("@TransactionsCompleted", Convert.ToInt64(transactionsCompleted));
                        command.Parameters.AddWithValue("@TransactionsFailed", Convert.ToInt64(transactionsFailed));
                        command.Parameters.AddWithValue("@TotalSeconds", Convert.ToInt64(totalSeconds));
                        command.Parameters.AddWithValue("@Operation", operation_.ToString());
                        command.Parameters.AddWithValue("@CollectedOn", collectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (MySqlException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsTotal(UInt64 dataProcessedBytes, UInt64 rowsAffected, UInt64 transactionsCompleted, UInt64 transactionsFailed, UInt64 totalSeconds, int numberOfThreads, int numberOfTables,
            Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            try
            {
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newTotalStatsString = "INSERT INTO DataEngineStatsTotal (ObjectID, Sequence, DataSizeBytes, RowsAffected, TransactionsCompleted, TransactionsFailed, TotalSeconds, NumberOfThreads," +
                        "NumberOfTables, Operation, CollectedOn) VALUES (@ObjectID, @Sequence, @DataSizeBytes, @RowsAffected, @TransactionsCompleted, @TransactionsFailed, @TotalSeconds, @NumberOfThreads," +
                        "@NumberOfTables, @Operation, @CollectedOn)";
                    using (MySqlCommand command = new MySqlCommand(newTotalStatsString, connection))
                    {
                        command.CommandTimeout = 1200;
                        command.Parameters.AddWithValue("@ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.AddWithValue("@Sequence", _analytic.Sequence);
                        command.Parameters.AddWithValue("@DataSizeBytes", Convert.ToInt64(dataProcessedBytes));
                        command.Parameters.AddWithValue("@RowsAffected", Convert.ToInt64(rowsAffected));
                        command.Parameters.AddWithValue("@TransactionsCompleted", Convert.ToInt64(transactionsCompleted));
                        command.Parameters.AddWithValue("@TransactionsFailed", Convert.ToInt64(transactionsFailed));
                        command.Parameters.AddWithValue("@TotalSeconds", Convert.ToInt64(totalSeconds));
                        command.Parameters.AddWithValue("@NumberOfThreads", numberOfThreads);
                        command.Parameters.AddWithValue("@NumberOfTables", numberOfTables);
                        command.Parameters.AddWithValue("@Operation", operation.ToString());
                        command.Parameters.AddWithValue("@CollectedOn", collectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (MySqlException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogDataEngineStatsFinalReport(string reportText, Dynamics.DataEngineOperation operation, DateTime collectedOn)
        {
            try
            {
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newFinalReportString = "INSERT INTO DataEngineFinalReports (ObjectID, Sequence, Report, Operation, CollectedOn) VALUES (@ObjectID, @Sequence, @Report, @Operation, @CollectedOn)";
                    using (MySqlCommand command = new MySqlCommand(newFinalReportString, connection))
                    {
                        command.CommandTimeout = 1200;
                        command.Parameters.AddWithValue("@ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.AddWithValue("@Sequence", _analytic.Sequence);
                        command.Parameters.AddWithValue("@Report", reportText);
                        command.Parameters.AddWithValue("@Operation", operation.ToString());
                        command.Parameters.AddWithValue("@CollectedOn", collectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (MySqlException ex)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public override void LogPlatformEngineWindowsResource(WindowsResource windowsData)
        {
            try
            {
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newResourceEntry = "INSERT INTO WindowsResourceData(ObjectID, Sequence, ProcessorTime, ProcessorPrivledgedTime, ProcessorInterruptTime, ProcessorDPCTime, " +
                        "CurrentClockSpeed, ExtClock, DataWidth, MaxClockSpeed, NumberOfLogicalProcessors, NumberOfProcessors, TotalPhysicalMemory, UsedPhysicalMemory, FreePhysicalMemory, " +
                        "PageFileUsed, PoolPagedBytesMemory, PoolNonPagedBytesMemory, CachedBytesMemory, PhysicalDiskAvgQueueLength, PhysicalDiskReadBytes, PhysicalDiskWriteBytes, " +
                        "PhysicalDiskAvgReadBytes, PhysicalDiskAvgWriteBytes, PhysicalDiskTime, ProcessHandleCount, ProcessThreadCount, ProcessContextSwitchCount, ProcessSystemCalls, ProcessorQueueLength, " +
                        "CollectedOn) " +
                        "VALUES (@ObjectID, @Sequence, @ProcessorTime, @ProcessorPrivledgedTime, @ProcessorInterruptTime, @ProcessorDPCTime, " +
                        "@CurrentClockSpeed, @ExtClock, @DataWidth, @MaxClockSpeed, @NumberOfLogicalProcessors, @NumberOfProcessors, @TotalPhysicalMemory, @UsedPhysicalMemory, @FreePhysicalMemory, " +
                        "@PageFileUsed, @PoolPagedBytesMemory, @PoolNonPagedBytesMemory, @CachedBytesMemory, @PhysicalDiskAvgQueueLength, @PhysicalDiskReadBytes, @PhysicalDiskWriteBytes, " +
                        "@PhysicalDiskAvgReadBytes, @PhysicalDiskAvgWriteBytes, @PhysicalDiskTime, @ProcessHandleCount, @ProcessThreadCount, @ProcessContextSwitchCount, @ProcessSystemCalls, @ProcessorQueueLength, " +
                        "@CollectedOn) ";
                    using (MySqlCommand command = new MySqlCommand(newResourceEntry, connection))
                    {
                        command.CommandTimeout = 600;
                        command.Parameters.AddWithValue("@ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.AddWithValue("@Sequence", _analytic.Sequence);
                        command.Parameters.AddWithValue("@ProcessorTime", windowsData.ProcessorTime);
                        command.Parameters.AddWithValue("@ProcessorPrivledgedTime", windowsData.ProcessorPrivledgedTime);
                        command.Parameters.AddWithValue("@ProcessorInterruptTime", windowsData.ProcessorInterruptTime);
                        command.Parameters.AddWithValue("@ProcessorDPCTime", windowsData.ProcessorDPCTime);
                        command.Parameters.AddWithValue("@CurrentClockSpeed", windowsData.CurrentClockSpeed);
                        command.Parameters.AddWithValue("@ExtClock", windowsData.ExtClock);
                        command.Parameters.AddWithValue("@DataWidth", windowsData.DataWidth);
                        command.Parameters.AddWithValue("@MaxClockSpeed", windowsData.MaxClockSpeed);
                        command.Parameters.AddWithValue("@NumberOfLogicalProcessors", windowsData.NumberOfLogicalProcessors);
                        command.Parameters.AddWithValue("@NumberOfProcessors", windowsData.NumberOfProcessors);
                        command.Parameters.AddWithValue("@TotalPhysicalMemory", windowsData.TotalPhysicalMemory);
                        command.Parameters.AddWithValue("@UsedPhysicalMemory", windowsData.UsedPhysicalMemory);
                        command.Parameters.AddWithValue("@FreePhysicalMemory", windowsData.FreePhysicalMemory);
                        command.Parameters.AddWithValue("@PageFileUsed", windowsData.PageFileUsed);
                        command.Parameters.AddWithValue("@PoolPagedBytesMemory", windowsData.PoolPagedBytesMemory);
                        command.Parameters.AddWithValue("@PoolNonPagedBytesMemory", windowsData.PoolNonPagedBytesMemory);
                        command.Parameters.AddWithValue("@CachedBytesMemory", windowsData.CachedBytesMemory);
                        command.Parameters.AddWithValue("@PhysicalDiskAvgQueueLength", windowsData.PhysicalDiskAvgQueueLength);
                        command.Parameters.AddWithValue("@PhysicalDiskReadBytes", windowsData.PhysicalDiskReadBytes);
                        command.Parameters.AddWithValue("@PhysicalDiskWriteBytes", windowsData.PhysicalDiskWriteBytes);
                        command.Parameters.AddWithValue("@PhysicalDiskAvgReadBytes", windowsData.PhysicalDiskAvgReadBytes);
                        command.Parameters.AddWithValue("@PhysicalDiskAvgWriteBytes", windowsData.PhysicalDiskAvgWriteBytes);
                        command.Parameters.AddWithValue("@PhysicalDiskTime", windowsData.PhysicalDiskTime);
                        command.Parameters.AddWithValue("@ProcessHandleCount", windowsData.ProcessHandleCount);
                        command.Parameters.AddWithValue("@ProcessThreadCount", windowsData.ProcessThreadCount);
                        command.Parameters.AddWithValue("@ProcessContextSwitchCount", windowsData.ProcessContextSwitchCount);
                        command.Parameters.AddWithValue("@ProcessSystemCalls", windowsData.ProcessSystemCalls);
                        command.Parameters.AddWithValue("@ProcessorQueueLength", windowsData.ProcessorQueueLength);
                        command.Parameters.AddWithValue("@CollectedOn", windowsData.CollectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (MySqlException error)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, error.Message, Message.MessageType.Error));
            }
        }

        public override void LogPlatformEngineLinuxResource(LinuxResource linuxData)
        {
            try
            {
                using (MySqlConnection connection = _analytic.Persistence.PersistenceInstance.GetConnection())
                {
                    string newResourceEntry = "INSERT INTO LinuxResourceData(ObjectID, Sequence, CPULoad, TotalPhysicalMemory, UsedPhysicalMemory, FreePhysicalMemory, " +
                        "CurrentClockSpeed, SDTransfersPerSecond, SDkBRead, SDkBWrite, SDkBReadps, SDkBWriteps, NVMeTransfersPerSecond, NVMekBRead, NVMekBWrite, NVMekBReadps, NVMekBWriteps," +
                        "DMTransfersPerSecond, DMkBRead, DMkBWrite, DMkBReadps, DMkBWriteps, CollectedOn) " +
                        "VALUES (@ObjectID, @Sequence, @CPULoad, @TotalPhysicalMemory, @UsedPhysicalMemory, @FreePhysicalMemory, " +
                        "@CurrentClockSpeed, @SDTransfersPerSecond, @SDkBRead, @SDkBWrite, @SDkBReadps, @SDkBWriteps, @NVMeTransfersPerSecond, @NVMekBRead, @NVMekBWrite, @NVMekBReadps, @NVMekBWriteps," +
                        "@DMTransfersPerSecond, @DMkBRead, @DMkBWrite, @DMkBReadps, @DMkBWriteps, @CollectedOn) ";
                    using (MySqlCommand command = new MySqlCommand(newResourceEntry, connection))
                    {
                        command.CommandTimeout = 600;
                        command.Parameters.AddWithValue("@ObjectID", _analytic.TestObject.ObjectID);
                        command.Parameters.AddWithValue("@Sequence", _analytic.Sequence);
                        command.Parameters.AddWithValue("@CPULoad", linuxData.CPULoad);
                        command.Parameters.AddWithValue("@TotalPhysicalMemory", linuxData.TotalPhysicalMemory);
                        command.Parameters.AddWithValue("@UsedPhysicalMemory", linuxData.UsedPhysicalMemory);
                        command.Parameters.AddWithValue("@FreePhysicalMemory", linuxData.FreePhysicalMemory);
                        command.Parameters.AddWithValue("@CurrentClockSpeed", linuxData.CurrentClockSpeed);
                        command.Parameters.AddWithValue("@SDTransfersPerSecond", linuxData.SDTransfersPerSecond);
                        command.Parameters.AddWithValue("@SDkBRead", linuxData.SDKBRead);
                        command.Parameters.AddWithValue("@SDkBWrite", linuxData.SDKBWrite);
                        command.Parameters.AddWithValue("@SDkBReadps", linuxData.SDKBReadps);
                        command.Parameters.AddWithValue("@SDkBWriteps", linuxData.SDKBWriteps);
                        command.Parameters.AddWithValue("@NVMeTransfersPerSecond", linuxData.NVMETransfersPerSecond);
                        command.Parameters.AddWithValue("@NVMekBRead", linuxData.NVMEKBRead);
                        command.Parameters.AddWithValue("@NVMekBWrite", linuxData.NVMEKBWrite);
                        command.Parameters.AddWithValue("@NVMekBReadps", linuxData.NVMEKBReadps);
                        command.Parameters.AddWithValue("@NVMekBWriteps", linuxData.NVMEKBWriteps);
                        command.Parameters.AddWithValue("@DMTransfersPerSecond", linuxData.DMTransfersPerSecond);
                        command.Parameters.AddWithValue("@DMkBRead", linuxData.DMKBRead);
                        command.Parameters.AddWithValue("@DMkBWrite", linuxData.DMKBWrite);
                        command.Parameters.AddWithValue("@DMkBReadps", linuxData.DMKBReadps);
                        command.Parameters.AddWithValue("@DMkBWriteps", linuxData.DMKBWriteps);
                        command.Parameters.AddWithValue("@CollectedOn", linuxData.CollectedOn);
                        connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (MySqlException error)
            {
                _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, error.Message, Message.MessageType.Error));
            }
        }

        public override bool CreateAnalyticsObjects()
        {
            bool sucess = true;
            MySqlConnection conn = _analytic.Persistence.PersistenceInstance.GetConnection();
            try
            {
                conn.Open();
                foreach (string table in _analytic.Persistence.AnalyticsTables)
                {
                    MySqlCommand command = new MySqlCommand("SELECT * FROM " + table + " LIMIT 1", conn);
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
                        using (MySqlCommand command = new MySqlCommand())
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
                                        catch (MySqlException sqlex)
                                        {
                                            if (!sqlex.Message.Contains("already exists"))
                                            {
                                                if (!sqlex.Message.Contains("Duplicate"))
                                                {
                                                    sucess = false;
                                                    _analytic.DebugQueue.AddMessage(new Message(DateTime.Now, sqlex.Message, Message.MessageType.Error));
                                                }
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
