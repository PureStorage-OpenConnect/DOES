using System;
using DOES.Shared.Resources;

namespace DOES.DataEngine.Resources
{
    /// <summary>
    /// This class tracks all of the results for the data engine operations. 
    /// </summary>
    public class DataEngineResults
    {
        private UInt64 _processedWebPages;
        private UInt64 _processedBytes;
        private UInt64 _processedTransactions;
        private UInt64 _processedRows;
        private UInt64 _failedProcessedRows;
        private DateTime _operationStart;
        private DateTime _operationEnd;
        private Dynamics.DataEngineOperation _resultType;

        /// <summary>
        /// Instantiates the DataEngineResults with starting values for the results set. 
        /// </summary>
        public DataEngineResults()
        {
            _processedWebPages = 0;
            _processedBytes = 0;
            _processedTransactions = 0;
            _processedRows = 0;
            _failedProcessedRows = 0;
        }

        /// <summary>
        /// Instantiates the DataEngineResults with starting values for the results set and a result type. 
        /// </summary>
        public DataEngineResults(Dynamics.DataEngineOperation resultType)
        {
            _processedBytes = 0;
            _processedRows = 0;
            _failedProcessedRows = 0;
            _processedTransactions = 0;
            _resultType = resultType;
        }

        /// <summary>
        /// Instantiates the DataEngineResults.
        /// </summary>
        public DataEngineResults(UInt64 processedBytes, UInt64 processedRows, UInt64 failedProcessedRows, 
            UInt64 processedTransactions)
        {
            _processedBytes = processedBytes;
            _processedRows = processedRows;
            _failedProcessedRows = failedProcessedRows;
            _processedTransactions = processedTransactions;
        }

        /// <summary>
        /// Instantiates the DataEngineResults with a result type. 
        /// </summary>
        public DataEngineResults(UInt64 processedBytes, UInt64 processedRows, UInt64 failedProcessedRows, 
            UInt64 processedTransactions, Dynamics.DataEngineOperation resultType)
        {
            _processedBytes = processedBytes;
            _processedRows = processedRows;
            _failedProcessedRows = failedProcessedRows;
            _processedTransactions = processedTransactions;
            _resultType = resultType;
        }

        /// <summary>
        /// Instantiates the DataEngineResults with a result type and stop and end. 
        /// </summary>
        public DataEngineResults(UInt64 processedBytes, UInt64 processedRows, UInt64 failedProcessedRows, 
            UInt64 processedTransactions, DateTime start, DateTime end, Dynamics.DataEngineOperation resultType)
        {
            _processedBytes = processedBytes;
            _processedRows = processedRows;
            _failedProcessedRows = failedProcessedRows;
            _processedTransactions = processedTransactions;
            _operationStart = start;
            _operationEnd = end;
            _resultType = resultType;
        }

        /// <summary>
        /// Returns the number of processed web pages. 
        /// </summary>
        public UInt64 ProcessedWebPages { get { return _processedWebPages; } set { _processedWebPages = value; } }

        /// <summary>
        /// Returns the amount of bytes processed. 
        /// 
        /// </summary>
        public UInt64 ProcessedBytes { get { return _processedBytes; } set { _processedBytes = value; } }

        /// <summary>
        /// Returns the number of transactions processed. 
        /// </summary>
        public UInt64 ProcessedTransactions { get { return _processedTransactions; } set { _processedTransactions = value; } }

        /// <summary>
        /// Returns the number of rows processed. 
        /// </summary>
        public UInt64 ProcessedRows { get { return _processedRows; } set { _processedRows = value; } }

        /// <summary>
        /// Returns the number of rows which failed to process. 
        /// </summary>
        public UInt64 FailedProcessedRows { get { return _failedProcessedRows; } set { _failedProcessedRows = value; } }

        /// <summary>
        /// Returns the start time and date for when the results are set. 
        /// </summary>
        public DateTime OperationStart { get { return _operationStart; } set { _operationStart = value; } }

        /// <summary>
        /// Returns the end time and date for the when the results are set. 
        /// </summary>
        public DateTime OperationEnd { get { return _operationEnd; } set { _operationEnd = value; } }

        /// <summary>
        /// Returns the type of result for the result set. 
        /// </summary>
        public Dynamics.DataEngineOperation ResultType { get { return _resultType; } set { _resultType = value; } }
    }
    /// <summary>
    /// This class contains a set of the results. 
    /// </summary>
    public class DataEngineResultSet
    {
        /// <summary>
        /// Instantiates a new DataEngineResult set with the starting values and an unkown operation type. 
        /// </summary>
        public DataEngineResultSet(int NumberOfThreads)
        {
            ResultSet = new DataEngineResults[NumberOfThreads];
            for (int i = 0; i < NumberOfThreads; i++)
            {
                ResultSet[i] = new DataEngineResults(0, 0, 0, 0, Dynamics.DataEngineOperation.Unknown);
            }
        }
        /// <summary>
        /// Instantiates a new DataEngineResult set in a 2 dimensional setup with the starting values and an unkown operation type. 
        /// </summary>
        public DataEngineResultSet(int NumberOfThreads, int NumberOfOperations)
        {
            ResultCompilation = new DataEngineResults[NumberOfOperations][];
            for (int i = 0; i < NumberOfOperations; i++)
            {
                ResultCompilation[i] = new DataEngineResults[NumberOfThreads];
                for (int i2 = 0; i2 < NumberOfThreads; i2++)
                {
                    ResultCompilation[i][i2] = new DataEngineResults(0, 0, 0, 0, Dynamics.DataEngineOperation.Unknown);
                }
            }
        }

        /// <summary>
        /// Instantiates a new DataEngineResult set with a 2 dimensional reporting model. 
        /// </summary>
        public DataEngineResultSet(DataEngineResults[][] results)
        {
            ResultCompilation = results;
        }

        /// <summary>
        /// Instantiates a new DataEngineResult set with start and end times. 
        /// </summary>
        public DataEngineResultSet(DataEngineResults[] results, DateTime start, DateTime end)
        {
            ResultSet = results;
            OperationStart = start;
            OperationEnd = end;
        }

        /// <summary>
        /// Instantiates a new DataEngineResult set with the starting values and an unkown operation type. 
        /// </summary>
        public DataEngineResultSet(DataEngineResults[][] results, DateTime start, DateTime end, Dynamics.DataEngineOperation operation)
        {
            ResultCompilation = results;
            OperationStart = start;
            OperationEnd = end;
            Operation = operation;
        }

        /// <summary>
        /// Returns the aggregated results for a result set for an operation. 
        /// </summary>
        public DataEngineResults ReturnAggregateResultSet()
        {
            UInt64 totalProcessedBytes = 0;
            UInt64 totalProcessedRows = 0;
            UInt64 totalProcessedTransactions = 0;
            UInt64 totalFailedProcessedRows = 0;
            Dynamics.DataEngineOperation operation_Type = Dynamics.DataEngineOperation.Unknown;

            foreach (DataEngineResults dr in ResultSet)
            {
                if (dr != null)
                {
                    operation_Type = dr.ResultType;
                    totalProcessedBytes += dr.ProcessedBytes;
                    totalProcessedRows += dr.ProcessedRows;
                    totalProcessedTransactions += dr.ProcessedTransactions;
                    totalFailedProcessedRows += dr.FailedProcessedRows;
                }
                else
                {
                    totalProcessedBytes += 0;
                    totalProcessedRows += 0;
                    totalProcessedTransactions += 0;
                    totalFailedProcessedRows += 0;
                }
            }

            DataEngineResults rolledUpResults = new DataEngineResults(totalProcessedBytes, totalProcessedRows, 
                totalFailedProcessedRows, totalProcessedTransactions, operation_Type);
            return rolledUpResults;
        }

        /// <summary>
        ///  Returns the aggregated results for a test operation. 
        /// </summary>
        public DataEngineResults ReturnAggregateTestResultSet(Dynamics.DataEngineOperation operation)
        {
            UInt64 totalProcessedBytes = 0;
            UInt64 totalProcessedRows = 0;
            UInt64 totalProcessedTransactions = 0;
            UInt64 totalFailedProcessedRows = 0; ;

            for (int operationArray = 0; operationArray < ResultCompilation.Length; operationArray++)
            {
                for (int valueOperations = 0; valueOperations < ResultCompilation[operationArray].Length; valueOperations++)
                {
                    if (ResultCompilation[operationArray][valueOperations] != null)
                    {
                        totalProcessedBytes += ResultCompilation[operationArray][valueOperations].ProcessedBytes;
                        totalProcessedRows += ResultCompilation[operationArray][valueOperations].ProcessedRows;
                        totalProcessedTransactions += ResultCompilation[operationArray][valueOperations].ProcessedTransactions;
                        totalFailedProcessedRows += ResultCompilation[operationArray][valueOperations].FailedProcessedRows;
                    }
                    else
                    {
                        totalProcessedBytes += 0;
                        totalProcessedRows += 0;
                        totalProcessedTransactions += 0;
                        totalFailedProcessedRows += 0;
                    }
                }
            }

            DataEngineResults rolledUpResults = new DataEngineResults(totalProcessedBytes, totalProcessedRows, 
                totalFailedProcessedRows, totalProcessedTransactions, operation);
            return rolledUpResults;
        }


        /// <summary>
        /// Returns the results as a compilation. 
        /// </summary>
        public DataEngineResults[][] ResultCompilation { get; set; }

        /// <summary>
        /// Returns the result set. 
        /// </summary>
        public DataEngineResults[] ResultSet { get; set; }

        /// <summary>
        /// Returns when the operation started. 
        /// </summary>
        public DateTime OperationStart { get; set; }

        /// <summary>
        /// Returns when the operation ended. 
        /// </summary>
        public DateTime OperationEnd { get; set; }

        /// <summary>
        /// Returns the operation type. 
        /// </summary>
        public Dynamics.DataEngineOperation Operation { get; set; }
    }
}
