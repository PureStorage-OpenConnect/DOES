using System;
using System.Collections.Generic;
using System.Threading;
using DOES.Shared.Resources;

namespace DOES.DataEngine.Resources
{
    /// <summary>
    /// This class must be inherited by all controller object implementations.  
    /// </summary>
    public abstract class Engine
    {
        /// <summary>
        /// Creates custom objects for a specific database type based on the type of schema selected. 
        /// </summary>
        public abstract bool CreateTablesAndIndexes();
        /// <summary>
        /// Imports data from web sites up to a specified value. 
        /// </summary>
        public abstract void ImportFromWeb(UInt64 RequestedBytes, CancellationTokenSource tokenSource, int? resultIndex);
        /// <summary>
        /// Imports data from data files up to a specified value. 
        /// </summary>
        public abstract void ImportFromFile(string Folder, UInt64 RequestedBytes, CancellationTokenSource tokenSource, int? resultIndex);
        /// <summary>
        /// Deletes data up to a specified byte value. 
        /// </summary>
        public abstract void Delete(UInt64 RequestedBytes, CancellationTokenSource tokenSource, 
            int? resultIndex, Queue<WebPage>[] webPagesToProcess);
        /// <summary>
        /// Updates data up to a specified byte value. 
        /// </summary>
        public abstract void Update(UInt64 RequestedBytes, CancellationTokenSource tokenSource, 
            bool replaceWebPages, string folder, int? resultIndex, Queue<WebPage>[] webPagesToProcess);
        /// <summary>
        /// Exports data from a database to a data file. 
        /// </summary>
        public abstract void Export(string Folder, CancellationTokenSource tokenSource);
        /// <summary>
        /// Write a single line of data to a database object. 
        /// </summary>
        public abstract void WriteInLine(CancellationTokenSource tokenSource);
        /// <summary>
        /// Destroys objects for a specific database type through drop or truncation capabilities. 
        /// </summary>
        public abstract bool ClearTablesAndIndexes(Dynamics.ClearingType cleaningType);
        /// <summary>
        /// Query web data base on the ANSI SQL Left Outer Join Syntax. 
        /// </summary>
        public abstract void QueryDataLeftOuterJoin(UInt64 RequestedBytes, CancellationTokenSource tokenSource, 
            int? resultIndex, Queue<WebPage>[] webPagesToProcess);
        /// <summary>
        /// Query web data base on the ANSI SQL Union All Syntax. 
        /// </summary>
        public abstract void QueryDataUnionAll(UInt64 RequestedBytes, CancellationTokenSource tokenSource, 
            int? resultIndex, Queue<WebPage>[] webPagesToProcess);
        /// <summary>
        /// Tests the database using the simple plan. 
        /// </summary>
        /// 
        public abstract void TestSimple(string Folder, UInt64 RequestedBytes, 
            CancellationTokenSource tokenSource, double growthRate);
        /// <summary>
        /// Tests the database using the advanced plan. 
        /// </summary>
        public abstract void TestAdvanced(string Folder, UInt64 RequestedBytes, 
            CancellationTokenSource tokenSource, double growthRate);
        /// <summary>
        /// Tests the database using the complex plan. 
        /// </summary>
        public abstract void TestComplex(string Folder, UInt64 RequestedBytes, 
            CancellationTokenSource tokenSource, double growthRate);
        /// <summary>
        /// Returns the state of the result set. 
        /// </summary>
        public abstract bool GetResultsState();
        /// <summary>
        /// Set the results state to complete. 
        /// </summary>
        public abstract void SetResultsStateCompleted();
        /// <summary>
        /// Set the final test results object to complete. 
        /// </summary>
        public abstract void SetFinalTestResults(DateTime operationStart, DateTime operationEnd, 
            Dynamics.DataEngineOperation testOperation);
        /// <summary>
        /// Return interim results. 
        /// </summary>
        public abstract DataEngineResults ReturnInterimResults();
        /// <summary>
        /// Return interim results per thread. 
        /// </summary>
        public abstract DataEngineResultSet ReturnInterimThreadResults();
        /// <summary>
        /// Return the final results set. 
        /// </summary>
        public abstract DataEngineResultSet ReturnFinalResults();
        /// <summary>
        /// Return interium results for a test. 
        /// </summary>
        public abstract DataEngineResults ReturnInterimTestResults(Dynamics.DataEngineOperation operation);
        /// <summary>
        /// Return interim results for a test thread object. 
        /// </summary>
        public abstract DataEngineResultSet ReturnInterimTestThreadResults();
    }
}
