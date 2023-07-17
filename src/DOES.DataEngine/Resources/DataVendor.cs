using System;
using System.Collections.Generic;
using DOES.Shared.Debug;
using DOES.DataEngine.FileOperations;
using DOES.Shared.Resources;
using System.Threading;

namespace DOES.DataEngine.Resources
{
    /// <summary>
    /// This class must be inherited by all vendor data object implementations. 
    /// </summary>
    public abstract class DataVendor
    {
        /// <summary>
        /// Creates vendor objects. 
        /// </summary>
        public abstract bool CreateObjects(List<string> baseSchemaObjects,
            List<string> extensionSchemaObjects);
        /// <summary>
        /// Destroys vendor objects. 
        /// </summary>
        public abstract bool DestroyObjects(Dynamics.ClearingType clearingType);
        /// <summary>
        /// Inserts an encoded web page to the vendor object. 
        /// </summary>
        public abstract int InsertEncodedWebPage(WebPage page);
        /// <summary>
        /// Insert a characterised web page into the vendor object. 
        /// </summary>
        public abstract int InsertCharacterisedWebPage(WebPage page);
        /// <summary>
        /// Insert a single , 2 column wide , entry into the vendor object. 
        /// </summary>
        public abstract int InsertPointInTimeWrite();
        /// <summary>
        /// Delete a webpage from vendor objects. 
        /// </summary>
        public abstract int DeleteWebPage(int pageID);
        /// <summary>
        /// Update a characterised web page in the vendor objects. 
        /// </summary>
        public abstract int UpdateCharacterisedWebPage(int pageID);
        /// <summary>
        /// Update an encoded web page in the vendor objects. 
        /// </summary>
        public abstract int UpdateEncodedWebPage(int pageID);
        /// <summary>
        /// Update an encoded web page in the vendor objects by replacing it with a new one.  
        /// </summary>
        public abstract int UpdateEncodedWebPageInPlace(int pageID, WebPage newPage);
        /// <summary>
        /// Update a characterised web page in the vendor objects by replacing it with a new one.  
        /// </summary>
        public abstract int UpdateCharacterisedWebPageInPlace(int pageID, WebPage newPage);
        /// <summary>
        /// Query vendor objects using ANSI SQL Left Outer Join operations. 
        /// </summary>
        public abstract Tuple<UInt64, UInt64> SelectWebPageLeftOuterJoin(int pageID, UInt64 bytesToProcess);
        /// <summary>
        /// Query vendor objects using ANSI SQL Union All operations. 
        /// </summary>
        public abstract Tuple<UInt64, UInt64> SelectWebPageUnionAll(int pageID, UInt64 bytesToProcess);
        /// <summary>
        /// get the vendor object connection. 
        /// </summary>
        public abstract dynamic GetConnection();
        /// <summary>
        /// return a sorted list of all Page ID's and their length. 
        /// </summary>
        public abstract SortedList<int, long> GetPageIDList();
        /// <summary>
        /// Retun the database type. 
        /// </summary>
        public abstract Dynamics.Database GetDatabaseType();
        /// <summary>
        /// Check the file import history for the relevant operation. 
        /// </summary>
        public abstract ImportHandler CheckFileImportHistory(string filename);
        /// <summary>
        /// Update the file import history for the relevant operation. 
        /// </summary>
        public abstract bool UpdateImportHistory(ImportHandler import);
        /// <summary>
        /// Create the file import history for the relevant operation. 
        /// </summary>
        public abstract bool CreateImportHistory(ImportHandler import);
        /// <summary>
        /// Read base web page data from the vendor object. 
        /// </summary>
        public abstract void ReadBaseWebPageData(OilPump pump);
        /// <summary>
        /// Return the common MessageQueue for the vendor object set. 
        /// </summary>
        public abstract MessageQueue DebugQueue();
        /// <summary>
        /// Return the type of schema implemented for the vendor objects.  
        /// </summary>
        public abstract Dynamics.DatabaseSchema GetSchemaType();
        /// <summary>
        /// Return the helper mechanic used with the vendor class. 
        /// </summary>
        public abstract Mechanic GetMechanic();
        /// <summary>
        /// Return the page ID's which need to be worked on for the operation. 
        /// </summary>
        public abstract Tuple<Random, UInt64> InitializeRandom(bool ForceRandom, UInt64 RequestedBytes);
        /// <summary>
        /// Check the database schema type. 
        /// </summary>
        public abstract void CheckSchemaType();
        /// <summary>
        /// Perform advanced operations for the vendor. 
        /// </summary>
        public abstract void VendorAdvancedOperations(int numberOfThreads);
        /// <summary>
        /// Perform complex operations for the vendor. 
        /// </summary>
        public abstract void VendorComplexOperations();
        /// <summary>
        /// Perform a consistency check for the vendor. 
        /// </summary>
        public abstract void VendorConsistencyCheck();
        /// <summary>
        /// Return the table amplification value used for the vendor. 
        /// </summary>
        public abstract int TableAmplifier { get; set; }
        /// <summary>
        /// Set and return the managed token to cancel operations if specific things go wrong with the database. 
        /// </summary>
        public abstract CancellationTokenSource TokenSource { get; set; }

    }
}
