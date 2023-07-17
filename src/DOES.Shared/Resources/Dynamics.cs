using System.ComponentModel;

namespace DOES.Shared.Resources
{
    public abstract class Dynamics
    {
        public enum SchemaResource
        {
            MSSQL_BaseTables,
            MSSQL_TablesWithIndexes,
            MSSQL_TablesWithoutIndexes,
            MSSQL_TablesWithIndexesLOB,
            MSSQL_TablesWithoutIndexesLOB,
            MSSQL_InMemoryTablesWithIndexes,
            MSSQL_InMemoryWithoutIndexes,
            MSSQL_InMemoryTablesWithIndexesLOB,
            MSSQL_InMemoryWithoutIndexesLOB,
            OracleDB_BaseTables,
            OracleDB_TablesWithIndexes,
            OracleDB_TablesWithoutIndexes,
            OracleDB_TablesWithIndexesLOB,
            OracleDB_TablesWithoutIndexesLOB,
            SAPHANA_BaseTables,
            SAPHANA_TablesWithIndexes,
            SAPHANA_TablesWithoutIndexes,
            SAPHANA_TablesWithIndexesLOB,
            SAPHANA_TablesWithoutIndexesLOB,
            MySQL_BaseTables,
            MySQL_TablesWithIndexes,
            MySQL_TablesWithoutIndexes,
            MySQL_TablesWithIndexesLOB,
            MySQL_TablesWithoutIndexesLOB,
            MariaDB_BaseTables,
            MariaDB_TablesWithIndexes,
            MariaDB_TablesWithoutIndexes,
            MariaDB_TablesWithIndexesLOB,
            MariaDB_TablesWithoutIndexesLOB,
            PostgreSQL_BaseTables,
            PostgreSQL_TablesWithIndexes,
            PostgreSQL_TablesWithoutIndexes,
            PostgreSQL_TablesWithIndexesLOB,
            PostgreSQL_TablesWithoutIndexesLOB,
            MongoDB_BaseCollections,
            MongoDB_CollectionsWithIndexes,
            MongoDB_CollectionsWithoutIndexes,
            MongoDB_CollectionsWithIndexesLOB,
            MongoDB_CollectionsWithoutIndexesLOB
        }

        public enum TestType
        {
            Simple,
            Advanced,
            Complex
        };

        public enum ClearingType
        {
            Drop,
            Truncate
        };

        public enum MySQLStorageEngine
        {
            InnoDB, 
            NDB
        }

        public enum MariaDBStorageEngine
        {
            InnoDB,
            ColumnStore,
            ROCKSDB, 
            S3
        }

        public enum Platform
        {
            [Description("Microsoft Windows Operating System")]
            Windows,
            [Description("Linux Operating System")]
            Linux
        };
        public enum Database
        {
            MicrosoftSQL,
            Oracle,
            SAPHANA,
            MySQL, 
            MariaDB, 
            PostgreSQL,
            MongoDB
        };

        public enum DatabaseSchema
        {
            WithIndexes,
            WithoutIndexes,
            WithIndexesLOB,
            WithoutIndexesLOB,
            MemoryOptimised,
            MemoryOptimisedWithoutIndexes,
            MemoryOptimisedLOB,
            MemoryOptimisedWithoutIndexesLOB
        };

        public enum StorageUnit
        {
            Bytes,
            Kilobytes,
            Megabytes,
            Gigabytes,
            Terabytes
        };

        public enum DataEngineOperation
        {
            [Description("Insert Data Into Database")]
            InsertData,
            [Description("Delete Data From Database")]
            DeleteData,
            [Description("Update Data In Database")]
            UpdateData,
            [Description("Export WebPages From Database")]
            ExportToFile,
            [Description("Continiously Write Data To Database")]
            ContinuousWrite,
            [Description("Drop or Truncate Database Tables")]
            CleanDB,
            [Description("Query Data In Database")]
            QueryData,
            [Description("SAP HANA Only - Delta Merge Tables")]
            DeltaMerge,
            [Description("Run a Simple OLTP Test")]
            TestSimple,
            [Description("Run a Complex Test")]
            TestComplex,
            [Description("Run an Advanced Test")]
            TestAdvanced,
            Unknown
        };

        public enum ReportOperation
        {
            [Description("Data Inserted Into Rows")]
            InsertData,
            [Description("Data Deleted From Rows")]
            DeleteData,
            [Description("Data Updated In Rows")]
            UpdateData,
            [Description("Data Queried With Select")]
            QueryData,
            [Description("Simple Test Type")]
            TestSimple,
            [Description("Complex Test Type")]
            TestComplex,
            [Description("Advanced Test Type")]
            TestAdvanced
        };

        public enum ReadQuery
        {
            LeftOuterJoin,
            UnionAll
        }

        public enum ResourceRetrievalType
        {
            PointInTime, 
            Duration, 
            UntilNotified,
            EndNotify
        }

        public enum MongoDBDeployment
        {
            StandAlone,
            ReplicaSet
        }
    }
}
