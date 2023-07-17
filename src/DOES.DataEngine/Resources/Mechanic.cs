using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Security.Cryptography;
using DOES.Shared.Resources;

namespace DOES.DataEngine.Resources
{
    /// <summary>
    /// This is a helper class containing common settings and a centralized data management layer. 
    /// </summary>
    public class Mechanic
    {
        private readonly int notFound;
        private readonly byte currentVersionID;
        private readonly string dataFileRootname;
        private readonly int fileAccessTimeout;
        private double _randomPercentage;

        private readonly string[] DBTables =
        {
            "Imports",
            "WebPageHeaders",
            "WebPageStats",
            "WebPagesUnicode_X00",
            "WebPagesASCII_X00",
            "WebPagesEBCDIC_X00",
            "WebPagesUTF32_X00",
            "WebPages",
            "PointInTimeWrite",
            "Configuration"
        };

        private string[] WebPageDataTables =
        {
            "WebPageHeaders",
            "WebPageStats",
            "WebPagesUnicode_X00",
            "WebPagesASCII_X00",
            "WebPagesEBCDIC_X00",
            "WebPagesUTF32_X00",
            "WebPages"
        };

        private readonly string[] WebPageBaseTables =
        {
            "WebPageHeaders",
            "WebPageStats",
            "WebPages",
            "PointInTimeWrite",
            "Imports",
            "Configuration"
        };

        private readonly string[] WebPageCoreTables =
        {
            "WebPageHeaders",
            "WebPageStats",
            "WebPages"
        };

        private readonly string[] WebPageEncodingTables =
        {
            "WebPagesUnicode_X00",
            "WebPagesASCII_X00",
            "WebPagesEBCDIC_X00",
            "WebPagesUTF32_X00"
        };

        private readonly string[] AllSequences = 
        { 
            "Seq_PageID", 
            "Seq_HeaderID", 
            "Seq_StatID", 
            "Seq_WriteID"
        };

        /// <summary>
        /// Return all sequence names for the database 
        /// </summary>
        public string[] ReturnAllSequences { get { return AllSequences; } }

        /// <summary>
        /// Retun an array of the web page encoding tables. 
        /// </summary>
        public string[] ReturnWebPageEncodingtables { get {return WebPageEncodingTables; } }

        /// <summary>
        /// Retun an array of the core web page tables. 
        /// </summary>
        public string[] ReturnWebPageCoreTables { get { return WebPageCoreTables; } }

        /// <summary>
        /// Instantiates a new mechanic with all of the default values set. 
        /// </summary>
        public Mechanic()
        {
            notFound = -1;
            currentVersionID = 3;
            dataFileRootname = "Engine.Oil-";
            fileAccessTimeout = 120;
        }

        /// <summary>
        /// Instantiates a new mechanic with all of the default values set , including the random data requirement. 
        /// </summary>
        public Mechanic(double randomPercentage)
        {
            notFound = -1;
            currentVersionID = 3;
            dataFileRootname = "Engine.Oil-";
            fileAccessTimeout = 30;
            _randomPercentage = randomPercentage;
        }

        /// <summary>
        /// Returns the file access timeout. 
        /// </summary>
        public int FileAccessTimeout { get { return fileAccessTimeout; } }
        /// <summary>
        /// Returns the current file version. 
        /// </summary>
        public int CurrentFileVersion { get { return currentVersionID; } }
        /// <summary>
        /// Returns the data root file name template.
        /// </summary>
        public string DataFileRootName { get {return dataFileRootname; } }

        /// <summary>
        /// Return the base table names. 
        /// </summary>
        public List<string> AllBaseTables()
        {
            List<string> tempList = new List<string>();
            foreach (string item in WebPageBaseTables)
            {
                tempList.Add(item);
            }
            return tempList;
        }

        /// <summary>
        /// Return all database tables. 
        /// </summary>
        public List<string> AllTables(int amplifier)
        {
            return ListOfTables(DBTables, amplifier);
        }

        /// <summary>
        /// Return a list of all web page data tables. 
        /// </summary>
        public List<string> AllWebPageDataTables(int amplifier)
        {
            return ListOfTables(WebPageDataTables, amplifier);
        }

        /// <summary>
        /// Return a list of the column tables for SAP HANA. 
        /// </summary>
        public List<string> AllWebPageDataColumnTables(int amplifier, int numColumnTables)
        {
            return ListOfTables(WebPageDataTables, amplifier, numColumnTables);
        }

        /// <summary>
        /// Return the encoding table names.
        /// </summary>
        public List<string> AllWebPageEncodingTables(int amplifier)
        {
            return ListOfTables(WebPageEncodingTables, amplifier);
        }


        /// <summary>
        /// Return the encoding table names in reverse order. 
        /// </summary>
        public List<string> AllWebPageEncodingTablesReversed(int amplifier, int originalAmplifier)
        {
            return ListOfTablesBackwards(WebPageEncodingTables, amplifier, originalAmplifier);
        }

        /// <summary>
        /// Returns a list of all tables. 
        /// </summary>
        private List<string> ListOfTables(string[] arrayTable, int amplifier, int numColumnTables = int.MaxValue)
        {
            List<string> AllTables = new List<string>();
            foreach (var Table in arrayTable)
            {
                if (Table.Contains("_X00"))
                {
                    for (int TableID = 0; TableID < Math.Min(amplifier, numColumnTables); TableID++)
                    {
                        AllTables.Add(NameWithID(Table, TableID));
                    }
                }
                else if (numColumnTables != -1)
                {
                    AllTables.Add(Table);
                }
            }
            return AllTables;
        }

        /// <summary>
        /// Return a list of the encoding tables in reverse order. 
        /// </summary>
        private List<string> ListOfTablesBackwards(string[] arrayTable, int amplifier, int originalDBFormat)
        {
            List<string> allTables = new List<string>();
            foreach (var table in arrayTable)
            {
                if (table.Contains("_X00"))
                {

                    for (int TableID = (originalDBFormat - 1); TableID > (originalDBFormat - amplifier); TableID--)
                    {
                        allTables.Add(NameWithID(table, TableID));
                    }
                }
                else
                {
                    allTables.Add(table);
                }
            }
            return allTables;
        }

        /// <summary>
        /// SPECIFIC FOR SAP HANA - returns the appropriate creation string for the object settings. 
        /// </summary>
        public string CreateHANATables(string Command, int TableID, int NumColumnTables,
            int NumPagedTables, int NumPartitions, int NumExtensionNodeTables, string ExtensionNodeGroupName)
        {
            const string _RowCol_ = "<RowCol>";
            const string _Partition_By_Hash = "<PARTITION BY HASH";
            const string Partitions_n_ = "PARTITIONS n>";
            const string Loadable_n_ = "<n LOADABLE";
            const string GroupType_n = "GROUP TYPE <n";
            bool RowTable = (NumColumnTables < TableID);
            string s = Command.Replace(_RowCol_, (RowTable ? "ROW" : "COLUMN"));
            int i = s.IndexOf(_Partition_By_Hash);
            if ((i == notFound))
            {
                return s;
            }

            //Some tables are always Row Table and do not have a tag <PARTITION BY HASH ...>
            if (RowTable)
            {
                //  RowTable, remove "<PARTITION BY HASH (PageId) PARTITIONS n>"
                s = s.Substring(0, i);
            }
            else
            {
                //  ColumnTable, replace "<PARTITION BY HASH (PageId) PARTITIONS n>" by "PARTITION BY HASH (PageId) PARTITIONS GET_NUM_SERVERS()"
                s = s.Replace(_Partition_By_Hash, "PARTITION BY HASH").Replace(Partitions_n_, ((NumPartitions == -1) ? "PARTITIONS GET_NUM_SERVERS()" : ("PARTITIONS " + NumPartitions.ToString()))).Replace(
                     Loadable_n_, ((NumPagedTables > TableID) ? "PAGE LOADABLE" : "")).Replace(GroupType_n, ((NumExtensionNodeTables > TableID) ? "GROUP TYPE \"" + ExtensionNodeGroupName + "\"" : ""));
            }
            return s;
        }

        /// <summary>
        /// SPECIFIC FOR MySQL - returns the appropriate creation string for the object settings. 
        /// </summary>
        public string CreateMySQLTables(string Command, Dynamics.MySQLStorageEngine engine, string tablespace)
        {
            string engine_n = "ENGINE= n>";
            string tablespace_local = "TABLESPACE tbsp n> STORAGE DISK";

            bool diskStorage;
            if (string.IsNullOrEmpty(tablespace))
            {
                diskStorage = false;
            }
            else
            {
                diskStorage = true;
            }

            string s = Command.Replace(engine_n, "ENGINE=" + engine.ToString());
            s = s.Replace(tablespace_local, (diskStorage ? "TABLESPACE " + tablespace + " STORAGE DISK" : ""));
            return s;
        }

        /// <summary>
        /// SPECIFIC FOR MariaDB - returns the appropriate creation string for the object settings. 
        /// </summary>
        public string CreateMariaDBTables(string Command, Dynamics.MariaDBStorageEngine engine,
           bool isBaseTable, Dynamics.DatabaseSchema schemaType)
        {
            string indexTracker = "CREATE INDEX";
            string uniqueIndexTracker = "CREATE UNIQUE INDEX";
            string engine_n = "ENGINE= n>";
            string primary_key_n = "PRIMARY KEY n>";
            string default_n_n = "DEFAULT NULL NULL n>";
            string s = Command.Replace(engine_n, "ENGINE=" + engine.ToString());
            if (engine == Dynamics.MariaDBStorageEngine.ColumnStore)
            {
                s = s.Replace(default_n_n, "NULL");
                s = s.Replace(primary_key_n, "");
                if(s.Contains(indexTracker) || s.Contains(uniqueIndexTracker)) { s = string.Empty; }
            }
            else if ((!isBaseTable) && (schemaType == Dynamics.DatabaseSchema.WithIndexes && engine == Dynamics.MariaDBStorageEngine.InnoDB) 
                || (schemaType == Dynamics.DatabaseSchema.WithoutIndexes && engine == Dynamics.MariaDBStorageEngine.InnoDB)
                || (schemaType == Dynamics.DatabaseSchema.WithIndexes && engine == Dynamics.MariaDBStorageEngine.ROCKSDB)
                || (schemaType == Dynamics.DatabaseSchema.WithoutIndexes && engine == Dynamics.MariaDBStorageEngine.ROCKSDB))
            {
                s = s.Replace(primary_key_n, "");
                s = s.Replace(default_n_n, "DEFAULT NULL NULL");
            }
            else
            {
                s = s.Replace(default_n_n, "DEFAULT NULL NULL");
                s = s.Replace(primary_key_n, "PRIMARY KEY");
            }
            return s;
        }

        /// <summary>
        /// Return the name of the table using a table ID. 
        /// </summary>
        public string NameWithID(string command, int tableID)
        {
            string tempdeleteme;
            tempdeleteme =  command.Replace("_X00", "_X" + tableID.ToString("X2"));
            return tempdeleteme;
        }

        /// <summary>
        /// Reponds with the appropriate string for which tables have been created. 
        /// </summary>
        public string HandleTableCreateResponse(Dynamics.DatabaseSchema _DatabaseSchema, bool state)
        {
            string response = null;
            if (state)
            {
                if (_DatabaseSchema == Dynamics.DatabaseSchema.WithIndexes)
                {
                    response = "Database Tables, Constraints and Indexes created.";
                }
                else if (_DatabaseSchema == Dynamics.DatabaseSchema.WithoutIndexes)
                {
                    response = "Database Tables and Constraints created.";
                }
                else if (_DatabaseSchema == Dynamics.DatabaseSchema.MemoryOptimised)
                {
                    response = "Database Memory Optimised Tables, Constraints and Indexes created.";
                }
                else if (_DatabaseSchema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexes)
                {
                    response = "Database Memory Optimised Tables and  Constraints created.";
                }
                else if (_DatabaseSchema == Dynamics.DatabaseSchema.MemoryOptimisedWithoutIndexesLOB)
                {
                    response = "Database Memory Optimised Tables and Constraints created for a LOB schema.";
                }
                else if (_DatabaseSchema == Dynamics.DatabaseSchema.MemoryOptimisedLOB)
                {
                    response = "Database Memory Optimised Tables and Constraints created for a LOB schema.";
                }
                else if (_DatabaseSchema == Dynamics.DatabaseSchema.WithIndexesLOB)
                {
                    response = "Database Tables, Constraints and Indexes created for a LOB schema.";
                }
                else if (_DatabaseSchema == Dynamics.DatabaseSchema.WithoutIndexesLOB)
                {
                    response = "Database Tables and Constraints created for a LOB schema.";
                }
            }
            else
            {
                response = "One or more Tables not created.";
            }
            return response;
        }

        /// <summary>
        /// Rturn the data unit as the desired value if it exceeds a smaller value. 
        /// </summary>
        public UInt64 ReturnDataUnitsAsBytes(UInt64 amount, Dynamics.StorageUnit unit)
        {
            UInt64 valueAsBytes = 0;
            if (unit != Dynamics.StorageUnit.Bytes)
            {
                if (unit == Dynamics.StorageUnit.Kilobytes)
                {
                    valueAsBytes = amount * 1024L;
                }
                else if (unit == Dynamics.StorageUnit.Megabytes)
                {
                    valueAsBytes = amount * (1024L * 1024L);
                }
                else if (unit == Dynamics.StorageUnit.Gigabytes)
                {
                    valueAsBytes = amount * (1024L * 1024L * 1024L);
                }
                else if (unit == Dynamics.StorageUnit.Terabytes)
                {
                    UInt64 TBCalc = (1024L * 1024L * 1024L * 1024L);
                    valueAsBytes = amount * TBCalc;
                }
            }
            else
            {
                valueAsBytes = amount;
            }
            return valueAsBytes;
        }

        /// <summary>
        /// Encrypt and return a randomized string. 
        /// </summary>
        public string Encrypt(string plainText)
        {
            if (plainText != "")
            {
                char[] chars = plainText.ToCharArray();
                int size = plainText.Length;

                // First calculate how much of the string is to be randomized 
                int numberOfCharsToRandomize = Convert.ToInt32(Math.Round(size * (_randomPercentage / 100), 0)); 


                byte[] data = RandomNumberGenerator.GetBytes(4 * size);
                StringBuilder result = new StringBuilder(size);
                for (int i = 0; i < size; i++)
                {
                    if (i >= numberOfCharsToRandomize)
                    {
                        result.Append(chars[i]);
                    }
                    else
                    {
                        var rnd = BitConverter.ToUInt32(data, i * 4);
                        var idx = rnd % chars.Length;
                        result.Append(chars[idx]);
                    }
                }
                return result.ToString();
            }
            else
            {
                return plainText;
            }
        }

        private static Random random = new Random();

        /// <summary>
        /// Chuck the string to return a list of smaller strings. 
        /// </summary>
        public List<string> ChunkString(string str, int maxChunkSize)
        {
            List<string> result = str.Select((c, i) => new { Char = c, Index = i }).GroupBy(o => o.Index / 
            maxChunkSize).Select(g => new String(g.Select(o => o.Char).ToArray())).ToList();
            return result;
        }
    }
}
