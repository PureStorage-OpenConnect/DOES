using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DOES.Shared.Debug;
using DOES.DataEngine.FileOperations;
using DOES.DataEngine.Resources;
using DOES.Shared.Resources;
using Sap.Data.Hana;
using System.IO;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Runtime.InteropServices;

namespace DOES.DataEngine.Operations
{
    /// <summary>
    /// This class handles SAP HANA database operations. 
    /// </summary>
    public class SAPHANA : DataVendor
    {
        private string _hostname;
        private string _databaseName;
        private string _userName;
        private string _password;
        private string _instance;
        private int _port;
        private int _percentColumnTables;
        private int _percentagePagedTables;
        private int _percentageWarmExtensionNodeTables;
        private string _extensionNodeGroupName;
        private int _numberOfPartitions;
        private int _amplifier;
        private Dynamics.DatabaseSchema _schema;
        private Mechanic _mechanic;
        private MessageQueue _messageQueue;
        private CancellationTokenSource _tokenSource;

        /// <summary>
        /// Instantiates the SAP HANA Data Driver interface. 
        /// </summary>
        public SAPHANA(string hostname, string dbname, string instance, string username, 
            string password, int port, int tableAmplifier, Mechanic mechanic,
           MessageQueue queue)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _instance = instance;
            _port = port;
            _amplifier = tableAmplifier;
            _mechanic = mechanic;
            _messageQueue = queue;
        }

        /// <summary>
        /// Instantiates the SAP HANA Data Driver interface. 
        /// </summary>
        public SAPHANA(string hostname, string dbname, string instance, string username,
            string password, int port, Mechanic mechanic,
           MessageQueue queue)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _instance = instance;
            _port = port;
            _mechanic = mechanic;
            _messageQueue = queue;
        }

        /// <summary>
        /// Instantiates the SAP HANA Data Driver interface. 
        /// </summary>
        public SAPHANA(string hostname, string dbname, string instance, string username, 
            string password, int port, int tableAmplifier,
            int percentageColumnTables, Mechanic mechanic, MessageQueue queue)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _instance = instance;
            _port = port;
            _percentColumnTables = percentageColumnTables;
            _amplifier = tableAmplifier;
            _mechanic = mechanic;
            _messageQueue = queue;
        }

        /// <summary>
        /// Instantiates the SAP HANA Data Driver interface. 
        /// </summary>
        public SAPHANA(string hostname, string dbname, string username, string password, string instance, 
            int amplifier, int percentColumnTables, int percentagePagedTables, int partitions,
           int percentageWarmExtensionNodeTables, string extensionNodeGroupName, int NumberOfThreads,
           int port, Dynamics.DatabaseSchema schema, MessageQueue messages, Mechanic mechanic)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _instance = instance;
            _amplifier = amplifier;
            _port = port;
            _percentColumnTables = percentColumnTables;
            _percentagePagedTables = percentagePagedTables;
            _numberOfPartitions = partitions;
            _percentageWarmExtensionNodeTables = percentageWarmExtensionNodeTables;
            _extensionNodeGroupName = extensionNodeGroupName;
            _schema = schema;
            _messageQueue = messages;
            _mechanic = mechanic;
        }

        /// <summary>
        /// Checks if the current file being used for operations has been used before. 
        /// </summary>
        public override ImportHandler CheckFileImportHistory(string filename)
        {
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    HanaCommand command = new HanaCommand("SELECT ImportedWebPages, ImportedAllWebPages from Imports WHERE FileName = '" + filename + "'", connection);
                    command.CommandTimeout = 600;
                    connection.Open();
                    ImportHandler importData;
                    int importedWebPages = 0;
                    bool importedAllWebPages = false;
                    bool found = false;
                    using (HanaDataReader dataReader = command.ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            found = true;
                            importedWebPages = Convert.ToInt32(dataReader[0]);
                            importedAllWebPages = Convert.ToBoolean(dataReader[1]);
                        }
                    }
                    importData = new ImportHandler(filename, importedWebPages, importedAllWebPages, found);
                    connection.Close();
                    return importData;
                }
                catch (Exception ex)
                {
                    _tokenSource.Cancel();
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    return null;
                }
            }
            
        }

        /// <summary>
        /// Checks the schema type for this database. 
        /// </summary>
        public override void CheckSchemaType()
        {
            string schemaResponse = string.Empty;
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaCommand command = new HanaCommand("SELECT SchemaType FROM Configuration", connection);
                    using (HanaDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Enum.TryParse(Convert.ToString(reader[0]), out _schema);
                        }
                    }
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
        }

        /// <summary>
        /// Creates a record for the importing of data from data files. 
        /// </summary>
        public override bool CreateImportHistory(ImportHandler import)
        {
            bool success = false;
            try
            {
                using (HanaConnection connection = GetConnection())
                {
                    connection.Open();
                    HanaCommand command = new HanaCommand("INSERT INTO Imports (FileName, ImportedWebPages, ImportedAllWebPages) VALUES ('" +
                    import.Filename + "', '" +
                    import.ImportedWebPages + "', '" +
                    Convert.ToInt32(import.AllImportedWebPages) + "'" +
                ")",
                connection);
                    success = Convert.ToBoolean(command.ExecuteNonQuery());
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return success;
        }

        /// <summary>
        /// Create the database objects from the base and extension templates. 
        /// </summary>
        public override bool CreateObjects(List<string> baseSchemaObjects, List<string> extensionSchemaObjects)
        {
            bool success = false;
            using (HanaConnection conn = GetConnection())
            {
                //Base Objects
                try
                {
                    conn.Open();
                    foreach (string table in _mechanic.AllBaseTables())
                    {
                        HanaCommand command = new HanaCommand("SELECT TOP 1 * FROM " + table + "", conn);
                        command.ExecuteScalar();
                    }
                    conn.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        success = false;
                        return success;
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                    //write ex to log 
                    StringBuilder sb = new StringBuilder();
                    foreach (string s in baseSchemaObjects)
                    {
                        if (s.Trim() == "")
                        {
                            continue;
                        }
                        else if (s.StartsWith("//") == true)
                        {
                            if (sb.Length != 0)
                            {
                                string sqlCommandString = sb.ToString();
                                string SQLString = Regex.Replace(sb.ToString(), @"\n|\r|\t", "");
                                HanaCommand command = new HanaCommand(SQLString, conn);
                                try
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                                    command.ExecuteNonQuery();
                                    success = true;
                                }
                                catch (HanaException sqlex)
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, sqlex.Message.ToString(), Message.MessageType.Command));
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
                    try
                    {
                        //Create the Configuration table objects and load them 
                        HanaCommand firstPointcommand = new HanaCommand("INSERT INTO Configuration (SchemaType, CreatedOn) VALUES" +
                            " (?, ?)", conn);

                        firstPointcommand.Parameters.AddWithValue("@SchemaType", _schema.ToString());
                        firstPointcommand.Parameters.AddWithValue("@CreatedOn", DateTime.Now);
                        firstPointcommand.ExecuteNonQuery();
                        conn.Close();
                        success = true;
                        if (_percentagePagedTables == 100)
                        {
                            SetBaseTablesPageLoadable();
                        }
                    }
                    catch (HanaException sql3)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, sql3.Message, Message.MessageType.Warning));
                        success = false;
                    }
                }
                try
                {
                    if (conn.State == System.Data.ConnectionState.Closed)
                    {
                        conn.Open();
                    }
                    foreach (string table in _mechanic.AllWebPageEncodingTables(_amplifier))
                    {
                        HanaCommand command = new HanaCommand("SELECT TOP 1 * FROM " + table + "", conn);
                        command.ExecuteScalar();
                    }
                    conn.Close();
                }
                catch (Exception ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    //write ex to log 
                    StringBuilder sb = new StringBuilder();
                    foreach (string s in extensionSchemaObjects)
                    {
                        if (s.Trim() == "")
                        {
                            continue;
                        }
                        else if (s.StartsWith("//") == true)
                        {
                            if (sb.Length != 0)
                            {
                                string sqlCommandString = sb.ToString();
                                for (int TableID = 0; (TableID < Convert.ToInt32((sqlCommandString.Contains("_X00") ? _amplifier : 0))); TableID++)
                                {
                                    try
                                    {
                                        string HanaCommandString = sb.ToString();
                                        HanaCommandString = _mechanic.CreateHANATables(HanaCommandString, TableID, GetNumberOfColumnTables(),
                                            GetNumberOfPagedTables(),
                                            _numberOfPartitions, GetNumberOfExtensionNodeTables(), _extensionNodeGroupName);
                                        HanaCommandString = _mechanic.NameWithID(HanaCommandString, TableID);
                                        HanaCommand command = new HanaCommand(_mechanic.NameWithID(HanaCommandString, TableID), conn);
                                        _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                                        command.ExecuteNonQuery();
                                    }
                                    catch (HanaException hxe)
                                    {
                                        _messageQueue.AddMessage(new Message(DateTime.Now, hxe.Message, Message.MessageType.Info));
                                    }
                                }
                                success = true;
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
                return success;
            }
        }

        /// <summary>
        /// Returns the common message queue used for this database. 
        /// </summary>
        public override MessageQueue DebugQueue()
        {
            return _messageQueue;
        }

        /// <summary>
        /// Deletes web pages from the database tables. 
        /// </summary>
        public override int DeleteWebPage(int PageID)
        {
            int rowsAffected = 0;
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(HanaIsolationLevel.ReadCommitted);
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        HanaCommand command = new HanaCommand("DELETE FROM " + TableName + " WHERE PageID" +
                            " = '" + PageID.ToString() + "'", connection, transaction);
                        command.CommandTimeout = 300;
                        rowsAffected += command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    rowsAffected = 0;
                }
            }
            return rowsAffected;
        }

        /// <summary>
        /// Drops or truncates database objects. 
        /// </summary>
        public override bool DestroyObjects(Dynamics.ClearingType ClearingType)
        {
            bool success = false;
            string operation = "DROP";
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    if (ClearingType == Dynamics.ClearingType.Drop)
                    {
                        foreach (string seq in _mechanic.ReturnAllSequences)
                        {
                            try
                            {
                                string commandString = "DROP SEQUENCE " + seq;
                                HanaCommand command = new HanaCommand(commandString, connection);
                                _messageQueue.AddMessage(new Message(DateTime.Now, commandString, Message.MessageType.Command));
                                command.ExecuteNonQuery();
                            }
                            catch (HanaException he)
                            {
                                success = false;
                                if (he.Message.ToLower().Contains("invalid sequence"))
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, he.Message, Message.MessageType.Warning));
                                }
                                else
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, he.Message, Message.MessageType.Error));
                                }
                            }
                        }
                        operation = "DROP";
                    }
                    else if (ClearingType == Dynamics.ClearingType.Truncate)
                    {
                        operation = "TRUNCATE";
                    }

                    foreach (string tableName in _mechanic.AllTables(_amplifier))
                    {
                        try
                        {
                            string commandString = operation + " TABLE " + tableName;
                            HanaCommand command = new HanaCommand(commandString, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, commandString, Message.MessageType.Command));
                            command.CommandTimeout = 600;
                            command.ExecuteNonQuery();
                        }
                        catch (HanaException ex)
                        {
                            success = false;
                            if (ex.Message.ToLower().Contains("invalid table name"))
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                            }
                            else
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                            }
                        }
                    }
                    success = true;
                }
                catch (Exception ex)
                {
                    success = false;
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected") 
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                }
                return success;
            }
        }

        /// <summary>
        /// Returns the connection object for this class. 
        /// </summary>
        public override dynamic GetConnection()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == true)
            {
                Environment.SetEnvironmentVariable("HDBDOTNETCORE", @"C:\Program Files\Pure Storage\D.O.E.S");
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == true)
            {
                //Then its a linux platform
                Environment.SetEnvironmentVariable("HDBDOTNETCORE", "/opt/purestorage/does");
            }
            string _connectionString;
            _connectionString = "SERVERNODE=" + _hostname + ":3" + _instance + _port + ";pooling=true;max pool size=512;min pool size=32;DATABASENAME=" +
                _databaseName + ";UID=" + _userName + ";PWD=" + _password + ";";
            HanaConnection connection = new HanaConnection(_connectionString);
            return connection;
        }

        /// <summary>
        /// Return the type of database being operated on. 
        /// </summary>
        public override Dynamics.Database GetDatabaseType()
        {
            return Dynamics.Database.SAPHANA;
        }

        /// <summary>
        /// Returns the mechanic helper class used in the DataVendor.  
        /// </summary>
        public override Mechanic GetMechanic()
        {
            return _mechanic;
        }

        /// <summary>
        /// Get a list and size of all of the web pages in the database.  
        /// </summary>
        public override SortedList<int, long> GetPageIDList()
        {
            SortedList<int, long> colPageIDToLength = new SortedList<int, long>();
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    HanaCommand command = new HanaCommand("SELECT PageID, TotalLength from WebPages ORDER BY PageID", connection, transaction);
                    using (HanaDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            colPageIDToLength.Add(Convert.ToInt32(reader[0]), Convert.ToUInt32(reader[1]));
                        }
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return colPageIDToLength;
        }

        /// <summary>
        /// Return the database type. 
        /// </summary>
        public override Dynamics.DatabaseSchema GetSchemaType()
        {
            return _schema;
        }

        /// <summary>
        /// Returns a list of web pages to perfom operations on. 
        /// </summary>
        public override Tuple<Random, ulong> InitializeRandom(bool ForceRandom, ulong RequestedBytes)
        {
            if (ForceRandom == true)
            {
                Random value = new Random();
                return Tuple.Create(value, RequestedBytes);
            }
            else
            {
                int numberOfRows = 0;
                UInt64 totalDBSize = 0;
                HanaConnection connection = GetConnection();
                HanaCommand commandRowCountNN = new HanaCommand("SELECT COUNT(*) FROM WebPages WHERE ModifiedOn IS NOT NULL", connection);
                HanaCommand commandRowCountTotal = new HanaCommand("SELECT COUNT(*) FROM WebPages", connection);
                HanaCommand commandTotalSize = new HanaCommand("SELECT SUM(TotalLength) FROM WebPages", connection);
                commandRowCountNN.CommandTimeout = 300;
                commandRowCountTotal.CommandTimeout = 300;
                commandTotalSize.CommandTimeout = 300;
                try
                {
                    connection.Open();
                    numberOfRows = Convert.ToInt32(commandRowCountNN.ExecuteScalar());
                    numberOfRows += Convert.ToInt32(commandRowCountTotal.ExecuteScalar());
                    totalDBSize = Convert.ToUInt64(commandTotalSize.ExecuteScalar());
                    connection.Close();

                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        _tokenSource.Cancel();
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                }

                if (totalDBSize < RequestedBytes)
                {
                    RequestedBytes = totalDBSize;
                }

                Random value = new Random(numberOfRows);
                return Tuple.Create(value, RequestedBytes);
            }
        }

        /// <summary>
        /// Inserts a characterised web page into the database objects. 
        /// </summary>
        public override int InsertCharacterisedWebPage(WebPage page)
        {
            int numberOfRowsAffected = 0;
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(HanaIsolationLevel.ReadCommitted);
                    HanaCommand pageIDCommand = new HanaCommand("SELECT Seq_PageID.NEXTVAL FROM DUMMY", connection, transaction);
                    page.PageID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                    HanaCommand command = new HanaCommand(
                    "INSERT INTO WebPages (PageID, LoadedOn, HeadersLength, StatsLength, TotalLength, HREFs, HashURL, HashHTML, URL, HTML) " +
                                          "VALUES (?, ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?)",
                    connection,
                    transaction);
                    command.Parameters.AddWithValue("@PageID", page.PageID); //                                       PageID Generated Seperately
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                    Current Date Time (DateTime2)
                                                                                //                                    Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(page.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(page.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(page.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(page.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(page.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML", page.HashHTML); //                                   Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", page.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", page.HTMLBinary); //                                     The fist 128 characters of HTML  
                    command.CommandTimeout = 600;
                    numberOfRowsAffected += command.ExecuteNonQuery();
                    if (numberOfRowsAffected == 0)
                    {
                        throw new Exception("Failed to insert base WebPage Row");
                    }
                    if (_amplifier != 0)
                    {
                        //Contains all 4 encoded table sets 
                        int numberOfEncoders = _mechanic.ReturnWebPageEncodingtables.Length;
                        HanaCommand[][][] allEncodedWebPagesCommands = new HanaCommand[numberOfEncoders][][];
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            //contains all the tables for the subset of the encoding up to the amplifier 
                            allEncodedWebPagesCommands[i] = new HanaCommand[_amplifier][];
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string tableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                int htmlIndex = tableID * 8 + i + 4;
                                int urlIndex = tableID * 8 + i;
                                int b64SHTMLIndex = tableID * 8 + i + 4;
                                int b64SURLIndex = tableID * 8 + i;

                                string convertedHTML = Encoding.ASCII.GetString(page.DataAsBytes[htmlIndex], 0, page.DataAsBytes[htmlIndex].Length);
                                string convertedURL = Encoding.ASCII.GetString(page.DataAsBytes[urlIndex], 0, page.DataAsBytes[urlIndex].Length);
                                string convertedB64SHTML = Encoding.ASCII.GetString(page.DataAsBS64s[b64SHTMLIndex]);
                                string convertedB64SURL = Encoding.ASCII.GetString(page.DataAsBS64s[b64SURLIndex]);


                                //split the string if it is larger than 5000

                                List<string> splitHTMLColumns = _mechanic.ChunkString(convertedHTML, 5000);
                                List<string> splitURLColumns = _mechanic.ChunkString(convertedURL, 5000);
                                List<string> splitB64SHTMLColumns = _mechanic.ChunkString(convertedB64SHTML, 5000);
                                List<string> splitB64SURLColumns = _mechanic.ChunkString(convertedB64SURL, 5000);

                                List<Tuple<string, string, string>> webPageCharacterSplit = new List<Tuple<string, string, string>>();
                                //get the longest length object

                                int splitHTMLColumnLength = splitHTMLColumns.Count;
                                int splitURLColumnLength = splitURLColumns.Count;
                                int splitB64SHTMLColumnLength = splitB64SHTMLColumns.Count;
                                int splitB64SURLColumnLength = splitB64SURLColumns.Count;

                                int maxValue = new[] { splitHTMLColumnLength, splitURLColumnLength,
                                splitB64SHTMLColumnLength, splitB64SURLColumnLength }.Max();
                                HanaCommand[] commandStruct = new HanaCommand[maxValue];
                                for (int ci = 0; ci < maxValue; ci++)
                                {
                                    string tURL = "";
                                    string tHTML = "";
                                    string tB64SHTML = "";
                                    string tB64SURL = "";
                                    int urlLength = 0;
                                    int htmlLength = 0;
                                    int urlB64SLength = 0;
                                    int htmlB64SLength = 0;
                                    if (splitURLColumns.Count > ci)
                                    {
                                        tURL = splitURLColumns[ci];
                                        urlLength = tURL.Length;
                                    }

                                    if (splitHTMLColumns.Count > ci)
                                    {
                                        tHTML = splitHTMLColumns[ci];
                                        htmlLength = tHTML.Length;
                                    }

                                    if (splitB64SHTMLColumns.Count > ci)
                                    {
                                        tB64SHTML = splitB64SHTMLColumns[ci];
                                        htmlB64SLength = tB64SHTML.Length;
                                    }

                                    if (splitB64SURLColumns.Count > ci)
                                    {
                                        tB64SURL = splitB64SURLColumns[ci];
                                        urlB64SLength = tB64SURL.Length;
                                    }

                                    command = new HanaCommand(
                                    "INSERT INTO " + tableName + " " +
                                    "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                    "          VALUES (?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?) ",
                                    connection,
                                    transaction);

                                    command.Parameters.AddWithValue("@PageID", page.PageID); //                                                   Integer (int)                                           //                                                   Columns ModifiedOn and Updates get default values
                                    command.Parameters.AddWithValue("@URL_Length", urlLength); //                                                 Integer (int)
                                    command.Parameters.AddWithValue("@URL", tURL); //                                                             Byte Array (binary)
                                    command.Parameters.AddWithValue("@HTML_Length", htmlLength); //                                               Integer (int)
                                    command.Parameters.AddWithValue("@HTML", tHTML); //                                                           Byte Array (binary)
                                    command.Parameters.AddWithValue("@URL_B64S_Length", urlB64SLength); //                                        Integer (int)
                                    command.Parameters.AddWithValue("@URL_B64S", tB64SURL); //                                                        Byte Array (binary)
                                    command.Parameters.AddWithValue("@HTML_B64S_Length", htmlB64SLength); //                                      Integer (int)
                                    command.Parameters.AddWithValue("@HTML_B64S", tB64SHTML); //                                                  Byte Array (binary)
                                    command.CommandTimeout = 600;
                                    commandStruct[ci] = command;
                                }
                                allEncodedWebPagesCommands[i][tableID] = commandStruct;
                            }
                        }

                        //first level are the 4 encoders
                        ConcurrentBag<int> numberOfParalellRowsAffected = new ConcurrentBag<int>();
                        Parallel.For(0, allEncodedWebPagesCommands.Length, encoder =>
                        {
                        //second is the number of tables per encoder 
                        Parallel.For(0, allEncodedWebPagesCommands[encoder].Length, tableid =>
                            {
                            //third are the actual tables - execute here
                            for (int commandIndex = 0; commandIndex < allEncodedWebPagesCommands[encoder][tableid].Length; commandIndex++)
                                {
                                    try
                                    {
                                        int numberOfAffectedRows = allEncodedWebPagesCommands[encoder][tableid][commandIndex].ExecuteNonQuery();
                                        numberOfParalellRowsAffected.Add(numberOfAffectedRows);
                                    }
                                    catch (HanaException ex)
                                    {
                                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                                    }
                                }
                            });
                        //the goal is to get each individual table running a query at the same time !
                        //with a table amplification of 8 the numbers of tables should be 36
                    });

                        foreach (int rowCount in numberOfParalellRowsAffected)
                        {
                            numberOfRowsAffected += rowCount;
                        }
                    }
                    foreach (KeyValuePair<string, string> kvp in page.Headers)
                    {
                        HanaCommand headerIDCommand = new HanaCommand("SELECT Seq_HeaderID.NEXTVAL FROM DUMMY", connection, transaction);
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new HanaCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (?, ?, ?,  ?,  ?,  ?)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@HeaderID", headerID);
                        command.Parameters.AddWithValue("@PageID", page.PageID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= page.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            HanaCommand statIDCommand = new HanaCommand("SELECT Seq_StatId.NEXTVAL FROM DUMMY", connection, transaction);
                            int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                            command = new HanaCommand(
                                "INSERT INTO WebPageStats (StatID ,PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (?, ?,  ?,  ?,  ?,  ?,  ?)",
                                connection,
                                transaction);
                            // Columns ModifiedOn and Updates get default values
                            command.Parameters.AddWithValue("@StatID", statID);
                            command.Parameters.AddWithValue("@PageID", page.PageID);
                            command.Parameters.AddWithValue("@CodeType", codeType);
                            command.Parameters.AddWithValue("@Length", page.StatsAsBytes[codeType].Length);
                            command.Parameters.AddWithValue("@Stats", page.StatsAsBytes[codeType]);
                            command.Parameters.AddWithValue("@B64S_Length", page.StatsAsBS64s[codeType].Length);
                            command.Parameters.AddWithValue("@B64S_Stats", page.StatsAsBS64s[codeType]);
                            command.CommandTimeout = 300;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    numberOfRowsAffected = 0;
                }
            }
            return numberOfRowsAffected;
        }

        /// <summary>
        /// Inserts an encoded web page into the database objects. 
        /// </summary>
        public override int InsertEncodedWebPage(WebPage page)
        {
            int numberOfRowsAffected = 0;
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(HanaIsolationLevel.ReadCommitted);
                    HanaCommand pageIDCommand = new HanaCommand("SELECT Seq_PageID.NEXTVAL FROM DUMMY", connection, transaction);
                    page.PageID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                    HanaCommand command = new HanaCommand(
                    "INSERT INTO WebPages (PageID, LoadedOn, HeadersLength, StatsLength, TotalLength, HREFs, HashURL, HashHTML, URL, HTML) " +
                                          "VALUES (?, ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?)",
                    connection,
                    transaction);
                    command.Parameters.AddWithValue("@PageID", page.PageID); //                                       PageID Generated Seperately
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                    Current Date Time (DateTime2)
                                                                                //                                    Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(page.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(page.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(page.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(page.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(page.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML", page.HashHTML); //                                   Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", page.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", page.HTMLBinary); //                                     The fist 128 characters of HTML  
                    command.CommandTimeout = 600;
                    numberOfRowsAffected += command.ExecuteNonQuery();
                    if (numberOfRowsAffected == 0)
                    {
                        throw new Exception("Failed to insert base WebPage Row");
                    }
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string tableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                command = new HanaCommand(
                                "INSERT INTO " + tableName + " " +
                                "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                "          VALUES (?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?) ",
                                connection,
                                transaction);
                                command.Parameters.AddWithValue("@PageID", page.PageID); //                                                   Integer (int)
                                                                                         //                                           Columns ModifiedOn and Updates get default values
                                command.Parameters.AddWithValue("@URL_Length", page.DataAsBytes[tableID * 8 + i].Length); //                  Integer (int)
                                command.Parameters.AddWithValue("@URL", page.DataAsBytes[tableID * 8 + i]); //                                Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_Length", page.DataAsBytes[tableID * 8 + i + 4].Length); //             Integer (int)
                                command.Parameters.AddWithValue("@HTML", page.DataAsBytes[tableID * 8 + i + 4]); //                           Byte Array (binary)
                                command.Parameters.AddWithValue("@URL_B64S_Length", page.DataAsBS64s[tableID * 8 + i].Length); //             Integer (int)
                                command.Parameters.AddWithValue("@URL_B64S", page.DataAsBS64s[tableID * 8 + i]); //                           Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_B64S_Length", page.DataAsBS64s[tableID * 8 + i + 4].Length); //        Integer (int)
                                command.Parameters.AddWithValue("@HTML_B64S", page.DataAsBS64s[tableID * 8 + i + 4]); //                      Byte Array (binary)
                                command.CommandTimeout = 600;
                                numberOfRowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }
                    foreach (KeyValuePair<string, string> kvp in page.Headers)
                    {
                        HanaCommand headerIDCommand = new HanaCommand("SELECT Seq_HeaderId.NEXTVAL FROM DUMMY", connection, transaction);
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new HanaCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (?, ?, ?,  ?,  ?,  ?)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@HeaderID", headerID);
                        command.Parameters.AddWithValue("@PageID", page.PageID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= page.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            HanaCommand statIDCommand = new HanaCommand("SELECT Seq_StatID.NEXTVAL FROM DUMMY", connection, transaction);
                            int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                            command = new HanaCommand(
                                "INSERT INTO WebPageStats (StatID ,PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (?, ?,  ?,  ?,  ?,  ?,  ?)",
                                connection,
                                transaction);
                            // Columns ModifiedOn and Updates get default values
                            command.Parameters.AddWithValue("@StatID", statID);
                            command.Parameters.AddWithValue("@PageID", page.PageID);
                            command.Parameters.AddWithValue("@CodeType", codeType);
                            command.Parameters.AddWithValue("@Length", page.StatsAsBytes[codeType].Length);
                            command.Parameters.AddWithValue("@Stats", page.StatsAsBytes[codeType]);
                            command.Parameters.AddWithValue("@B64S_Length", page.StatsAsBS64s[codeType].Length);
                            command.Parameters.AddWithValue("@B64S_Stats", page.StatsAsBS64s[codeType]);
                            command.CommandTimeout = 300;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    numberOfRowsAffected = 0;
                }
            }
            return numberOfRowsAffected;
        }

        /// <summary>
        /// Inserts a single entry into the database every 100ms until cancelled. 
        /// </summary>
        public override int InsertPointInTimeWrite()
        {
            int rowsAffected = 0;
            using (HanaConnection connection = GetConnection())
            {
                try 
                { 
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(HanaIsolationLevel.ReadCommitted);
                    HanaCommand pageIDCommand = new HanaCommand("SELECT Seq_WriteID.NEXTVAL FROM DUMMY", connection, transaction);
                    int WriteID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                    HanaCommand command = new HanaCommand("INSERT INTO PointInTimeWrite(WriteID, WrittenOn, WriteHash) VALUES (?,?,?)", connection, transaction);
                    DateTime writeTime = DateTime.Now;
                    command.Parameters.AddWithValue("@WriteID", WriteID);
                    command.Parameters.AddWithValue("@WrittenOn", writeTime);
                    using (var stream = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(stream, Encoding.ASCII, true))
                        {
                            writer.Write(writeTime.Ticks);
                        }
                        using (var hash = SHA256.Create())
                        {
                            string hashVal = Encoding.ASCII.GetString(hash.ComputeHash(stream));
                            command.Parameters.AddWithValue("@WriteHash", hashVal);
                        }
                    }
                    rowsAffected += command.ExecuteNonQuery();
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return rowsAffected;
        }

        /// <summary>
        /// Read base web page data from the SAP HANA database. 
        /// </summary>
        public override void ReadBaseWebPageData(OilPump pump)
        {
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    HanaCommand command = new HanaCommand("SELECT PageID, URL, HTML " +
                    "FROM   Webpages " +
                    "ORDER BY PageID", connection);
                    command.CommandTimeout = 600000;
                    connection.Open();
                    using (HanaDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int PageID = Convert.ToInt32(reader[0]);
                            HanaCommand heaaderCommand = new HanaCommand("SELECT HeaderKey, HeaderValue FROM WebPageHeaders WHERE PageID = '" + PageID + "'", connection);
                            heaaderCommand.CommandTimeout = 600000;
                            SortedList<string, string> headerList = new SortedList<string, string>();
                            using (HanaDataReader headerReader = heaaderCommand.ExecuteReader())
                            {
                                while (headerReader.Read())
                                {
                                    headerList.Add(Convert.ToString(headerReader[0]), Convert.ToString(headerReader[1]));
                                }
                            }
                            byte[] bHTML = (byte[])reader[2];
                            WebPage webpage = new WebPage(PageID, Convert.ToString(reader[1]), Encoding.ASCII.GetString(bHTML), headerList);
                            if (!_tokenSource.Token.IsCancellationRequested)
                            {
                                pump.IncreaseOilReserve(webpage);
                            }
                            else
                            {
                                reader.Close();
                            }
                        }
                    }
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        _tokenSource.Cancel();
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                }
            }
        }

        /// <summary>
        /// Queries web page data using the ANSI SQL Lef Outer Join Command Set. 
        /// </summary>
        public override Tuple<ulong, ulong> SelectWebPageLeftOuterJoin(int PageID, UInt64 bytesToProcess)
        {
            UInt64 rowDataprocessed = 0;
            UInt64 rowsProcessed = 0;
            DateTime timestamp = DateTime.Now;

            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    string query = "SELECT WebPages.PageID,WebPages.URL,WebPages.HTML, WebPages.HeadersLength, WebPages.StatsLength, WebPages.TotalLength, "
                         + "WebPages.HREFS, WebPages.HASHURL, WebPages.HashHTML, ";
                    //int webPageEncodingTablesIndex = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0);

                    //GO THrough Everything from webpageencoding lower to webpage encoding to higher 
                    //InSide that go through all of the tables ID's up to DB Format 
                    //If the tables in the query are more than 4 then go back 4 tableID or WebpageEncodingID's and 

                    int lowerBoundmarker = 0;
                    int upperBoundMarker = 0;
                    int lowerTableMarker = 0;
                    int upperTableMarker = 0;
                    int lastValueMod = 0;
                    int numberOfTablesInQuery = 0;
                    int tablesToTruncateInQuery = 4;
                    List<string> queriesToRun = new List<string>();
                    //Query over standard and encoded Web Pages
                    for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                    {
                        upperBoundMarker = i;
                        for (int tableID = 0; tableID < _amplifier; tableID++)
                        {
                            upperTableMarker = tableID;
                            string tableName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                            query = query + tableName + ".URL AS " + tableName + "EncodedURL, " + tableName + ".HTML AS " + tableName + "EncodedHTML, "
                                + tableName + ".URL_LENGTH AS " + tableName + "URL_LENGTH, " + tableName + ".HTML_LENGTH AS " + tableName + "HTML_LENGTH, "
                                + tableName + ".URL_B64S_LENGTH AS " + tableName + "URL_B64S_LENGTH, " + tableName + ".URL_B64S AS " + tableName + "URL_B64S, "
                                + tableName + ".HTML_B64S_LENGTH AS " + tableName + "HTML_B64S_LENGTH, " + tableName + ".HTML_B64S AS " + tableName + "HTML_B64S, ";
                            numberOfTablesInQuery++;
                            if (numberOfTablesInQuery == tablesToTruncateInQuery)
                            {
                                //if the  lowertablemarker is more than the upperTableMarker , it means the whole thing is rolled over 
                                query = query.Substring(0, query.Length - 2);
                                query = query + " FROM WebPages ";

                                if (lowerTableMarker > upperTableMarker)
                                {
                                    //Second
                                    for (int i2 = lowerBoundmarker; i2 <= (upperBoundMarker - 1); i2++)
                                    {
                                        for (int tableID2 = lowerTableMarker; tableID2 < _amplifier; tableID2++)
                                        {
                                            string tableName2 = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i2], tableID2);
                                            query = query + "LEFT JOIN " + tableName2 + " ON WebPages.PageID = " + tableName2 + ".PageID ";
                                            lastValueMod = tableID2;
                                        }
                                    }
                                    lowerTableMarker = 0;
                                    for (int i2 = (lowerBoundmarker + 1); i2 <= upperBoundMarker; i2++)
                                    {
                                        for (int tableID2 = lowerTableMarker; tableID2 <= upperTableMarker; tableID2++)
                                        {
                                            string tableName2 = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i2], tableID2);
                                            query = query + "LEFT JOIN " + tableName2 + " ON WebPages.PageID = " + tableName2 + ".PageID ";
                                            lastValueMod = tableID2;
                                        }
                                    }
                                }
                                else
                                {
                                    for (int i2 = lowerBoundmarker; i2 <= upperBoundMarker; i2++)
                                    {
                                        for (int tableID2 = lowerTableMarker; tableID2 <= upperTableMarker; tableID2++)
                                        {
                                            string tableName2 = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i2], tableID2);
                                            query = query + "LEFT JOIN " + tableName2 + " ON WebPages.PageID = " + tableName2 + ".PageID ";
                                            lastValueMod = tableID2;
                                        }
                                    }
                                }
                                query = query + " WHERE WebPages.PageID = " + PageID;
                                queriesToRun.Add(query);
                                query = "SELECT WebPages.PageID,WebPages.URL,WebPages.HTML, WebPages.HeadersLength, WebPages.StatsLength, WebPages.TotalLength, "
                                + "WebPages.HREFS, WebPages.HASHURL, WebPages.HashHTML, ";
                                numberOfTablesInQuery = 0;
                                lowerBoundmarker = i;
                                lowerTableMarker = tableID + 1;
                            }
                        }
                    }
                    query = query.Substring(0, query.Length - 2);
                    query = query + " FROM WebPages ";
                    for (int i = lowerBoundmarker; i <= upperBoundMarker; i++)
                    {
                        for (int tableID = lowerTableMarker; tableID < upperTableMarker; tableID++)
                        {
                            string tableName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                            query = query + "LEFT JOIN " + tableName + " ON WebPages.PageID = " + tableName + ".PageID ";
                        }
                    }
                    query = query + " WHERE WebPages.PageID = " + PageID + ";";
                    queriesToRun.Add(query);
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                    Tuple<UInt64, UInt64>[] collation = new Tuple<UInt64, UInt64>[queriesToRun.Count];

                    CheckSchemaType();
                    if (GetSchemaType() == Dynamics.DatabaseSchema.WithIndexes ||
                    GetSchemaType() == Dynamics.DatabaseSchema.WithoutIndexes)
                    {
                        for (int i = 0; i < queriesToRun.Count; i++)
                        {
                            HanaCommand command = new HanaCommand(queriesToRun[i], connection, transaction);
                            command.CommandTimeout = 600;
                            using (HanaDataReader dataReader = command.ExecuteReader())
                            {
                                while (dataReader.Read())
                                {
                                    while (rowDataprocessed < bytesToProcess)
                                    {
                                        for (int index = 0; index < dataReader.FieldCount; index++)
                                        {
                                            string field = "";
                                            var type = dataReader[index].GetType();
                                            if (type.Name == "Byte[]")
                                            {
                                                byte[] byteCodes = (byte[])dataReader.GetValue(index);
                                                field = Encoding.UTF8.GetString(byteCodes, 0, byteCodes.Length);
                                            }
                                            else
                                            {
                                                field = dataReader[index].ToString();
                                            }
                                            rowDataprocessed += Convert.ToUInt64(field.Length);
                                        }
                                        rowsProcessed++;
                                    }
                                    i = queriesToRun.Count;
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        ParallelOptions po = new ParallelOptions();
                        po.MaxDegreeOfParallelism = 8;
                        Parallel.For(0, queriesToRun.Count, po, i =>
                        {
                            UInt64 rowDataprocessedInParallel = 0;
                            UInt64 rowsProcessedInParalell = 0;
                            HanaCommand command = new HanaCommand(queriesToRun[i], connection, transaction);
                            command.CommandTimeout = 600;
                            using (HanaDataReader dataReader = command.ExecuteReader())
                            {
                                while (dataReader.Read())
                                {
                                    for (int index = 0; index < dataReader.FieldCount; index++)
                                    {
                                        string field = "";
                                        var type = dataReader[index].GetType();
                                        if (type.Name == "Byte[]")
                                        {
                                            byte[] byteCodes = (byte[])dataReader.GetValue(index);
                                            field = Encoding.UTF8.GetString(byteCodes, 0, byteCodes.Length);
                                        }
                                        else
                                        {
                                            field = dataReader[index].ToString();
                                        }
                                        rowDataprocessedInParallel += Convert.ToUInt64(field.Length);
                                    }
                                    rowsProcessedInParalell++;
                                }
                            }
                            Tuple<ulong, ulong> collationInParalell = Tuple.Create(rowDataprocessedInParallel, rowsProcessedInParalell);
                            collation[i] = collationInParalell;
                        });

                        foreach (Tuple<ulong, ulong> collator in collation)
                        {
                            rowDataprocessed += collator.Item1;
                            rowsProcessed += collator.Item2;
                        }
                    }

                    //Retrieve Header Data

                    string queryHeaderData = "SELECT HeaderID, PageID, HeaderKeyLength, Headerkey, HeaderValueLength, HeaderValue " +
                                             "FROM WebPageHeaders " +
                                             "WHERE PageID = " + PageID;
                    HanaCommand HeaderDataValues = new HanaCommand(queryHeaderData, connection, transaction);
                    HeaderDataValues.CommandTimeout = 300;
                    using (HanaDataReader dataReader = HeaderDataValues.ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            for (int index = 0; index < dataReader.FieldCount; index++)
                            {
                                string field = dataReader[index].ToString();
                                rowDataprocessed += Convert.ToUInt64(field.Length);
                            }
                            rowsProcessed++;
                        }
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return Tuple.Create(rowDataprocessed, rowsProcessed);
        }

        /// <summary>
        /// Queries web page data using the ANSI SQL Union All command set; 
        /// </summary>
        public override Tuple<ulong, ulong> SelectWebPageUnionAll(int PageID, UInt64 bytesToProcess)
        {
            UInt64 rowDataprocessed = 0;
            UInt64 rowsProcessed = 0;
            DateTime timestamp = DateTime.Now;
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    string query = "";
                    int webPageEncodingTablesIndex = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0);
                    int tableIDIndex = 0;
                    int numberOfTablesInQuery = 1;
                    int tablesToTruncateInQuery = 32;
                    List<string> queriesToRun = new List<string>();
                    //Query over standard and encoded Web Pages
                    for (int i = webPageEncodingTablesIndex; i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                    {
                        for (int tableID = tableIDIndex; tableID < _amplifier; tableID++)
                        {
                            string tableName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                            if (numberOfTablesInQuery > tablesToTruncateInQuery)
                            {
                                query = query + "SELECT PageID, ModifiedOn, Updates, URL, HTML, URL_LENGTH, HTML_LENGTH, URL_B64S_LENGTH, URL_B64S, HTML_B64S_LENGTH, HTML_B64S " +
                                    "FROM " + tableName + " WHERE PageID = " + PageID + ";";
                                queriesToRun.Add(query);
                                query = "";
                                numberOfTablesInQuery = 1;
                            }
                            else
                            {
                                query = query + "SELECT PageID, ModifiedOn, Updates, URL, HTML, URL_LENGTH, HTML_LENGTH, URL_B64S_LENGTH, URL_B64S, HTML_B64S_LENGTH, HTML_B64S " +
                                    "FROM " + tableName + " WHERE PageID = " + PageID;
                                query = query + " UNION ALL ";
                            }
                            numberOfTablesInQuery++;
                        }
                    }
                    query = query.Substring(0, query.Length - 10);
                    query = query + ";";
                    queriesToRun.Add(query);

                    Tuple<UInt64, UInt64>[] collation = new Tuple<UInt64, UInt64>[queriesToRun.Count];

                    CheckSchemaType();
                    if (GetSchemaType() == Dynamics.DatabaseSchema.WithIndexes ||
                    GetSchemaType() == Dynamics.DatabaseSchema.WithoutIndexes)
                    {
                        for (int i = 0; i < queriesToRun.Count; i++)
                        {
                            HanaCommand command = new HanaCommand(queriesToRun[i], connection, transaction);
                            command.CommandTimeout = 600;
                            using (HanaDataReader dataReader = command.ExecuteReader())
                            {
                                while (dataReader.Read())
                                {
                                    while (rowDataprocessed < bytesToProcess)
                                    {
                                        for (int index = 0; index < dataReader.FieldCount; index++)
                                        {
                                            string field = "";
                                            var type = dataReader[index].GetType();
                                            if (type.Name == "Byte[]")
                                            {
                                                byte[] byteCodes = (byte[])dataReader.GetValue(index);
                                                field = Encoding.UTF8.GetString(byteCodes, 0, byteCodes.Length);
                                            }
                                            else
                                            {
                                                field = dataReader[index].ToString();
                                            }
                                            rowDataprocessed += Convert.ToUInt64(field.Length);
                                        }
                                        rowsProcessed++;
                                    }
                                    i = queriesToRun.Count;
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        ParallelOptions po = new ParallelOptions();
                        po.MaxDegreeOfParallelism = 8;
                        Parallel.For(0, queriesToRun.Count, po, i =>
                        {
                            UInt64 rowDataprocessedInParallel = 0;
                            UInt64 rowsProcessedInParalell = 0;
                            HanaCommand command = new HanaCommand(queriesToRun[i], connection, transaction);
                            command.CommandTimeout = 600;
                            using (HanaDataReader dataReader = command.ExecuteReader())
                            {
                                while (dataReader.Read())
                                {
                                    for (int index = 0; index < dataReader.FieldCount; index++)
                                    {
                                        string field = "";
                                        var type = dataReader[index].GetType();
                                        if (type.Name == "Byte[]")
                                        {
                                            byte[] byteCodes = (byte[])dataReader.GetValue(index);
                                            field = Encoding.UTF8.GetString(byteCodes, 0, byteCodes.Length);
                                        }
                                        else
                                        {
                                            field = dataReader[index].ToString();
                                        }
                                        rowDataprocessedInParallel += Convert.ToUInt64(field.Length);
                                    }
                                    rowsProcessedInParalell++;
                                }
                            }
                            Tuple<ulong, ulong> collationInParalell = Tuple.Create(rowDataprocessedInParallel, rowsProcessedInParalell);
                            collation[i] = collationInParalell;
                        });

                        foreach (Tuple<ulong, ulong> collator in collation)
                        {
                            rowDataprocessed += collator.Item1;
                            rowsProcessed += collator.Item2;
                        }
                    }


                    string queryHeaderData = "SELECT HeaderID, PageID, HeaderKeyLength, Headerkey, HeaderValueLength, HeaderValue " +
                                             "FROM WebPageHeaders " +
                                             "WHERE PageID = " + PageID;
                    HanaCommand HeaderDataValues = new HanaCommand(queryHeaderData, connection, transaction);
                    HeaderDataValues.CommandTimeout = 300;
                    using (HanaDataReader dataReader = HeaderDataValues.ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            for (int index = 0; index < dataReader.FieldCount; index++)
                            {
                                string field = dataReader[index].ToString();
                                rowDataprocessed += Convert.ToUInt64(field.Length);
                            }
                            rowsProcessed++;
                        }
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return Tuple.Create(rowDataprocessed, rowsProcessed);
        }

        /// <summary>
        /// Returns the table amplification value used in this database. 
        /// </summary>
        public override int TableAmplifier { get { return _amplifier; } set { _amplifier = value; } }

        /// <summary>
        /// Set and return the managed token to cancel operations if specific things go wrong with the database. 
        /// </summary>
        public override CancellationTokenSource TokenSource { get { return _tokenSource; } set { _tokenSource = value; } }

        /// <summary>
        /// Updates the contents of a characterised web page. 
        /// </summary>
        public override int UpdateCharacterisedWebPage(int PageID)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (HanaConnection connection = GetConnection())
            {
                try 
                { 
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    if (_amplifier != 0)
                    {
                        //Contains all 4 encoded table sets 
                        int numberOfEncoders = _mechanic.ReturnWebPageEncodingtables.Length;
                        HanaCommand[][][] allEncodedWebPagesCommands = new HanaCommand[numberOfEncoders][][];
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            //contains all the tables for the subset of the encoding up to the amplifier 
                            allEncodedWebPagesCommands[i] = new HanaCommand[_amplifier][];
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                int totalRowsForPage = 0;
                                HanaCommand rowCountCommand = new HanaCommand("SELECT COUNT(*) FROM " + TableName + " WHERE PageID = ?", connection, transaction);
                                rowCountCommand.CommandTimeout = 12000;
                                rowCountCommand.Parameters.AddWithValue("@PageID", PageID);
                                using (HanaDataReader rowcommandReader = rowCountCommand.ExecuteReader())
                                {
                                    while (rowcommandReader.Read())
                                    {
                                        totalRowsForPage = Convert.ToInt32(rowcommandReader[0]);
                                    }
                                }
                                HanaCommand readCommand = new HanaCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                TableName + " WHERE PageID = ?", connection, transaction);
                                readCommand.CommandTimeout = 12000;
                                readCommand.Parameters.AddWithValue("@PageID", PageID);
                                string URL = string.Empty;
                                string HTML = string.Empty;
                                string URL_B64S = string.Empty;
                                string HTML_B64S = string.Empty;
                                using (HanaDataReader commandReader = readCommand.ExecuteReader())
                                {
                                    HanaCommand[] commandStruct = new HanaCommand[totalRowsForPage];
                                    int rowCounter = 0;
                                    while (commandReader.Read())
                                    {
                                        URL = commandReader[0].ToString();
                                        HTML = commandReader[1].ToString();
                                        URL_B64S = commandReader[2].ToString();
                                        HTML_B64S = commandReader[3].ToString();
                                        string URLReset = _mechanic.Encrypt(URL);
                                        string HTMLReset = _mechanic.Encrypt(HTML);
                                        string URL_B64SReset = _mechanic.Encrypt(URL_B64S);
                                        string HTML_B64SReset = _mechanic.Encrypt(HTML_B64S);

                                        HanaCommand command = new HanaCommand("UPDATE " + TableName + " SET URL = ?, " +
                                            "HTML = ?, URL_B64S = ?," +
                                            " HTML_B64S = ?, ModifiedOn = ? WHERE PageID = ? AND URL = ? AND HTML = ? " +
                                            " AND URL_B64S = ? AND HTML_B64S = ?", connection, transaction);
                                        string commtest = command.CommandText;
                                        HanaParameter parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.VarChar;
                                        parm.ParameterName = "URLReset";
                                        parm.Value = URLReset;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.VarChar;
                                        parm.ParameterName = "HTMLReset";
                                        parm.Value = HTMLReset;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.VarChar;
                                        parm.ParameterName = "URL_B64SReset";
                                        parm.Value = URL_B64SReset;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.VarChar;
                                        parm.ParameterName = "HTML_B64SReset";
                                        parm.Value = HTML_B64SReset;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.TimeStamp;
                                        parm.ParameterName = "timestamp";
                                        parm.Value = timestamp;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.Integer;
                                        parm.ParameterName = "PageID";
                                        parm.Value = PageID;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.VarChar;
                                        parm.ParameterName = "URL";
                                        parm.Value = URL;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.VarChar;
                                        parm.ParameterName = "HTML";
                                        parm.Value = HTML;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.VarChar;
                                        parm.ParameterName = "URL_B64S";
                                        parm.Value = URL_B64S;
                                        command.Parameters.Add(parm);
                                        parm = new HanaParameter();
                                        parm.HanaDbType = HanaDbType.VarChar;
                                        parm.ParameterName = "HTML_B64S";
                                        parm.Value = HTML_B64S;
                                        command.Parameters.Add(parm);
                                        command.CommandTimeout = 300;
                                        commandStruct[rowCounter] = command;
                                        rowCounter++;
                                    }
                                    allEncodedWebPagesCommands[i][tableID] = commandStruct;
                                } 
                            }
                        }
                        ConcurrentBag<int> numberOfParalellRowsAffected = new ConcurrentBag<int>();
                        Parallel.For(0, allEncodedWebPagesCommands.Length, encoder =>
                        {
                            //second is the number of tables per encoder 
                            Parallel.For(0, allEncodedWebPagesCommands[encoder].Length, tableid =>
                            {
                                //third are the actual tables - execute here
                                for (int commandIndex = 0; commandIndex < allEncodedWebPagesCommands[encoder][tableid].Length; commandIndex++)
                                {
                                    try
                                    {
                                        int numberOfAffectedRows = allEncodedWebPagesCommands[encoder][tableid][commandIndex].ExecuteNonQuery();
                                        numberOfParalellRowsAffected.Add(numberOfAffectedRows);
                                    }
                                    catch (HanaException ex)
                                    {
                                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                                    }
                                }
                            });
                            //the goal is to get each individual table running a query at the same time !
                            //with a table amplification of 8 the numbers of tables should be 36
                        });

                        foreach (int rowCount in numberOfParalellRowsAffected)
                        {
                            rowsAffected += rowCount;
                        }

                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        HanaCommand command = new HanaCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
                            " ModifiedOn = ? WHERE PageID = ?", connection, transaction);
                        command.Parameters.AddWithValue("@ModifiedOn", timestamp);
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.CommandTimeout = 300;
                        rowsAffected += command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                    rowsAffected = 0;
                }
            }
            return rowsAffected;
        }

        /// <summary>
        /// Update a characterised web page with a new web page. 
        /// </summary>
        public override int UpdateCharacterisedWebPageInPlace(int PageID, WebPage newPage)
        {
            int numberOfRowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    DateTime modificationDate = DateTime.Now;
                    HanaCommand command = new HanaCommand(
                    "UPDATE WebPages SET LoadedOn = ?, HeadersLength = ?, StatsLength =  ?," +
                    " TotalLength = ?, HREFs = ?, HashURL = ?, HashHTML = ?, URL = ? , HTML = ?" +
                    " WHERE PageID = ?",
                    connection,
                    transaction);
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                                // Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(newPage.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(newPage.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(newPage.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(newPage.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(newPage.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML128", newPage.HashHTML); //                             Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", newPage.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", newPage.HTMLBinary); //                                     The fist 128 characters of HTML  
                    command.Parameters.AddWithValue("@PageID", PageID);
                    command.CommandTimeout = 300;
                    numberOfRowsAffected += command.ExecuteNonQuery();



                    //Contains all 4 encoded table sets 
                    int numberOfEncoders = _mechanic.ReturnWebPageEncodingtables.Length;
                    HanaCommand[][][] allEncodedWebPagesCommands = new HanaCommand[numberOfEncoders][][];
                    for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                    {
                        //contains all the tables for the subset of the encoding up to the amplifier 
                        allEncodedWebPagesCommands[i] = new HanaCommand[_amplifier][];
                        for (int tableID = 0; tableID < _amplifier; tableID++)
                        {
                            string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                            string tableName = _mechanic.NameWithID(encodingTables[i], tableID);

                            command = new HanaCommand("DELETE FROM " + tableName + " WHERE PageID = " + PageID, connection, transaction);
                            command.ExecuteNonQuery();

                            int htmlIndex = tableID * 8 + i + 4;
                            int urlIndex = tableID * 8 + i;
                            int b64SHTMLIndex = tableID * 8 + i + 4;
                            int b64SURLIndex = tableID * 8 + i;

                            string convertedHTML = Encoding.ASCII.GetString(newPage.DataAsBytes[htmlIndex], 0, newPage.DataAsBytes[htmlIndex].Length);
                            string convertedURL = Encoding.ASCII.GetString(newPage.DataAsBytes[urlIndex], 0, newPage.DataAsBytes[urlIndex].Length);
                            string convertedB64SHTML = Encoding.ASCII.GetString(newPage.DataAsBS64s[b64SHTMLIndex]);
                            string convertedB64SURL = Encoding.ASCII.GetString(newPage.DataAsBS64s[b64SURLIndex]);


                            //split the string if it is larger than 5000

                            List<string> splitHTMLColumns = _mechanic.ChunkString(convertedHTML, 5000);
                            List<string> splitURLColumns = _mechanic.ChunkString(convertedURL, 5000);
                            List<string> splitB64SHTMLColumns = _mechanic.ChunkString(convertedB64SHTML, 5000);
                            List<string> splitB64SURLColumns = _mechanic.ChunkString(convertedB64SURL, 5000);

                            List<Tuple<string, string, string>> webPageCharacterSplit = new List<Tuple<string, string, string>>();
                            //get the longest length object

                            int splitHTMLColumnLength = splitHTMLColumns.Count;
                            int splitURLColumnLength = splitURLColumns.Count;
                            int splitB64SHTMLColumnLength = splitB64SHTMLColumns.Count;
                            int splitB64SURLColumnLength = splitB64SURLColumns.Count;

                            int maxValue = new[] { splitHTMLColumnLength, splitURLColumnLength,
                                splitB64SHTMLColumnLength, splitB64SURLColumnLength }.Max();
                            HanaCommand[] commandStruct = new HanaCommand[maxValue];
                            for (int ci = 0; ci < maxValue; ci++)
                            {
                                string tURL = "";
                                string tHTML = "";
                                string tB64SHTML = "";
                                string tB64SURL = "";
                                int urlLength = 0;
                                int htmlLength = 0;
                                int urlB64SLength = 0;
                                int htmlB64SLength = 0;
                                if (splitURLColumns.Count > ci)
                                {
                                    tURL = splitURLColumns[ci];
                                    urlLength = tURL.Length;
                                }

                                if (splitHTMLColumns.Count > ci)
                                {
                                    tHTML = splitHTMLColumns[ci];
                                    htmlLength = tHTML.Length;
                                }

                                if (splitB64SHTMLColumns.Count > ci)
                                {
                                    tB64SHTML = splitB64SHTMLColumns[ci];
                                    htmlB64SLength = tB64SHTML.Length;
                                }

                                if (splitB64SURLColumns.Count > ci)
                                {
                                    tB64SURL = splitB64SURLColumns[ci];
                                    urlB64SLength = tB64SURL.Length;
                                }

                                command = new HanaCommand(
                                "INSERT INTO " + tableName + " " +
                                "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                "          VALUES (?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?) ",
                                connection,
                                transaction);

                                command.Parameters.AddWithValue("@PageID", PageID); //                                                   Integer (int)                                           //                                                   Columns ModifiedOn and Updates get default values
                                command.Parameters.AddWithValue("@URL_Length", urlLength); //                                                 Integer (int)
                                command.Parameters.AddWithValue("@URL", tURL); //                                                             Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_Length", htmlLength); //                                               Integer (int)
                                command.Parameters.AddWithValue("@HTML", tHTML); //                                                           Byte Array (binary)
                                command.Parameters.AddWithValue("@URL_B64S_Length", urlB64SLength); //                                        Integer (int)
                                command.Parameters.AddWithValue("@URL_B64S", tB64SURL); //                                                        Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_B64S_Length", htmlB64SLength); //                                      Integer (int)
                                command.Parameters.AddWithValue("@HTML_B64S", tB64SHTML); //                                                  Byte Array (binary)
                                command.CommandTimeout = 600;
                                commandStruct[ci] = command;
                            }
                            allEncodedWebPagesCommands[i][tableID] = commandStruct;
                        }
                    }

                    //first level are the 4 encoders
                    ConcurrentBag<int> numberOfParalellRowsAffected = new ConcurrentBag<int>();
                    Parallel.For(0, allEncodedWebPagesCommands.Length, encoder =>
                    {
                        //second is the number of tables per encoder 
                        Parallel.For(0, allEncodedWebPagesCommands[encoder].Length, tableid =>
                        {
                            //third are the actual tables - execute here
                            for (int commandIndex = 0; commandIndex < allEncodedWebPagesCommands[encoder][tableid].Length; commandIndex++)
                            {
                                try
                                {
                                    int numberOfAffectedRows = allEncodedWebPagesCommands[encoder][tableid][commandIndex].ExecuteNonQuery();
                                    numberOfParalellRowsAffected.Add(numberOfAffectedRows);
                                }
                                catch (HanaException ex)
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                                }
                            }
                        });
                        //the goal is to get each individual table running a query at the same time !
                        //with a table amplification of 8 the numbers of tables should be 36
                    });

                    foreach (int rowCount in numberOfParalellRowsAffected)
                    {
                        numberOfRowsAffected += rowCount;
                    }

                    //delete all existing keys
                    command = new HanaCommand("DELETE FROM WebPageHeaders WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        HanaCommand headerIDCommand = new HanaCommand("SELECT Seq_HeaderId.NEXTVAL FROM DUMMY", connection, transaction);
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new HanaCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (?, ?, ?,  ?,  ?,  ?)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@HeaderID", headerID);
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new HanaCommand("DELETE FROM WebPageStats WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                    {
                        HanaCommand statIDCommand = new HanaCommand("SELECT Seq_StatId.NEXTVAL FROM DUMMY", connection, transaction);
                        int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                        command = new HanaCommand(
                            "INSERT INTO WebPageStats (StatID ,PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                      "VALUES (?, ?,  ?,  ?,  ?,  ?,  ?)",
                            connection,
                            transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@StatID", statID);
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.Parameters.AddWithValue("@CodeType", codeType);
                        command.Parameters.AddWithValue("@Length", newPage.StatsAsBytes[codeType].Length);
                        command.Parameters.AddWithValue("@Stats", newPage.StatsAsBytes[codeType]);
                        command.Parameters.AddWithValue("@B64S_Length", newPage.StatsAsBS64s[codeType].Length);
                        command.Parameters.AddWithValue("@B64S_Stats", newPage.StatsAsBS64s[codeType]);
                        command.CommandTimeout = 300;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        HanaCommand updateCommand = new HanaCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
                            " ModifiedOn = ? WHERE PageID = ?", connection, transaction);
                        updateCommand.Parameters.AddWithValue("@ModifiedOn", timestamp);
                        updateCommand.Parameters.AddWithValue("@PageID", PageID);
                        updateCommand.CommandTimeout = 300;
                        numberOfRowsAffected += updateCommand.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if(ex.Message.ToLower().Contains("authentication failed")
                         || ex.Message.ToLower().Contains("not connected")
                         || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                    numberOfRowsAffected = 0;
                }
            }
            return numberOfRowsAffected;
        }

        /// <summary>
        /// Updated the contents of an encoded web page. 
        /// </summary>
        public override int UpdateEncodedWebPage(int PageID)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                HanaCommand readCommand = new HanaCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                    TableName + " WHERE PageID = ?", connection, transaction);
                                readCommand.CommandTimeout = 12000;
                                readCommand.Parameters.AddWithValue("@PageID", PageID);
                                string URL = string.Empty;
                                string HTML = string.Empty;
                                string URL_B64S = string.Empty;
                                string HTML_B64S = string.Empty;
                                using (HanaDataReader commandReader = readCommand.ExecuteReader())
                                {
                                    while (commandReader.Read())
                                    {
                                        URL = commandReader[0].ToString();
                                        HTML = commandReader[1].ToString();
                                        URL_B64S = commandReader[2].ToString();
                                        HTML_B64S = commandReader[3].ToString();
                                    }
                                }

                                URL = _mechanic.Encrypt(URL);
                                HTML = _mechanic.Encrypt(HTML);
                                URL_B64S = _mechanic.Encrypt(URL_B64S);
                                HTML_B64S = _mechanic.Encrypt(HTML_B64S);

                                byte[] URLEncoded;
                                byte[] HTMLEncoded;
                                byte[] URL_B64SEncoded;
                                byte[] HTML_B64S_Encoded;
                                Encoding encoder = Encoding.ASCII;
                                if (TableName.Contains("Unicode"))
                                {
                                    encoder = Encoding.Unicode;
                                }
                                else if (TableName.Contains("ASCII"))
                                {
                                    encoder = Encoding.ASCII;
                                }
                                else if (TableName.Contains("EBCDIC"))
                                {
                                    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                                    encoder = Encoding.GetEncoding(37);
                                }
                                else if (TableName.Contains("UTF32"))
                                {
                                    encoder = Encoding.UTF32;
                                }
                                URLEncoded = encoder.GetBytes(URL);
                                HTMLEncoded = encoder.GetBytes(HTML);
                                URL_B64SEncoded = Encoding.ASCII.GetBytes(Convert.ToBase64String(URLEncoded));
                                HTML_B64S_Encoded = Encoding.ASCII.GetBytes(Convert.ToBase64String(HTMLEncoded));

                                HanaCommand command = new HanaCommand("UPDATE " + TableName + " SET URL = ?, " +
                                    "HTML = ?, URL_B64S = ?," +
                                    " HTML_B64S = ? WHERE PageID = ?", connection, transaction);
                                command.Parameters.AddWithValue("@URL", URLEncoded);
                                command.Parameters.AddWithValue("@HTML", HTMLEncoded);
                                command.Parameters.AddWithValue("@URL_B64S", URL_B64SEncoded);
                                command.Parameters.AddWithValue("@HTML_B64S", HTML_B64S_Encoded);
                                command.Parameters.AddWithValue("@PageID", PageID);
                                command.CommandTimeout = 300;
                                rowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        HanaCommand command = new HanaCommand("UPDATE " + TableName + " SET " +
                            " ModifiedOn = ? WHERE PageID = ?", connection, transaction);
                        command.Parameters.AddWithValue("@ModifiedOn", timestamp);
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.CommandTimeout = 300;
                        rowsAffected += command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                    rowsAffected = 0;
                }
            }
            return rowsAffected;
        }

        /// <summary>
        /// Update the encoded web page in the database with a new web page. 
        /// </summary>
        public override int UpdateEncodedWebPageInPlace(int PageID, WebPage newPage)
        {
            int numberOfRowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    HanaTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    HanaCommand command = new HanaCommand(
                    "UPDATE WebPages SET LoadedOn = ?, HeadersLength = ?, StatsLength =  ?," +
                    " TotalLength = ?, HREFs = ?, HashURL = ?, HashHTML = ?, URL = ? , HTML = ?" +
                    " WHERE PageID = ?",
                    connection,
                    transaction);
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                                // Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(newPage.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(newPage.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(newPage.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(newPage.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(newPage.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML", newPage.HashHTML); //                             Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", newPage.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", newPage.HTMLBinary); //                                     The fist 128 characters of HTML  
                    command.Parameters.AddWithValue("@PageID", PageID);
                    command.CommandTimeout = 300;
                    numberOfRowsAffected += command.ExecuteNonQuery();

                    for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                    {
                        for (int tableID = 0; tableID < _amplifier; tableID++)
                        {
                            string tableName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                            command = new HanaCommand(
                            "UPDATE " + tableName + " " +
                            " SET URL_Length = ?, URL = ?, HTML_Length = ?, HTML = ?, URL_B64S_Length = ?," +
                            " URL_B64S = ?, HTML_B64S_Length = ?, HTML_B64S = ? " +
                            " WHERE PageID = ?",
                            connection,
                            transaction);
                            command.Parameters.AddWithValue("@URL_Length", newPage.DataAsBytes[tableID * 8 + i].Length); //                  Integer (int)
                            command.Parameters.AddWithValue("@URL", newPage.DataAsBytes[tableID * 8 + i]); //                                Byte Array (binary)
                            command.Parameters.AddWithValue("@HTML_Length", newPage.DataAsBytes[tableID * 8 + i + 4].Length); //             Integer (int)
                            command.Parameters.AddWithValue("@HTML", newPage.DataAsBytes[tableID * 8 + i + 4]); //                           Byte Array (binary)
                            command.Parameters.AddWithValue("@URL_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i].Length); //             Integer (int)
                            command.Parameters.AddWithValue("@URL_B64S", newPage.DataAsBS64s[tableID * 8 + i]); //                           Byte Array (binary)
                            command.Parameters.AddWithValue("@HTML_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i + 4].Length); //        Integer (int)
                            command.Parameters.AddWithValue("@HTML_B64S", newPage.DataAsBS64s[tableID * 8 + i + 4]); //                      Byte Array (binary)
                            command.Parameters.AddWithValue("@PageID", PageID);
                            command.CommandTimeout = 600;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }

                   
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        HanaCommand updateCommand = new HanaCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
                            " ModifiedOn = ? WHERE PageID = ?", connection, transaction);
                        updateCommand.Parameters.AddWithValue("@ModifiedOn", timestamp);
                        updateCommand.Parameters.AddWithValue("@PageID", PageID);
                        updateCommand.CommandTimeout = 300;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    //delete all existing keys
                    transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    command = new HanaCommand("DELETE FROM WebPageHeaders WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        HanaCommand headerIDCommand = new HanaCommand("SELECT Seq_HeaderId.NEXTVAL FROM DUMMY", connection, transaction);
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new HanaCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (?, ?, ?,  ?,  ?,  ?)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@HeaderID", headerID);
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new HanaCommand("DELETE FROM WebPageStats WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                    {
                        HanaCommand statIDCommand = new HanaCommand("SELECT Seq_StatId.NEXTVAL FROM DUMMY", connection, transaction);
                        int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                        command = new HanaCommand(
                            "INSERT INTO WebPageStats (StatID ,PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                      "VALUES (?, ?,  ?,  ?,  ?,  ?,  ?)",
                            connection,
                            transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@StatID", statID);
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.Parameters.AddWithValue("@CodeType", codeType);
                        command.Parameters.AddWithValue("@Length", newPage.StatsAsBytes[codeType].Length);
                        command.Parameters.AddWithValue("@Stats", newPage.StatsAsBytes[codeType]);
                        command.Parameters.AddWithValue("@B64S_Length", newPage.StatsAsBS64s[codeType].Length);
                        command.Parameters.AddWithValue("@B64S_Stats", newPage.StatsAsBS64s[codeType]);
                        command.CommandTimeout = 300;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        command = new HanaCommand("UPDATE " + TableName + " SET " +
                            " ModifiedOn = ? WHERE PageID = ?", connection, transaction);
                        command.Parameters.AddWithValue("@ModifiedOn", timestamp);
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.CommandTimeout = 300;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if(ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                    {
                        _tokenSource.Cancel();
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                    numberOfRowsAffected = 0;
                }
            }
            return numberOfRowsAffected;
        }

        /// <summary>
        /// Update the import history for the data file records in the database. 
        /// </summary>
        public override bool UpdateImportHistory(ImportHandler import)
        {
            bool success = false;
            try
            {
                using (HanaConnection connection = GetConnection())
                {
                    connection.Open();
                    HanaCommand command = new HanaCommand("UPDATE Imports SET " +
                    "ImportedWebPages = '" + import.ImportedWebPages + "', " +
                    "ImportedAllWebPages = '" + Convert.ToInt32(import.AllImportedWebPages) + "' " +
                    "WHERE FileName = '" + import.Filename + "'",
                    connection);
                    command.CommandTimeout = 600;
                    success = Convert.ToBoolean(command.ExecuteNonQuery());
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("authentication failed")
                        || ex.Message.ToLower().Contains("not connected")
                        || ex.Message.ToLower().Contains("connection failed"))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return success;
        }

        /// <summary>
        /// Perfom the advanced operations for SAP HANA. 
        /// </summary>
        public override void VendorAdvancedOperations(int numberOfThreads)
        {
            DeltaMergeColumnTables();
        }

        /// <summary>
        /// Perfom the complex operations for SAP HANA. 
        /// </summary>
        public override void VendorComplexOperations()
        {
            UnloadTables(30);
        }

        /// <summary>
        /// Delta merge the column tables. 
        /// </summary>
        public void DeltaMergeColumnTables()
        {
            //Delta Merge column tables
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    foreach (string tablename in _mechanic.AllWebPageDataColumnTables(_amplifier, GetNumberOfColumnTables()))
                    {

                        string commandText = "MERGE DELTA OF " + tablename + " WITH PARAMETERS('FORCED_MERGE' = 'ON')";
                        HanaCommand command = new HanaCommand(commandText, connection);
                        _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                        command.ExecuteNonQuery();
                    }
                    connection.Close();
                }
                catch (HanaException ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
        }

        /// <summary>
        /// Perform the SAP HANA Consistency check. 
        /// </summary>
        public override void VendorConsistencyCheck()
        {
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    connection.InfoMessage += delegate (object sender, HanaInfoMessageEventArgs e)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, e.Message, Message.MessageType.Info));
                };
               
                    string commandText = "CALL CHECK_TABLE_CONSISTENCY('CHECK', NULL, NULL);";
                    HanaCommand command = new HanaCommand(commandText, connection);
                    command.CommandTimeout = 600000;
                    _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                    HanaDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        string message = "Consistency check returned corruption : SCHEMA -  " + Convert.ToString(reader[0]) + " TABLE - " + Convert.ToString(reader[1]) +
                            " COLUMN - " + Convert.ToString(reader[2]) + " PARTITION" + Convert.ToString(reader[3]) +
                            " ERROR " + Convert.ToString(reader[4]) + " : " + Convert.ToString(reader[5]) +
                            " DETAILS - " + Convert.ToString(reader[8]);
                        _messageQueue.AddMessage(new Message(DateTime.Now, message, Message.MessageType.Info));
                    }
                }
                catch (HanaException ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
                connection.Close();
            }
        }

        /// <summary>
        /// Set the Base Tables to be page loadable. 
        /// </summary>
        public void SetBaseTablesPageLoadable()
        {
            using (HanaConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    foreach (string table in _mechanic.AllBaseTables())
                    {
                        string setPagedTableCommand = "ALTER TABLE " + table + " PAGE LOADABLE ;";
                        HanaCommand command = new HanaCommand(setPagedTableCommand, connection);
                        command.ExecuteNonQuery();
                    }
                    connection.Close();
                }
                catch (HanaException ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
        }

        /// <summary>
        /// Set a table to be used by an extenion node. 
        /// </summary>
        public void SetTableForExtensionNode(int percentageToSet, string extensionNodeGroupName)
        {
            if (percentageToSet != 0)
            {
                try
                {
                    int adjustedDBFormat = Convert.ToInt32((double)_amplifier * (double)((double)percentageToSet / 100));
                    using (HanaConnection connection = GetConnection())
                    {
                        connection.Open();
                        HanaCommand command;
                        foreach (string tablename in _mechanic.AllWebPageEncodingTablesReversed(adjustedDBFormat, _amplifier))
                        {

                            string commandText = "ALTER TABLE " + tablename + " SET GROUP TYPE \"" + extensionNodeGroupName + "\"";
                            command = new HanaCommand(commandText, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                            command.CommandTimeout = 6000;
                            command.ExecuteNonQuery();
                        }
                        string reorgText = "CALL REORG_GENERATE(6, 'GROUP_TYPE=>" + extensionNodeGroupName + "');";
                        string execText = "CALL REORG_EXECUTE(?);";
                        command = new HanaCommand(reorgText, connection);
                        command.ExecuteNonQuery();
                        command = new HanaCommand(execText, connection);
                        command.CommandTimeout = 60000;
                        DateTime start = DateTime.Now;
                        command.ExecuteNonQuery();
                        connection.Close();
                        DateTime end = DateTime.Now;
                        TimeSpan duration = end - start;
                        _messageQueue.AddMessage(new Message(DateTime.Now, "Table Redistribution to extension node has taken " + duration.TotalSeconds, Message.MessageType.Error));

                    }
                }
                catch (HanaException ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }

            }
        }

        /// <summary>
        /// Set the preload attribute on tables. 
        /// </summary>
        public void SetPreloadAttributeOnTables(int percentageToSet)
        {
            if (percentageToSet != 0)
            {
                int adjustedDBFormat = Convert.ToInt32((double)_amplifier * (double)((double)percentageToSet / 100));
                using (HanaConnection connection = GetConnection())
                {
                    try
                    {
                        connection.Open();
                        foreach (string tablename in _mechanic.AllWebPageEncodingTablesReversed(adjustedDBFormat, _amplifier))
                        {

                            string commandText = "ALTER TABLE " + tablename + " PRELOAD ALL";
                            HanaCommand command = new HanaCommand(commandText, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                            command.CommandTimeout = 6000;
                            command.ExecuteNonQuery();
                        }
                        connection.Close();
                    }
                    catch (HanaException ex)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                }
            }
        }

        /// <summary>
        /// Set the column loadable attribute on tables. 
        /// </summary>
        public void SetColumnLoadableAttribute(int percentageToSet)
        {
            if (percentageToSet != 0)
            {
                int adjustedDBFormat = Convert.ToInt32((double)_amplifier * (double)((double)percentageToSet / 100));
                using (HanaConnection connection = GetConnection())
                {
                    try
                    {
                        connection.Open();
                        foreach (string tablename in _mechanic.AllWebPageEncodingTablesReversed(adjustedDBFormat, _amplifier))
                        {

                            string commandText = "ALTER TABLE " + tablename + " COLUMN LOADABLE";
                            HanaCommand command = new HanaCommand(commandText, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                            command.CommandTimeout = 6000;
                            command.ExecuteNonQuery();
                        }
                        connection.Close();
                    }
                    catch (HanaException ex)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                }
            }
        }

        /// <summary>
        /// Set the page loadable attribute on tables. 
        /// </summary>
        public void SetPagedAttributeOnTables(int percentageToSet)
        {
            if (percentageToSet != 0)
            {
                int adjustedDBFormat = Convert.ToInt32((double)_amplifier * (double)((double)percentageToSet / 100));
                using (HanaConnection connection = GetConnection())
                {
                    try
                    {
                        connection.Open();
                        foreach (string tablename in _mechanic.AllWebPageEncodingTablesReversed(adjustedDBFormat, _amplifier))
                        {

                            string commandText = "ALTER TABLE " + tablename + " PAGE LOADABLE";
                            HanaCommand command = new HanaCommand(commandText, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                            command.CommandTimeout = 6000;
                            command.ExecuteNonQuery();
                        }
                        connection.Close();
                    }
                    catch (HanaException ex)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }                   
                }
            }
        }

        /// <summary>
        /// Unloads the SAP HANA tables.
        /// </summary>
        public void UnloadTables(int percentageToUnload)
        {
            if (percentageToUnload != 0)
            {
                int adjustedDBFormat = Convert.ToInt32((double)_amplifier * (double)((double)percentageToUnload / 100));
                using (HanaConnection connection = GetConnection())
                {
                    try
                    {
                        connection.Open();
                        foreach (string tablename in _mechanic.AllWebPageEncodingTablesReversed(adjustedDBFormat, _amplifier))
                        {

                            string commandText = "UNLOAD " + tablename;
                            HanaCommand command = new HanaCommand(commandText, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                            command.CommandTimeout = 6000;
                            command.ExecuteNonQuery();
                        }
                        connection.Close();
                    }
                    catch (HanaException ex)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }                    
                }
            }
        }

        /// <summary>
        /// Returns the Number of Column tables. 
        /// </summary>
        private int GetNumberOfColumnTables()
        {
            int numberOfColumnTables = (int)Math.Round((double)(_amplifier * _percentColumnTables) / 100, 0);
            return numberOfColumnTables;
        }
        /// <summary>
        /// Returns the number of page loadable tables.
        /// </summary>

        private int GetNumberOfPagedTables()
        {
            int numberOfPagedTables = (int)Math.Round((double)(GetNumberOfColumnTables() * _percentagePagedTables) / 100, 0);
            return numberOfPagedTables;
        }

        /// <summary>
        /// Returns the number of extenion node tables. 
        /// </summary>
        private int GetNumberOfExtensionNodeTables()
        {
            int numberOfExtensionNodeTables = (int)Math.Round((double)(_amplifier * _percentageWarmExtensionNodeTables) / 100, 0);
            return numberOfExtensionNodeTables;
        }       
    }
}
