using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DOES.Shared.Debug;
using DOES.Shared.Resources;
using DOES.DataEngine.FileOperations;
using DOES.DataEngine.Resources;
using Oracle.ManagedDataAccess.Client;

namespace DOES.DataEngine.Operations
{
    /// <summary>
    /// This class handles Oracle database operations. 
    /// </summary>
    public class OracleDB : DataVendor
    {
        private string _hostname;
        private string _databaseName;
        private bool _useSID;
        private string _userName;
        private string _password;
        private int _amplifier;
        private int _port;
        private Dynamics.DatabaseSchema _schema;
        private Mechanic _mechanic;
        private MessageQueue _messageQueue;
        private CancellationTokenSource _tokenSource;

        /// <summary>
        /// Instantiates the OracleDB class. 
        /// </summary>
        public OracleDB(string hostname, string dbname, string username, string password, bool connectWithSID, int amplifier, 
            int port, Mechanic mechanic, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _useSID = connectWithSID;
            _amplifier = amplifier;
            _port = port;
            _messageQueue = messages;
            _mechanic = mechanic;
        }

        /// <summary>
        /// Instantiates the OracleDB class. 
        /// </summary>
        public OracleDB(string hostname, string dbname, string username, string password, bool connectWithSID, int port,
            Mechanic mechanic, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _useSID = connectWithSID;
            _port = port;
            _messageQueue = messages;
            _mechanic = mechanic;
        }

        /// <summary>
        /// Instantiates the OracleDB class. 
        /// </summary>
        public OracleDB(string hostname, string dbname, string username, string password, bool connectWithSID, int amplifier,
            int port, Dynamics.DatabaseSchema schema, Mechanic mechanic, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _useSID = connectWithSID;
            _amplifier = amplifier;
            _port = port;
            _schema = schema;
            _messageQueue = messages;
            _mechanic = mechanic;
        }

        /// <summary>
        /// Return the table amplifier for this database. 
        /// </summary>
        public override int TableAmplifier { get { return _amplifier; } set { _amplifier = value; } }

        /// <summary>
        /// Set and return the managed token to cancel operations if specific things go wrong with the database. 
        /// </summary>
        public override CancellationTokenSource TokenSource { get { return _tokenSource; } set { _tokenSource = value; } }

        /// <summary>
        /// Check the import history for a data file. 
        /// </summary>
        public override ImportHandler CheckFileImportHistory(string filename)
        {
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    OracleCommand command = new OracleCommand("SELECT ImportedWebPages, " +
                        "ImportedAllWebPages from Imports WHERE FileName = '" + filename + "'"
                        , connection);
                    command.CommandTimeout = 600;
                    connection.Open();
                    ImportHandler importData;
                    int importedWebPages = 0;
                    bool importedAllWebPages = false;
                    bool found = false;
                    using (OracleDataReader dataReader = command.ExecuteReader())
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
        /// Check the database schema type. 
        /// </summary>
        public override void CheckSchemaType()
        {
            string schemaResponse = string.Empty;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleCommand command = new OracleCommand("SELECT SchemaType FROM Configuration", connection);
                    using (OracleDataReader reader = command.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
        }

        /// <summary>
        /// Create an import history entry for the data file being used for operations. 
        /// </summary>
        public override bool CreateImportHistory(ImportHandler import)
        {
            bool success = false;
            try
            {
                using (OracleConnection connection = GetConnection())
                {
                    connection.Open();
                    OracleCommand command = new OracleCommand("INSERT INTO Imports (FileName, " +
                        "ImportedWebPages, ImportedAllWebPages) VALUES ('" +
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
                if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
            using (OracleConnection conn = GetConnection())
            {
                //Base Objects
                try
                {
                    conn.Open();
                    foreach (string table in _mechanic.AllBaseTables())
                    {
                        OracleCommand command = new OracleCommand("SELECT * FROM " + table + " WHERE rownum = 1", conn);
                        command.ExecuteScalar();
                    }
                    conn.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied") 
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname") 
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
                                string OracleCommandString = sb.ToString();
                                string SQLString = Regex.Replace(sb.ToString(), @"\n|\r|\t", "");
                                OracleCommand command = new OracleCommand(SQLString, conn);
                                try
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                                    command.ExecuteNonQuery();
                                    success = true;
                                }
                                catch (OracleException sqlex)
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
                        OracleCommand firstPointcommand = new OracleCommand("INSERT INTO Configuration (SchemaType, CreatedOn) VALUES" +
                            " (:SchemaType, :CreatedOn)", conn);

                        firstPointcommand.Parameters.Add(":SchemaType", _schema.ToString());
                        firstPointcommand.Parameters.Add(":CreatedOn", DateTime.Now);
                        firstPointcommand.ExecuteNonQuery();
                        conn.Close();
                        success = true;
                    }
                    catch (OracleException sql3)
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
                        OracleCommand command = new OracleCommand("SELECT * FROM " + table + " WHERE rownum = 1", conn);
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
                                string OracleCommandString = sb.ToString();
                                for (int TableID = 0; (TableID < Convert.ToInt32((OracleCommandString.Contains("_X00") ? _amplifier : 0))); TableID++)
                                {
                                    try
                                    {
                                        OracleCommand command = new OracleCommand(_mechanic.NameWithID(OracleCommandString, TableID), conn);
                                        _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                                        command.ExecuteNonQuery();
                                    }
                                    catch (OracleException sqle)
                                    {
                                        _messageQueue.AddMessage(new Message(DateTime.Now, sqle.Message, Message.MessageType.Info));
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
        /// Return the message queue associated with this class. 
        /// </summary>
        public override MessageQueue DebugQueue()
        {
            return _messageQueue;
        }

        /// <summary>
        /// Delete the web page from the database objects.   
        /// </summary>
        public override int DeleteWebPage(int PageID)
        {
            int rowsAffected = 0;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        OracleCommand command = new OracleCommand("DELETE FROM " + TableName + " WHERE PageID" +
                            " = '" + PageID.ToString() + "'", connection)
                        {
                            Transaction = transaction,
                            CommandTimeout = 300
                        };
                        rowsAffected += command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return rowsAffected;
        }

        /// <summary>
        /// Destroy the database objects. 
        /// </summary>
        public override bool DestroyObjects(Dynamics.ClearingType ClearingType)
        {
            bool success = false;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    string operation = "DROP";
                    if (ClearingType == Dynamics.ClearingType.Drop)
                    {
                        foreach (string seq in _mechanic.ReturnAllSequences)
                        {
                            try
                            {
                                string commandString = "DROP SEQUENCE " + seq;
                                OracleCommand command = new OracleCommand(commandString, connection);
                                _messageQueue.AddMessage(new Message(DateTime.Now, commandString, Message.MessageType.Command));
                                command.ExecuteNonQuery();
                            }
                            catch (OracleException oe)
                            {
                                success = false;
                                if (oe.Message.ToLower().Contains("ora-02289: sequence does not exist"))
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, oe.Message, Message.MessageType.Warning));
                                }
                                else
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, oe.Message, Message.MessageType.Error));
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
                            OracleCommand command = new OracleCommand(commandString, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, commandString, Message.MessageType.Command));
                            command.CommandTimeout = 600;
                            command.ExecuteNonQuery();
                        }
                        catch (OracleException ex)
                        {
                            success = false;
                            if (ex.Message.ToLower().Contains("ora-00942: table or view does not exist"))
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
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Return the oracle database connection object. 
        /// </summary>
        public override dynamic GetConnection()
        {
            string _connectionString;

            if(_useSID)
            {
                if (_userName == null && _password == null)
                {
                    _connectionString = "Data Source = (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = " + _hostname + ")(PORT = " + _port + ")))(CONNECT_DATA = " +
                        "(SID = " + _databaseName + ")));Integrated Security=SSPI;Max Pool Size=1000";
                }
                else
                {
                    _connectionString = "Data Source = (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = " + _hostname + ")(PORT = " + _port + ")))(CONNECT_DATA = " +
                        "(SID = " + _databaseName + ")));User Id=" + _userName + ";Password=" + _password + ";Max Pool Size=1000";
                }
            }
            else
            {
                if (_userName == null && _password == null)
                {
                    _connectionString = "Data Source = (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = " + _hostname + ")(PORT = " + _port + ")))(CONNECT_DATA = " +
                        "(SERVICE_NAME = " + _databaseName + ")));Integrated Security=SSPI;Max Pool Size=1000";
                }
                else
                {
                    _connectionString = "Data Source = (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = " + _hostname + ")(PORT = " + _port + ")))(CONNECT_DATA = " +
                        "(SERVICE_NAME = " + _databaseName + ")));User Id=" + _userName + ";Password=" + _password + ";Max Pool Size=1000";
                }
            }
            
            OracleConnection connection = new OracleConnection(_connectionString);
            return connection;
        }

        /// <summary>
        /// Return the database type. 
        /// </summary>
        public override Dynamics.Database GetDatabaseType()
        {
            return Dynamics.Database.Oracle;
        }

        /// <summary>
        /// Return the helper mechanic class used in this class. 
        /// </summary>
        public override Mechanic GetMechanic()
        {
            return _mechanic;
        }

        /// <summary>
        /// Rerturn a list of the database web page ID's and the total size for them. 
        /// </summary>
        public override SortedList<int, long> GetPageIDList()
        {
            SortedList<int, long> colPageIDToLength = new SortedList<int, long>();
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleCommand command = new OracleCommand("SELECT PageID, TotalLength from WebPages ORDER BY PageID", connection);
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    command.Transaction = transaction;
                    using (OracleDataReader reader = command.ExecuteReader())
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
        /// Return the database schema type. 
        /// </summary>
        public override Dynamics.DatabaseSchema GetSchemaType()
        {
            return _schema;
        }

        /// <summary>
        /// Return the web pages to operate on. 
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
                OracleConnection connection = GetConnection();
                OracleCommand commandRowCountNN = new OracleCommand("SELECT COUNT(*) FROM WebPages WHERE ModifiedOn IS NOT NULL", connection);
                OracleCommand commandRowCountTotal = new OracleCommand("SELECT COUNT(*) FROM WebPages", connection);
                OracleCommand commandTotalSize = new OracleCommand("SELECT SUM(TotalLength) FROM WebPages", connection);
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
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Insert a characterised web page into the database objects. 
        /// </summary>
        public override int InsertCharacterisedWebPage(WebPage page)
        {
            int numberOfRowsAffected = 0;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    OracleCommand pageIDCommand = new OracleCommand("SELECT Seq_PageID.NEXTVAL FROM DUAL", connection);
                    pageIDCommand.Transaction = transaction;
                    page.PageID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                    OracleCommand command = new OracleCommand(
                    "INSERT INTO WebPages (PageID, LoadedOn, HeadersLength, StatsLength, TotalLength, HREFs, HashURL, HashHTML, URL, HTML) " +
                                          "VALUES (:PageID, :LoadedOn, :HeadersLength, :StatsLength, :TotalLength, :HREFs, :HashURL, :HashHTML, :URL, :HTML)",
                    connection);
                    command.Transaction = transaction;
                    command.Parameters.Add(":PageID", page.PageID); //                                       PageID Generated Seperately
                    command.Parameters.Add(":LoadedOn", DateTime.Now); //                                    Current Date Time (DateTime2)
                                                                       //                           Columns ModifiedOn and Updates get default values
                    command.Parameters.Add(":HeadersLength", Convert.ToInt32(page.HeadersLength)); //        Integer (int)
                    command.Parameters.Add(":StatsLength", Convert.ToInt32(page.StatsLength)); //            Integer (int)
                    command.Parameters.Add(":TotalLength", Convert.ToInt32(page.TotalLength)); //            Integer (int)
                    command.Parameters.Add(":HREFs", Convert.ToInt32(page.HREFS)); //                        Integer (int)
                    command.Parameters.Add(":HashURL", Convert.ToInt32(page.HashURL)); //                    Integer (int)
                    command.Parameters.Add(":HashHTML", page.HashHTML); //                                   Byte Array (binary)
                    command.Parameters.Add(":URL", page.URL); //                                             The complete URL
                    command.Parameters.Add(":HTML", page.HTMLBinary); //                                     The HTML in encoded format  
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
                        OracleCommand[][][] allEncodedWebPagesCommands = new OracleCommand[numberOfEncoders][][];
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            //contains all the tables for the subset of the encoding up to the amplifier 
                            allEncodedWebPagesCommands[i] = new OracleCommand[_amplifier][];
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


                                //split the string if it is larger than 4000

                                List<string> splitHTMLColumns = _mechanic.ChunkString(convertedHTML, 4000);
                                List<string> splitURLColumns = _mechanic.ChunkString(convertedURL, 4000);
                                List<string> splitB64SHTMLColumns = _mechanic.ChunkString(convertedB64SHTML, 4000);
                                List<string> splitB64SURLColumns = _mechanic.ChunkString(convertedB64SURL, 4000);

                                List<Tuple<string, string, string>> webPageCharacterSplit = new List<Tuple<string, string, string>>();
                                //get the longest length object

                                int splitHTMLColumnLength = splitHTMLColumns.Count;
                                int splitURLColumnLength = splitURLColumns.Count;
                                int splitB64SHTMLColumnLength = splitB64SHTMLColumns.Count;
                                int splitB64SURLColumnLength = splitB64SURLColumns.Count;

                                int maxValue = new[] { splitHTMLColumnLength, splitURLColumnLength,
                                splitB64SHTMLColumnLength, splitB64SURLColumnLength }.Max();
                                OracleCommand[] commandStruct = new OracleCommand[maxValue];
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

                                    command = new OracleCommand(
                                    "INSERT INTO " + tableName + " " +
                                    "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                    "          VALUES (:PageID, :URL_Length, :URL, :HTML_Length, :HTML, :URL_B64S_Length, :URL_B64S, :HTML_B64S_Length, :HTML_B64S) ",
                                    connection);
                                    command.Transaction = transaction;
                                    command.Parameters.Add(":PageID", page.PageID); //                                                   Integer (int)         Columns ModifiedOn and Updates get default values
                                    command.Parameters.Add(":URL_Length", urlLength); //                                                 Integer (int)
                                    command.Parameters.Add(":URL", tURL); //                                                             Byte Array (binary)
                                    command.Parameters.Add(":HTML_Length", htmlLength); //                                               Integer (int)
                                    command.Parameters.Add(":HTML", tHTML); //                                                           Byte Array (binary)
                                    command.Parameters.Add(":URL_B64S_Length", urlB64SLength); //                                        Integer (int)
                                    command.Parameters.Add(":URL_B64S", tB64SURL); //                                                        Byte Array (binary)
                                    command.Parameters.Add(":HTML_B64S_Length", htmlB64SLength); //                                      Integer (int)
                                    command.Parameters.Add(":HTML_B64S", tB64SHTML); //                                                  Byte Array (binary)
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
                                        catch (OracleException ex)
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
                        OracleCommand headerIDCommand = new OracleCommand("SELECT Seq_HeaderID.NEXTVAL FROM DUAL", connection);
                        headerIDCommand.Transaction = transaction;
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new OracleCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (:HeaderID, :PageID, :HeaderKeyLength, :HeaderKey, :HeaderValueLength, :HeaderValue)",
                       connection);
                        command.Transaction = transaction;
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.Add(":HeaderID", headerID);
                        command.Parameters.Add(":PageID", page.PageID);
                        command.Parameters.Add(":HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.Add(":HeaderKey", kvp.Key);
                        command.Parameters.Add(":HeaderValueLength", kvp.Value.Length);
                        command.Parameters.Add(":HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= page.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            OracleCommand statIDCommand = new OracleCommand("SELECT Seq_StatId.NEXTVAL FROM DUAL", connection);
                            statIDCommand.Transaction = transaction;
                            int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                            command = new OracleCommand(
                                "INSERT INTO WebPageStats (StatID ,PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (:StatID ,:PageID, :CodeType, :Length, :Stats, :B64S_Length, :B64S_Stats)",
                                connection);
                            command.Transaction = transaction;
                            // Columns ModifiedOn and Updates get default values
                            command.Parameters.Add(":StatID", statID);
                            command.Parameters.Add(":PageID", page.PageID);
                            command.Parameters.Add(":CodeType", codeType);
                            command.Parameters.Add(":Length", page.StatsAsBytes[codeType].Length);
                            command.Parameters.Add(":Stats", page.StatsAsBytes[codeType]);
                            command.Parameters.Add(":B64S_Length", page.StatsAsBS64s[codeType].Length);
                            command.Parameters.Add(":B64S_Stats", page.StatsAsBS64s[codeType]);
                            command.CommandTimeout = 300;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Insert an encoded web page into the database objects. 
        /// </summary>
        public override int InsertEncodedWebPage(WebPage page)
        {
            int numberOfRowsAffected = 0;
            OracleTransaction transaction;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    OracleCommand pageIDCommand = new OracleCommand("SELECT Seq_PageID.NEXTVAL FROM DUAL", connection);
                    pageIDCommand.Transaction = transaction;
                    page.PageID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                    OracleCommand command = new OracleCommand(
                    "INSERT INTO WebPages (PageID, LoadedOn, HeadersLength, StatsLength, TotalLength, HREFs, HashURL, HashHTML, URL, HTML) " +
                                          "VALUES (:PageID, :LoadedOn, :HeadersLength, :StatsLength, :TotalLength, :HREFs, :HashURL, :HashHTML, :URL, :HTML)",
                    connection);
                    command.Transaction = transaction;
                    command.Parameters.Add(":PageID", page.PageID); //                                       PageID Generated Seperately
                    command.Parameters.Add(":LoadedOn", DateTime.Now); //                                            Current Date Time (DateTime2)
                                                                       //                                             Columns ModifiedOn and Updates get default values
                    command.Parameters.Add(":HeadersLength", Convert.ToInt32(page.HeadersLength)); //        Integer (int)
                    command.Parameters.Add(":StatsLength", Convert.ToInt32(page.StatsLength)); //            Integer (int)
                    command.Parameters.Add(":TotalLength", Convert.ToInt32(page.TotalLength)); //            Integer (int)
                    command.Parameters.Add(":HREFs", Convert.ToInt32(page.HREFS)); //                        Integer (int)
                    command.Parameters.Add(":HashURL", Convert.ToInt32(page.HashURL)); //                    Integer (int)
                    command.Parameters.Add(":HashHTML", page.HashHTML); //                             Byte Array (binary)
                    command.Parameters.Add(":URL", page.URL); //                                             The complete URL
                    command.Parameters.Add(":HTML", page.HTMLBinary); //                                     The fist 128 characters of HTML  
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
                                command = new OracleCommand(
                                "INSERT INTO " + tableName + " " +
                                "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                "          VALUES (:PageID, :URL_Length, :URL, :HTML_Length, :HTML, :URL_B64S_Length, :URL_B64S, :HTML_B64S_Length, :HTML_B64S) ",
                                connection);
                                command.Transaction = transaction;
                                command.Parameters.Add(":PageID", page.PageID); //                                                   Integer (int)
                                                                                //                                                   Columns ModifiedOn and Updates get default values
                                command.Parameters.Add(":URL_Length", page.DataAsBytes[tableID * 8 + i].Length); //                  Integer (int)
                                command.Parameters.Add(":URL", page.DataAsBytes[tableID * 8 + i]); //                                Byte Array (binary)
                                command.Parameters.Add(":HTML_Length", page.DataAsBytes[tableID * 8 + i + 4].Length); //             Integer (int)
                                command.Parameters.Add(":HTML", page.DataAsBytes[tableID * 8 + i + 4]); //                           Byte Array (binary)
                                command.Parameters.Add(":URL_B64S_Length", page.DataAsBS64s[tableID * 8 + i].Length); //             Integer (int)
                                command.Parameters.Add(":URL_B64S", page.DataAsBS64s[tableID * 8 + i]); //                           Byte Array (binary)
                                command.Parameters.Add(":HTML_B64S_Length", page.DataAsBS64s[tableID * 8 + i + 4].Length); //        Integer (int)
                                command.Parameters.Add(":HTML_B64S", page.DataAsBS64s[tableID * 8 + i + 4]); //                      Byte Array (binary)
                                command.CommandTimeout = 600;
                                numberOfRowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }
                    foreach (KeyValuePair<string, string> kvp in page.Headers)
                    {
                        OracleCommand headerIDCommand = new OracleCommand("SELECT Seq_HeaderId.NEXTVAL FROM DUAL", connection);
                        headerIDCommand.Transaction = transaction;
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new OracleCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (:HeaderID, :PageID, :HeaderKeyLength, :HeaderKey, :HeaderValueLength, :HeaderValue)",
                       connection);
                        command.Transaction = transaction;
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.Add(":HeaderID", headerID);
                        command.Parameters.Add(":PageID", page.PageID);
                        command.Parameters.Add(":HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.Add(":HeaderKey", kvp.Key);
                        command.Parameters.Add(":HeaderValueLength", kvp.Value.Length);
                        command.Parameters.Add(":HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= page.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            OracleCommand statIDCommand = new OracleCommand("SELECT Seq_StatId.NEXTVAL FROM DUAL", connection);
                            statIDCommand.Transaction = transaction;
                            int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                            command = new OracleCommand(
                                "INSERT INTO WebPageStats (StatID ,PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (:StatID ,:PageID, :CodeType, :Length, :Stats, :B64S_Length, :B64S_Stats)",
                                connection);
                            command.Transaction = transaction;
                            // Columns ModifiedOn and Updates get default values
                            command.Parameters.Add(":StatID", statID);
                            command.Parameters.Add(":PageID", page.PageID);
                            command.Parameters.Add(":CodeType", codeType);
                            command.Parameters.Add(":Length", page.StatsAsBytes[codeType].Length);
                            command.Parameters.Add(":Stats", page.StatsAsBytes[codeType]);
                            command.Parameters.Add(":B64S_Length", page.StatsAsBS64s[codeType].Length);
                            command.Parameters.Add(":B64S_Stats", page.StatsAsBS64s[codeType]);
                            command.CommandTimeout = 300;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Insert a single line entry into a single table every 100ms.
        /// </summary>
        public override int InsertPointInTimeWrite()
        {
            int rowsAffected = 0;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    OracleCommand pageIDCommand = new OracleCommand("SELECT Seq_WriteID.NEXTVAL FROM DUAL", connection);
                    pageIDCommand.Transaction = transaction;
                    int WriteID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                    OracleCommand command = new OracleCommand("INSERT INTO PointInTimeWrite(WriteID, WrittenOn, WriteHash) VALUES (:WriteID, :WrittenOn, :WriteHash)", connection);
                    command.Transaction = transaction;
                    DateTime writeTime = DateTime.Now;
                    command.Parameters.Add(":WriteID", WriteID);
                    command.Parameters.Add(":WrittenOn", writeTime);
                    using (var stream = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(stream, Encoding.ASCII, true))
                        {
                            writer.Write(writeTime.Ticks);
                        }
                        using (var hash = SHA256.Create())
                        {
                            string hashVal = Encoding.UTF8.GetString(hash.ComputeHash(stream));
                            command.Parameters.Add(":WriteHash", hashVal);
                        }
                    }
                    rowsAffected += command.ExecuteNonQuery();
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return rowsAffected;
        }

        /// <summary>
        /// Read base web page data from the database objects. 
        /// </summary>
        public override void ReadBaseWebPageData(OilPump pump)
        {
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    OracleCommand command = new OracleCommand("SELECT PageID, URL, HTML " +
                    "FROM   WebPages " +
                    "ORDER BY PageID", connection);
                    command.CommandTimeout = 600000;
                    connection.Open();
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int PageID = Convert.ToInt32(reader[0]);
                            OracleCommand heaaderCommand = new OracleCommand("SELECT HeaderKey, HeaderValue FROM WebPageHeaders WHERE PageID = '" + PageID + "'", connection);
                            heaaderCommand.CommandTimeout = 600000;
                            SortedList<string, string> headerList = new SortedList<string, string>();
                            using (OracleDataReader headerReader = heaaderCommand.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Query database objects using ANSI SQL Left Outer Join syntax. 
        /// </summary>
        public override Tuple<ulong, ulong> SelectWebPageLeftOuterJoin(int PageID, UInt64 bytesToProcess)
        {
            UInt64 rowDataprocessed = 0;
            UInt64 rowsProcessed = 0;
            DateTime timestamp = DateTime.Now;

            using (OracleConnection connection = GetConnection())
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
                    query = query + " WHERE WebPages.PageID = " + PageID;
                    queriesToRun.Add(query);
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                    Tuple<UInt64, UInt64>[] collation = new Tuple<UInt64, UInt64>[queriesToRun.Count];

                    CheckSchemaType();
                    if (GetSchemaType() == Dynamics.DatabaseSchema.WithIndexes ||
                    GetSchemaType() == Dynamics.DatabaseSchema.WithoutIndexes)
                    {
                        for (int i = 0; i < queriesToRun.Count; i++)
                        {
                            OracleCommand command = new OracleCommand(queriesToRun[i], connection);
                            command.Transaction = transaction;
                            command.CommandTimeout = 600;
                            using (OracleDataReader dataReader = command.ExecuteReader())
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
                            OracleCommand command = new OracleCommand(queriesToRun[i], connection);
                            command.Transaction = transaction;
                            command.CommandTimeout = 600;
                            using (OracleDataReader dataReader = command.ExecuteReader())
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
                    OracleCommand HeaderDataValues = new OracleCommand(queryHeaderData, connection);
                    HeaderDataValues.Transaction = transaction;
                    HeaderDataValues.CommandTimeout = 300;
                    using (OracleDataReader dataReader = HeaderDataValues.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return Tuple.Create(rowDataprocessed, rowsProcessed);
        }

        /// <summary>
        /// Query database objects using ANSI SQL Union All syntax. 
        /// </summary>
        public override Tuple<ulong, ulong> SelectWebPageUnionAll(int PageID, UInt64 bytesToProcess)
        {
            UInt64 rowDataprocessed = 0;
            UInt64 rowsProcessed = 0;
            DateTime timestamp = DateTime.Now;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
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
                                    "FROM " + tableName + " WHERE PageID = " + PageID;
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
                    //query = query + " ;";
                    queriesToRun.Add(query);

                    Tuple<UInt64, UInt64>[] collation = new Tuple<UInt64, UInt64>[queriesToRun.Count];

                    CheckSchemaType();
                    if (GetSchemaType() == Dynamics.DatabaseSchema.WithIndexes ||
                    GetSchemaType() == Dynamics.DatabaseSchema.WithoutIndexes)
                    {
                        for (int i = 0; i < queriesToRun.Count; i++)
                        {
                            OracleCommand command = new OracleCommand(queriesToRun[i], connection);
                            command.Transaction = transaction;
                            command.CommandTimeout = 600;
                            using (OracleDataReader dataReader = command.ExecuteReader())
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
                            OracleCommand command = new OracleCommand(queriesToRun[i], connection);
                            command.Transaction = transaction;
                            command.CommandTimeout = 600;
                            using (OracleDataReader dataReader = command.ExecuteReader())
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
                    OracleCommand HeaderDataValues = new OracleCommand(queryHeaderData, connection);
                    HeaderDataValues.Transaction = transaction;
                    HeaderDataValues.CommandTimeout = 300;
                    using (OracleDataReader dataReader = HeaderDataValues.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return Tuple.Create(rowDataprocessed, rowsProcessed);
        }

        /// <summary>
        /// Update a characterised web page. 
        /// </summary>
        public override int UpdateCharacterisedWebPage(int PageID)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                OracleCommand readCommand = new OracleCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                TableName + " WHERE PageID = :PageID", connection);
                                readCommand.Transaction = transaction;
                                readCommand.CommandTimeout = 12000;
                                readCommand.Parameters.Add(":PageID", PageID);
                                string URL = string.Empty;
                                string HTML = string.Empty;
                                string URL_B64S = string.Empty;
                                string HTML_B64S = string.Empty;
                                using (OracleDataReader commandReader = readCommand.ExecuteReader())
                                {
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

                                        OracleCommand command = new OracleCommand("UPDATE " + TableName + " SET URL = :URL, Updates = Updates + 1, " +
                                            " HTML = :HTML, URL_B64S = :URL_B64S," +
                                            " HTML_B64S = :HTML_B64S, ModifiedOn = :ModifiedOn WHERE PageID = :PageID AND URL = :OriginalURL AND HTML = :OriginalHTML " +
                                            " AND URL_B64S = :OriginalURL_B64S AND HTML_B64S = :Original_HTML_B64S", connection);
                                        command.Transaction = transaction;
                                        string commtest = command.CommandText;
                                        command.Parameters.Add(":URL", URLReset);
                                        command.Parameters.Add(":HTML", HTMLReset);
                                        command.Parameters.Add(":URL_B64S", URL_B64SReset);
                                        command.Parameters.Add(":HTML_B64S", HTML_B64SReset);
                                        command.Parameters.Add(":ModifiedOn", timestamp);
                                        command.Parameters.Add(":PageID", PageID);
                                        command.Parameters.Add(":OriginalURL", URL);
                                        command.Parameters.Add(":OriginalHTML", HTML);
                                        command.Parameters.Add(":OriginalURL_B64S", URL_B64S);
                                        command.Parameters.Add(":Original_HTML_B64S", HTML_B64S);
                                        command.CommandTimeout = 300;
                                        rowsAffected += command.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                    }
                    //Update Core Tables
                    foreach (string table in _mechanic.ReturnWebPageCoreTables)
                    {

                        OracleCommand coreTableUpdateCommand = new OracleCommand("UPDATE " + table + " SET Updates = Updates + 1," +
                            "                                           ModifiedOn = :ModifiedOn WHERE PageID = :PageID", connection);
                        coreTableUpdateCommand.Transaction = transaction;
                        coreTableUpdateCommand.Parameters.Add(":ModifiedOn", timestamp);
                        coreTableUpdateCommand.Parameters.Add(":PageID", PageID);
                        coreTableUpdateCommand.CommandTimeout = 300;
                        rowsAffected += coreTableUpdateCommand.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Update a characterised web page by replacing it. 
        /// </summary>
        public override int UpdateCharacterisedWebPageInPlace(int PageID, WebPage newPage)
        {
            int numberOfRowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    //DateTime modificationDate = DateTime.Now;
                    OracleCommand command = new OracleCommand(
                    "UPDATE WebPages SET LoadedOn = :LoadedOn, HeadersLength = :HeadersLength, StatsLength =  :StatsLength," +
                    " TotalLength = :TotalLength, HREFs = :HREFs, HashURL = :HashURL, HashHTML = :HashHTML, URL = :URL , HTML = :HTML," +
                    " ModifiedOn = NULL, Updates = 0 WHERE PageID = :PageID",
                    connection);
                    command.Transaction = transaction;
                    command.Parameters.Add(":LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                       // Columns ModifiedOn and Updates get default values
                    command.Parameters.Add(":HeadersLength", Convert.ToInt32(newPage.HeadersLength)); //        Integer (int)
                    command.Parameters.Add(":StatsLength", Convert.ToInt32(newPage.StatsLength)); //            Integer (int)
                    command.Parameters.Add(":TotalLength", Convert.ToInt32(newPage.TotalLength)); //            Integer (int)
                    command.Parameters.Add(":HREFs", Convert.ToInt32(newPage.HREFS)); //                        Integer (int)
                    command.Parameters.Add(":HashURL", Convert.ToInt32(newPage.HashURL)); //                    Integer (int)
                    command.Parameters.Add(":HashHTML", newPage.HashHTML); //                             Byte Array (binary)
                    command.Parameters.Add(":URL", newPage.URL); //                                             The complete URL
                    command.Parameters.Add(":HTML", newPage.HTMLBinary); //                                     The fist 128 characters of HTML  
                    command.Parameters.Add(":PageID", PageID);
                    command.CommandTimeout = 300;
                    numberOfRowsAffected += command.ExecuteNonQuery();

                    //Contains all 4 encoded table sets 
                    int numberOfEncoders = _mechanic.ReturnWebPageEncodingtables.Length;
                    OracleCommand[][][] allEncodedWebPagesCommands = new OracleCommand[numberOfEncoders][][];
                    for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                    {
                        //contains all the tables for the subset of the encoding up to the amplifier 
                        allEncodedWebPagesCommands[i] = new OracleCommand[_amplifier][];
                        for (int tableID = 0; tableID < _amplifier; tableID++)
                        {
                            string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                            string tableName = _mechanic.NameWithID(encodingTables[i], tableID);

                            command = new OracleCommand("DELETE FROM " + tableName + " WHERE PageID = " + PageID, connection);
                            command.Transaction = transaction;
                            command.ExecuteNonQuery();

                            int htmlIndex = tableID * 8 + i + 4;
                            int urlIndex = tableID * 8 + i;
                            int b64SHTMLIndex = tableID * 8 + i + 4;
                            int b64SURLIndex = tableID * 8 + i;

                            string convertedHTML = Encoding.ASCII.GetString(newPage.DataAsBytes[htmlIndex], 0, newPage.DataAsBytes[htmlIndex].Length);
                            string convertedURL = Encoding.ASCII.GetString(newPage.DataAsBytes[urlIndex], 0, newPage.DataAsBytes[urlIndex].Length);
                            string convertedB64SHTML = Encoding.ASCII.GetString(newPage.DataAsBS64s[b64SHTMLIndex]);
                            string convertedB64SURL = Encoding.ASCII.GetString(newPage.DataAsBS64s[b64SURLIndex]);


                            //split the string if it is larger than 4000

                            List<string> splitHTMLColumns = _mechanic.ChunkString(convertedHTML, 4000);
                            List<string> splitURLColumns = _mechanic.ChunkString(convertedURL, 4000);
                            List<string> splitB64SHTMLColumns = _mechanic.ChunkString(convertedB64SHTML, 4000);
                            List<string> splitB64SURLColumns = _mechanic.ChunkString(convertedB64SURL, 4000);

                            List<Tuple<string, string, string>> webPageCharacterSplit = new List<Tuple<string, string, string>>();
                            //get the longest length object

                            int splitHTMLColumnLength = splitHTMLColumns.Count;
                            int splitURLColumnLength = splitURLColumns.Count;
                            int splitB64SHTMLColumnLength = splitB64SHTMLColumns.Count;
                            int splitB64SURLColumnLength = splitB64SURLColumns.Count;

                            int maxValue = new[] { splitHTMLColumnLength, splitURLColumnLength,
                                splitB64SHTMLColumnLength, splitB64SURLColumnLength }.Max();
                            OracleCommand[] commandStruct = new OracleCommand[maxValue];
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

                                command = new OracleCommand(
                                "INSERT INTO " + tableName + " " +
                                "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                "          VALUES (:PageID, :URL_Length, :URL, :HTML_Length, :HTML, :URL_B64S_Length, :URL_B64S, :HTML_B64S_Length, :HTML_B64S) ",
                                connection);
                                command.Transaction = transaction;
                                command.Parameters.Add(":PageID", PageID); //                                                   Integer (int)                                           //                                                   Columns ModifiedOn and Updates get default values
                                command.Parameters.Add(":URL_Length", urlLength); //                                                 Integer (int)
                                command.Parameters.Add(":URL", tURL); //                                                             Byte Array (binary)
                                command.Parameters.Add(":HTML_Length", htmlLength); //                                               Integer (int)
                                command.Parameters.Add(":HTML", tHTML); //                                                           Byte Array (binary)
                                command.Parameters.Add(":URL_B64S_Length", urlB64SLength); //                                        Integer (int)
                                command.Parameters.Add(":URL_B64S", tB64SURL); //                                                        Byte Array (binary)
                                command.Parameters.Add(":HTML_B64S_Length", htmlB64SLength); //                                      Integer (int)
                                command.Parameters.Add(":HTML_B64S", tB64SHTML); //                                                  Byte Array (binary)
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
                                catch (OracleException ex)
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
                    command = new OracleCommand("DELETE FROM WebPageHeaders WHERE PageID = " + PageID, connection);
                    command.Transaction = transaction;
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        OracleCommand headerIDCommand = new OracleCommand("SELECT Seq_HeaderId.NEXTVAL FROM DUAL", connection);
                        command.Transaction = transaction;
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new OracleCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (:HeaderID, :PageID, :HeaderKeyLength, :HeaderKey, :HeaderValueLength, :HeaderValue)",
                       connection);
                        command.Transaction = transaction;
                        command.Parameters.Add(":HeaderID", headerID);
                        command.Parameters.Add(":PageID", PageID);
                        command.Parameters.Add(":HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.Add(":HeaderKey", kvp.Key);
                        command.Parameters.Add(":HeaderValueLength", kvp.Value.Length);
                        command.Parameters.Add(":HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new OracleCommand("DELETE FROM WebPageStats WHERE PageID = " + PageID, connection);
                    command.Transaction = transaction;
                    command.ExecuteNonQuery();

                    for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                    {
                        OracleCommand statIDCommand = new OracleCommand("SELECT Seq_StatId.NEXTVAL FROM DUAL", connection);
                        command.Transaction = transaction;
                        int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                        command = new OracleCommand(
                            "INSERT INTO WebPageStats (StatID ,PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                      "VALUES (:StatID ,:PageID, :CodeType, :Length, :Stats, :B64S_Length, :B64S_Stats)",
                            connection);
                        command.Transaction = transaction;
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.Add(":StatID", statID);
                        command.Parameters.Add(":PageID", PageID);
                        command.Parameters.Add(":CodeType", codeType);
                        command.Parameters.Add(":Length", newPage.StatsAsBytes[codeType].Length);
                        command.Parameters.Add(":Stats", newPage.StatsAsBytes[codeType]);
                        command.Parameters.Add(":B64S_Length", newPage.StatsAsBS64s[codeType].Length);
                        command.Parameters.Add(":B64S_Stats", newPage.StatsAsBS64s[codeType]);
                        command.CommandTimeout = 300;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        OracleCommand updateCommand = new OracleCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
                            " ModifiedOn = :ModifiedOn WHERE PageID = :PageID", connection);
                        command.Transaction = transaction;
                        updateCommand.Parameters.Add(":ModifiedOn", timestamp);
                        updateCommand.Parameters.Add(":PageID", PageID);
                        updateCommand.CommandTimeout = 300;
                        numberOfRowsAffected += updateCommand.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Update and encoded web page. 
        /// </summary>
        public override int UpdateEncodedWebPage(int PageID)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                OracleCommand readCommand = new OracleCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                    TableName + " WHERE PageID = :PageID", connection);
                                readCommand.Transaction = transaction;
                                readCommand.CommandTimeout = 12000;
                                readCommand.Parameters.Add(":PageID", PageID);
                                string URL = string.Empty;
                                string HTML = string.Empty;
                                string URL_B64S = string.Empty;
                                string HTML_B64S = string.Empty;
                                using (OracleDataReader commandReader = readCommand.ExecuteReader())
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

                                OracleCommand command = new OracleCommand("UPDATE " + TableName + " SET URL = :URL, " +
                                    "HTML = :HTML, URL_B64S = :URL_B64S," +
                                    " HTML_B64S = :HTML_B64S WHERE PageID = :PageID", connection);
                                command.Transaction = transaction;
                                command.Parameters.Add(":URL", URLEncoded);
                                command.Parameters.Add(":HTML", HTMLEncoded);
                                command.Parameters.Add(":URL_B64S", URL_B64SEncoded);
                                command.Parameters.Add(":HTML_B64S", HTML_B64S_Encoded);
                                command.Parameters.Add(":PageID", PageID);
                                command.CommandTimeout = 300;
                                rowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        OracleCommand command = new OracleCommand("UPDATE " + TableName + " SET " +
                            " ModifiedOn = :ModifiedOn WHERE PageID = :PageID", connection);
                        command.Transaction = transaction;
                        command.Parameters.Add(":ModifiedOn", timestamp);
                        command.Parameters.Add(":PageID", PageID);
                        command.CommandTimeout = 300;
                        rowsAffected += command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Update and encoded web page. 
        /// </summary>
        public override int UpdateEncodedWebPageInPlace(int PageID, WebPage newPage)
        {
            int numberOfRowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (OracleConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    OracleTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    OracleCommand command = new OracleCommand(
                    "UPDATE WebPages SET LoadedOn = :LoadedOn, HeadersLength = :HeadersLength, StatsLength =  :StatsLength," +
                    " TotalLength = :TotalLength, HREFs = :HREFs, HashURL = :HashURL, HashHTML = :HashHTML, URL = :URL , HTML = :HTML," +
                    " ModifiedOn = NULL, Updates = 0 WHERE PageID = :PageID",
                    connection);
                    command.Transaction = transaction;
                    command.Parameters.Add(":LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                       // Columns ModifiedOn and Updates get default values
                    command.Parameters.Add(":HeadersLength", Convert.ToInt32(newPage.HeadersLength)); //        Integer (int)
                    command.Parameters.Add(":StatsLength", Convert.ToInt32(newPage.StatsLength)); //            Integer (int)
                    command.Parameters.Add(":TotalLength", Convert.ToInt32(newPage.TotalLength)); //            Integer (int)
                    command.Parameters.Add(":HREFs", Convert.ToInt32(newPage.HREFS)); //                        Integer (int)
                    command.Parameters.Add(":HashURL", Convert.ToInt32(newPage.HashURL)); //                    Integer (int)
                    command.Parameters.Add(":HashHTML", newPage.HashHTML); //                             Byte Array (binary)
                    command.Parameters.Add(":URL", newPage.URL); //                                             The complete URL
                    command.Parameters.Add(":HTML", newPage.HTMLBinary); //                                     The fist 128 characters of HTML  
                    command.Parameters.Add(":PageID", PageID);
                    command.CommandTimeout = 300;
                    numberOfRowsAffected += command.ExecuteNonQuery();

                    for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                    {
                        for (int tableID = 0; tableID < _amplifier; tableID++)
                        {
                            string tableName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                            command = new OracleCommand(
                            "UPDATE " + tableName + " " +
                            " SET URL_Length = :URL_Length, URL = :URL, HTML_Length = :HTML_Length, HTML = :HTML, URL_B64S_Length = :URL_B64S_Length," +
                            " URL_B64S = :URL_B64S, HTML_B64S_Length = :HTML_B64S_Length, HTML_B64S = :HTML_B64S, " +
                            " ModifiedOn = NULL, Updates = 0 WHERE PageID = :PageID",
                            connection);
                            command.Transaction = transaction;
                            command.Parameters.Add(":URL_Length", newPage.DataAsBytes[tableID * 8 + i].Length); //                  Integer (int)
                            command.Parameters.Add(":URL", newPage.DataAsBytes[tableID * 8 + i]); //                                Byte Array (binary)
                            command.Parameters.Add(":HTML_Length", newPage.DataAsBytes[tableID * 8 + i + 4].Length); //             Integer (int)
                            command.Parameters.Add(":HTML", newPage.DataAsBytes[tableID * 8 + i + 4]); //                           Byte Array (binary)
                            command.Parameters.Add(":URL_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i].Length); //             Integer (int)
                            command.Parameters.Add(":URL_B64S", newPage.DataAsBS64s[tableID * 8 + i]); //                           Byte Array (binary)
                            command.Parameters.Add(":HTML_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i + 4].Length); //        Integer (int)
                            command.Parameters.Add(":HTML_B64S", newPage.DataAsBS64s[tableID * 8 + i + 4]); //                      Byte Array (binary)
                            command.Parameters.Add(":PageID", PageID);
                            command.CommandTimeout = 600;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }

                    //delete all existing keys

                    command = new OracleCommand("DELETE FROM WebPageHeaders WHERE PageID = " + PageID, connection);
                    command.Transaction = transaction;
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        OracleCommand headerIDCommand = new OracleCommand("SELECT Seq_HeaderId.NEXTVAL FROM DUAL", connection);
                        headerIDCommand.Transaction = transaction;
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new OracleCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (:HeaderID, :PageID, :HeaderKeyLength, :HeaderKey, :HeaderValueLength, :HeaderValue)",
                       connection);
                        command.Transaction = transaction;
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.Add(":HeaderID", headerID);
                        command.Parameters.Add(":PageID", PageID);
                        command.Parameters.Add(":HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.Add(":HeaderKey", kvp.Key);
                        command.Parameters.Add(":HeaderValueLength", kvp.Value.Length);
                        command.Parameters.Add(":HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new OracleCommand("DELETE FROM WebPageStats WHERE PageID = " + PageID, connection);
                    command.Transaction = transaction;
                    command.ExecuteNonQuery();

                    for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                    {
                        OracleCommand statIDCommand = new OracleCommand("SELECT Seq_StatId.NEXTVAL FROM DUAL", connection);
                        command.Transaction = transaction;
                        int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                        command = new OracleCommand(
                            "INSERT INTO WebPageStats (StatID ,PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                      "VALUES (:StatID ,:PageID, :CodeType, :Length, :Stats, :B64S_Length, :B64S_Stats)",
                            connection);
                        command.Transaction = transaction;
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.Add(":StatID", statID);
                        command.Parameters.Add(":PageID", PageID);
                        command.Parameters.Add(":CodeType", codeType);
                        command.Parameters.Add(":Length", newPage.StatsAsBytes[codeType].Length);
                        command.Parameters.Add(":Stats", newPage.StatsAsBytes[codeType]);
                        command.Parameters.Add(":B64S_Length", newPage.StatsAsBS64s[codeType].Length);
                        command.Parameters.Add(":B64S_Stats", newPage.StatsAsBS64s[codeType]);
                        command.CommandTimeout = 300;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        OracleCommand updateCommand = new OracleCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
                            " ModifiedOn = :ModifiedOn WHERE PageID = :PageID", connection);
                        command.Transaction = transaction;
                        updateCommand.Parameters.Add(":ModifiedOn", timestamp);
                        updateCommand.Parameters.Add(":PageID", PageID);
                        updateCommand.CommandTimeout = 300;
                        numberOfRowsAffected += updateCommand.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
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
        /// Update the import history for any used data files. 
        /// </summary>
        public override bool UpdateImportHistory(ImportHandler import)
        {
            bool success = false;
            try
            {
                using (OracleConnection connection = GetConnection())
                {
                    connection.Open();
                    OracleCommand command = new OracleCommand("UPDATE Imports SET " +
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
                if (ex.Message.ToLower().Contains("ora-01017: invalid username/password; logon denied")
                        || ex.Message.ToLower().Contains("ora-12545: network transport: unable to resolve connect hostname")
                        || ex.Message.ToLower().Contains("ora-28000: the account is locked."))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return success;
        }

        /// <summary>
        /// Not implemented for oracle operations. 
        /// </summary>
        public override void VendorAdvancedOperations(int numberOfThreads)
        {
            _messageQueue.AddMessage(new Message(DateTime.Now, "Oracle has no Advanced operation type specified at this time", Message.MessageType.Error));
        }

        /// <summary>
        /// Not implemented for oracle databases. 
        /// </summary>
        public override void VendorComplexOperations()
        {
            _messageQueue.AddMessage(new Message(DateTime.Now, "Oracle has no Complex operation type specified at this time", Message.MessageType.Error));
        }

        /// <summary>
        /// Performs a vendor consistency check. 
        /// </summary>
        public override void VendorConsistencyCheck()
        {
            _messageQueue.AddMessage(new Message(DateTime.Now, "Oracle has no Consistency check operation type specified at this time", Message.MessageType.Error));
        }
    }
}
