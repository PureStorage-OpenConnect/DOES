using System;
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
using MySqlConnector;

namespace DOES.DataEngine.Operations
{
    /// <summary>
    /// This class handles MariaDB database operations. 
    /// </summary>
    public class MariaDB : DataVendor
    {
        private string _hostname;
        private string _databaseName;
        private string _userName;
        private string _password;
        private int _amplifier;
        private int _port;
        private Dynamics.DatabaseSchema _schema;
        private Mechanic _mechanic;
        private MessageQueue _messageQueue;
        private int varcharMaxLength = 1000;
        private Dynamics.MariaDBStorageEngine _engine;
        private CancellationTokenSource _tokenSource;

        /// <summary>
        /// Instantiates the MariaDB class. 
        /// </summary>
        public MariaDB(string hostname, string dbname, string username, string password, int amplifier,
            int port, Mechanic mechanic, Dynamics.MariaDBStorageEngine storageEngine, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _amplifier = amplifier;
            _port = port;
            _messageQueue = messages;
            _mechanic = mechanic;
            _engine = storageEngine;
        }

        /// <summary>
        /// Instantiates the MariaDB class. 
        /// </summary>
        public MariaDB(string hostname, string dbname, string username, string password, int amplifier,
            int port, Dynamics.DatabaseSchema schema, Mechanic mechanic, Dynamics.MariaDBStorageEngine storageEngine, 
            MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _amplifier = amplifier;
            _port = port;
            _schema = schema;
            _messageQueue = messages;
            _mechanic = mechanic;
            _engine = storageEngine;
        }

        /// <summary>
        /// Instantiates the MariaDB class. 
        /// </summary>
        public MariaDB(string hostname, string dbname, string username, string password, int port,
            Mechanic mechanic, Dynamics.MariaDBStorageEngine storageEngine, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _port = port;
            _messageQueue = messages;
            _mechanic = mechanic;
            _engine = storageEngine;
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    MySqlCommand command = new MySqlCommand("SELECT ImportedWebPages, ImportedAllWebPages from Imports WHERE FileName = '" + filename + "'", connection);
                    command.CommandTimeout = 600;
                    connection.Open();
                    ImportHandler importData;
                    int importedWebPages = 0;
                    bool importedAllWebPages = false;
                    bool found = false;
                    using (MySqlDataReader dataReader = command.ExecuteReader())
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlCommand command = new MySqlCommand("SELECT SchemaType FROM Configuration", connection);
                    using (MySqlDataReader reader = command.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect"))
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
                using (MySqlConnection connection = GetConnection())
                {
                    connection.Open();
                    MySqlCommand command = new MySqlCommand("INSERT INTO Imports (FileName, ImportedWebPages, ImportedAllWebPages) VALUES ('" +
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
                if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
            using (MySqlConnection conn = GetConnection())
            {
                //Base Objects
                try
                {
                    conn.Open();
                    foreach (string table in _mechanic.AllBaseTables())
                    {
                        MySqlCommand command = new MySqlCommand("SELECT * FROM " + table + " LIMIT 1", conn);
                        command.ExecuteScalar();
                    }
                    conn.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
                                string MySqlCommandString = sb.ToString();
                                string SQLString = Regex.Replace(sb.ToString(), @"\n|\r|\t", "");
                                SQLString = _mechanic.CreateMariaDBTables(SQLString, _engine, true, _schema);
                                if (SQLString != string.Empty)
                                {
                                    MySqlCommand command = new MySqlCommand(SQLString, conn);
                                    try
                                    {
                                        _messageQueue.AddMessage(new Message(DateTime.Now, SQLString, Message.MessageType.Command));
                                        command.ExecuteNonQuery();
                                        success = true;
                                    }
                                    catch (MySqlException sqlex)
                                    {
                                        _messageQueue.AddMessage(new Message(DateTime.Now, sqlex.Message.ToString(), Message.MessageType.Command));
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
                    try
                    {
                        //Create the Configuration table objects and load them 
                        MySqlCommand firstPointcommand = new MySqlCommand("INSERT INTO Configuration (SchemaType, CreatedOn) VALUES" +
                            " (@SchemaType, @CreatedOn)", conn);

                        firstPointcommand.Parameters.AddWithValue("@SchemaType", _schema.ToString());
                        firstPointcommand.Parameters.AddWithValue("@CreatedOn", DateTime.Now);
                        firstPointcommand.ExecuteNonQuery();
                        conn.Close();
                        success = true;
                    }
                    catch (MySqlException sql3)
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
                        MySqlCommand command = new MySqlCommand("SELECT * FROM " + table + " LIMIT 1", conn);
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
                                string MySqlCommandString = sb.ToString();
                                MySqlCommandString = _mechanic.CreateMariaDBTables(MySqlCommandString, _engine, false, _schema);
                                for (int TableID = 0; (TableID < Convert.ToInt32((MySqlCommandString.Contains("_X00") ? _amplifier : 0))); TableID++)
                                {
                                    try
                                    {
                                        MySqlCommand command = new MySqlCommand(_mechanic.NameWithID(MySqlCommandString, TableID), conn);
                                        _messageQueue.AddMessage(new Message(DateTime.Now, MySqlCommandString, Message.MessageType.Command));
                                        command.ExecuteNonQuery();
                                    }
                                    catch (MySqlException sqle)
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        MySqlCommand command = new MySqlCommand("DELETE FROM " + TableName + " WHERE PageID" +
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
            string operation = "DROP";
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    if (ClearingType == Dynamics.ClearingType.Drop)
                    {
                        operation = "DROP";
                        foreach (string seq in _mechanic.ReturnAllSequences)
                        {
                            try
                            {
                                string commandString = "DROP SEQUENCE " + seq;
                                MySqlCommand command = new MySqlCommand(commandString, connection);
                                _messageQueue.AddMessage(new Message(DateTime.Now, commandString, Message.MessageType.Command));
                                command.ExecuteNonQuery();
                            }
                            catch (MySqlException mex)
                            {
                                success = false;
                                if (mex.Message.ToLower().Contains("unknown sequence"))
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, mex.Message, Message.MessageType.Warning));
                                }
                                else
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, mex.Message, Message.MessageType.Error));
                                }
                            }
                        }
                    }
                    else if (ClearingType == Dynamics.ClearingType.Truncate)
                    {
                        operation = "TRUNCATE";
                    }
                    foreach (string table in _mechanic.AllTables(_amplifier))
                    {
                        try
                        {
                            MySqlCommand command = new MySqlCommand("SELECT * FROM " + table + " LIMIT 1", connection);
                            command.ExecuteScalar();
                            command = new MySqlCommand(operation + " TABLE " + table, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                            command.CommandTimeout = 600;
                            command.ExecuteNonQuery();
                        }
                        catch (MySqlException ex)
                        {
                            success = false;
                            if (ex.Message.ToLower().Contains("doesn't exist"))
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
                    connection.Close();

                    return success;
                }
                catch (Exception ex)
                {
                    success = false;
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
        /// Return the MariaDB database connection object. 
        /// </summary>
        public override dynamic GetConnection()
        {
            string _connectionString = "server=" + _hostname + ";port=" + _port + ";user id=" + _userName + "; password=" + _password + ";database=" + _databaseName + 
                ";ConnectionTimeout=0;DefaultCommandTimeout=0;";
            MySqlConnection connection = new MySqlConnection(_connectionString);
            return connection;
        }

        /// <summary>
        /// Return the database type. 
        /// </summary>
        public override Dynamics.Database GetDatabaseType()
        {
            return Dynamics.Database.MariaDB;
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    MySqlCommand command = new MySqlCommand("SELECT PageID, TotalLength from WebPages ORDER BY PageID", connection, transaction);
                    using (MySqlDataReader reader = command.ExecuteReader())
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
                MySqlConnection connection = GetConnection();
                MySqlCommand commandRowCountNN = new MySqlCommand("SELECT COUNT(*) FROM WebPages WHERE ModifiedOn IS NOT NULL", connection);
                MySqlCommand commandRowCountTotal = new MySqlCommand("SELECT COUNT(*) FROM WebPages", connection);
                MySqlCommand commandTotalSize = new MySqlCommand("SELECT SUM(TotalLength) FROM WebPages", connection);
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
        /// Insert a characterised web page into the database.  
        /// </summary>
        public override int InsertCharacterisedWebPage(WebPage page)
        {
            int numberOfRowsAffected = 0;
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    MySqlCommand pageIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_PageID)", connection, transaction);
                    page.PageID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                
                    MySqlCommand command = new MySqlCommand(
                    "INSERT INTO WebPages (PageID, LoadedOn, HeadersLength, StatsLength, TotalLength, HREFs, HashURL, HashHTML, URL, HTML) " +
                                          "VALUES (@PageID, @LoadedOn,  @HeadersLength,  @StatsLength,  @TotalLength,  @HREFs,  @HashURL,  @HashHTML,  @URL,  @HTML)",
                    connection,
                    transaction);
                    command.Parameters.AddWithValue("@PageID", page.PageID); //                                       PageID Generated Seperately
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                                // Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(page.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(page.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(page.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(page.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(page.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML", page.HashHTML); //                                   Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", page.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", page.HTMLBinary); //                                     The fist 8096 characters of HTML  
                    command.CommandTimeout = 300;
                    numberOfRowsAffected += command.ExecuteNonQuery(); //                                        Insert and get the unique PageID - One row is inserted
                    numberOfRowsAffected++;
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables; ;
                                string tableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                int htmlIndex = tableID * 8 + i + 4;
                                int urlIndex = tableID * 8 + i;
                                int b64SHTMLIndex = tableID * 8 + i + 4;
                                int b64SURLIndex = tableID * 8 + i;

                                string convertedHTML = Encoding.ASCII.GetString(page.DataAsBytes[htmlIndex], 0, page.DataAsBytes[htmlIndex].Length);
                                string convertedURL = Encoding.ASCII.GetString(page.DataAsBytes[urlIndex], 0, page.DataAsBytes[urlIndex].Length);
                                string convertedB64SHTML = Encoding.ASCII.GetString(page.DataAsBS64s[b64SHTMLIndex]);
                                string convertedB64SURL = Encoding.ASCII.GetString(page.DataAsBS64s[b64SURLIndex]);


                                //split the string if it is larger than varcharmaxlength

                                List<string> splitHTMLColumns = _mechanic.ChunkString(convertedHTML, varcharMaxLength);
                                List<string> splitURLColumns = _mechanic.ChunkString(convertedURL, varcharMaxLength);
                                List<string> splitB64SHTMLColumns = _mechanic.ChunkString(convertedB64SHTML, varcharMaxLength);
                                List<string> splitB64SURLColumns = _mechanic.ChunkString(convertedB64SURL, varcharMaxLength);

                                List<Tuple<string, string, string>> webPageCharacterSplit = new List<Tuple<string, string, string>>();
                                //get the longest length object

                                int splitHTMLColumnLength = splitHTMLColumns.Count;
                                int splitURLColumnLength = splitURLColumns.Count;
                                int splitB64SHTMLColumnLength = splitB64SHTMLColumns.Count;
                                int splitB64SURLColumnLength = splitB64SURLColumns.Count;

                                int maxValue = new[] { splitHTMLColumnLength, splitURLColumnLength,
                                splitB64SHTMLColumnLength, splitB64SURLColumnLength }.Max();

                                StringBuilder sCommand = new StringBuilder("INSERT INTO " + tableName + " " +
                                    "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                    "          VALUES ");

                                List<string> Rows = new List<string>();

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


                                    Rows.Add(string.Format("('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')", MySqlHelper.EscapeString(page.PageID.ToString()), MySqlHelper.EscapeString(urlLength.ToString()),
                                        MySqlHelper.EscapeString(tURL), MySqlHelper.EscapeString(htmlLength.ToString()), MySqlHelper.EscapeString(tHTML), MySqlHelper.EscapeString(urlB64SLength.ToString()),
                                        MySqlHelper.EscapeString(tB64SURL), MySqlHelper.EscapeString(htmlB64SLength.ToString()), MySqlHelper.EscapeString(tB64SHTML)));

                                }
                                sCommand.Append(string.Join(",", Rows));
                                sCommand.Append(";");


                                using (command = new MySqlCommand(sCommand.ToString(), connection, transaction))
                                {
                                    command.CommandType = System.Data.CommandType.Text;
                                    command.CommandTimeout = 600;
                                    numberOfRowsAffected += command.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    foreach (KeyValuePair<string, string> kvp in page.Headers)
                    {
                        MySqlCommand headerIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_HeaderID)", connection, transaction);
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new MySqlCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (@HeaderID, @PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", page.PageID);
                        command.Parameters.AddWithValue("@HeaderID", headerID);
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
                            MySqlCommand statIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_StatID)", connection, transaction);
                            int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                            command = new MySqlCommand(
                                "INSERT INTO WebPageStats (StatID, PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (@StatID, @PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
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
                            command.CommandTimeout = 600;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
        /// Insert encoded web pages into the database. 
        /// </summary>
        public override int InsertEncodedWebPage(WebPage page)
        {
            int numberOfRowsAffected = 0;
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    MySqlCommand pageIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_PageID)", connection, transaction);
                    page.PageID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                
                    MySqlCommand command = new MySqlCommand(
                    "INSERT INTO WebPages (PageID, LoadedOn, HeadersLength, StatsLength, TotalLength, HREFs, HashURL, HashHTML, URL, HTML) " +
                                          "VALUES (@PageID, @LoadedOn,  @HeadersLength,  @StatsLength,  @TotalLength,  @HREFs,  @HashURL,  @HashHTML,  @URL,  @HTML)",
                    connection,
                    transaction);
                    command.Parameters.AddWithValue("@PageID", page.PageID); //                                       PageID Generated Seperately
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                                // Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(page.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(page.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(page.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(page.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(page.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML", page.HashHTML); //                                   Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", page.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", page.HTMLBinary); //                                     The fist 8096 characters of HTML  
                    command.CommandTimeout = 600;
                    numberOfRowsAffected += command.ExecuteNonQuery(); //                                        Insert and get the unique PageID - One row is inserted
                    numberOfRowsAffected++;

                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables; ;
                                string tableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                command = new MySqlCommand(
                                "INSERT INTO " + tableName + 
                                "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                "          VALUES (@PageID,  @URL_Length,  @URL,  @HTML_Length,  @HTML,  @URL_B64S_Length,  @URL_B64S,  @HTML_B64S_Length,  @HTML_B64S); ",
                                connection,
                                transaction);
                                command.Parameters.AddWithValue("@PageID", page.PageID); //                                                  Integer (int)
                                                                                         // Columns ModifiedOn and Updates get default values
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
                        MySqlCommand headerIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_HeaderID)", connection, transaction);
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new MySqlCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (@HeaderID, @PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", page.PageID);
                        command.Parameters.AddWithValue("@HeaderID", headerID);
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
                            MySqlCommand statIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_StatID)", connection, transaction);
                            int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                            command = new MySqlCommand(
                                "INSERT INTO WebPageStats (StatID, PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (@StatID, @PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
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
                            command.CommandTimeout = 600;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
        /// Changes The storage engine for MariaDB base tables
        /// </summary>
        public void SetMariaDBStorageEngineBaseTables(Dynamics.MariaDBStorageEngine engine)
        {
            using (MySqlConnection connection = GetConnection())
            {
                connection.Open();
                foreach (string tablename in _mechanic.AllBaseTables())
                {
                    try
                    {
                        string commandText = "ALTER TABLE " + tablename + " ENGINE=" + engine.ToString();
                        MySqlCommand command = new MySqlCommand(commandText, connection);
                        _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                        command.CommandTimeout = 6000;
                        command.ExecuteNonQuery();
                    }
                    catch (MySqlException ex)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                }
                connection.Close();
            }
        }

        /// <summary>
        /// Changes The storage engine for MariaDB encoding tables
        /// </summary>
        public void SetMariaDBStorageEngineEncodedTables(Dynamics.MariaDBStorageEngine engine, int percentageToSet)
        {
            if (percentageToSet != 0)
            {
                int adjustedDBFormat = Convert.ToInt32((double)_amplifier * (double)((double)percentageToSet / 100));
                using (MySqlConnection connection = GetConnection())
                {
                    connection.Open();
                    foreach (string tablename in _mechanic.AllWebPageEncodingTablesReversed(adjustedDBFormat, _amplifier))
                    {
                        try
                        {
                            string commandText = "ALTER TABLE " + tablename + " ENGINE=" + engine.ToString();
                            MySqlCommand command = new MySqlCommand(commandText, connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, commandText, Message.MessageType.Command));
                            command.CommandTimeout = 6000;
                            command.ExecuteNonQuery();
                        }
                        catch (MySqlException ex)
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        }
                    }
                    connection.Close();
                }
            }
        }

        /// <summary>
        /// Insert a single entry into a table every 100ms. 
        /// </summary>
        public override int InsertPointInTimeWrite()
        {
            int rowsAffected = 0;
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    MySqlCommand pageIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_WriteID)", connection, transaction);
                    int WriteID = Convert.ToInt32(pageIDCommand.ExecuteScalar());
                    MySqlCommand command = new MySqlCommand("INSERT INTO PointInTimeWrite(WriteID, WrittenOn, WriteHash) VALUES (@WriteID, @WrittenOn, @WriteHash)", connection, transaction);
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    MySqlCommand command = new MySqlCommand("SELECT PageID, URL, HTML " +
                    "FROM   WebPages " +
                    "ORDER BY PageID", connection);
                    command.CommandTimeout = 600000;
                    connection.Open();
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int PageID = Convert.ToInt32(reader[0]);
                            SortedList<string, string> headerList = new SortedList<string, string>();
                            using (MySqlConnection connection2 = GetConnection())
                            {
                                MySqlCommand heaaderCommand = new MySqlCommand("SELECT HeaderKey, HeaderValue FROM WebPageHeaders WHERE PageID = '" + PageID + "'", connection2);
                                heaaderCommand.CommandTimeout = 600000;
                                connection2.Open();
                                using (MySqlDataReader headerReader = heaaderCommand.ExecuteReader())
                                {
                                    while (headerReader.Read())
                                    {
                                        headerList.Add(Convert.ToString(headerReader[0]), Convert.ToString(headerReader[1]));
                                    }
                                }
                                connection2.Close();
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
                        connection.Close();
                    }
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
       
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    string query = "SELECT WebPages.PageID,WebPages.URL,WebPages.HTML, WebPages.HeadersLength, WebPages.StatsLength, WebPages.TotalLength, "
                         + "WebPages.HREFS, WebPages.HASHURL, WebPages.HashHTML, ";

                    //Go Through Everything from webpageencoding lower to webpage encoding to higher 
                    //Inside that go through all of the tables ID's up to DB Format 
                    //If the tables in the query are more than 4 then go back 4 tableID or WebpageEncodingID's  

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
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                    Tuple<UInt64, UInt64>[] collation = new Tuple<UInt64, UInt64>[queriesToRun.Count];

                    CheckSchemaType();
                    if (GetSchemaType() == Dynamics.DatabaseSchema.WithIndexes ||
                    GetSchemaType() == Dynamics.DatabaseSchema.WithoutIndexes)
                    {
                        for (int i = 0; i < queriesToRun.Count; i++)
                        {
                            MySqlCommand command = new MySqlCommand(queriesToRun[i], connection);
                            command.Transaction = transaction;
                            command.CommandTimeout = 600;
                            using (MySqlDataReader dataReader = command.ExecuteReader())
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
                            MySqlConnection conn = GetConnection();
                            MySqlCommand command = new MySqlCommand(queriesToRun[i], conn);
                            command.CommandTimeout = 600;
                            conn.Open();
                            using (MySqlDataReader dataReader = command.ExecuteReader())
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
                            conn.Close();
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
                    MySqlCommand HeaderDataValues = new MySqlCommand(queryHeaderData, connection);
                    HeaderDataValues.Transaction = transaction;
                    HeaderDataValues.CommandTimeout = 300;
                    using (MySqlDataReader dataReader = HeaderDataValues.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
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
                            MySqlCommand command = new MySqlCommand(queriesToRun[i], connection);
                            command.Transaction = transaction;
                            command.CommandTimeout = 600;
                            using (MySqlDataReader dataReader = command.ExecuteReader())
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
                            MySqlConnection conn = GetConnection();
                            MySqlCommand command = new MySqlCommand(queriesToRun[i], conn);
                            command.CommandTimeout = 600;
                            conn.Open();
                            using (MySqlDataReader dataReader = command.ExecuteReader())
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
                            conn.Close();
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
                    MySqlCommand HeaderDataValues = new MySqlCommand(queryHeaderData, connection);
                    HeaderDataValues.Transaction = transaction;
                    HeaderDataValues.CommandTimeout = 300;
                    using (MySqlDataReader dataReader = HeaderDataValues.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                List<WebPage> webPages = new List<WebPage>();
                                using (MySqlConnection fopConnection = GetConnection())
                                {
                                    MySqlCommand readCommand = new MySqlCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                        TableName + " WHERE PageID = @PageID", fopConnection);
                                    readCommand.CommandTimeout = 12000;
                                    readCommand.Parameters.AddWithValue("@PageID", PageID);
                                    string URL = string.Empty;
                                    string HTML = string.Empty;
                                    string URL_B64S = string.Empty;
                                    string HTML_B64S = string.Empty;
                                    fopConnection.Open();
                                    using (MySqlDataReader commandReader = readCommand.ExecuteReader())
                                    {
                                        while (commandReader.Read())
                                        {
                                            WebPage existingPage = new WebPage();
                                            existingPage.URL = commandReader[0].ToString();
                                            existingPage.HTML = commandReader[1].ToString();
                                            existingPage.URLB64S = commandReader[2].ToString();
                                            existingPage.HTMLB64S = commandReader[3].ToString();
                                            webPages.Add(existingPage);
                                        }
                                    }
                                }

                                foreach (WebPage page in webPages)
                                {
                                    MySqlCommand updateCommand = new MySqlCommand("UPDATE " + TableName + " SET URL = @URL, Updates = Updates + 1, " +
                                                "HTML = @HTML, URL_B64S = @URL_B64S," +
                                                " HTML_B64S = @HTML_B64S, ModifiedOn = @ModifiedOn WHERE PageID = @PageID AND URL = @OriginalURL AND HTML =@OriginalHTML " +
                                                " AND URL_B64S = @OriginalURL_B64S AND HTML_B64S = @Original_HTML_B64S", connection, transaction);
                                    updateCommand.Parameters.AddWithValue("@URL", _mechanic.Encrypt(page.URL));
                                    updateCommand.Parameters.AddWithValue("@HTML", _mechanic.Encrypt(page.HTML));
                                    updateCommand.Parameters.AddWithValue("@URL_B64S", _mechanic.Encrypt(page.URLB64S));
                                    updateCommand.Parameters.AddWithValue("@HTML_B64S", _mechanic.Encrypt(page.HTMLB64S));
                                    updateCommand.Parameters.AddWithValue("@ModifiedOn", timestamp);
                                    updateCommand.Parameters.AddWithValue("@PageID", PageID);
                                    updateCommand.Parameters.AddWithValue("@OriginalURL", page.URL);
                                    updateCommand.Parameters.AddWithValue("@OriginalHTML", page.HTML);
                                    updateCommand.Parameters.AddWithValue("@OriginalURL_B64S", page.URLB64S);
                                    updateCommand.Parameters.AddWithValue("@Original_HTML_B64S", page.HTMLB64S);
                                    updateCommand.CommandTimeout = 300;
                                    rowsAffected += updateCommand.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    //Update Core Tables
                    foreach (string table in _mechanic.ReturnWebPageCoreTables)
                    {

                        MySqlCommand coreTableUpdateCommand = new MySqlCommand("UPDATE " + table + " SET Updates = Updates + 1," +
                            "                                           ModifiedOn = @ModifiedOn WHERE PageID = @PageID", connection, transaction);
                        coreTableUpdateCommand.Parameters.AddWithValue("@ModifiedOn", timestamp);
                        coreTableUpdateCommand.Parameters.AddWithValue("@PageID", PageID);
                        coreTableUpdateCommand.CommandTimeout = 300;
                        rowsAffected += coreTableUpdateCommand.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
                    {
                        _tokenSource.Cancel();
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    MySqlCommand command = new MySqlCommand(
                    "UPDATE WebPages SET LoadedOn = @LoadedOn, HeadersLength = @HeadersLength, StatsLength =  @StatsLength," +
                    " TotalLength = @TotalLength, HREFs = @HREFs, HashURL = @HashURL, HashHTML = @HashHTML, URL = @URL , HTML = @HTML," +
                    " ModifiedOn = NULL, Updates = 0 WHERE PageID = @PageID",
                    connection, transaction);
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
                            string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                            string tableName = _mechanic.NameWithID(encodingTables[i], tableID);

                            command = new MySqlCommand("DELETE FROM " + tableName + " WHERE PageID = " + PageID, connection, transaction);
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

                            List<string> splitHTMLColumns = _mechanic.ChunkString(convertedHTML, varcharMaxLength);
                            List<string> splitURLColumns = _mechanic.ChunkString(convertedURL, varcharMaxLength);
                            List<string> splitB64SHTMLColumns = _mechanic.ChunkString(convertedB64SHTML, varcharMaxLength);
                            List<string> splitB64SURLColumns = _mechanic.ChunkString(convertedB64SURL, varcharMaxLength);

                            List<Tuple<string, string, string>> webPageCharacterSplit = new List<Tuple<string, string, string>>();
                            //get the longest length object

                            int splitHTMLColumnLength = splitHTMLColumns.Count;
                            int splitURLColumnLength = splitURLColumns.Count;
                            int splitB64SHTMLColumnLength = splitB64SHTMLColumns.Count;
                            int splitB64SURLColumnLength = splitB64SURLColumns.Count;

                            int maxValue = new[] { splitHTMLColumnLength, splitURLColumnLength,
                                splitB64SHTMLColumnLength, splitB64SURLColumnLength }.Max();
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

                                command = new MySqlCommand(
                                "INSERT INTO " + tableName + " " +
                                "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                "          VALUES (@PageID, @URL_Length, @URL, @HTML_Length, @HTML, @URL_B64S_Length, @URL_B64S, @HTML_B64S_Length, @HTML_B64S) ",
                                connection, transaction);
                                command.Parameters.AddWithValue("@PageID", PageID); //                                                   Integer (int)                                      
                                //                                                   Columns ModifiedOn and Updates get default values
                                command.Parameters.AddWithValue("@URL_Length", urlLength); //                                                 Integer (int)
                                command.Parameters.AddWithValue("@URL", tURL); //                                                             Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_Length", htmlLength); //                                               Integer (int)
                                command.Parameters.AddWithValue("@HTML", tHTML); //                                                           Byte Array (binary)
                                command.Parameters.AddWithValue("@URL_B64S_Length", urlB64SLength); //                                        Integer (int)
                                command.Parameters.AddWithValue("@URL_B64S", tURL); //                                                        Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_B64S_Length", htmlB64SLength); //                                      Integer (int)
                                command.Parameters.AddWithValue("@HTML_B64S", tB64SHTML); //                                                  Byte Array (binary)
                                command.CommandTimeout = 600;
                                numberOfRowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }

                   

                    //delete all existing keys
                    command = new MySqlCommand("DELETE FROM WebPageHeaders WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        MySqlCommand headerIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_HeaderID)", connection, transaction);
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new MySqlCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (@HeaderID, @PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", newPage.PageID);
                        command.Parameters.AddWithValue("@HeaderID", headerID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new MySqlCommand("DELETE FROM WebPageStats WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            MySqlCommand statIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_StatID)", connection, transaction);
                            int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                            command = new MySqlCommand(
                                "INSERT INTO WebPageStats (StatID, PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (@StatID, @PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
                                connection,
                                transaction);
                            // Columns ModifiedOn and Updates get default values
                            command.Parameters.AddWithValue("@StatID", statID);
                            command.Parameters.AddWithValue("@PageID", newPage.PageID);
                            command.Parameters.AddWithValue("@CodeType", codeType);
                            command.Parameters.AddWithValue("@Length", newPage.StatsAsBytes[codeType].Length);
                            command.Parameters.AddWithValue("@Stats", newPage.StatsAsBytes[codeType]);
                            command.Parameters.AddWithValue("@B64S_Length", newPage.StatsAsBS64s[codeType].Length);
                            command.Parameters.AddWithValue("@B64S_Stats", newPage.StatsAsBS64s[codeType]);
                            command.CommandTimeout = 600;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        MySqlCommand updateCommand = new MySqlCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
                            " ModifiedOn = @ModifiedOn WHERE PageID = @PageID", connection, transaction);
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
                    {
                        _tokenSource.Cancel();
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                MySqlCommand readCommand = new MySqlCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                    TableName + " WHERE PageID = @PageID", connection, transaction);
                                readCommand.CommandTimeout = 12000;
                                readCommand.Parameters.AddWithValue("@PageID", PageID);
                                string URL = string.Empty;
                                string HTML = string.Empty;
                                string URL_B64S = string.Empty;
                                string HTML_B64S = string.Empty;
                                using (MySqlDataReader commandReader = readCommand.ExecuteReader())
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

                                MySqlCommand command = new MySqlCommand("UPDATE " + TableName + " SET URL = @URL, " +
                                    "HTML = @HTML, URL_B64S = @URL_B64S," +
                                    " HTML_B64S = @HTML_B64S WHERE PageID = @PageID", connection, transaction);
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
                        MySqlCommand command = new MySqlCommand("UPDATE " + TableName + " SET " +
                            " ModifiedOn = @ModifiedOn WHERE PageID = @PageID", connection, transaction);
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
                    {
                        _tokenSource.Cancel();
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
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
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    MySqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    MySqlCommand command = new MySqlCommand(
                    "UPDATE WebPages SET LoadedOn = @LoadedOn, HeadersLength = @HeadersLength, StatsLength =  @StatsLength," +
                    " TotalLength = @TotalLength, HREFs = @HREFs, HashURL = @HashURL, HashHTML = @HashHTML, URL = @URL , HTML = @HTML," +
                    " ModifiedOn = NULL, Updates = 0 WHERE PageID = @PageID",
                    connection, transaction);
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
                            command = new MySqlCommand(
                            "UPDATE " + tableName + " " +
                            " SET URL_Length = @URL_Length, URL = @URL, HTML_Length = @HTML_Length, HTML = @HTML, URL_B64S_Length = @URL_B64S_Length," +
                            " URL_B64S = @URL_B64S, HTML_B64S_Length = @HTML_B64S_Length, HTML_B64S = @HTML_B64S, " +
                            " ModifiedOn = NULL, Updates = 0 WHERE PageID = @PageID",
                            connection, transaction);
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

                    //delete all existing keys

                    command = new MySqlCommand("DELETE FROM WebPageHeaders WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        MySqlCommand headerIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_HeaderID)", connection, transaction);
                        int headerID = Convert.ToInt32(headerIDCommand.ExecuteScalar());
                        command = new MySqlCommand(
                       "INSERT INTO WebPageHeaders (HeaderID, PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (@HeaderID, @PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", newPage.PageID);
                        command.Parameters.AddWithValue("@HeaderID", headerID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new MySqlCommand("DELETE FROM WebPageStats WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            MySqlCommand statIDCommand = new MySqlCommand("SELECT NEXTVAL(Seq_StatID)", connection, transaction);
                            int statID = Convert.ToInt32(statIDCommand.ExecuteScalar());
                            command = new MySqlCommand(
                                "INSERT INTO WebPageStats (StatID, PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (@StatID, @PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
                                connection,
                                transaction);
                            // Columns ModifiedOn and Updates get default values
                            command.Parameters.AddWithValue("@StatID", statID);
                            command.Parameters.AddWithValue("@PageID", newPage.PageID);
                            command.Parameters.AddWithValue("@CodeType", codeType);
                            command.Parameters.AddWithValue("@Length", newPage.StatsAsBytes[codeType].Length);
                            command.Parameters.AddWithValue("@Stats", newPage.StatsAsBytes[codeType]);
                            command.Parameters.AddWithValue("@B64S_Length", newPage.StatsAsBS64s[codeType].Length);
                            command.Parameters.AddWithValue("@B64S_Stats", newPage.StatsAsBS64s[codeType]);
                            command.CommandTimeout = 600;
                            numberOfRowsAffected += command.ExecuteNonQuery();
                        }
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        MySqlCommand updateCommand = new MySqlCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
                            " ModifiedOn = @ModifiedOn WHERE PageID = @PageID", connection, transaction);
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
                    if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
                    {
                        _tokenSource.Cancel();
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
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
                using (MySqlConnection connection = GetConnection())
                {
                    connection.Open();
                    MySqlCommand command = new MySqlCommand("UPDATE Imports SET " +
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
                if (ex.Message.ToLower().Contains("access denied for user")
                        || ex.Message.ToLower().Contains("unable to connect")
                        || ex.Message.ToLower().Contains("unknown database"))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return success;
        }

        /// <summary>
        /// Update statistics and rebuild indexes
        /// </summary>
        /// <param name="numberOfThreads"></param>
        public override void VendorAdvancedOperations(int numberOfThreads)
        {
            try
            {
                List<string> tablesToOptimise = new List<string>();
                using (MySqlConnection connection = GetConnection())
                {

                    MySqlCommand getIndexNumbers = new MySqlCommand("select table_name, " +
                                                                "round(data_length / 1024 / 1024) as data_length_mb, " +
                                                                "round(data_free / 1024 / 1024) as data_free_mb " +
                                                                "from information_schema.tables " +
                                                                "where round(data_free / 1024 / 1024) > 500 " +
                                                                " order by data_free_mb; ", connection);
                    connection.Open();
                    using (MySqlDataReader dataReader = getIndexNumbers.ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            tablesToOptimise.Add(Convert.ToString(dataReader[0]));
                        }
                    }
                    if (tablesToOptimise.Count > 0)
                    {
                        string commandString = "OPTIMIZE TABLE";
                        foreach (string table in tablesToOptimise)
                        {
                            commandString = commandString + " " + table + ",";
                        }

                        commandString = commandString.Remove(commandString.Length - 1);
                        MySqlCommand command = new MySqlCommand(commandString, connection);
                        command.CommandTimeout = 600000;
                        command.ExecuteNonQuery();
                    }
                    connection.Close();
                }
            }
            catch (MySqlException ex)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        /// <summary>
        /// Forces a FLUSH tables scenario for MariaDB
        /// </summary>
        public override void VendorComplexOperations()
        {
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    connection.InfoMessage += delegate (object sender, MySqlInfoMessageEventArgs e)
                    {
                        
                        //for (int i = 0; i < e.Errors i++)
                        //{
                        //    _messageQueue.AddMessage(new Message(DateTime.Now, e.errors[i].Message, Message.MessageType.Info));
                        //}
                    };
                
                    foreach (string table in _mechanic.AllTables(_amplifier))
                    {
                        MySqlCommand command = new MySqlCommand("FLUSH TABLES " + table, connection);
                        command.CommandTimeout = 600000;
                        command.ExecuteNonQuery();
                    }
                    connection.Close();
                }
                catch (Exception ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
                
            }
        }
        /// <summary>
        /// For MariaDB the CHECK Table option is run for each table in the database/schema
        /// </summary>
        public override void VendorConsistencyCheck()
        {
            using (MySqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    connection.InfoMessage += delegate (object sender, MySqlInfoMessageEventArgs e)
                    {
                    //for (int i = 0; i < e.errors.Length; i++)
                    //{
                    //    _messageQueue.AddMessage(new Message(DateTime.Now, e.errors[i].Message, Message.MessageType.Info));
                    //}
                    };

                    foreach (string table in _mechanic.AllTables(_amplifier))
                    {
                        MySqlCommand command = new MySqlCommand("CHECK " + table + " EXTENDED", connection);
                        command.CommandTimeout = 600000;
                        command.ExecuteNonQuery();
                    }
                    connection.Close();
                }
                catch (Exception ex)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
                
            }
        }
    }
}
