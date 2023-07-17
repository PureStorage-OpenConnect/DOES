using DOES.DataEngine.FileOperations;
using DOES.DataEngine.Resources;
using DOES.Shared.Debug;
using DOES.Shared.Resources;
using Npgsql;
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

namespace DOES.DataEngine.Operations
{
    /// <summary>
    /// This class handles PostgreSQL database operations. 
    /// </summary>
    public class PostgreSQLDB : DataVendor
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
        private int varcharMaxLength = 512;
        private CancellationTokenSource _tokenSource;


        /// <summary>
        /// Instantiates the PostgreSQL class. 
        /// </summary>
        public PostgreSQLDB(string hostname, string dbname, string username, string password, int amplifier,
            int port, Dynamics.DatabaseSchema schema, Mechanic mechanic, MessageQueue messages)
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
        }

        /// <summary>
        /// Instantiates the PostgreSQL class. 
        /// </summary>
        public PostgreSQLDB(string hostname, string dbname, string username, string password, int amplifier,
            int port, Mechanic mechanic, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _amplifier = amplifier;
            _port = port;
            _messageQueue = messages;
            _mechanic = mechanic;
        }

        /// <summary>
        /// Instantiates the PostgreSQL class. 
        /// </summary>
        public PostgreSQLDB(string hostname, string dbname, string username, string password, int port,
            Mechanic mechanic, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _port = port;
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    NpgsqlCommand command = new NpgsqlCommand("SELECT ImportedWebPages, ImportedAllWebPages from Imports WHERE FileName = '" + filename + "'", connection);
                    command.CommandTimeout = 600;
                    connection.Open();
                    ImportHandler importData;
                    int importedWebPages = 0;
                    bool importedAllWebPages = false;
                    bool found = false;
                    using (NpgsqlDataReader dataReader = command.ExecuteReader())
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlCommand command = new NpgsqlCommand("SELECT SchemaType FROM Configuration", connection);
                    using (NpgsqlDataReader reader = command.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
                using (NpgsqlConnection connection = GetConnection())
                {
                    connection.Open();
                    NpgsqlCommand command = new NpgsqlCommand("INSERT INTO Imports (FileName, ImportedWebPages, ImportedAllWebPages) VALUES ('" +
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
                if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
            using (NpgsqlConnection conn = GetConnection())
            {
                //Base Objects
                try
                {
                    conn.Open();
                    foreach (string table in _mechanic.AllBaseTables())
                    {
                        NpgsqlCommand command = new NpgsqlCommand("SELECT * FROM " + table + " LIMIT 1", conn);
                        command.ExecuteScalar();
                    }
                    conn.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
                                    if (SQLString != string.Empty)
                                    {
                                        NpgsqlCommand command = new NpgsqlCommand(SQLString, conn);
                                        try
                                        {
                                            _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                                            command.ExecuteNonQuery();
                                            success = true;
                                        }
                                        catch (NpgsqlException sqlex)
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
                            NpgsqlCommand firstPointcommand = new NpgsqlCommand("INSERT INTO Configuration (SchemaType, CreatedOn) VALUES" +
                                " (@SchemaType, @CreatedOn)", conn);

                            firstPointcommand.Parameters.AddWithValue("@SchemaType", _schema.ToString());
                            firstPointcommand.Parameters.AddWithValue("@CreatedOn", DateTime.Now);
                            firstPointcommand.ExecuteNonQuery();
                            conn.Close();
                            success = true;
                        }
                        catch (NpgsqlException sql3)
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
                            NpgsqlCommand command = new NpgsqlCommand("SELECT * FROM " + table + " LIMIT 1", conn);
                            command.ExecuteScalar();
                        }
                        conn.Close();
                    }
                    catch (NpgsqlException ex1)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex1.Message, Message.MessageType.Warning));
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
                                    for (int TableID = 0; (TableID < Convert.ToInt32((MySqlCommandString.Contains("_X00") ? _amplifier : 0))); TableID++)
                                    {
                                        try
                                        {
                                            NpgsqlCommand command = new NpgsqlCommand(_mechanic.NameWithID(MySqlCommandString, TableID), conn);
                                            _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                                            command.ExecuteNonQuery();
                                        }
                                        catch (NpgsqlException sqle)
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
            NpgsqlTransaction transaction;
            using (NpgsqlConnection connection = GetConnection())
            {
                connection.Open();
                transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                try
                {
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        NpgsqlCommand command = new NpgsqlCommand("DELETE FROM " + TableName + " WHERE PageID" +
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
        public override bool DestroyObjects(Dynamics.ClearingType clearingType)
        {
            bool success = false;
            string operation = "DROP";

            if (clearingType == Dynamics.ClearingType.Drop)
            {
                operation = "DROP";
            }
            else if (clearingType == Dynamics.ClearingType.Truncate)
            {
                operation = "TRUNCATE";
            }

            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    foreach (string table in _mechanic.AllTables(_amplifier))
                    {
                        try
                        {
                            NpgsqlCommand command = new NpgsqlCommand("SELECT * FROM " + table + " LIMIT 1", connection);
                            command.ExecuteScalar();
                            command = new NpgsqlCommand(operation + " TABLE " + table + "", connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                            command.CommandTimeout = 600;
                            command.ExecuteNonQuery();
                        }
                        catch (NpgsqlException ex)
                        {
                            success = false;
                            if (ex.Message.ToLower().Contains("does not exist"))
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    }
                    else
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
                }
            }
            return success;
        }

        /// <summary>
        /// Return the PostgreSQL database connection object. 
        /// </summary>
        public override dynamic GetConnection()
        {
            string  _connectionString = "Host=" + _hostname + ";Username=" + _userName + ";Password=" + _password + ";Database=" + _databaseName + ";Port=" + _port + ";Minimum Pool Size=10; Maximum Pool Size=128";
            NpgsqlConnection connection = new NpgsqlConnection(_connectionString);
            return connection;
        }

        /// <summary>
        /// Return the database type. 
        /// </summary>
        public override Dynamics.Database GetDatabaseType()
        {
            return Dynamics.Database.PostgreSQL;
        }

        /// <summary>
        /// Return the local controller mechanic. 
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
            NpgsqlTransaction transaction;
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlCommand command = new NpgsqlCommand("SELECT PageID, TotalLength from WebPages ORDER BY PageID", connection);
                    transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    command.Transaction = transaction;
                    using (NpgsqlDataReader reader = command.ExecuteReader())
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
                NpgsqlConnection connection = GetConnection();
                NpgsqlCommand commandRowCountNN = new NpgsqlCommand("SELECT COUNT(*) FROM WebPages WHERE ModifiedOn IS NOT NULL", connection);
                NpgsqlCommand commandRowCountTotal = new NpgsqlCommand("SELECT COUNT(*) FROM WebPages", connection);
                NpgsqlCommand commandTotalSize = new NpgsqlCommand("SELECT SUM(TotalLength) FROM WebPages", connection);
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
        public override int     InsertCharacterisedWebPage(WebPage page)
        {
            int numberOfRowsAffected = 0;
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    //NpgsqlCommand encodingCommand = new NpgsqlCommand("set client_encoding = 'UTF32'", connection);
                    //encodingCommand.ExecuteNonQuery();


                    NpgsqlCommand command = new NpgsqlCommand(
                    "INSERT INTO WebPages (LoadedOn, HeadersLength, StatsLength, TotalLength, HREFs, HashURL, HashHTML, URL, HTML) " +
                                          "VALUES (@LoadedOn,  @HeadersLength,  @StatsLength,  @TotalLength,  @HREFs,  @HashURL,  @HashHTML,  @URL,  @HTML) " +
                    "RETURNING PageID;",
                    connection,
                    transaction);
                    // Columns PageID is an identity
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
                    page.PageID = Convert.ToInt32(command.ExecuteScalar()); //                                        Insert and get the unique PageID - One row is inserted
                    numberOfRowsAffected++;


                    if (_amplifier != 0)
                    {
                        //Contains all 4 encoded table sets 
                        int numberOfEncoders = _mechanic.ReturnWebPageEncodingtables.Length;
                        NpgsqlCommand[][][] allEncodedWebPagesCommands = new NpgsqlCommand[numberOfEncoders][][];
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            //contains all the tables for the subset of the encoding up to the amplifier 
                            allEncodedWebPagesCommands[i] = new NpgsqlCommand[_amplifier][];
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


                                //split the string if it is larger than varcharMaxLength

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
                                NpgsqlCommand[] commandStruct = new NpgsqlCommand[maxValue];
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
                                        tURL = splitURLColumns[ci].Replace("\0", string.Empty);
                                        urlLength = tURL.Length;
                                    }

                                    if (splitHTMLColumns.Count > ci)
                                    {
                                        tHTML = splitHTMLColumns[ci].Replace("\0", string.Empty);
                                        htmlLength = tHTML.Length;
                                    }

                                    if (splitB64SHTMLColumns.Count > ci)
                                    {
                                        tB64SHTML = splitB64SHTMLColumns[ci].Replace("\0", string.Empty);
                                        htmlB64SLength = tB64SHTML.Length;
                                    }

                                    if (splitB64SURLColumns.Count > ci)
                                    {
                                        tB64SURL = splitB64SURLColumns[ci].Replace("\0", string.Empty);
                                        urlB64SLength = tB64SURL.Length;
                                    }

                                    command = new NpgsqlCommand(
                                    "INSERT INTO " + tableName + " " +
                                    "                 (PageID, URL_Length, URL, HTML_Length, HTML, URL_B64S_Length, URL_B64S, HTML_B64S_Length, HTML_B64S) " +
                                    "          VALUES (@PageID, @URL_Length, @URL, @HTML_Length, @HTML, @URL_B64S_Length, @URL_B64S, @HTML_B64S_Length, @HTML_B64S) ",
                                    connection);
                                    command.Transaction = transaction;
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
                        ParallelOptions po = new ParallelOptions();
                        po.MaxDegreeOfParallelism = 1;
                        Parallel.For(0, allEncodedWebPagesCommands.Length, po, encoder =>
                        {
                            //second is the number of tables per encoder 
                            Parallel.For(0, allEncodedWebPagesCommands[encoder].Length, po, tableid =>
                            {
                                //third are the actual tables - execute here
                                for (int commandIndex = 0; commandIndex < allEncodedWebPagesCommands[encoder][tableid].Length; commandIndex++)
                                {
                                    try
                                    {
                                        int numberOfAffectedRows = allEncodedWebPagesCommands[encoder][tableid][commandIndex].ExecuteNonQuery();
                                        numberOfParalellRowsAffected.Add(numberOfAffectedRows);
                                    }
                                    catch (NpgsqlException ex)
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
                        command = new NpgsqlCommand(
                       "INSERT INTO WebPageHeaders (PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (@PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
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
                            command = new NpgsqlCommand(
                                "INSERT INTO WebPageStats (PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (@PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
                                connection,
                                transaction);
                            // Columns ModifiedOn and Updates get default values
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    NpgsqlCommand command = new NpgsqlCommand(
                    "INSERT INTO WebPages (LoadedOn, HeadersLength, StatsLength, TotalLength, HREFs, HashURL, HashHTML, URL, HTML) " +
                                          "VALUES (@LoadedOn,  @HeadersLength,  @StatsLength,  @TotalLength,  @HREFs,  @HashURL,  @HashHTML,  @URL,  @HTML) " +
                    "RETURNING PageID",
                    connection,
                    transaction);
                    // Columns PageID is an identity
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                                // Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(page.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(page.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(page.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(page.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(page.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML", page.HashHTML); //                               Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", page.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", page.HTMLBinary); //                                       The fist 8096 characters of HTML  
                    command.CommandTimeout = 300;
                    page.PageID = Convert.ToInt32(command.ExecuteScalar()); //                                        Insert and get the unique PageID - One row is inserted
                    numberOfRowsAffected++;

                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string tableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                command = new NpgsqlCommand(
                                "INSERT INTO " + tableName + " " +
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
                        command = new NpgsqlCommand(
                       "INSERT INTO WebPageHeaders (PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (@PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
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
                            command = new NpgsqlCommand(
                                "INSERT INTO WebPageStats (PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                          "VALUES (@PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
                                connection,
                                transaction);
                            // Columns ModifiedOn and Updates get default values
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
        /// Insert a single entry into a table every 100ms. 
        /// </summary>
        public override int InsertPointInTimeWrite()
        {
            int rowsAffected = 0;
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    NpgsqlCommand command = new NpgsqlCommand("INSERT INTO PointInTimeWrite(WrittenOn, WriteHash) VALUES (@WrittenOn, @WriteHash)", connection, transaction);
                    DateTime writeTime = DateTime.Now;
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    NpgsqlCommand command = new NpgsqlCommand("SELECT PageID, URL, HTML " +
                    "FROM   Webpages " +
                    "ORDER BY PageID", connection);
                    command.CommandTimeout = 600000;
                    connection.Open();
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int PageID = Convert.ToInt32(reader[0]);
                            SortedList<string, string> headerList = new SortedList<string, string>();
                            using (NpgsqlConnection connection2 = GetConnection())
                            {
                                NpgsqlCommand heaaderCommand = new NpgsqlCommand("SELECT HeaderKey, HeaderValue FROM WebPageHeaders WHERE PageID = '" + PageID + "'", connection2);
                                heaaderCommand.CommandTimeout = 600000;
                                connection2.Open();
                                using (NpgsqlDataReader headerReader = heaaderCommand.ExecuteReader())
                                {
                                    while (headerReader.Read())
                                    {
                                        headerList.Add(Convert.ToString(headerReader[0]), Convert.ToString(headerReader[1]));
                                    }
                                }
                                connection2.Close();
                            }
                            byte[] bHTML = (byte[])reader[2];
                            WebPage webpage = new WebPage(PageID, Convert.ToString(reader[1]), Encoding.ASCII.GetString(bHTML), 
                                headerList);
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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

            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    string query = "SELECT WebPages.PageID,WebPages.URL,WebPages.HTML, WebPages.HeadersLength, WebPages.StatsLength, WebPages.TotalLength, "
                         + "WebPages.HREFS, WebPages.HASHURL, WebPages.HashHTML, ";

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
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                    Tuple<UInt64, UInt64>[] collation = new Tuple<UInt64, UInt64>[queriesToRun.Count];

                    CheckSchemaType();
                    if (GetSchemaType() == Dynamics.DatabaseSchema.WithIndexes ||
                    GetSchemaType() == Dynamics.DatabaseSchema.WithoutIndexes)
                    {
                        for (int i = 0; i < queriesToRun.Count; i++)
                        {
                            NpgsqlCommand command = new NpgsqlCommand(queriesToRun[i], connection);
                            command.Transaction = transaction;
                            command.CommandTimeout = 600;
                            using (NpgsqlDataReader dataReader = command.ExecuteReader())
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
                                    command.Cancel();
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
                            NpgsqlConnection conn = GetConnection();
                            NpgsqlCommand command = new NpgsqlCommand(queriesToRun[i], conn);
                            command.CommandTimeout = 600;
                            conn.Open();
                            using (NpgsqlDataReader dataReader = command.ExecuteReader())
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
                    NpgsqlCommand HeaderDataValues = new NpgsqlCommand(queryHeaderData, connection);
                    HeaderDataValues.Transaction = transaction;
                    HeaderDataValues.CommandTimeout = 300;
                    using (NpgsqlDataReader dataReader = HeaderDataValues.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
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
                            NpgsqlCommand command = new NpgsqlCommand(queriesToRun[i], connection);
                            command.Transaction = transaction;
                            command.CommandTimeout = 600;
                            using (NpgsqlDataReader dataReader = command.ExecuteReader())
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
                                    command.Cancel();
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
                            NpgsqlConnection conn = GetConnection();
                            NpgsqlCommand command = new NpgsqlCommand(queriesToRun[i], conn);
                            command.CommandTimeout = 600;
                            conn.Open();
                            using (NpgsqlDataReader dataReader = command.ExecuteReader())
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
                    NpgsqlCommand HeaderDataValues = new NpgsqlCommand(queryHeaderData, connection);
                    HeaderDataValues.Transaction = transaction;
                    HeaderDataValues.CommandTimeout = 300;
                    using (NpgsqlDataReader dataReader = HeaderDataValues.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                List<WebPage> webPages = new List<WebPage>();
                                using (NpgsqlConnection fopConnection = GetConnection())
                                {
                                    NpgsqlCommand readCommand = new NpgsqlCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                        TableName + " WHERE PageID = @PageID", fopConnection);
                                    readCommand.CommandTimeout = 12000;
                                    readCommand.Parameters.AddWithValue("@PageID", PageID);
                                    string URL = string.Empty;
                                    string HTML = string.Empty;
                                    string URL_B64S = string.Empty;
                                    string HTML_B64S = string.Empty;
                                    fopConnection.Open();
                                    using (NpgsqlDataReader commandReader = readCommand.ExecuteReader())
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
                                    NpgsqlCommand updateCommand = new NpgsqlCommand("UPDATE " + TableName + " SET URL = @URL, Updates = Updates + 1, " +
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

                        NpgsqlCommand coreTableUpdateCommand = new NpgsqlCommand("UPDATE " + table + " SET Updates = Updates + 1," +
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    //DateTime modificationDate = DateTime.Now;
                    NpgsqlCommand command = new NpgsqlCommand(
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
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string tableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                command = new NpgsqlCommand("DELETE FROM " + tableName + " WHERE PageID = " + PageID, connection, transaction);
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
                                        tURL = splitURLColumns[ci].Replace("\0", string.Empty);
                                        urlLength = tURL.Length;
                                    }

                                    if (splitHTMLColumns.Count > ci)
                                    {
                                        tHTML = splitHTMLColumns[ci].Replace("\0", string.Empty);
                                        htmlLength = tHTML.Length;
                                    }

                                    if (splitB64SHTMLColumns.Count > ci)
                                    {
                                        tB64SHTML = splitB64SHTMLColumns[ci].Replace("\0", string.Empty);
                                        htmlB64SLength = tB64SHTML.Length;
                                    }

                                    if (splitB64SURLColumns.Count > ci)
                                    {
                                        tB64SURL = splitB64SURLColumns[ci].Replace("\0", string.Empty);
                                        urlB64SLength = tB64SURL.Length;
                                    }

                                    command = new NpgsqlCommand(
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
                    }
                    //delete all existing keys
                    command = new NpgsqlCommand("DELETE FROM WebPageHeaders WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();


                    NpgsqlCommand checkHeadersPresent = new NpgsqlCommand("SELECT * FROM WebPageHeaders WHERE PageID = " +
                        PageID, connection, transaction);

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        command = new NpgsqlCommand(
                       "INSERT INTO WebPageHeaders (PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (@PageID, @HeaderKeyLength, @HeaderKey, @HeaderValueLength, @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", newPage.PageID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new NpgsqlCommand("DELETE FROM WebPageStats WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                    {
                        command = new NpgsqlCommand(
                            "INSERT INTO WebPageStats (PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                      "VALUES (@PageID, @CodeType, @Length, @Stats, @B64S_Length, @B64S_Stats)",
                            connection,
                            transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", newPage.PageID);
                        command.Parameters.AddWithValue("@CodeType", codeType);
                        command.Parameters.AddWithValue("@Length", newPage.StatsAsBytes[codeType].Length);
                        command.Parameters.AddWithValue("@Stats", newPage.StatsAsBytes[codeType]);
                        command.Parameters.AddWithValue("@B64S_Length", newPage.StatsAsBS64s[codeType].Length);
                        command.Parameters.AddWithValue("@B64S_Stats", newPage.StatsAsBS64s[codeType]);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        NpgsqlCommand updateCommand = new NpgsqlCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                NpgsqlCommand readCommand = new NpgsqlCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                    TableName + " WHERE PageID = @PageID", connection);
                                readCommand.Transaction = transaction;
                                readCommand.CommandTimeout = 12000;
                                readCommand.Parameters.AddWithValue("@PageID", PageID);
                                string URL = string.Empty;
                                string HTML = string.Empty;
                                string URL_B64S = string.Empty;
                                string HTML_B64S = string.Empty;
                                using (NpgsqlDataReader commandReader = readCommand.ExecuteReader())
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

                                NpgsqlCommand command = new NpgsqlCommand("UPDATE " + TableName + " SET URL = @URL, " +
                                    "HTML = @HTML, URL_B64S = @URL_B64S," +
                                    " HTML_B64S = @HTML_B64S WHERE PageID = @PageID", connection);
                                command.Transaction = transaction;
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
                        NpgsqlCommand command = new NpgsqlCommand("UPDATE " + TableName + " SET " +
                            " ModifiedOn = @ModifiedOn WHERE PageID = @PageID", connection);
                        command.Transaction = transaction;
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    NpgsqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                
                    NpgsqlCommand command = new NpgsqlCommand(
                    "UPDATE WebPages SET LoadedOn = @LoadedOn, HeadersLength = @HeadersLength, StatsLength =  @StatsLength," +
                    " TotalLength = @TotalLength, HREFs = @HREFs, HashURL = @HashURL, HashHTML = @HashHTML, URL = @URL , HTML = @HTML," +
                    " ModifiedOn = NULL, Updates = 0 WHERE PageID = @PageID",
                    connection);
                    command.Transaction = transaction;
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
                            command = new NpgsqlCommand(
                            "UPDATE " + tableName + " " +
                            " SET URL_Length = @URL_Length, URL = @URL, HTML_Length = @HTML_Length, HTML = @HTML, URL_B64S_Length = @URL_B64S_Length," +
                            " URL_B64S = @URL_B64S, HTML_B64S_Length = @HTML_B64S_Length, HTML_B64S = @HTML_B64S, " +
                            " ModifiedOn = NULL, Updates = 0 WHERE PageID = @PageID",
                            connection);
                            command.Transaction = transaction;
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

                    command = new NpgsqlCommand("DELETE FROM WebPageHeaders WHERE PageID = " + PageID, connection);
                    command.Transaction = transaction;
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        command = new NpgsqlCommand(
                       "INSERT INTO WebPageHeaders (PageID, HeaderKeyLength, HeaderKey, HeaderValueLength, HeaderValue) " +
                                                   "VALUES (@PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", newPage.PageID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new NpgsqlCommand("DELETE FROM WebPageStats WHERE PageID = " + PageID, connection);
                    command.Transaction = transaction;
                    command.ExecuteNonQuery();

                    for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                    {
                        command = new NpgsqlCommand(
                            "INSERT INTO WebPageStats (PageID, CodeType, Length, Stats, B64S_Length, B64S_Stats) " +
                                                      "VALUES (@PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
                            connection,
                            transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", newPage.PageID);
                        command.Parameters.AddWithValue("@CodeType", codeType);
                        command.Parameters.AddWithValue("@Length", newPage.StatsAsBytes[codeType].Length);
                        command.Parameters.AddWithValue("@Stats", newPage.StatsAsBytes[codeType]);
                        command.Parameters.AddWithValue("@B64S_Length", newPage.StatsAsBS64s[codeType].Length);
                        command.Parameters.AddWithValue("@B64S_Stats", newPage.StatsAsBS64s[codeType]);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        NpgsqlCommand updateCommand = new NpgsqlCommand("UPDATE " + TableName + " SET Updates = Updates + 1," +
                            " ModifiedOn = @ModifiedOn WHERE PageID = @PageID", connection);
                        command.Transaction = transaction;
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
                    if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
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
                using (NpgsqlConnection connection = GetConnection())
                {
                    connection.Open();
                    NpgsqlCommand command = new NpgsqlCommand("UPDATE Imports SET " +
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
                if (ex.Message.ToLower().Contains("no such host is known")
                        || ex.Message.ToLower().Contains("password authentication failed")
                        || ex.Message.ToLower().Contains("3d000")
                        || ex.Message.ToLower().Contains("no pg_hba.conf entry"))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return success;
        }

        /// <summary>
        /// Performs  VACUUM operations on tables.
        /// </summary>
        public override void VendorAdvancedOperations(int numberOfThreads)
        {
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    foreach (string table in _mechanic.AllTables(_amplifier))
                    {
                        NpgsqlCommand command = new NpgsqlCommand("VACUUM(FULL) " + table, connection);
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
        /// Recreates all indexes on each table.
        /// </summary>
        public override void VendorComplexOperations()
        {
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    foreach (string table in _mechanic.AllTables(_amplifier))
                    {
                        NpgsqlCommand command = new NpgsqlCommand("REINDEX TABLE " + table, connection);
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
        /// Performs ANALYZE operations on tables. 
        /// </summary>
        public override void VendorConsistencyCheck()
        {
            using (NpgsqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    foreach (string table in _mechanic.AllTables(_amplifier))
                    {
                        NpgsqlCommand command = new NpgsqlCommand("ANALYZE " + table , connection);
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
