using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DOES.Shared.Debug;
using DOES.DataEngine.FileOperations;
using DOES.DataEngine.Resources;
using DOES.Shared.Resources;
using System.IO;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Threading;

namespace DOES.DataEngine.Operations
{
    /// <summary>
    /// This class handles Microsoft SQL server database operations. 
    /// </summary>
    public class MicrosoftSQL : DataVendor
    {
        private string _hostname;
        private string _databaseName;
        private string _userName;
        private string _password;
        private string _instance;
        private int _amplifier;
        private int _port;
        private Dynamics.DatabaseSchema _schema;
        private Mechanic _mechanic;
        private MessageQueue _messageQueue;
        private CancellationTokenSource _tokenSource;

        /// <summary>
        /// Instantiates the MicrosoftSQL class. 
        /// </summary>
        public MicrosoftSQL(string hostname, string dbname, string instance, string username, 
            string password, int port, int amplifier, Dynamics.DatabaseSchema schema, Mechanic mechanic, MessageQueue queue)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _port = port;
            _amplifier = amplifier;
            _instance = instance;
            _schema = schema;
            _mechanic = mechanic;
            _messageQueue = queue;
        }

        /// <summary>
        /// Instantiates the MicrosoftSQL class. 
        /// </summary>
        public MicrosoftSQL(string hostname, string dbname, string instance, string username, 
            string password, int port, Mechanic mechanic, MessageQueue queue)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _port = port;
            _instance = instance;
            _mechanic = mechanic;
            _messageQueue = queue;
        }

        /// <summary>
        /// Instantiates the MicrosoftSQL class. 
        /// </summary>
        public MicrosoftSQL(string hostname, string dbname, string instance, string username, 
            string password, int port, int amplifier, Mechanic mechanic, MessageQueue queue)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _port = port;
            _amplifier = amplifier;
            _instance = instance;
            _mechanic = mechanic;
            _messageQueue = queue;
        }

        /// <summary>
        /// Check the import history for a data file. 
        /// </summary>
        public override ImportHandler CheckFileImportHistory(string filename)
        {
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    SqlCommand command = new SqlCommand("SELECT [ImportedWebPages], [ImportedAllWebPages] from [Imports] WHERE [FileName] = '" + filename + "'", connection);
                    command.CommandTimeout = 600;
                    connection.Open();
                    ImportHandler importData;
                    int importedWebPages = 0;
                    bool importedAllWebPages = false;
                    bool found = false;
                    using (SqlDataReader dataReader = command.ExecuteReader())
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
        /// Create the database objects from the base and extension templates. 
        /// </summary>
        public override bool CreateObjects(List<string> baseSchemaObjects,
            List<string> extensionSchemaObjects)
        {
            bool success = false;
            using (SqlConnection conn = GetConnection())
            {
                //Base Objects
                try
                {
                    conn.Open();
                    foreach (string table in _mechanic.AllBaseTables())
                    {
                        SqlCommand command = new SqlCommand("SELECT TOP 1 * FROM [" + table + "]", conn);
                        command.ExecuteScalar();
                    }
                    conn.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
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
                                SqlCommand command = new SqlCommand(SQLString, conn);
                                try
                                {
                                    _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                                    command.ExecuteNonQuery();
                                    success = true;
                                }
                                catch (Exception sqlex)
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
                        SqlCommand firstPointcommand = new SqlCommand("INSERT INTO Configuration (SchemaType, CreatedOn) VALUES" +
                            " (@SchemaType, @CreatedOn)", conn);

                        firstPointcommand.Parameters.AddWithValue("@SchemaType", _schema.ToString());
                        firstPointcommand.Parameters.AddWithValue("@CreatedOn", DateTime.Now);
                        firstPointcommand.ExecuteNonQuery();
                        conn.Close();
                        success = true;
                    }
                    catch (SqlException sql3)
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
                        SqlCommand command = new SqlCommand("SELECT TOP 1 * FROM [" + table + "]", conn);
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
                                        SqlCommand command = new SqlCommand(_mechanic.NameWithID(sqlCommandString, TableID), conn);
                                        _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                                        command.ExecuteNonQuery();
                                    }
                                    catch(SqlException sqle)
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
        /// Delete a webpage from the database objects. 
        /// </summary>
        public override int DeleteWebPage(int PageID)
        {
            int rowsAffected = 0;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    foreach (string TableName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        SqlCommand command = new SqlCommand("DELETE FROM [" + TableName + "] WHERE [PageID] = '" +
                            PageID.ToString() + "'", connection, transaction);
                        command.CommandTimeout = 300;
                        rowsAffected += command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
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
        /// Destroy the database objects. 
        /// </summary>
        public override bool DestroyObjects(Dynamics.ClearingType ClearingType)
        {
            bool success = false;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    string operation = "DROP";

                    if (ClearingType == Dynamics.ClearingType.Drop)
                    {
                        operation = "DROP";
                    }
                    else if (ClearingType == Dynamics.ClearingType.Truncate)
                    {
                        operation = "TRUNCATE";
                    }
                    foreach (string table in _mechanic.AllTables(_amplifier))
                    {
                        try
                        {
                            SqlCommand command = new SqlCommand("SELECT COUNT(*) FROM [" + table + "]", connection);
                            command.ExecuteScalar();
                            command = new SqlCommand(operation + " TABLE [" + table + "]", connection);
                            _messageQueue.AddMessage(new Message(DateTime.Now, command.CommandText, Message.MessageType.Command));
                            command.CommandTimeout = 600;
                            command.ExecuteNonQuery();
                        }
                        catch (SqlException ex)
                        {
                            success = false;
                            if(ex.Message.ToLower().Contains("invalid object name"))
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                            }
                            else
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                            }
                        }
                        success = true;
                    }
                    connection.Close();
                }
                catch (Exception ex)
                {
                    success = false;
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
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
        /// Get the Microsoft SQL connection
        /// </summary>
        public override dynamic GetConnection()
        {
            string _connectionString;
            if (_instance != null)
            {
                if (_userName == null && _password == null)
                {
                    _connectionString = @"Data Source=" + _hostname + "," + _port + "\\" + _instance + ";Initial Catalog=" + _databaseName +
                        ";MultipleActiveResultSets = true;Max Pool Size=1000;Integrated Security=False" + "; Application Name = D.O.E.S DataEngine; ";
                }
                else
                {
                    _connectionString = @"Data Source=" + _hostname + "," + _port + "\\" + _instance + ";Initial Catalog=" + _databaseName +
                        ";MultipleActiveResultSets = true;Max Pool Size=1000;User ID=" + _userName + ";Password=" + _password + ";Application Name=D.O.E.S DataEngine;";
                }
            }
            else
            {
                if (_userName == null && _password == null)
                {
                    _connectionString = "Data Source=" + _hostname + "," + _port + ";Initial Catalog=" + _databaseName +
                        ";MultipleActiveResultSets = true;Max Pool Size=1000;Integrated Security=False" + ";Application Name=D.O.E.S DataEngine;";
                }
                else
                {
                    _connectionString = "Data Source=" + _hostname + "," + _port + ";Initial Catalog=" + _databaseName +
                        ";MultipleActiveResultSets = true;Max Pool Size=1000;User ID=" + _userName + ";Password=" + _password + ";Application Name=D.O.E.S DataEngine;";
                }
            }

            SqlConnection connection = new SqlConnection(_connectionString);
            return connection;
        }

        /// <summary>
        /// Return the database type. 
        /// </summary>
        public override Dynamics.Database GetDatabaseType()
        {
            return Dynamics.Database.MicrosoftSQL;
        }

        /// <summary>
        ///Return the mechanic helper class associated with this database. 
        /// </summary>
        public override Mechanic GetMechanic()
        {
            return _mechanic;
        }

        /// <summary>
        /// Return a list of the web page page ID's and the associated size for each.  
        /// </summary>
        public override SortedList<int, long> GetPageIDList()
        {
            SortedList<int, long> colPageIDToLength = new SortedList<int, long>();
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    SqlCommand command = new SqlCommand("SELECT [PageID], [TotalLength] from [WebPages] ORDER BY [PageID]", connection, transaction);
                    using (SqlDataReader reader = command.ExecuteReader())
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
        /// Return the schema type for this database. 
        /// </summary>
        public override Dynamics.DatabaseSchema GetSchemaType()
        {
            return _schema;
        }

        /// <summary>
        /// Insert a characterised web page into the database.  
        /// </summary>
        public override int InsertCharacterisedWebPage(WebPage page)
        {
            int numberOfRowsAffected = 0;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                    SqlCommand command = new SqlCommand(
                    "INSERT INTO [WebPages] ([LoadedOn], [HeadersLength], [StatsLength], [TotalLength], [HREFs], [HashURL], [HashHTML], [URL], [HTML]) " +
                                          "VALUES (@LoadedOn,  @HeadersLength,  @StatsLength,  @TotalLength,  @HREFs,  @HashURL,  @HashHTML,  @URL,  @HTML) " +
                    "; SELECT CAST(SCOPE_IDENTITY() AS int)",
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
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string tableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                command = new SqlCommand(
                                "INSERT INTO [" + tableName + "] " +
                                "                 ([PageID], [URL_Length], [URL], [HTML_Length], [HTML], [URL_B64S_Length], [URL_B64S], [HTML_B64S_Length], [HTML_B64S]) " +
                                "          VALUES (@PageID,  @URL_Length,  @URL,  @HTML_Length,  @HTML,  @URL_B64S_Length,  @URL_B64S,  @HTML_B64S_Length,  @HTML_B64S); ",
                                connection,
                                transaction);

                                int htmlIndex = tableID * 8 + i + 4;
                                int urlIndex = tableID * 8 + i;

                                string convertedHTML = Encoding.ASCII.GetString(page.DataAsBytes[htmlIndex], 0, page.DataAsBytes[htmlIndex].Length);
                                string convertedURL = Encoding.ASCII.GetString(page.DataAsBytes[urlIndex], 0, page.DataAsBytes[urlIndex].Length);
                                command.Parameters.AddWithValue("@PageID", page.PageID); //                                                  Integer (int)
                                                                                         // Columns ModifiedOn and Updates get default values
                                command.Parameters.AddWithValue("@URL_Length", page.DataAsBytes[tableID * 8 + i].Length); //                  Integer (int)
                                command.Parameters.AddWithValue("@URL", convertedURL); //                                                     varchar 2048 (encrypted)
                                command.Parameters.AddWithValue("@HTML_Length", page.DataAsBytes[tableID * 8 + i + 4].Length); //             Integer (int)
                                command.Parameters.AddWithValue("@HTML", convertedHTML); //                                                   varchar max (encrypted)
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
                        command = new SqlCommand(
                       "INSERT INTO [WebPageHeaders] ([PageID], [HeaderKeyLength], [HeaderKey], [HeaderValueLength], [HeaderValue]) " +
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
                            command = new SqlCommand(
                                "INSERT INTO [WebPageStats] ([PageID], [CodeType], [Length], [Stats], [B64S_Length], [B64S_Stats]) " +
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        _tokenSource.Cancel();
                    }
                    else if (ex.Message.ToLower().Contains("cannot insert duplicate key"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
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
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                    SqlCommand command = new SqlCommand(
                    "INSERT INTO [WebPages] ([LoadedOn], [HeadersLength], [StatsLength], [TotalLength], [HREFs], [HashURL], [HashHTML], [URL], [HTML]) " +
                                          "VALUES (@LoadedOn,  @HeadersLength,  @StatsLength,  @TotalLength,  @HREFs,  @HashURL,  @HashHTML,  @URL,  @HTML) " +
                    "; SELECT CAST(SCOPE_IDENTITY() AS int)",
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
                                command = new SqlCommand(
                                "INSERT INTO [" + tableName + "] " +
                                "                 ([PageID], [URL_Length], [URL], [HTML_Length], [HTML], [URL_B64S_Length], [URL_B64S], [HTML_B64S_Length], [HTML_B64S]) " +
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
                        command = new SqlCommand(
                       "INSERT INTO [WebPageHeaders] ([PageID], [HeaderKeyLength], [HeaderKey], [HeaderValueLength], [HeaderValue]) " +
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
                            command = new SqlCommand(
                                "INSERT INTO [WebPageStats] ([PageID], [CodeType], [Length], [Stats], [B64S_Length], [B64S_Stats]) " +
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        _tokenSource.Cancel();
                    }
                    else if (ex.Message.ToLower().Contains("cannot insert duplicate key"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                    }
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
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    SqlCommand command = new SqlCommand("INSERT INTO PointInTimeWrite(WrittenOn, WriteHash) VALUES (@WrittenOn, @WriteHash)", connection, transaction);
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return rowsAffected;
        }

        /// <summary>
        /// Read base web page data from the database. 
        /// </summary>
        public override void ReadBaseWebPageData(OilPump pump)
        {
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    SqlCommand command = new SqlCommand("SELECT [WebPages].[PageID], [Webpages].[URL], [WebPages].[HTML]  " +
                    "FROM   [Webpages] " +
                    "ORDER BY WebPages.[PageID]", connection);
                    command.CommandTimeout = 600000;
                    connection.Open();
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int PageID = Convert.ToInt32(reader[0]);
                            SqlCommand heaaderCommand = new SqlCommand("SELECT [HeaderKey], [HeaderValue] FROM [WebPageHeaders] WHERE [PageID] = '" + PageID + "'", connection);
                            heaaderCommand.CommandTimeout = 600000;
                            SortedList<string, string> headerList = new SortedList<string, string>();
                            using (SqlDataReader headerReader = heaaderCommand.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
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
        /// Query the database using the ANSI SQL Left Outer Join Syntax. 
        /// </summary>
        public override Tuple<ulong, ulong> SelectWebPageLeftOuterJoin(int PageID, UInt64 bytesToProcess)
        {
            UInt64 rowDataprocessed = 0;
            UInt64 rowsProcessed = 0;
            DateTime timestamp = DateTime.Now;

            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    string query = "SELECT WebPages.PageID,WebPages.URL,WebPages.HTML, WebPages.HeadersLength, WebPages.StatsLength, WebPages.TotalLength, "
                         + "WebPages.HREFS, WebPages.HASHURL, WebPages.HashHTML, ";

                    //Go THrough Everything from webpageencoding lower to webpage encoding to higher 
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
                    if (_amplifier != 0)
                    {
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
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    for (int i = 0; i < queriesToRun.Count; i++)
                    {
                        SqlCommand command = new SqlCommand(queriesToRun[i], connection, transaction);
                        command.CommandTimeout = 600;
                        using (SqlDataReader dataReader = command.ExecuteReader())
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
                                    rowDataprocessed += Convert.ToUInt64(field.Length);
                                }
                                rowsProcessed++;
                            }
                        }
                    }

                    //Retrieve Header Data
                    string queryHeaderData = "SELECT HeaderID, PageID, HeaderKeyLength, Headerkey, HeaderValueLength, HeaderValue " +
                                             "FROM WebPageHeaders " +
                                             "WHERE PageID = " + PageID;
                    SqlCommand HeaderDataValues = new SqlCommand(queryHeaderData, connection, transaction);
                    HeaderDataValues.CommandTimeout = 300;
                    using (SqlDataReader dataReader = HeaderDataValues.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return Tuple.Create(rowDataprocessed, rowsProcessed);
        }

        /// <summary>
        /// Query the database using the ANSI SQL Union all Syntax. 
        /// </summary>
        public override Tuple<ulong, ulong> SelectWebPageUnionAll(int PageID, UInt64 bytesToProcess)
        {
            UInt64 rowDataprocessed = 0;
            UInt64 rowsProcessed = 0;
            DateTime timestamp = DateTime.Now;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    string query = "";
                    int webPageEncodingTablesIndex = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0);
                    int tableIDIndex = 0;
                    int numberOfTablesInQuery = 1;
                    int tablesToTruncateInQuery = 32;
                    //Query over standard and encoded Web Pages
                    if (_amplifier != 0)
                    {
                        for (int i = webPageEncodingTablesIndex; i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = tableIDIndex; tableID < _amplifier; tableID++)
                            {
                                string tableName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                                if (numberOfTablesInQuery > tablesToTruncateInQuery)
                                {
                                    query = query + "SELECT PageID, ModifiedOn, Updates, URL, HTML, URL_LENGTH, HTML_LENGTH, URL_B64S_LENGTH, URL_B64S, " +
                                        "HTML_B64S_LENGTH, HTML_B64S " +
                                        "FROM " + tableName + " WHERE PageID = " + PageID + ";";
                                    numberOfTablesInQuery = 1;
                                }
                                else
                                {
                                    query = query + "SELECT PageID, ModifiedOn, Updates, URL, HTML, URL_LENGTH, HTML_LENGTH, URL_B64S_LENGTH, URL_B64S," +
                                        " HTML_B64S_LENGTH, HTML_B64S " +
                                        "FROM " + tableName + " WHERE PageID = " + PageID;
                                    query = query + " UNION ALL ";
                                }
                                numberOfTablesInQuery++;
                            }
                        }
                        query = query.Substring(0, query.Length - 10);
                        query = query + ";";

                    }

                    SqlCommand command = new SqlCommand(query, connection, transaction);
                    command.CommandTimeout = 300;
                    using (SqlDataReader dataReader = command.ExecuteReader())
                    {
                        while (dataReader.HasRows)
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
                                    rowDataprocessed += Convert.ToUInt64(field.Length);
                                }
                            }
                            rowsProcessed++;
                            dataReader.NextResult();
                        }
                    }
                    //Retrieve Header Data

                    string queryHeaderData = "SELECT HeaderID, PageID, HeaderKeyLength, Headerkey, HeaderValueLength, HeaderValue " +
                                             "FROM WebPageHeaders " +
                                             "WHERE PageID = " + PageID;
                    SqlCommand HeaderDataValues = new SqlCommand(queryHeaderData, connection, transaction);
                    HeaderDataValues.CommandTimeout = 300;
                    using (SqlDataReader dataReader = HeaderDataValues.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            return Tuple.Create(rowDataprocessed, rowsProcessed);
        }

        /// <summary>
        /// Return the table amplification value for this database object. 
        /// </summary>
        public override int TableAmplifier { get { return _amplifier; } set { _amplifier = value; } }

        /// <summary>
        /// Set and return the managed token to cancel operations if specific things go wrong with the database. 
        /// </summary>
        public override CancellationTokenSource TokenSource { get { return _tokenSource; } set { _tokenSource = value; } }

        /// <summary>
        /// Update a characterised web page.  
        /// </summary>
        public override int UpdateCharacterisedWebPage(int PageID)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                SqlCommand readCommand = new SqlCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                    TableName + " WHERE [PageID] = @PageID", connection, transaction);
                                readCommand.CommandTimeout = 12000;
                                readCommand.Parameters.AddWithValue("@PageID", PageID);
                                string URL = string.Empty;
                                string HTML = string.Empty;
                                string URL_B64S = string.Empty;
                                string HTML_B64S = string.Empty;
                                using (SqlDataReader commandReader = readCommand.ExecuteReader())
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

                                SqlCommand command = new SqlCommand("UPDATE [" + TableName + "] SET [URL] = @URL, [Updates] =" +
                                    " [Updates] + 1, [HTML] = @HTML, [URL_B64S] = @URL_B64S," +
                                    " [HTML_B64S] = @HTML_B64S WHERE [PageID] = @PageID", connection, transaction);
                                command.Parameters.AddWithValue("@URL", URL);
                                command.Parameters.AddWithValue("@HTML", HTML);
                                command.Parameters.AddWithValue("@URL_B64S", URL_B64S);
                                command.Parameters.AddWithValue("@HTML_B64S", HTML_B64S);
                                command.Parameters.AddWithValue("@PageID", PageID);
                                command.Parameters.AddWithValue("@ModifiedOn", timestamp);
                                command.CommandTimeout = 300;
                                rowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }
                    //Update Core Tables
                    foreach (string table in _mechanic.ReturnWebPageCoreTables)
                    {

                        SqlCommand coreTableUpdateCommand = new SqlCommand("UPDATE " + table + " SET Updates = Updates + 1," +
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        _tokenSource.Cancel();
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
        /// Update encoded web page. 
        /// </summary>
        public override int UpdateEncodedWebPage(int PageID)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string TableName = _mechanic.NameWithID(encodingTables[i], tableID);
                                SqlCommand readCommand = new SqlCommand("SELECT URL, HTML, URL_B64S, HTML_B64S FROM " +
                                TableName + " WHERE[PageID] = @PageID", connection, transaction);
                                readCommand.CommandTimeout = 12000;
                                readCommand.Parameters.AddWithValue("@PageID", PageID);
                                string URL = string.Empty;
                                string HTML = string.Empty;
                                string URL_B64S = string.Empty;
                                string HTML_B64S = string.Empty;
                                using (SqlDataReader commandReader = readCommand.ExecuteReader())
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

                                SqlCommand command = new SqlCommand("UPDATE [" + TableName + "] SET [URL] = @URL, [Updates] =" +
                                    " [Updates] + 1, [HTML] = @HTML, [URL_B64S] = @URL_B64S," +
                                    " [HTML_B64S] = @HTML_B64S WHERE [PageID] = @PageID", connection, transaction);
                                command.Parameters.AddWithValue("@URL", URLEncoded);
                                command.Parameters.AddWithValue("@HTML", HTMLEncoded);
                                command.Parameters.AddWithValue("@URL_B64S", URL_B64SEncoded);
                                command.Parameters.AddWithValue("@HTML_B64S", HTML_B64S_Encoded);
                                command.Parameters.AddWithValue("@PageID", PageID);
                                command.Parameters.AddWithValue("@ModifiedOn", timestamp);
                                command.CommandTimeout = 300;
                                rowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        _tokenSource.Cancel();
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
        /// Update an encoded web page. 
        /// </summary>
        public override int UpdateEncodedWebPageInPlace(int PageID, WebPage newPage)
        {
            int numberOfRowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                    DateTime modificationDate = DateTime.Now;
                    SqlCommand command = new SqlCommand(
                    "UPDATE [WebPages] SET [LoadedOn] = @LoadedOn, [HeadersLength] = @HeadersLength, [StatsLength] =  @StatsLength," +
                    " [TotalLength] = @TotalLength, [HREFs] = @HREFs, [HashURL] = @HashURL, [HashHTML] = @HashHTML, [URL] = @URL ,[HTML] = @HTML," +
                    " [Updates] = [Updates] + 1, [ModifiedOn] = @ModifiedOn WHERE PageID = @PageID",
                    connection,
                    transaction);
                    // Columns PageID is an identity
                    command.Parameters.AddWithValue("@PageID", PageID);
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                                // Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(newPage.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(newPage.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(newPage.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(newPage.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(newPage.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML", newPage.HashHTML); //                                   Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", newPage.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", newPage.HTMLBinary); //                                           The fist 128 characters of HTML  
                    command.Parameters.AddWithValue("@ModifiedOn", modificationDate); //                                 The date of last modification 
                    command.CommandTimeout = 300;
                    numberOfRowsAffected += command.ExecuteNonQuery();
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string tableName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                                command = new SqlCommand(
                                "UPDATE [" + tableName + "] " +
                                " SET [URL_Length] = @URL_Length, [URL] = @URL, [HTML_Length] = @HTML_Length, [HTML] = @HTML, [URL_B64S_Length] = @URL_B64S_Length," +
                                " [URL_B64S] = @URL_B64S, [HTML_B64S_Length] = @HTML_B64S_Length, [HTML_B64S] = @HTML_B64S, " +
                                " [Updates] = [Updates] + 1, [ModifiedOn] = @ModifiedOn WHERE PageID = @PageID",
                                connection,
                                transaction);
                                command.Parameters.AddWithValue("@PageID", PageID);
                                command.Parameters.AddWithValue("@URL_Length", newPage.DataAsBytes[tableID * 8 + i].Length); //                  Integer (int)
                                command.Parameters.AddWithValue("@URL", newPage.DataAsBytes[tableID * 8 + i]); //                                Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_Length", newPage.DataAsBytes[tableID * 8 + i + 4].Length); //             Integer (int)
                                command.Parameters.AddWithValue("@HTML", newPage.DataAsBytes[tableID * 8 + i + 4]); //                           Byte Array (binary)
                                command.Parameters.AddWithValue("@URL_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i].Length); //             Integer (int)
                                command.Parameters.AddWithValue("@URL_B64S", newPage.DataAsBS64s[tableID * 8 + i]); //                           Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i + 4].Length); //        Integer (int)
                                command.Parameters.AddWithValue("@HTML_B64S", newPage.DataAsBS64s[tableID * 8 + i + 4]); //                      Byte Array (binary)
                                command.Parameters.AddWithValue("@ModifiedOn", modificationDate); //                                             The date of last modification 
                                command.CommandTimeout = 600;
                                numberOfRowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }
                    //delete all existing keys

                    command = new SqlCommand("DELETE FROM [WebPageHeaders] WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        command = new SqlCommand(
                       "INSERT INTO [WebPageHeaders] ([PageID], [HeaderKeyLength], [HeaderKey], [HeaderValueLength], [HeaderValue]) " +
                                                   "VALUES (@PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new SqlCommand("DELETE FROM [WebPageStats] WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                    {
                        command = new SqlCommand(
                            "INSERT INTO [WebPageStats] ([PageID], [CodeType], [Length], [Stats], [B64S_Length], [B64S_Stats]) " +
                                                      "VALUES (@PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
                            connection,
                            transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", PageID);
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
                        command = new SqlCommand("UPDATE [" + TableName + "] SET [Updates] =" +
                            " [Updates] + 1, [ModifiedOn] = @ModifiedOn WHERE [PageID] = @PageID", connection, transaction);
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        _tokenSource.Cancel();
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
        /// Update an encoded web page by replacing its content with a new web page. 
        /// </summary>
        public override int UpdateCharacterisedWebPageInPlace(int PageID, WebPage newPage)
        {
            int numberOfRowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlTransaction transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    DateTime modificationDate = DateTime.Now;
                    SqlCommand command = new SqlCommand(
                    "UPDATE [WebPages] SET [LoadedOn] = @LoadedOn, [HeadersLength] = @HeadersLength, [StatsLength] =  @StatsLength," +
                    " [TotalLength] = @TotalLength, [HREFs] = @HREFs, [HashURL] = @HashURL, [HashHTML] = @HashHTML, [URL] = @URL ,[HTML] = @HTML," +
                    " [Updates] = [Updates] + 1, [ModifiedOn] = @ModifiedOn WHERE PageID = @PageID",
                    connection,
                    transaction);
                    // Columns PageID is an identity
                    command.Parameters.AddWithValue("@PageID", PageID);
                    command.Parameters.AddWithValue("@LoadedOn", DateTime.Now); //                                           Current Date Time (DateTime2)
                                                                                // Columns ModifiedOn and Updates get default values
                    command.Parameters.AddWithValue("@HeadersLength", Convert.ToInt32(newPage.HeadersLength)); //        Integer (int)
                    command.Parameters.AddWithValue("@StatsLength", Convert.ToInt32(newPage.StatsLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@TotalLength", Convert.ToInt32(newPage.TotalLength)); //            Integer (int)
                    command.Parameters.AddWithValue("@HREFs", Convert.ToInt32(newPage.HREFS)); //                        Integer (int)
                    command.Parameters.AddWithValue("@HashURL", Convert.ToInt32(newPage.HashURL)); //                    Integer (int)
                    command.Parameters.AddWithValue("@HashHTML", newPage.HashHTML); //                                   Byte Array (binary)
                    command.Parameters.AddWithValue("@URL", newPage.URL); //                                             The complete URL
                    command.Parameters.AddWithValue("@HTML", newPage.HTMLBinary); //                                     The fist 8096 characters of HTML  
                    command.Parameters.AddWithValue("@ModifiedOn", modificationDate); //                                 The date of last modification 
                    command.CommandTimeout = 300;
                    numberOfRowsAffected += command.ExecuteNonQuery();
                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string tableName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                                command = new SqlCommand(
                                "UPDATE [" + tableName + "] " +
                                " SET [URL_Length] = @URL_Length, [URL] = @URL, [HTML_Length] = @HTML_Length, [HTML] = @HTML, [URL_B64S_Length] = @URL_B64S_Length," +
                                " [URL_B64S] = @URL_B64S, [HTML_B64S_Length] = @HTML_B64S_Length, [HTML_B64S] = @HTML_B64S, " +
                                " [Updates] = [Updates] + 1, [ModifiedOn] = @ModifiedOn WHERE PageID = @PageID",
                                connection,
                                transaction);

                                int htmlIndex = tableID * 8 + i + 4;
                                int urlIndex = tableID * 8 + i;

                                string convertedHTML = Encoding.ASCII.GetString(newPage.DataAsBytes[htmlIndex], 0, newPage.DataAsBytes[htmlIndex].Length);
                                string convertedURL = Encoding.ASCII.GetString(newPage.DataAsBytes[urlIndex], 0, newPage.DataAsBytes[urlIndex].Length);


                                command.Parameters.AddWithValue("@PageID", newPage.PageID); //                                                  Integer (int)
                                                                                            // Columns ModifiedOn and Updates get default values
                                command.Parameters.AddWithValue("@URL_Length", newPage.DataAsBytes[tableID * 8 + i].Length); //                  Integer (int)
                                command.Parameters.AddWithValue("@URL", convertedURL); //                                                     varchar 2048 (encrypted)
                                command.Parameters.AddWithValue("@HTML_Length", newPage.DataAsBytes[tableID * 8 + i + 4].Length); //             Integer (int)
                                command.Parameters.AddWithValue("@HTML", convertedHTML); //                                                   varchar max (encrypted)
                                command.Parameters.AddWithValue("@URL_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i].Length); //             Integer (int)
                                command.Parameters.AddWithValue("@URL_B64S", newPage.DataAsBS64s[tableID * 8 + i]); //                           Byte Array (binary)
                                command.Parameters.AddWithValue("@HTML_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i + 4].Length); //        Integer (int)
                                command.Parameters.AddWithValue("@HTML_B64S", newPage.DataAsBS64s[tableID * 8 + i + 4]); //                      Byte Array (binary)
                                command.Parameters.AddWithValue("@ModifiedOn", modificationDate); //                                 The date of last modification 
                                command.CommandTimeout = 600;
                                numberOfRowsAffected += command.ExecuteNonQuery();
                            }
                        }
                    }

                    //delete all existing keys

                    command = new SqlCommand("DELETE FROM [WebPageHeaders] WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        command = new SqlCommand(
                       "INSERT INTO [WebPageHeaders] ([PageID], [HeaderKeyLength], [HeaderKey], [HeaderValueLength], [HeaderValue]) " +
                                                   "VALUES (@PageID,  @HeaderKeyLength,  @HeaderKey,  @HeaderValueLength,  @HeaderValue)",
                       connection,
                       transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", PageID);
                        command.Parameters.AddWithValue("@HeaderKeyLength", kvp.Key.Length);
                        command.Parameters.AddWithValue("@HeaderKey", kvp.Key);
                        command.Parameters.AddWithValue("@HeaderValueLength", kvp.Value.Length);
                        command.Parameters.AddWithValue("@HeaderValue", kvp.Value);
                        command.CommandTimeout = 600;
                        numberOfRowsAffected += command.ExecuteNonQuery();
                    }

                    command = new SqlCommand("DELETE FROM [WebPageStats] WHERE PageID = " + PageID, connection, transaction);
                    command.ExecuteNonQuery();

                    for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                    {
                        command = new SqlCommand(
                            "INSERT INTO [WebPageStats] ([PageID], [CodeType], [Length], [Stats], [B64S_Length], [B64S_Stats]) " +
                                                      "VALUES (@PageID,  @CodeType,  @Length,  @Stats,  @B64S_Length,  @B64S_Stats)",
                            connection,
                            transaction);
                        // Columns ModifiedOn and Updates get default values
                        command.Parameters.AddWithValue("@PageID", PageID);
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
                        command = new SqlCommand("UPDATE [" + TableName + "] SET [Updates] =" +
                            " [Updates] + 1, [ModifiedOn] = @ModifiedOn WHERE [PageID] = @PageID", connection, transaction);
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                        _tokenSource.Cancel();
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
        /// Update a characterised web page by replacing its content with a new web page. 
        /// </summary>
        public override bool UpdateImportHistory(ImportHandler import)
        {
            bool success = false;
            try
            {
                using (SqlConnection connection = GetConnection())
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand("UPDATE [Imports] SET " +
                    "[ImportedWebPages] = '" + import.ImportedWebPages + "', " +
                    "[ImportedAllWebPages] = '" + Convert.ToInt32(import.AllImportedWebPages) + "' " +
                    "WHERE [FileName] = '" + import.Filename + "'",
                    connection);
                    command.CommandTimeout = 600;
                    success = Convert.ToBoolean(command.ExecuteNonQuery());
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return success;
        }

        /// <summary>
        /// Create an import history entry for the data file being used for operations. 
        /// </summary>
        public override bool CreateImportHistory(ImportHandler import)
        {
            bool success = false;
            try
            {
                using (SqlConnection connection = GetConnection())
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand("INSERT INTO [Imports] ([FileName], [ImportedWebPages], [ImportedAllWebPages]) VALUES ('" +
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
                if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return success;
        }

        /// <summary>
        /// Return the web pages to operate on. 
        /// </summary>
        public override Tuple<Random, ulong> InitializeRandom(bool ForceRandom, ulong RequestedBytes)
        {
            if (ForceRandom == true)
            {
                return Tuple.Create(new Random(), RequestedBytes);
            }
            else
            {
                int numberOfRows = 0;
                UInt64 totalDBSize = 0;
                SqlConnection connection = GetConnection();
                SqlCommand commandRowCountNN = new SqlCommand("SELECT COUNT(*) FROM [WebPages] WHERE [ModifiedOn] IS NOT NULL", connection);
                SqlCommand commandRowCountTotal = new SqlCommand("SELECT Count(*), SUM(TotalLength) FROM [WebPages]", connection);
                SqlCommand commandTotalSize = new SqlCommand("SELECT SUM(TotalLength) FROM [WebPages]", connection);
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
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
        /// Check the schema type of the database objects.
        /// </summary>
        public override void CheckSchemaType()
        {
            string schemaResponse = string.Empty;
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand("SELECT [SchemaType] FROM Configuration", connection);
                    using (SqlDataReader reader = command.ExecuteReader())
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
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
            
        }

        /// <summary>
        /// Perform advanced operations for the database vendor. 
        /// </summary>
        public override void VendorAdvancedOperations(int numberOfThreads)
        {
            BlockingCollection<Tuple<string, string>> indexesToRebuild = new BlockingCollection<Tuple<string, string>>(new ConcurrentBag<Tuple<string, string>>());
            BlockingCollection<Tuple<string, string>> indexesToReorganize = new BlockingCollection<Tuple<string, string>>(new ConcurrentBag<Tuple<string, string>>());
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    SqlCommand getIndexNumbers = new SqlCommand("SELECT dbschemas.[name] as 'Schema', " +
                                                                "dbtables.[name] as 'Table', " +
                                                                "dbindexes.[name] as 'Index', " +
                                                                "indexstats.avg_fragmentation_in_percent, " +
                                                                "indexstats.page_count " +
                                                                "FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS indexstats " +
                                                                "INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id] " +
                                                                "INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id] " +
                                                                "INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id] " +
                                                                "AND indexstats.index_id = dbindexes.index_id " +
                                                                "WHERE indexstats.database_id = DB_ID() " +
                                                                "ORDER BY indexstats.avg_fragmentation_in_percent desc", connection);
                    connection.Open();
                    using (SqlDataReader dataReader = getIndexNumbers.ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            double avg_frag = Convert.ToDouble(dataReader[3]);
                            if (avg_frag > 5 && avg_frag < 30)
                            {
                                //reorg index
                                Tuple<string, string> tempTuple = new Tuple<string, string>(Convert.ToString(dataReader[1]), Convert.ToString(dataReader[2]));
                                indexesToReorganize.Add(tempTuple);
                            }
                            else if (avg_frag > 30)
                            {
                                //rebuild index
                                Tuple<string, string> tempTuple = new Tuple<string, string>(Convert.ToString(dataReader[1]), Convert.ToString(dataReader[2]));
                                indexesToRebuild.Add(tempTuple);
                            }
                        }
                    }

                    Parallel.For(0, numberOfThreads, i =>
                    {
                        if (indexesToReorganize.Count != 0)
                        {
                            Tuple<string, string> tempValue = indexesToReorganize.Take();
                            string table = tempValue.Item1;
                            string index = tempValue.Item2;
                            SqlCommand reorgIndexex = new SqlCommand("ALTER INDEX " + index + " ON " + table + " REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON)", connection);
                            reorgIndexex.CommandTimeout = 600;
                            try
                            {
                                reorgIndexex.ExecuteNonQuery();
                            }
                            catch (SqlException ex)
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                            }
                        }
                    });

                    Parallel.For(0, numberOfThreads, i =>
                    {
                        if (indexesToReorganize.Count != 0)
                        {
                            Tuple<string, string> tempValue = indexesToRebuild.Take();
                            string table = tempValue.Item1;
                            string index = tempValue.Item2;
                            SqlCommand rebuildIndexex = new SqlCommand("ALTER INDEX " + index + " ON " + table + " REBUILD", connection);
                            rebuildIndexex.CommandTimeout = 600;
                            try
                            {
                                rebuildIndexex.ExecuteNonQuery();
                            }
                            catch (SqlException ex)
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                            }
                        }
                    });

                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
        }

        /// <summary>
        /// Perform complex operations for the complex test type. 
        /// </summary>
        public override void VendorComplexOperations()
        {
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    connection.InfoMessage += delegate (object sender, SqlInfoMessageEventArgs e)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, e.Message, Message.MessageType.Info));
                    };

                    SqlCommand command = new SqlCommand("CHECKPOINT 30", connection);
                    command.CommandTimeout = 600;
                    command.ExecuteNonQuery();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
        }

        /// <summary>
        /// Perform a consistency check of the database objects. 
        /// </summary>
        public override void VendorConsistencyCheck()
        {
            using (SqlConnection connection = GetConnection())
            {
                try
                {
                    connection.Open();
                    connection.InfoMessage += delegate (object sender, SqlInfoMessageEventArgs e)
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, e.Message, Message.MessageType.Info));
                    };

                    SqlCommand command = new SqlCommand("DBCC CHECKDB", connection);
                    command.CommandTimeout = 600000;
                    command.ExecuteNonQuery();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("login failed") || ex.Message.ToLower().Contains("a network-related or instance-specific error occurred"))
                    {
                        _tokenSource.Cancel();
                    }
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                }
            }
        }
    }
}
