using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DOES.DataEngine.FileOperations;
using DOES.DataEngine.Resources;
using DOES.Shared.Debug;
using DOES.Shared.Resources;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DOES.DataEngine.Operations
{
    /// <summary>
    /// This class handles MongoDB database operations. 
    /// </summary>
    public class MongoDBOper : DataVendor
    {
        private string _hostname;
        private string _databaseName;
        private string _userName;
        private string _password;
        private int _amplifier;
        private int _port;
        private Dynamics.DatabaseSchema _schema;
        private Dynamics.MongoDBDeployment _deploymentType;
        private Mechanic _mechanic;
        private MessageQueue _messageQueue;
        private CancellationTokenSource _tokenSource;

        /// <summary>
        /// Return the table amplifier for this database. 
        /// </summary>
        public override int TableAmplifier { get { return _amplifier; } set { _amplifier = value; } }
        /// <summary>
        /// Set and return the managed token to cancel operations if specific things go wrong with the database. 
        /// </summary>
        public override CancellationTokenSource TokenSource { get { return _tokenSource; } set { _tokenSource = value; } }

        /// <summary>
        /// Instantiates the MongoDB class. 
        /// </summary>
        public MongoDBOper(string hostname, string dbname, string username,
            string password, int port, int amplifier, Dynamics.DatabaseSchema schema, Dynamics.MongoDBDeployment deploymentType,
            Mechanic mechanic, MessageQueue queue)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _port = port;
            _amplifier = amplifier;
            _schema = schema;
            _deploymentType = deploymentType;
            _mechanic = mechanic;
            _messageQueue = queue;
        }

        /// <summary>
        /// Instantiates the MongoDB class. 
        /// </summary>
        public MongoDBOper(string hostname, string dbname, string username, string password, int amplifier,
            int port, Dynamics.MongoDBDeployment deploymentType, Mechanic mechanic, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _amplifier = amplifier;
            _port = port;
            _deploymentType = deploymentType;
            _messageQueue = messages;
            _mechanic = mechanic;
        }

        /// <summary>
        /// Instantiates the MongoDB class. 
        /// </summary>
        public MongoDBOper(string hostname, string dbname, string username, string password, int port,
           Dynamics.MongoDBDeployment deploymentType, Mechanic mechanic, MessageQueue messages)
        {
            _hostname = hostname;
            _databaseName = dbname;
            _userName = username;
            _password = password;
            _deploymentType = deploymentType; ;
            _port = port;
            _messageQueue = messages;
            _mechanic = mechanic;
        }

        /// <summary>
        /// Check the import history for a data file. 
        /// </summary>
        public override ImportHandler CheckFileImportHistory(string filename)
        {
            var client = GetConnection();
            IMongoDatabase database = client.GetDatabase(_databaseName);
            try
            {
                ImportHandler importData;
                int importedWebPages = 0;
                bool importedAllWebPages = false;
                bool found = false;
                var collection = database.GetCollection<BsonDocument>("Imports");
                var filter = Builders<BsonDocument>.Filter.Eq("FileName", filename);
                try
                {
                    var document = collection.Find(filter).First();
                    found = true;
                    importedWebPages = document["ImportedWebPages"].ToInt32();
                    importedAllWebPages = document["ImportedAllWebPages"].ToBoolean();
                }
                catch (InvalidOperationException ioe)
                {
                    if (ioe.Message.ToLower().Contains("sequence contains no elements"))
                    {
                        found = false;
                    }
                }
                importData = new ImportHandler(filename, importedWebPages, importedAllWebPages, found);
                return importData;
            }
            catch (Exception ex)
            {
                _tokenSource.Cancel();
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                return null;
            }
        }

        /// <summary>
        /// Check the schema type of the database objects.
        /// </summary>
        public override void CheckSchemaType()
        {
            try
            {
                var client = GetConnection();
                IMongoDatabase database = client.GetDatabase(_databaseName);
                var configurationCollection = database.GetCollection<BsonDocument>("Configuration");
                var filter = Builders<BsonDocument>.Filter.Empty;
                var configurationDocument = configurationCollection.Find(filter).First();
                Enum.TryParse(Convert.ToString(configurationDocument[1]), out _schema);
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                    || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        /// <summary>
        /// Create an import history entry for the data file being used for operations. 
        /// </summary>
        public override bool CreateImportHistory(ImportHandler import)
        {
            bool success = false;
            var client = GetConnection();
            IMongoDatabase database = client.GetDatabase(_databaseName);
            try
            {
                var configurationDocument = new BsonDocument
                    {
                        { "FileName", import.Filename},
                        { "ImportedWebPages", import.ImportedWebPages },
                        { "ImportedAllWebPages", import.AllImportedWebPages }
                    };
                var collection = database.GetCollection<BsonDocument>("Imports");
                collection.InsertOne(configurationDocument);
                success = true;

            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                    || ex.Message.ToLower().Contains("a timeout occurred "))
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
            //Base Objects
            var client = GetConnection();
            IMongoDatabase database = client.GetDatabase(_databaseName);
            try
            {
                foreach (string table in _mechanic.AllBaseTables())
                {
                    var filter = new BsonDocument("name", table);
                    var collections = database.ListCollections(new ListCollectionsOptions { Filter = filter });
                    bool exists = collections.Any();
                    if (!exists)
                    {
                        throw new Exception("The collection " + table + " does not exist");
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                    || ex.Message.ToLower().Contains("a timeout occurred "))
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
                            string collectionName = sb.ToString();
                            try
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, "Creating collection '" + collectionName + "'",
                                    Message.MessageType.Command));
                                database.CreateCollection(collectionName);
                                success = true;
                            }
                            catch (Exception mex)
                            {
                                _messageQueue.AddMessage(new Message(DateTime.Now, mex.Message.ToString(), Message.MessageType.Warning));
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
                    var configurationDocument = new BsonDocument
                    {
                        { "SchemaType", _schema.ToString()},
                        { "CreatedOn", DateTime.Now }
                    };

                    var collection = database.GetCollection<BsonDocument>("Configuration");
                    collection.InsertOne(configurationDocument);

                    //Generic JSON Document 
                    var sequenceDocument = new BsonDocument
                    {
                        { "_id", "IDseq"},
                        { "sequence_value", 1 }
                    };

                    //Populate Seq_PageID
                    collection = database.GetCollection<BsonDocument>("Seq_PageID");
                    collection.InsertOne(sequenceDocument);

                    //Populate Seq_HeaderID
                    collection = database.GetCollection<BsonDocument>("Seq_HeaderID");
                    collection.InsertOne(sequenceDocument);

                    //Populate Seq_StatID
                    collection = database.GetCollection<BsonDocument>("Seq_StatID");
                    collection.InsertOne(sequenceDocument);

                    //Populate Seq_WriteID
                    collection = database.GetCollection<BsonDocument>("Seq_WriteID");
                    collection.InsertOne(sequenceDocument);


                    //Create Indexes on Base Tables 
                    /////
                    //Web Page Indexes
                    /////
                    collection = database.GetCollection<BsonDocument>("WebPages");
                    //LoadedOn_WP
                    var indexBuilder = Builders<BsonDocument>.IndexKeys;
                    var keys = indexBuilder.Ascending("LoadedOn");
                    var options = new CreateIndexOptions
                    {
                        Name = "LoadedOn_WP_IDX"
                    };
                    var indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //ModifiedOn_WP
                    keys = indexBuilder.Ascending("ModifiedOn");
                    options = new CreateIndexOptions
                    {
                        Name = "ModifiedOn_WP_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //Update_WP
                    keys = indexBuilder.Ascending("Updates");
                    options = new CreateIndexOptions
                    {
                        Name = "Update_WP_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //HashURL_WP
                    keys = indexBuilder.Ascending("HashURL");
                    options = new CreateIndexOptions
                    {
                        Name = "HashURL_WP_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //HashHTML_WP
                    keys = indexBuilder.Ascending("HashHTML");
                    options = new CreateIndexOptions
                    {
                        Name = "HashHTML_WP_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //HeadersLength_WP
                    keys = indexBuilder.Ascending("HeadersLength");
                    options = new CreateIndexOptions
                    {
                        Name = "HeadersLength_WP_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //StatsLength_WP
                    keys = indexBuilder.Ascending("StatsLength");
                    options = new CreateIndexOptions
                    {
                        Name = "StatsLength_WP_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //TotalLength_WP
                    keys = indexBuilder.Ascending("TotalLength");
                    options = new CreateIndexOptions
                    {
                        Name = "TotalLength_WP_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //URL_WP
                    keys = indexBuilder.Ascending("URL");
                    options = new CreateIndexOptions
                    {
                        Name = "URL_WP_UQ_IDX",
                        Unique = true
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    /////
                    //Web Page Headers Indexes
                    /////
                    collection = database.GetCollection<BsonDocument>("WebPageHeaders");
                    //Unique WebPage Header Keyset - PageIdHeaderKey_WPH_UQ
                    var pageIDKey = indexBuilder.Ascending("PageID");
                    var headerKey = indexBuilder.Ascending("HeaderKey");
                    keys = indexBuilder.Combine(pageIDKey, headerKey);
                    options = new CreateIndexOptions
                    {
                        Name = "PageIdHeaderKey_WPH_UQ_IDX",
                        Unique = true
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //HeaderKeyLength_WPH
                    keys = indexBuilder.Ascending("HeaderKeyLength");
                    options = new CreateIndexOptions
                    {
                        Name = "HeaderKeyLength_WPH_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //HeaderValueLength_WPH
                    keys = indexBuilder.Ascending("HeaderValueLength");
                    options = new CreateIndexOptions
                    {
                        Name = "HeaderValueLength_WPH_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //ModifiedOn_WPH
                    keys = indexBuilder.Ascending("ModifiedOn");
                    options = new CreateIndexOptions
                    {
                        Name = "ModifiedOn_WPH_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //Updates_WPH
                    keys = indexBuilder.Ascending("Updates");
                    options = new CreateIndexOptions
                    {
                        Name = "Updates_WPH_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    /////
                    //Web Page Stats Indexes
                    /////
                    collection = database.GetCollection<BsonDocument>("WebPageStats");
                    //Unique WebPage Header Keyset - PageIDCodeType_WPS_UQ
                    pageIDKey = indexBuilder.Ascending("PageID");
                    var codeTypeKey = indexBuilder.Ascending("CodeType");
                    keys = indexBuilder.Combine(pageIDKey, codeTypeKey);
                    options = new CreateIndexOptions
                    {
                        Name = "PageIDCodeType_WPS_UQ_IDX",
                        Unique = true
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //Length_WPS
                    keys = indexBuilder.Ascending("Length");
                    options = new CreateIndexOptions
                    {
                        Name = "Length_WPS_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //B64S_Length_WPS
                    keys = indexBuilder.Ascending("B64S_Length");
                    options = new CreateIndexOptions
                    {
                        Name = "B64S_Length_WPS_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //ModifiedOn_WPS
                    keys = indexBuilder.Ascending("ModifiedOn");
                    options = new CreateIndexOptions
                    {
                        Name = "ModifiedOn_WP_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                    //Updates_WPS
                    keys = indexBuilder.Ascending("Updates");
                    options = new CreateIndexOptions
                    {
                        Name = "Updates_WPS_IDX"
                    };
                    indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                    collection.Indexes.CreateOne(indexModel);
                }
                catch (Exception sql3)
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, sql3.Message, Message.MessageType.Warning));
                    success = false;
                }
                try
                {
                    foreach (string table in _mechanic.AllWebPageEncodingTables(_amplifier))
                    {
                        var filter = new BsonDocument("name", table);
                        var collections = database.ListCollections(new ListCollectionsOptions { Filter = filter });
                        bool exists = collections.Any();
                        if (!exists)
                        {
                            throw new Exception("The collection " + table + " does not exist");
                        }
                    }
                }
                catch (Exception encodingEx)
                {
                    //write ex to log 
                    _messageQueue.AddMessage(new Message(DateTime.Now, encodingEx.Message, Message.MessageType.Warning));
                    sb.Clear();
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
                                string collectionName = sb.ToString();
                                for (int TableID = 0; (TableID < Convert.ToInt32((collectionName.Contains("_X00") ? _amplifier : 0))); TableID++)
                                {
                                    try
                                    {
                                        string[] splitObj = collectionName.Split(',');
                                        if (splitObj.Length == 1)
                                        {
                                            string encodingCollectionname = _mechanic.NameWithID(collectionName, TableID);
                                            _messageQueue.AddMessage(new Message(DateTime.Now, "Creating collection '" + encodingCollectionname + "'",
                                            Message.MessageType.Command));
                                            database.CreateCollection(encodingCollectionname);
                                            success = true;
                                        }
                                        else if (splitObj.Length == 2)
                                        {
                                            //Then its an index
                                            //First string is the collection
                                            //Second string is the field to apply the index to
                                            var collection = database.GetCollection<BsonDocument>(splitObj[0]);
                                            var indexBuilder = Builders<BsonDocument>.IndexKeys;
                                            var keys = indexBuilder.Ascending(splitObj[1]);
                                            var options = new CreateIndexOptions
                                            {
                                                Name = splitObj[1] + "_" + splitObj[0] + "_IDX"
                                            };
                                            var indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                                            collection.Indexes.CreateOne(indexModel);
                                        }
                                        else if (splitObj.Length == 3)
                                        {
                                            //Then its an index
                                            //First string is the collection
                                            //Second string is the field to apply the index to
                                            var collection = database.GetCollection<BsonDocument>(splitObj[0]);
                                            var indexBuilder = Builders<BsonDocument>.IndexKeys;
                                            var keys = indexBuilder.Ascending(splitObj[1]).Ascending(splitObj[2]);
                                            var options = new CreateIndexOptions
                                            {
                                                Name = splitObj[1] + "_" + splitObj[2] + "_" + splitObj[0] + "_IDX" 
                                            };
                                            var indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                                            collection.Indexes.CreateOne(indexModel);
                                        }
                                        else
                                        {
                                            throw new Exception("Parsing the collection or index definition failed");
                                        }
                                    }
                                    catch (Exception sqle)
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
            }
            return success;

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
        public override int DeleteWebPage(int pageID)
        {
            int rowsAffected = 0;
            try
            {
                var client = GetConnection();
                var sessionOptions = new ClientSessionOptions { };

                using (IClientSessionHandle session = client.StartSession(sessionOptions, _tokenSource.Token))
                {
                    var clientInSession = session.Client;
                    IMongoDatabase database = clientInSession.GetDatabase(_databaseName);
                    foreach (string CollectionName in _mechanic.AllWebPageDataTables(_amplifier))
                    {
                        FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                        IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>(CollectionName);
                        var result = collection.DeleteMany(filter);
                        rowsAffected += Convert.ToInt32(result.DeletedCount);
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                   || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return rowsAffected;
        }

        /// <summary>
        /// Destroy the database objects. 
        /// </summary>
        public override bool DestroyObjects(Dynamics.ClearingType clearingType)
        {
            bool success = false;
            var client = GetConnection();
            IMongoDatabase database = client.GetDatabase(_databaseName);

            if (clearingType == Dynamics.ClearingType.Drop)
            {
                foreach (string seq in _mechanic.ReturnAllSequences)
                {
                    try
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, "Drop collection " + seq, Message.MessageType.Command));
                        database.DropCollection(seq);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        if (ex.Message.ToLower().Contains("unable to authenticate")
                        || ex.Message.ToLower().Contains("a timeout occurred "))
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                            success = false;
                            return success;
                        }
                        else
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                        }
                    }
                }
                foreach (string table in _mechanic.AllTables(_amplifier))
                {
                    try
                    {
                        _messageQueue.AddMessage(new Message(DateTime.Now, "Drop collection " + table, Message.MessageType.Command));
                        database.DropCollection(table);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        if (ex.Message.ToLower().Contains("unable to authenticate")
                        || ex.Message.ToLower().Contains("a timeout occurred "))
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                            success = false;
                            return success;
                        }
                        else
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                        }
                    }
                }
            }
            else if (clearingType == Dynamics.ClearingType.Truncate)
            {
                foreach (string table in _mechanic.AllTables(_amplifier))
                {
                    try
                    {
                        var collection = database.GetCollection<BsonDocument>(table);
                        _messageQueue.AddMessage(new Message(DateTime.Now, "Delete many in collection " + table, Message.MessageType.Command));
                        collection.DeleteMany("{ }");
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        if (ex.Message.ToLower().Contains("unable to authenticate")
                        || ex.Message.ToLower().Contains("a timeout occurred "))
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                            success = false;
                            return success;
                        }
                        else
                        {
                            _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                        }
                    }
                }
            }
            return success;
        }

        /// <summary>
        /// Return the MongoDB connection object. 
        /// </summary>
        public override dynamic GetConnection()
        {
            MongoClient client;
            if (_deploymentType == Dynamics.MongoDBDeployment.StandAlone)
            {
                client = new MongoClient("mongodb://" + _userName + ":" + _password + "@" + _hostname + ":" + _port);
            }
            else 
            {
                client = new MongoClient("mongodb://" + _userName + ":" + _password + "@" + _hostname + ":" + _port + "?readPreference=nearest&connect=replicaSet");
            }
            return client;
        }

        /// <summary>
        /// Return the database type. 
        /// </summary>
        public override Dynamics.Database GetDatabaseType()
        {
            return Dynamics.Database.MongoDB;
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

            try
            {
                var client = GetConnection();
                IMongoDatabase database = client.GetDatabase(_databaseName);
                var webPageCollection = database.GetCollection<BsonDocument>("WebPages");
                var emptyFilter = Builders<BsonDocument>.Filter.Empty;

                var projection = Builders<BsonDocument>
                     .Projection
                     .Include("PageID").Include("TotalLength");

                var result = webPageCollection.Find(emptyFilter).Project(projection).ToList();

                for (int i = 0; i < result.Count; i++)
                {
                    colPageIDToLength.Add(Convert.ToInt32(result[i]["PageID"]), Convert.ToUInt32(result[i]["TotalLength"]));
                }
            }
            catch (Exception ex)
            {
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
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
                return Tuple.Create(new Random(), RequestedBytes);
            }
            else
            {
                int numberOfRows = 0;
                UInt64 totalDBSize = 0;
                try
                {
                    var client = GetConnection();
                    IMongoDatabase database = client.GetDatabase(_databaseName);
                    var webPageCollection = database.GetCollection<BsonDocument>("WebPages");
                    //Get a count all documents in WebPages where ModifiedON is not null
                    var modifiedOnNotNullfilter = Builders<BsonDocument>.Filter.Ne("ModifiedOn", BsonNull.Value);
                    numberOfRows += Convert.ToInt32(webPageCollection.CountDocuments(modifiedOnNotNullfilter));

                    //Get a count summing up total length from webpages AND TOTAL DOCUMENTS
                    var aggregation = webPageCollection.Aggregate<BsonDocument>().Group(new BsonDocument
                    {
                        { "_id", BsonNull.Value
                        },
                        {
                            "TotalLength", new BsonDocument
                                         {
                                             {
                                                 "$sum", "$TotalLength"
                                             }
                                         }
                        },
                         {
                            "CountAll", new BsonDocument
                                         {
                                             {
                                                 "$sum", 1
                                             }
                                         }
                        }
                    });
                    var doc = aggregation.Single();
                    BsonDocument result = doc.AsBsonDocument;
                    totalDBSize = Convert.ToUInt64(result["TotalLength"].ToString());
                    numberOfRows += Convert.ToInt32(result["CountAll"].ToString());

                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("unable to authenticate")
                        || ex.Message.ToLower().Contains("a timeout occurred "))
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
            var client = GetConnection();
            var sessionOptions = new ClientSessionOptions { };

            using (IClientSessionHandle session = client.StartSession(sessionOptions, _tokenSource.Token))
            {
                var clientInSession = session.Client;
                IMongoDatabase database = clientInSession.GetDatabase(_databaseName);
                try
                {
                    //Get next sequence ID
                    page.PageID = GetNextSequenceValue("Seq_PageID");
                    //Web Page Base JSON Object
                    var webpageDocument = new BsonDocument
                    {
                        { "PageID", page.PageID},                                    //      PageID Generated Seperately
                        { "LoadedOn", DateTime.Now},                                 //      Current Date Time (DateTime2)
                        { "ModifiedOn", BsonNull.Value},                                       //      Columns ModifiedOn and Updates get default values
                        { "Updates", 0},                                             //      Default Value
                        { "HeadersLength",  Convert.ToInt32(page.HeadersLength)},    //      Integer (int)
                        { "StatsLength", Convert.ToInt32(page.StatsLength)},         //      Integer (int)
                        { "TotalLength", Convert.ToInt32(page.TotalLength)},         //      Integer (int)
                        { "HREFs",  Convert.ToInt32(page.HREFS)},                    //      Integer (int)
                        { "HashURL", Convert.ToInt32(page.HashURL)},                 //      Integer (int)
                        { "HashHTML", page.HashHTML},                                //      Byte Array (binary)
                        { "URL", page.URL},                                          //      The complete URL
                        { "HTML", page.HTMLBinary}                                   //      The fist 128 characters of HTML  
                    };

                    var collection = database.GetCollection<BsonDocument>("WebPages");
                    collection.InsertOne(webpageDocument);
                    numberOfRowsAffected++;

                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
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

                                //Web Page Base JSON Object
                                var encodedWebPageDocument = new BsonDocument
                                {
                                    { "PageID", page.PageID},                                             //      PageID Generated Seperately
                                    { "ModifiedOn", BsonNull.Value},                                                  //      Columns ModifiedOn and Updates get default values
                                    { "Updates", 0},                                                      //      Default Value
                                    { "URL_Length", page.DataAsBytes[tableID * 8 + i].Length},            //      Integer (int)
                                    { "URL", convertedURL},                                               //      Byte Array (binary)
                                    { "HTML_Length",  page.DataAsBytes[tableID * 8 + i + 4].Length},      //      Integer (int)
                                    { "HTML",  convertedHTML},                                            //      Byte Array (binary)
                                    { "URL_B64S_Length", page.DataAsBS64s[tableID * 8 + i].Length},       //      Integer (int)
                                    { "URL_B64S", convertedB64SURL},                      //      Byte Array (binary)
                                    { "HTML_B64S_Length",  page.DataAsBS64s[tableID * 8 + i + 4].Length}, //      Integer (int)
                                    { "HTML_B64S", convertedB64SHTML}                 //      Byte Array (binary)
                                };

                                var encodingCollection = database.GetCollection<BsonDocument>(tableName);
                                encodingCollection.InsertOne(encodedWebPageDocument);
                                numberOfRowsAffected++;
                            }
                        }
                    }
                    foreach (KeyValuePair<string, string> kvp in page.Headers)
                    {
                        //Get next sequence ID
                        int headerID = GetNextSequenceValue("Seq_HeaderID");
                        //Web Page Base JSON Object
                        var headerDocument = new BsonDocument
                        {
                            { "HeaderID",headerID},
                            { "PageID", page.PageID},
                            { "ModifiedOn", BsonNull.Value},
                            { "Updates", 0},
                            { "HeaderKeyLength", kvp.Key.Length},
                            { "HeaderKey", kvp.Key},
                            { "HeaderValueLength", kvp.Value.Length},
                            { "HeaderValue",  kvp.Value}
                        };

                        var headersCollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                        headersCollection.InsertOne(headerDocument);
                        numberOfRowsAffected++;
                    }
                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= page.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            //Get next sequence ID
                            int statID = GetNextSequenceValue("Seq_StatID");
                            //Web Page Base JSON Object
                            var statDocument = new BsonDocument
                            {
                                { "StatID", statID},
                                { "PageID", page.PageID},
                                { "ModifiedOn", BsonNull.Value},
                                { "Updates", 0},
                                { "CodeType", codeType},
                                { "Length", page.StatsAsBytes[codeType].Length},
                                { "Stats", page.StatsAsBytes[codeType]},
                                { "B64S_Length", page.StatsAsBS64s[codeType].Length},
                                { "B64S_Stats", page.StatsAsBS64s[codeType]}
                            };

                            var statsCollection = database.GetCollection<BsonDocument>("WebPageStats");
                            statsCollection.InsertOne(statDocument);
                            numberOfRowsAffected++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("unable to authenticate")
                       || ex.Message.ToLower().Contains("a timeout occurred "))
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
            var client = GetConnection();
            var sessionOptions = new ClientSessionOptions { };

            using (IClientSessionHandle session = client.StartSession(sessionOptions, _tokenSource.Token))
            {
                // Begin transaction
                //session.StartTransaction();
                var clientInSession = session.Client;
                IMongoDatabase database = clientInSession.GetDatabase(_databaseName);
                try
                {
                    //Get next sequence ID
                    page.PageID = GetNextSequenceValue("Seq_PageID");
                    //Web Page Base JSON Object
                    var webpageDocument = new BsonDocument
                    {
                        { "PageID", page.PageID},                                    //      PageID Generated Seperately
                        { "LoadedOn", DateTime.Now},                                 //      Current Date Time (DateTime2)
                        { "ModifiedOn", BsonNull.Value},                                       //      Columns ModifiedOn and Updates get default values
                        { "Updates", 0},                                             //      Default Value
                        { "HeadersLength",  Convert.ToInt32(page.HeadersLength)},    //      Integer (int)
                        { "StatsLength", Convert.ToInt32(page.StatsLength)},         //      Integer (int)
                        { "TotalLength", Convert.ToInt32(page.TotalLength)},         //      Integer (int)
                        { "HREFs",  Convert.ToInt32(page.HREFS)},                    //      Integer (int)
                        { "HashURL", Convert.ToInt32(page.HashURL)},                 //      Integer (int)
                        { "HashHTML", page.HashHTML},                                //      Byte Array (binary)
                        { "URL", page.URL},                                          //      The complete URL
                        { "HTML", page.HTMLBinary}                                   //      The fist 128 characters of HTML  
                    };

                    var collection = database.GetCollection<BsonDocument>("WebPages");
                    collection.InsertOne(webpageDocument);
                    numberOfRowsAffected++;

                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string tableName = _mechanic.NameWithID(encodingTables[i], tableID);

                                //Web Page Base JSON Object
                                var encodedWebPageDocument = new BsonDocument
                                {
                                    { "PageID", page.PageID},                                             //      PageID Generated Seperately
                                    { "ModifiedOn", BsonNull.Value},                                                //      Columns ModifiedOn and Updates get default values
                                    { "Updates", 0},                                                      //      Default Value
                                    { "URL_Length", page.DataAsBytes[tableID * 8 + i].Length},            //      Integer (int)
                                    { "URL",  page.DataAsBytes[tableID * 8 + i]},                         //      Byte Array (binary)
                                    { "HTML_Length",  page.DataAsBytes[tableID * 8 + i + 4].Length},      //      Integer (int)
                                    { "HTML",  page.DataAsBytes[tableID * 8 + i + 4]},                    //      Byte Array (binary)
                                    { "URL_B64S_Length", page.DataAsBS64s[tableID * 8 + i].Length},       //      Integer (int)
                                    { "URL_B64S",page.DataAsBS64s[tableID * 8 + i]},                      //      Byte Array (binary)
                                    { "HTML_B64S_Length",  page.DataAsBS64s[tableID * 8 + i + 4].Length}, //      Integer (int)
                                    { "HTML_B64S", page.DataAsBS64s[tableID * 8 + i + 4]}                 //      Byte Array (binary)
                                };

                                var encodingCollection = database.GetCollection<BsonDocument>(tableName);
                                encodingCollection.InsertOne(encodedWebPageDocument);
                                numberOfRowsAffected++;
                            }
                        }
                    }
                    foreach (KeyValuePair<string, string> kvp in page.Headers)
                    {
                        //Get next sequence ID
                        int headerID = GetNextSequenceValue("Seq_HeaderID");
                        //Web Page Base JSON Object
                        var headerDocument = new BsonDocument
                        {
                            { "HeaderID",headerID},
                            { "PageID", page.PageID},
                            { "ModifiedOn", BsonNull.Value},
                            { "Updates", 0},
                            { "HeaderKeyLength", kvp.Key.Length},
                            { "HeaderKey", kvp.Key},
                            { "HeaderValueLength", kvp.Value.Length},
                            { "HeaderValue",  kvp.Value}
                        };

                        var headersCollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                        headersCollection.InsertOne(headerDocument);
                        numberOfRowsAffected++;
                    }
                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= page.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            //Get next sequence ID
                            int statID = GetNextSequenceValue("Seq_StatID");
                            //Web Page Base JSON Object
                            var statDocument = new BsonDocument
                            {
                                { "StatID", statID},
                                { "PageID", page.PageID},
                                { "ModifiedOn", BsonNull.Value},
                                { "Updates", 0},
                                { "CodeType", codeType},
                                { "Length", page.StatsAsBytes[codeType].Length},
                                { "Stats", page.StatsAsBytes[codeType]},
                                { "B64S_Length", page.StatsAsBS64s[codeType].Length},
                                { "B64S_Stats", page.StatsAsBS64s[codeType]}
                            };

                            var statsCollection = database.GetCollection<BsonDocument>("WebPageStats");
                            statsCollection.InsertOne(statDocument);
                            numberOfRowsAffected++;
                        }
                    }

                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().Contains("unable to authenticate")
                       || ex.Message.ToLower().Contains("a timeout occurred "))
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
            try
            {
                var client = GetConnection();
                var sessionOptions = new ClientSessionOptions { };

                using (IClientSessionHandle session = client.StartSession(sessionOptions, _tokenSource.Token))
                {
                    // Begin transaction
                    //session.StartTransaction();
                    var clientInSession = session.Client;
                    IMongoDatabase database = clientInSession.GetDatabase(_databaseName);
                    //get WriteHash
                    DateTime writeTime = DateTime.Now;
                    string hashVal = string.Empty;
                    using (var stream = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(stream, Encoding.ASCII, true))
                        {
                            writer.Write(writeTime.Ticks);
                        }
                        using (var hash = SHA256.Create())
                        {
                            hashVal = Encoding.ASCII.GetString(hash.ComputeHash(stream));
                        }
                    }
                    //Get next sequence ID
                    int writeID = GetNextSequenceValue("Seq_WriteID");
                    //Web Page Base JSON Object
                    var headerDocument = new BsonDocument
                        {
                            { "WriteID", writeID},
                            { "WrittenOn", writeTime},
                            { "WriteHash", hashVal}
                        };

                    var headersCollection = database.GetCollection<BsonDocument>("PointInTimeWrite");
                    headersCollection.InsertOne(headerDocument);
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                       || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return rowsAffected;
        }

        /// <summary>
        /// Read base web page data from the database objects. 
        /// </summary>
        public override void ReadBaseWebPageData(OilPump pump)
        {
            try
            {
                var client = GetConnection();
                IMongoDatabase database = client.GetDatabase(_databaseName);
                ReadPreference preference = ReadPreference.SecondaryPreferred;
                database = database.WithReadPreference(preference);

                var webPageCollection = database.GetCollection<BsonDocument>("WebPages");

                var projection = Builders<BsonDocument>
                                     .Projection
                                     .Include("PageID").Include("URL").Include("HTML");
                var filter = Builders<BsonDocument>.Filter.Empty;

                var cursor = webPageCollection.Find(filter).Project(projection).ToCursor();
                foreach (var document in cursor.ToEnumerable())
                {
                    int PageID = Convert.ToInt32(document["PageID"]);
                    SortedList<string, string> headerList = new SortedList<string, string>();

                    //Now retirve the rest of the headers for the web page

                    var headerPageCollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                    var pageIDFilter = Builders<BsonDocument>.Filter.Eq("PageID", PageID);
                    var headerProjection = Builders<BsonDocument>
                                     .Projection
                                     .Include("HeaderKey").Include("HeaderValue");
                    var headerCursor = headerPageCollection.Find(pageIDFilter).
                        Project(headerProjection).ToCursor();
                    foreach (var headerDocument in headerCursor.ToEnumerable())
                    {
                        headerList.Add(Convert.ToString(headerDocument["HeaderKey"]),
                            Convert.ToString(headerDocument["HeaderValue"]));
                    }
                    byte[] bHTML = null;
                    try

                    {
                        bHTML = (byte[])document["HTML"];
                    }
                    catch (Exception)
                    { }
                    WebPage webpage = new WebPage(PageID, Convert.ToString(document["URL"]), Encoding.ASCII.GetString(bHTML),
                        headerList);
                    if (!_tokenSource.Token.IsCancellationRequested)
                    {
                        pump.IncreaseOilReserve(webpage);
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                   || ex.Message.ToLower().Contains("a timeout occurred "))
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

        /// <summary>
        /// Query database objects using the Lookup Aggregation
        /// </summary>
        public override Tuple<ulong, ulong> SelectWebPageLeftOuterJoin(int pageID, ulong bytesToProcess)
        {
            UInt64 rowDataprocessed = 0;
            UInt64 rowsProcessed = 0;
            DateTime timestamp = DateTime.Now;
            try
            {
                var client = GetConnection();
                IMongoDatabase database = client.GetDatabase(_databaseName);
                int webPageEncodingTablesIndex = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0);
                int tableIDIndex = 0;
                int numberOfTablesInQuery = 1;
                List<IAggregateFluent<BsonDocument>> aggregationsToRun = new List<IAggregateFluent<BsonDocument>>();
                var filter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                var webPageCollection = database.GetCollection<BsonDocument>("WebPages");
                //bool startingCollection = true;
                var webPageProjection = Builders<BsonDocument>
                                   .Projection
                                   .Include("PageID").Include("URL")
                                   .Include("HTML");

                //Encoding Tables 
                for (int i = webPageEncodingTablesIndex; i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                {
                    for (int tableID = tableIDIndex; tableID < _amplifier; tableID++)
                    {
                        string collectionName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                        var encodingCollection = database.GetCollection<BsonDocument>(collectionName);

                       var aggregation =  webPageCollection.Aggregate().Lookup(collectionName, "PageID", "PageID", "LinkedEncoding")
                            .Project(Builders<BsonDocument>.Projection.Exclude("_id"));
                        aggregationsToRun.Add(aggregation);
                        numberOfTablesInQuery++;
                    }
                }
                Tuple<UInt64, UInt64>[] collation = new Tuple<UInt64, UInt64>[aggregationsToRun.Count];

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = 4;
                Parallel.For(0, aggregationsToRun.Count, po, i =>
                {
                    UInt64 rowDataprocessedInParallel = 0;
                    UInt64 rowsProcessedInParalell = 0;
                    var resultSet = aggregationsToRun[i].Match(filter);
                    //Read each result
                    foreach (var document in resultSet.ToEnumerable())
                    {
                        //Process Each Field 
                        int pageID = Convert.ToInt32(document["PageID"]);
                        var modifiedOn = document["ModifiedOn"];
                        int updates = Convert.ToInt32(document["Updates"]);

                        var LinkedPage = (BsonArray)document["LinkedEncoding"];
                        foreach(BsonValue value in LinkedPage)
                        {
                            int urlLength = Convert.ToInt32(value["URL_Length"]);
                            var URL = value["URL"];
                            if (URL.GetType() == typeof(BsonBinaryData))
                            {
                                byte[] URLAsByte = URL.AsByteArray;
                                rowDataprocessedInParallel += Convert.ToUInt64(URLAsByte.Length);
                            }
                            else if (URL.GetType() == typeof(BsonString))
                            {
                                string URLAsString = URL.ToString();
                                rowDataprocessedInParallel += Convert.ToUInt64(URLAsString.Length);

                            }
                            int htmlLength = Convert.ToInt32(value["HTML_Length"]);
                            var HTML = value["HTML"];
                            if (HTML.GetType() == typeof(BsonBinaryData))
                            {
                                byte[] HTMLAsByte = HTML.AsByteArray;
                                rowDataprocessedInParallel += Convert.ToUInt64(HTMLAsByte.Length);
                            }
                            else if (HTML.GetType() == typeof(BsonString))
                            {
                                string HTMLAsString = HTML.ToString();
                                rowDataprocessedInParallel += Convert.ToUInt64(HTMLAsString.Length);
                            }
                            int urlB64SLength = Convert.ToInt32(value["URL_B64S_Length"]);
                            var URL_B64S = value["URL_B64S"];
                            if (URL_B64S.GetType() == typeof(BsonBinaryData))
                            {
                                byte[] URL_B64SAsByte = URL_B64S.AsByteArray;
                                rowDataprocessedInParallel += Convert.ToUInt64(URL_B64SAsByte.Length);
                            }
                            else if (URL_B64S.GetType() == typeof(BsonString))
                            {
                                string URLB64SAsString = URL_B64S.ToString();
                                rowDataprocessedInParallel += Convert.ToUInt64(URLB64SAsString.Length);
                            }
                            int htmlB64SLength = Convert.ToInt32(value["HTML_B64S_Length"]);
                            var HTML_B64S = value["HTML_B64S"];
                            if (HTML_B64S.GetType() == typeof(BsonBinaryData))
                            {
                                byte[] HTML_B64SAsByte = HTML_B64S.AsByteArray;
                                rowDataprocessedInParallel += Convert.ToUInt64(HTML_B64SAsByte.Length);
                            }
                            else if (HTML_B64S.GetType() == typeof(BsonString))
                            {
                                string HTML_BS64SAsString = HTML_B64S.ToString();
                                rowDataprocessedInParallel += Convert.ToUInt64(HTML_BS64SAsString.Length);
                            }
                        }
                        int HeadersLength = Convert.ToInt32(document["HeadersLength"]);
                        int StatsLength = Convert.ToInt32(document["StatsLength"]);
                        int TotalLength = Convert.ToInt32(document["TotalLength"]);
                        int HREFs = Convert.ToInt32(document["HREFs"]);
                        int HashURL = Convert.ToInt32(document["HashURL"]);
                        var HashHTML = document["HashHTML"];
                        if (HashHTML.GetType() == typeof(BsonBinaryData))
                        {
                            byte[] HTML_B64SAsByte = HashHTML.AsByteArray;
                            rowDataprocessedInParallel += Convert.ToUInt64(HTML_B64SAsByte.Length);
                        }
                        else if (HashHTML.GetType() == typeof(BsonString))
                        {
                            string HashHTML_AsString = HashHTML.ToString();
                            rowDataprocessedInParallel += Convert.ToUInt64(HashHTML_AsString.Length);
                        }
                        string URLOuter = Convert.ToString(document["URL"]);
                        var HTMLOuter = document["HTML"];
                        if (HTMLOuter.GetType() == typeof(BsonBinaryData))
                        {
                            byte[] HTML_AsByte = HTMLOuter.AsByteArray;
                            rowDataprocessedInParallel += Convert.ToUInt64(HTML_AsByte.Length);
                        }
                        else if (HTMLOuter.GetType() == typeof(BsonString))
                        {
                            string HTMLASString= HTMLOuter.ToString();
                            rowDataprocessedInParallel += Convert.ToUInt64(HTMLASString.Length);
                        }
                        rowsProcessedInParalell++;
                    }
                    Tuple<ulong, ulong> collationInParalell = Tuple.Create(rowDataprocessedInParallel, rowsProcessedInParalell);
                    collation[i] = collationInParalell;
                });

                foreach (Tuple<ulong, ulong> collator in collation)
                {
                    rowDataprocessed += collator.Item1;
                    rowsProcessed += collator.Item2;
                }


                //WebPageHeaders
                var headersCollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                var projection = Builders<BsonDocument>
                                    .Projection
                                    .Include("HeaderID").Include("PageID")
                                    .Include("HeaderKeyLength").Include("HeaderKey")
                                    .Include("HeaderValueLength").Include("HeaderValue");
                var queryResult = headersCollection.Find(filter).Project(projection).ToCursor();
                foreach (var document in queryResult.ToEnumerable())
                {
                    int headerID = Convert.ToInt32(document["HeaderID"]);
                    int PID = Convert.ToInt32(document["PageID"]);
                    int headerKeyLength = Convert.ToInt32(document["HeaderKeyLength"]);
                    rowDataprocessed += Convert.ToUInt64(headerKeyLength);
                    int headerValueLength = Convert.ToInt32(document["HeaderValueLength"]);
                    rowDataprocessed += Convert.ToUInt64(headerValueLength);
                    var headerKey = document["HeaderKey"];
                    var headerValue = document["HeaderValue"];
                    rowsProcessed++;
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
               || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return Tuple.Create(rowDataprocessed, rowsProcessed);
        }

        /// <summary>
        /// Query database objects using the Union Aggregation
        /// </summary>
        public override Tuple<ulong, ulong> SelectWebPageUnionAll(int pageID, ulong bytesToProcess)
        {
            UInt64 rowDataprocessed = 0;
            UInt64 rowsProcessed = 0;
            DateTime timestamp = DateTime.Now;
            try
            {
                var client = GetConnection();
                IMongoDatabase database = client.GetDatabase(_databaseName);
                int webPageEncodingTablesIndex = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0);
                int tableIDIndex = 0;
                int numberOfTablesInQuery = 1;
                int tablesToTruncateInQuery = 4;
                List<IAggregateFluent<BsonDocument>> aggregationsToRun = new List<IAggregateFluent<BsonDocument>>();
                var filter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                var aggregation = database.GetCollection<BsonDocument>("WebPages").Aggregate();
                bool startingCollection = true;


                //Encoding Tables 
                for (int i = webPageEncodingTablesIndex; i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                {
                    for (int tableID = tableIDIndex; tableID < _amplifier; tableID++)
                    {
                        string collectionName = _mechanic.NameWithID(_mechanic.ReturnWebPageEncodingtables[i], tableID);
                        var encodingCollection = database.GetCollection<BsonDocument>(collectionName);
                        if (startingCollection)
                        {
                            aggregation = database.GetCollection<BsonDocument>(collectionName).Aggregate();
                            startingCollection = false;
                        }
                        if (numberOfTablesInQuery >= tablesToTruncateInQuery)
                        {
                            aggregation = aggregation.UnionWith(database.GetCollection<BsonDocument>(collectionName));
                            aggregationsToRun.Add(aggregation);
                            startingCollection = true;
                            numberOfTablesInQuery = 1;
                        }
                        else
                        {
                            aggregation = aggregation.UnionWith(database.GetCollection<BsonDocument>(collectionName));
                        }
                        numberOfTablesInQuery++;
                    }
                }
                aggregationsToRun.Add(aggregation);
                Tuple<UInt64, UInt64>[] collation = new Tuple<UInt64, UInt64>[aggregationsToRun.Count];

                ParallelOptions po = new ParallelOptions();
                po.MaxDegreeOfParallelism = 8;
                Parallel.For(0, aggregationsToRun.Count, po, i =>
                {
                    UInt64 rowDataprocessedInParallel = 0;
                    UInt64 rowsProcessedInParalell = 0;
                    var resultSet = aggregationsToRun[i].Match(filter);
                    //Read each result
                    foreach (var document in resultSet.ToEnumerable())
                    {
                        //Process Each Field 
                        int pageID = Convert.ToInt32(document["PageID"]);
                        var modifiedOn = document["ModifiedOn"];
                        int updates = Convert.ToInt32(document["Updates"]);
                        int urlLength = Convert.ToInt32(document["URL_Length"]);
                        var URL = document["URL"];
                        if (URL.GetType() == typeof(BsonBinaryData))
                        {
                            byte[] URLAsByte = URL.AsByteArray;
                            rowDataprocessedInParallel += Convert.ToUInt64(URLAsByte.Length);
                        }
                        else if (URL.GetType() == typeof(BsonString))
                        {
                            string URLAsString = URL.ToString();
                            rowDataprocessedInParallel += Convert.ToUInt64(URLAsString.Length);

                        }
                        int htmlLength = Convert.ToInt32(document["HTML_Length"]);
                        var HTML = document["HTML"];
                        if (HTML.GetType() == typeof(BsonBinaryData))
                        {
                            byte[] HTMLAsByte = HTML.AsByteArray;
                            rowDataprocessedInParallel += Convert.ToUInt64(HTMLAsByte.Length);
                        }
                        else if (HTML.GetType() == typeof(BsonString))
                        {
                            string HTMLAsString = HTML.ToString();
                            rowDataprocessedInParallel += Convert.ToUInt64(HTMLAsString.Length);
                        }
                        int urlB64SLength = Convert.ToInt32(document["URL_B64S_Length"]);
                        var URL_B64S = document["URL_B64S"];
                        if (URL_B64S.GetType() == typeof(MongoDB.Bson.BsonBinaryData))
                        {
                            byte[] URL_B64SAsByte = URL_B64S.AsByteArray;
                            rowDataprocessedInParallel += Convert.ToUInt64(URL_B64SAsByte.Length);
                        }
                        else if (URL_B64S.GetType() == typeof(BsonString))
                        {
                            string URLB64SAsString = URL_B64S.ToString();
                            rowDataprocessedInParallel += Convert.ToUInt64(URLB64SAsString.Length);
                        }
                        int htmlB64SLength = Convert.ToInt32(document["HTML_B64S_Length"]);
                        var HTML_B64S = document["HTML_B64S"];
                        if (HTML_B64S.GetType() == typeof(BsonBinaryData))
                        {
                            byte[] HTML_B64SAsByte = HTML_B64S.AsByteArray;
                            rowDataprocessedInParallel += Convert.ToUInt64(HTML_B64SAsByte.Length);
                        }
                        else if (HTML_B64S.GetType() == typeof(BsonString))
                        {
                            string HTML_BS64SAsString = HTML_B64S.ToString();
                            rowDataprocessedInParallel += Convert.ToUInt64(HTML_BS64SAsString.Length);
                        }
                        rowsProcessedInParalell++;
                    }
                    Tuple<ulong, ulong> collationInParalell = Tuple.Create(rowDataprocessedInParallel, rowsProcessedInParalell);
                    collation[i] = collationInParalell;
                });

                foreach (Tuple<ulong, ulong> collator in collation)
                {
                    rowDataprocessed += collator.Item1;
                    rowsProcessed += collator.Item2;
                }


                //WebPageHeaders
                var headersCollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                var projection = Builders<BsonDocument>
                                    .Projection
                                    .Include("HeaderID").Include("PageID")
                                    .Include("HeaderKeyLength").Include("HeaderKey")
                                    .Include("HeaderValueLength").Include("HeaderValue");
                var queryResult = headersCollection.Find(filter).Project(projection).ToCursor();
                foreach (var document in queryResult.ToEnumerable())
                {
                    int headerID = Convert.ToInt32(document["HeaderID"]);
                    int PID = Convert.ToInt32(document["PageID"]);
                    int headerKeyLength = Convert.ToInt32(document["HeaderKeyLength"]);
                    rowDataprocessed += Convert.ToUInt64(headerKeyLength);
                    int headerValueLength = Convert.ToInt32(document["HeaderValueLength"]);
                    rowDataprocessed += Convert.ToUInt64(headerValueLength);
                    var headerKey = document["HeaderKey"];
                    var headerValue = document["HeaderValue"];
                    rowsProcessed++;
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
               || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return Tuple.Create(rowDataprocessed, rowsProcessed);
        }

        /// <summary>
        /// Update a characterised web page.  
        /// </summary>
        public override int UpdateCharacterisedWebPage(int pageID)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            try
            {
                var client = GetConnection();
                var sessionOptions = new ClientSessionOptions { };

                using (IClientSessionHandle session = client.StartSession(sessionOptions, _tokenSource.Token))
                {
                    var clientInSession = session.Client;
                    IMongoDatabase database = clientInSession.GetDatabase(_databaseName);
                    if (_amplifier != 0)
                    {
                        var updateList = new List<Tuple<string, FilterDefinition<BsonDocument>, 
                            UpdateDefinition<BsonDocument>>>();
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string CollectionName = _mechanic.NameWithID(encodingTables[i], tableID);
                                //First read the specific document in the collection 
                                var filter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                                var collection = database.GetCollection<BsonDocument>(CollectionName);

                                var projection = Builders<BsonDocument>
                                     .Projection
                                     .Include("URL").Include("HTML").Include("URL_B64S").Include("HTML_B64S");

                                var queryResult = collection.Find(filter).Project(projection).First();

                                string URL = Convert.ToString(queryResult["URL"]);
                                string HTML = Convert.ToString(queryResult["HTML"]);
                                string URL_B64S = Convert.ToString(queryResult["URL_B64S"]);
                                string HTML_B64S = Convert.ToString(queryResult["HTML_B64S"]);

                                URL = _mechanic.Encrypt(URL);
                                HTML = _mechanic.Encrypt(HTML);
                                URL_B64S = _mechanic.Encrypt(URL_B64S);
                                HTML_B64S = _mechanic.Encrypt(HTML_B64S);
                                var updates = Builders<BsonDocument>.Update.Set("URL", URL)
                                              .Set("HTML", HTML).Set("URL_B64S", URL_B64S)
                                              .Set("HTML_B64S", HTML_B64S).Set("ModifiedOn", timestamp);
                                updateList.Add(Tuple.Create(CollectionName, filter, updates));
                            }
                        }
                        for (int i = 0; i < updateList.Count; i++)
                        {
                            string collectionName = updateList[i].Item1;
                            FilterDefinition<BsonDocument> filter = updateList[i].Item2;
                            UpdateDefinition<BsonDocument> updates = updateList[i].Item3;
                            var collection = database.GetCollection<BsonDocument>(collectionName);
                            var updateResult = collection.UpdateOne(filter, updates);
                            rowsAffected += Convert.ToInt32(updateResult.ModifiedCount);
                            var updateCounter = Builders<BsonDocument>.Update.Inc("Updates", 1);
                            collection.UpdateOne(filter, updateCounter);
                        }                        
                    }
                    FilterDefinition<BsonDocument> pageIDfilter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                    UpdateDefinition<BsonDocument> updateset = Builders<BsonDocument>.Update.Set("ModifiedOn", timestamp);
                    UpdateDefinition<BsonDocument> updateCounterO = Builders<BsonDocument>.Update.Inc("Updates", 1);
                    //Then Update WebPages
                    var webPagescollection = database.GetCollection<BsonDocument>("WebPages");
                    var updateResultO = webPagescollection.UpdateOne(pageIDfilter, updateset);
                    webPagescollection.UpdateOne(pageIDfilter, updateCounterO);
                    rowsAffected += Convert.ToInt32(updateResultO.ModifiedCount);

                    //Then Update WebPageStats
                    var webPageStatscollection = database.GetCollection<BsonDocument>("WebPageStats");
                    updateResultO = webPageStatscollection.UpdateOne(pageIDfilter, updateset);
                    webPageStatscollection.UpdateOne(pageIDfilter, updateCounterO);
                    rowsAffected += Convert.ToInt32(updateResultO.ModifiedCount);

                    //Then Update WebPageHeaders
                    var webPageHeaderscollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                    updateResultO = webPageHeaderscollection.UpdateOne(pageIDfilter, updateset);
                    webPageHeaderscollection.UpdateOne(pageIDfilter, updateCounterO);
                    rowsAffected += Convert.ToInt32(updateResultO.ModifiedCount);
                }
                return rowsAffected;
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                    || ex.Message.ToLower().Contains("a timeout occurred "))
                { 
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    _tokenSource.Cancel();
                }
                else
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                }
                rowsAffected = 0;
                return rowsAffected;
            }
        }

        /// <summary>
        /// Update a characterised web page by replacing it. 
        /// </summary>
        public override int UpdateCharacterisedWebPageInPlace(int pageID, WebPage newPage)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            try
            {
                var client = GetConnection();
                var sessionOptions = new ClientSessionOptions { };

                using (IClientSessionHandle session = client.StartSession(sessionOptions, _tokenSource.Token))
                {
                    var clientInSession = session.Client;
                    IMongoDatabase database = clientInSession.GetDatabase(_databaseName);
                    var collection = database.GetCollection<BsonDocument>("WebPages");
                    var filter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                    var updates = Builders<BsonDocument>.Update.Set("LoadedOn", timestamp)
                                            .Set("HeadersLength", newPage.HeadersLength)
                                             .Set("StatsLength", newPage.StatsLength)
                                             .Set("TotalLength", newPage.TotalLength)
                                             .Set("HREFs", newPage.HREFS)
                                             .Set("HashURL", newPage.HashURL)
                                              .Set("HashHTML", newPage.HashHTML)
                                              .Set("URL", newPage.URL)
                                               .Set("HTML", newPage.HTMLBinary);
                    var updateResult = collection.UpdateOne(filter, updates);
                    rowsAffected += Convert.ToInt32(updateResult.ModifiedCount);

                    if (_amplifier != 0)
                    { 
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string CollectionName = _mechanic.NameWithID(encodingTables[i], tableID);
                                //Delete the existing pages
                                collection = database.GetCollection<BsonDocument>(CollectionName);
                                collection.DeleteMany(filter);


                                //Now insert the new page
                                int htmlIndex = tableID * 8 + i + 4;
                                int urlIndex = tableID * 8 + i;
                                int b64SHTMLIndex = tableID * 8 + i + 4;
                                int b64SURLIndex = tableID * 8 + i;

                                string convertedHTML = Encoding.ASCII.GetString(newPage.DataAsBytes[htmlIndex], 0, newPage.DataAsBytes[htmlIndex].Length);
                                string convertedURL = Encoding.ASCII.GetString(newPage.DataAsBytes[urlIndex], 0, newPage.DataAsBytes[urlIndex].Length);
                                string convertedB64SHTML = Encoding.ASCII.GetString(newPage.DataAsBS64s[b64SHTMLIndex]);
                                string convertedB64SURL = Encoding.ASCII.GetString(newPage.DataAsBS64s[b64SURLIndex]);

                                //Web Page Base JSON Object
                                var encodedWebPageDocument = new BsonDocument
                                {
                                    { "PageID", pageID},                                             //      PageID Generated Seperately
                                    { "ModifiedOn", BsonNull.Value},                                                  //      Columns ModifiedOn and Updates get default values
                                    { "Updates", 0},                                                      //      Default Value
                                    { "URL_Length", newPage.DataAsBytes[tableID * 8 + i].Length},            //      Integer (int)
                                    { "URL", convertedURL},                                               //      Byte Array (binary)
                                    { "HTML_Length",  newPage.DataAsBytes[tableID * 8 + i + 4].Length},      //      Integer (int)
                                    { "HTML",  convertedHTML},                                            //      Byte Array (binary)
                                    { "URL_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i].Length},       //      Integer (int)
                                    { "URL_B64S", convertedB64SURL},                      //      Byte Array (binary)
                                    { "HTML_B64S_Length",  newPage.DataAsBS64s[tableID * 8 + i + 4].Length}, //      Integer (int)
                                    { "HTML_B64S", convertedB64SHTML}                 //      Byte Array (binary)
                                };
                                collection.InsertOne(encodedWebPageDocument);
                                rowsAffected++;
                            }
                        }
                    }

                    // Delete the existing entries in webPageHeaders
                    collection = database.GetCollection<BsonDocument>("WebPageHeaders");
                    collection.DeleteMany(filter);

                    //Now insert the new header entries
                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        //Get next sequence ID
                        int headerID = GetNextSequenceValue("Seq_HeaderID");
                        //Web Page Base JSON Object
                        var headerDocument = new BsonDocument
                        {
                            { "HeaderID",headerID},
                            { "PageID", pageID},
                            { "ModifiedOn", BsonNull.Value},
                            { "Updates", 0},
                            { "HeaderKeyLength", kvp.Key.Length},
                            { "HeaderKey", kvp.Key},
                            { "HeaderValueLength", kvp.Value.Length},
                            { "HeaderValue",  kvp.Value}
                        };

                        var headersCollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                        headersCollection.InsertOne(headerDocument);
                        rowsAffected++;
                    }

                    // Delete the existing entries in webPageStats
                    collection = database.GetCollection<BsonDocument>("WebPageStats");
                    collection.DeleteMany(filter);


                   // Now insert the new stat entries
                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            //Get next sequence ID
                            int statID = GetNextSequenceValue("Seq_StatID");
                            //Web Page Base JSON Object
                            var statDocument = new BsonDocument
                            {
                                { "StatID", statID},
                                { "PageID", pageID},
                                { "ModifiedOn", BsonNull.Value},
                                { "Updates", 0},
                                { "CodeType", codeType},
                                { "Length", newPage.StatsAsBytes[codeType].Length},
                                { "Stats", newPage.StatsAsBytes[codeType]},
                                { "B64S_Length", newPage.StatsAsBS64s[codeType].Length},
                                { "B64S_Stats", newPage.StatsAsBS64s[codeType]}
                            };

                            var statsCollection = database.GetCollection<BsonDocument>("WebPageStats");
                            statsCollection.InsertOne(statDocument);
                            rowsAffected++;
                        }
                    }
                }
                return rowsAffected;
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                    || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    _tokenSource.Cancel();
                }
                else
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                }
                rowsAffected = 0;
                return rowsAffected;
            }
        }

        /// <summary>
        /// Update a characterised web page.  
        /// </summary>
        public override int UpdateEncodedWebPage(int pageID)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            try
            {
                var client = GetConnection();
                var sessionOptions = new ClientSessionOptions { };

                using (IClientSessionHandle session = client.StartSession(sessionOptions, _tokenSource.Token))
                {
                    var clientInSession = session.Client;
                    IMongoDatabase database = clientInSession.GetDatabase(_databaseName);
                    if (_amplifier != 0)
                    {
                        var updateList = new List<Tuple<string, FilterDefinition<BsonDocument>,
                            UpdateDefinition<BsonDocument>>>();
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string CollectionName = _mechanic.NameWithID(encodingTables[i], tableID);
                                //First read the specific document in the collection 
                                var filter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                                var collection = database.GetCollection<BsonDocument>(CollectionName);

                                var projection = Builders<BsonDocument>
                                     .Projection
                                     .Include("URL").Include("HTML").Include("URL_B64S").Include("HTML_B64S");

                                var queryResult = collection.Find(filter).Project(projection).First();

                                var URLResult = queryResult["URL"];
                                byte[] URLEncoded = URLResult.AsByteArray;
                                var HTMLResult = queryResult["HTML"];
                                byte[] HTMLEncoded = HTMLResult.AsByteArray;
                                var URL_B64SResult = queryResult["URL_B64S"];
                                byte[] URL_B64SEncoded = URL_B64SResult.AsByteArray;
                                var HTML_B64SResult =queryResult["HTML_B64S"];
                                byte[] HTML_B64S_Encoded = HTML_B64SResult.AsByteArray;

                                Encoding encoder = Encoding.ASCII;
                                if (CollectionName.Contains("Unicode"))
                                {
                                    encoder = Encoding.Unicode;
                                }
                                else if (CollectionName.Contains("ASCII"))
                                {
                                    encoder = Encoding.ASCII;
                                }
                                else if (CollectionName.Contains("EBCDIC"))
                                {
                                    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                                    encoder = Encoding.GetEncoding(37);
                                }
                                else if (CollectionName.Contains("UTF32"))
                                {
                                    encoder = Encoding.UTF32;
                                }

                                string URL = _mechanic.Encrypt(encoder.GetString(URLEncoded));
                                string HTML = _mechanic.Encrypt(encoder.GetString(HTMLEncoded));
                                string URL_B64S = _mechanic.Encrypt(encoder.GetString(URL_B64SEncoded));
                                string HTML_B64S = _mechanic.Encrypt(encoder.GetString(HTML_B64S_Encoded));

                                URLEncoded = encoder.GetBytes(URL);
                                HTMLEncoded = encoder.GetBytes(HTML);
                                URL_B64SEncoded = Encoding.ASCII.GetBytes(Convert.ToBase64String(URLEncoded));
                                HTML_B64S_Encoded = Encoding.ASCII.GetBytes(Convert.ToBase64String(HTMLEncoded));

                                var updates = Builders<BsonDocument>.Update.Set("URL", URLEncoded)
                                .Set("HTML", HTMLEncoded).Set("URL_B64S", URL_B64SEncoded)
                                .Set("HTML_B64S_Encoded", HTML_B64S).Set("ModifiedOn", timestamp);
                                 updateList.Add(Tuple.Create(CollectionName, filter, updates));
                            }
                        }
                        for (int i = 0; i < updateList.Count; i++)
                        {
                            string collectionName = updateList[i].Item1;
                            FilterDefinition<BsonDocument> filter = updateList[i].Item2;
                            UpdateDefinition<BsonDocument> updates = updateList[i].Item3;
                            var collection = database.GetCollection<BsonDocument>(collectionName);
                            var updateResult = collection.UpdateOne(filter, updates);
                            rowsAffected += Convert.ToInt32(updateResult.ModifiedCount);
                            var updateCounter = Builders<BsonDocument>.Update.Inc("Updates", 1);
                            collection.FindOneAndUpdate(filter, updateCounter);
                        }
                    }
                    FilterDefinition<BsonDocument> pageIDfilter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                    UpdateDefinition<BsonDocument> updateset = Builders<BsonDocument>.Update.Set("ModifiedOn", timestamp);
                    UpdateDefinition<BsonDocument> updateCounterO = Builders<BsonDocument>.Update.Inc("Updates", 1);
                    //Then Update WebPages
                    var webPagescollection = database.GetCollection<BsonDocument>("WebPages");
                    var updateResultO = webPagescollection.UpdateOne(pageIDfilter, updateset);
                    webPagescollection.UpdateOne(pageIDfilter, updateCounterO);
                    rowsAffected += Convert.ToInt32(updateResultO.ModifiedCount);

                    //Then Update WebPageStats
                    var webPageStatscollection = database.GetCollection<BsonDocument>("WebPageStats");
                    updateResultO = webPageStatscollection.UpdateOne(pageIDfilter, updateset);
                    webPageStatscollection.UpdateOne(pageIDfilter, updateCounterO);
                    rowsAffected += Convert.ToInt32(updateResultO.ModifiedCount);

                    //Then Update WebPageHeaders
                    var webPageHeaderscollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                    updateResultO = webPageHeaderscollection.UpdateOne(pageIDfilter, updateset);
                    webPageHeaderscollection.UpdateOne(pageIDfilter, updateCounterO);
                    rowsAffected += Convert.ToInt32(updateResultO.ModifiedCount);
                }
                return rowsAffected;
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                    || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    _tokenSource.Cancel();
                }
                else
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                }
                rowsAffected = 0;
                return rowsAffected;
            }
        }

        /// <summary>
        /// Update and encoded web page. 
        /// </summary>
        public override int UpdateEncodedWebPageInPlace(int pageID, WebPage newPage)
        {
            int rowsAffected = 0;
            DateTime timestamp = DateTime.Now;
            try
            {
                var client = GetConnection();
                var sessionOptions = new ClientSessionOptions { };

                using (IClientSessionHandle session = client.StartSession(sessionOptions, _tokenSource.Token))
                {
                    var clientInSession = session.Client;
                    IMongoDatabase database = clientInSession.GetDatabase(_databaseName);
                    var collection = database.GetCollection<BsonDocument>("WebPages");
                    var filter = Builders<BsonDocument>.Filter.Eq("PageID", pageID);
                    var updates = Builders<BsonDocument>.Update.Set("LoadedOn", timestamp)
                                            .Set("HeadersLength", newPage.HeadersLength)
                                             .Set("StatsLength", newPage.StatsLength)
                                             .Set("TotalLength", newPage.TotalLength)
                                             .Set("HREFs", newPage.HREFS)
                                             .Set("HashURL", newPage.HashURL)
                                              .Set("HashHTML", newPage.HashHTML)
                                              .Set("URL", newPage.URL)
                                               .Set("HTML", newPage.HTMLBinary);
                    var updateResult = collection.UpdateOne(filter, updates);
                    rowsAffected += Convert.ToInt32(updateResult.ModifiedCount);

                    if (_amplifier != 0)
                    {
                        for (int i = _mechanic.ReturnWebPageEncodingtables.GetLowerBound(0); i <= _mechanic.ReturnWebPageEncodingtables.GetUpperBound(0); i++)
                        {
                            for (int tableID = 0; tableID < _amplifier; tableID++)
                            {
                                string[] encodingTables = _mechanic.ReturnWebPageEncodingtables;
                                string CollectionName = _mechanic.NameWithID(encodingTables[i], tableID);
                                //Delete the existing pages
                                collection = database.GetCollection<BsonDocument>(CollectionName);
                                collection.DeleteMany(filter);

                                //Now insert the new page
                                //Web Page Base JSON Object
                                var encodedWebPageDocument = new BsonDocument
                                {
                                    { "PageID", pageID},                                             //      PageID Generated Seperately
                                    { "ModifiedOn", BsonNull.Value},                                                //      Columns ModifiedOn and Updates get default values
                                    { "Updates", 0},                                                      //      Default Value
                                    { "URL_Length", newPage.DataAsBytes[tableID * 8 + i].Length},            //      Integer (int)
                                    { "URL",  newPage.DataAsBytes[tableID * 8 + i]},                         //      Byte Array (binary)
                                    { "HTML_Length", newPage.DataAsBytes[tableID * 8 + i + 4].Length},      //      Integer (int)
                                    { "HTML",  newPage.DataAsBytes[tableID * 8 + i + 4]},                    //      Byte Array (binary)
                                    { "URL_B64S_Length", newPage.DataAsBS64s[tableID * 8 + i].Length},       //      Integer (int)
                                    { "URL_B64S", newPage.DataAsBS64s[tableID * 8 + i]},                      //      Byte Array (binary)
                                    { "HTML_B64S_Length",  newPage.DataAsBS64s[tableID * 8 + i + 4].Length}, //      Integer (int)
                                    { "HTML_B64S", newPage.DataAsBS64s[tableID * 8 + i + 4]}                 //      Byte Array (binary)
                                };
                                collection.InsertOne(encodedWebPageDocument);
                                rowsAffected++;
                            }
                        }
                    }

                    // Delete the existing entries in webPageHeaders
                    collection = database.GetCollection<BsonDocument>("WebPageHeaders");
                    collection.DeleteMany(filter);

                    //Now insert the new header entries
                    foreach (KeyValuePair<string, string> kvp in newPage.Headers)
                    {
                        //Get next sequence ID
                        int headerID = GetNextSequenceValue("Seq_HeaderID");
                        //Web Page Base JSON Object
                        var headerDocument = new BsonDocument
                        {
                            { "HeaderID",headerID},
                            { "PageID", pageID},
                            { "ModifiedOn", BsonNull.Value},
                            { "Updates", 0},
                            { "HeaderKeyLength", kvp.Key.Length},
                            { "HeaderKey", kvp.Key},
                            { "HeaderValueLength", kvp.Value.Length},
                            { "HeaderValue",  kvp.Value}
                        };

                        var headersCollection = database.GetCollection<BsonDocument>("WebPageHeaders");
                        headersCollection.InsertOne(headerDocument);
                        rowsAffected++;
                    }

                    // Delete the existing entries in webPageStats
                    collection = database.GetCollection<BsonDocument>("WebPageStats");
                    collection.DeleteMany(filter);


                    // Now insert the new stat entries
                    if (_amplifier != 0)
                    {
                        for (int codeType = 0; codeType <= newPage.StatsAsBytes.GetUpperBound(0); codeType++)
                        {
                            //Get next sequence ID
                            int statID = GetNextSequenceValue("Seq_StatID");
                            //Web Page Base JSON Object
                            var statDocument = new BsonDocument
                            {
                                { "StatID", statID},
                                { "PageID", pageID},
                                { "ModifiedOn", BsonNull.Value},
                                { "Updates", 0},
                                { "CodeType", codeType},
                                { "Length", newPage.StatsAsBytes[codeType].Length},
                                { "Stats", newPage.StatsAsBytes[codeType]},
                                { "B64S_Length", newPage.StatsAsBS64s[codeType].Length},
                                { "B64S_Stats", newPage.StatsAsBS64s[codeType]}
                            };

                            var statsCollection = database.GetCollection<BsonDocument>("WebPageStats");
                            statsCollection.InsertOne(statDocument);
                            rowsAffected++;
                        }
                    }
                }
                return rowsAffected;
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                    || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                    _tokenSource.Cancel();
                }
                else
                {
                    _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Warning));
                }
                rowsAffected = 0;
                return rowsAffected;
            }
        }

        /// <summary>
        /// Update the import history for any used data files. 
        /// </summary>
        public override bool UpdateImportHistory(ImportHandler import)
        {
            bool success = false;
            var client = GetConnection();
            IMongoDatabase database = client.GetDatabase(_databaseName);
            try
            {
                var filter = Builders<BsonDocument>.Filter.Eq("FileName", import.Filename);
                var updates = Builders<BsonDocument>.Update.Set("ImportedWebPages", import.ImportedWebPages)
                                                          .Set("ImportedAllWebPages", import.AllImportedWebPages);
                var collection = database.GetCollection<BsonDocument>("Imports");
                var result = collection.UpdateMany(filter, updates);
                if (result.IsModifiedCountAvailable)
                {
                   if(result.ModifiedCount != 0)
                    {
                        success = true;
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.ToLower().Contains("unable to authenticate")
                       || ex.Message.ToLower().Contains("a timeout occurred "))
                {
                    _tokenSource.Cancel();
                }
                _messageQueue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
            return success;
        }

        /// <summary>
        /// Not implemented
        /// </summary>
        public override void VendorAdvancedOperations(int numberOfThreads)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not implemented
        /// </summary>
        public override void VendorComplexOperations()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not implemented
        /// </summary>
        public override void VendorConsistencyCheck()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Retrieves next sequence value from collection
        /// </summary>
        private int GetNextSequenceValue(string sequenceName)
        {
            var client = GetConnection();
            IMongoDatabase database = client.GetDatabase(_databaseName);
            var sequenceCollection = database.GetCollection<BsonDocument>(sequenceName);
            var filterQuery = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", "IDseq"));
            var updateQuery = Builders<BsonDocument>.Update.Inc("sequence_value", 1);
            BsonDocument sequenceDocument = sequenceCollection.FindOneAndUpdate(filterQuery, updateQuery);
            return (int)sequenceDocument["sequence_value"];
        }

    }
}
