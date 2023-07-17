using System;
using System.IO;
using DOES.Shared.Resources;
using DOES.Shared.Debug;
using System.Collections.Generic;
using System.Xml.Linq;
using System.Linq;
using System.Runtime.InteropServices;

namespace DOES.Shared.Operations
{
    public class AnalyticsPersistence
    {
        private AnalyticsDatabase _database;
        private MessageQueue _queue;
        private string _dbHostName = null;
        private string _dbname = null;
        private string _dbuser = null;
        private string _dbpassword = null;
        private string _dbinstance = null;
        private string _dbport = null;
        Dynamics.Database _dbType;

        private string[] AnalyticsTablesSet =
        {
            "Tests",
            "Objects",
            "SequenceStats",
            "WindowsResourceData",
            "LinuxResourceData",
            "DataEngineStatsTotal",
            "DataEngineStatsThreadsInterim",
            "DataEngineStatsThreads",
            "DataEngineStatsInterim",
            "DataEngineFinalReports",
        };

        public List<string> GetXMLResource()
        {
            string result = string.Empty;
            List<string> xmlListing = new List<string>();
            string resouceLocal = string.Empty;

            if (_dbType == Dynamics.Database.MicrosoftSQL)
            {
                resouceLocal = "DOES.Shared.Configuration.MSSQL_Analytics.xml";
            }
            else if (_dbType == Dynamics.Database.Oracle)
            {
                resouceLocal = "DOES.Shared.Configuration.OracleDB_Analytics.xml";
            }
            else if (_dbType == Dynamics.Database.SAPHANA)
            {
                resouceLocal = "DOES.Shared.Configuration.SAPHANA_Analytics.xml";
            }
            else if (_dbType == Dynamics.Database.MySQL)
            {
                resouceLocal = "DOES.Shared.Configuration.MySQL_Analytics.xml";
            }
            else if (_dbType == Dynamics.Database.MariaDB)
            {
                resouceLocal = "DOES.Shared.Configuration.MariaDB_Analytics.xml";
            }
            else if (_dbType == Dynamics.Database.PostgreSQL)
            {
                resouceLocal = "DOES.Shared.Configuration.PostgreSQL_Analytics.xml";
            }

            using (Stream stream = this.GetType().Assembly.
                       GetManifestResourceStream(resouceLocal))
            {
                using StreamReader sr = new StreamReader(stream);
                result = sr.ReadToEnd();
            }

            XDocument doc = XDocument.Parse(result);
            var list = doc.Root.Elements("string")
                           .Select(element => element.Value)
                           .ToList();
            foreach (string value in list)
            {
                string item = value;
                xmlListing.Add(item);
            }
            return xmlListing;
        }

        public string[] AnalyticsTables { get { return AnalyticsTablesSet; } }

        public AnalyticsPersistence(MessageQueue queue)
        {
            _dbType = Dynamics.Database.MicrosoftSQL;
            _queue = queue;
            string dbConfigFile = string.Empty;
            string dbconfigFileName = "Analytics.conf";
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == true)
                {
                    dbConfigFile = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData) + @"\Pure Storage\D.O.E.S\Config\" + dbconfigFileName;
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == true)
                {
                    //Then its a linux platform
                    dbConfigFile = @"/opt/purestorage/does/config/" + dbconfigFileName;
                }
                
                using (var file = new FileStream(dbConfigFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    using (var configReader = new StreamReader(file))
                    {
                        string line;
                        while ((line = configReader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("#")))
                            {
                                string[] parsedLine = line.Split('=');
                                if (parsedLine[0] == "DBVENDOR")
                                {
                                    if (parsedLine[1].ToLower() == "mssql")
                                    {
                                        _dbType = Dynamics.Database.MicrosoftSQL;
                                    }
                                    else if (parsedLine[1].ToLower() == "oracle")
                                    {
                                        _dbType = Dynamics.Database.Oracle;
                                    }
                                    else if (parsedLine[1].ToLower() == "saphana")
                                    {
                                        _dbType = Dynamics.Database.SAPHANA;
                                    }
                                    else if (parsedLine[1].ToLower() == "mysql")
                                    {
                                        _dbType = Dynamics.Database.MySQL;
                                    }
                                    else if (parsedLine[1].ToLower() == "mariadb")
                                    {
                                        _dbType = Dynamics.Database.MariaDB;
                                    }
                                    else if (parsedLine[1].ToLower() == "postgresql")
                                    {
                                        _dbType = Dynamics.Database.PostgreSQL;
                                    }
                                }
                                else if (parsedLine[0] == "HOSTNAME")
                                {
                                    if (parsedLine[1] != "")
                                    {
                                        _dbHostName = parsedLine[1];
                                    }
                                    else
                                    {
                                        _dbHostName = "localhost";
                                    }
                                }
                                else if (parsedLine[0] == "DATABASE")
                                {
                                    _dbname = parsedLine[1];
                                }
                                else if (parsedLine[0] == "USER")
                                {
                                    _dbuser = parsedLine[1];
                                    if (_dbuser == "") { _dbuser = null; }
                                }
                                else if (parsedLine[0] == "PASSWORD")
                                {
                                    _dbpassword = parsedLine[1];
                                    if (_dbpassword == "") { _dbpassword = null; }
                                }
                                else if (parsedLine[0] == "INSTANCE")
                                {
                                    _dbinstance = parsedLine[1];
                                    if(_dbinstance == "") { _dbinstance = null; }
                                }
                                else if (parsedLine[0] == "PORT")
                                {
                                    _dbport = parsedLine[1];
                                    if (_dbport == "") { _dbport = null; }
                                }
                            }
                        }
                        if (_dbHostName != null && _dbname != null)
                        {
                            if (_dbport == null)
                            {
                                switch(_dbType)
                                {
                                    case Dynamics.Database.MicrosoftSQL:
                                        _dbport = "1433";
                                        break;
                                    case Dynamics.Database.Oracle:
                                        _dbport = "1521";
                                        break;
                                    case Dynamics.Database.SAPHANA:
                                        _dbport = "15";
                                        break;
                                    case Dynamics.Database.MySQL:
                                        _dbport = "3306";
                                        break;
                                    case Dynamics.Database.MariaDB:
                                        _dbport = "3306";
                                        break;
                                    case Dynamics.Database.PostgreSQL:
                                        _dbport = "5432";
                                        break;
                                }
                            }
                            if (_dbinstance == null)
                            {
                                switch (_dbType)
                                {
                                    case Dynamics.Database.MicrosoftSQL:
                                        _dbinstance = "MSSQLSERVER";
                                        break;
                                    case Dynamics.Database.Oracle:
                                        _dbinstance = "orcl";
                                        break;
                                    case Dynamics.Database.SAPHANA:
                                        _dbinstance = "00";
                                        break;
                                }
                            }
                            _database = new AnalyticsDatabase(_dbType, _dbHostName, _dbname, _dbuser, _dbpassword, _dbinstance, _dbport);
                        }
                        else
                        {
                            _queue.AddMessage(new Message(DateTime.Now,
                                "Not Enough Data supplied to log data to database , reporting front end active only", 
                                Message.MessageType.Warning));
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                _queue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
            }
        }

        public AnalyticsDatabase PersistenceInstance { get {return _database; }}
    }
}
