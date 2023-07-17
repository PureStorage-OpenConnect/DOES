using DOES.Shared.Resources;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Sap.Data.Hana;
using System;
using System.Data.SqlClient;
using System.Runtime.InteropServices;

namespace DOES.Shared.Operations
{
    public class AnalyticsDatabase
    {
        Dynamics.Database _dbType;
        string _hostname;
        string _databaseName;
        string _userName;
        string _password;
        string _instance;
        string _port;
        //This needs cleaning up. Need to have config option in file for oracle connection type switch
        bool _useSID = false;

        public AnalyticsDatabase(Dynamics.Database vendor, string hostname, string name, string user, string password, string instance, string port)
        {
            _dbType = vendor;
            _hostname = hostname;
            _databaseName = name;
            _userName = user;
            _password = password;
            _instance = instance;
            _port = port;
        }

        public Dynamics.Database DatabaseVendor { get { return _dbType; } set { _dbType = value; } }
        public string Hostename { get { return _hostname; } set { _hostname = value; } }
        public string DatabaseName { get { return _databaseName; } set { _databaseName = value; } }
        public string User { get { return _userName; } set { _userName = value; } }
        public string Password { get { return _password; } set { _password = value; } }
        public string Instance { get { return _instance; } set { _instance = value; } }
        public string Port { get { return _port; } set { _port = value; } }


        private SqlConnection GetMicrosoftSQLConnection()
        {
            string _connectionString;
            if (_instance != null)
            {
                if (_userName == null && _password == null)
                {
                    _connectionString = @"Data Source=" + _hostname + "," + _port + "\\" + _instance + ";Initial Catalog=" + _databaseName +
                        ";MultipleActiveResultSets = true;Max Pool Size=1000;Integrated Security=True" + "; Application Name=D.O.E.S Analytics Foundation; ";
                }
                else
                {
                    _connectionString = @"Data Source=" + _hostname + "," + _port + "\\" + _instance + ";Initial Catalog=" + _databaseName +
                        ";MultipleActiveResultSets = true;Max Pool Size=1000;User ID=" + _userName + ";Password=" + _password + ";Application Name=D.O.E.S Analytics Foundation;";
                }
            }
            else
            {
                if (_userName == null && _password == null)
                {
                    _connectionString = "Data Source=" + _hostname + "," + _port + ";Initial Catalog=" + _databaseName +
                        ";MultipleActiveResultSets = true;Max Pool Size=1000;Integrated Security=True" + ";Application Name=D.O.E.S Analytics Foundation;";
                }
                else
                {
                    _connectionString = "Data Source=" + _hostname + "," + _port + ";Initial Catalog=" + _databaseName +
                        ";MultipleActiveResultSets = true;Max Pool Size=1000;User ID=" + _userName + ";Password=" + _password + ";Application Name=D.O.E.S Analytics Foundation;";
                }
            }

            SqlConnection connection = new SqlConnection(_connectionString);
            return connection;
        }

        private OracleConnection GetOracleConnection()
        {
            string _connectionString;

            if (_useSID)
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

        private MySqlConnection GetMySQLConnection()
        {
            string _connectionString;
            _connectionString = "server=" + _hostname + ";port=" + _port + ";user id=" + _userName + "; password=" + _password + ";database=" + _databaseName;
            MySqlConnection connection = new MySqlConnection(_connectionString);
            return connection;
        }

        private NpgsqlConnection GetPostgreSQLConnection()
        {
            string _connectionString = "Host=" + _hostname + ";Username=" + _userName + ";Password=" + _password + ";Database=" + _databaseName + ";Port=" + _port;
            NpgsqlConnection connection = new NpgsqlConnection(_connectionString);
            return connection;
        }

        private MySqlConnection GetMariaDBConnection()
        {
            string _connectionString;
            _connectionString = "server=" + _hostname + ";port=" + _port + ";user id=" + _userName + "; password=" + _password + ";database=" + _databaseName;
            MySqlConnection connection = new MySqlConnection(_connectionString);
            return connection;
        }

        private HanaConnection GetHANAConnection()
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

        public dynamic GetConnection()
        {
            if(_dbType == Dynamics.Database.MicrosoftSQL)
            {
                return GetMicrosoftSQLConnection();
            }
            else if(_dbType == Dynamics.Database.Oracle)
            {
                return GetOracleConnection();
            }
            else if (_dbType == Dynamics.Database.SAPHANA)
            {
                return GetHANAConnection();
            }
            else if (_dbType == Dynamics.Database.MySQL)
            {
                return GetMySQLConnection();
            }
            else if (_dbType == Dynamics.Database.MariaDB)
            {
                return GetMariaDBConnection();
            }
            else if (_dbType == Dynamics.Database.PostgreSQL)
            {
                return GetPostgreSQLConnection();
            }
            else
            {
                return null;
            }
        }

    }
}