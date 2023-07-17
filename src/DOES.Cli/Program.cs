using DOES.Shared.Debug;
using DOES.Shared.Operations;
using DOES.Shared.Resources;
using DOES.DataEngine.Operations;
using DOES.DataEngine.Resources;
using Microsoft.Extensions.CommandLineUtils;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DOES.Cli
{
    class Program
    {

        static void Main(string[] args)
        {
            Operation operatorObj;
            CancellationTokenSource _tokenSource = new CancellationTokenSource();

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                _tokenSource.Cancel();
            };


            CommandLineApplication commandLineApplication =
                new CommandLineApplication(throwOnUnexpectedArg: false);

            #region Arguments
            CommandOption engineArg = commandLineApplication.Option("-e |--engine <engine>", "The DOES.Cli engine <data | platform | analysis> " + 
                "to work with. ", CommandOptionType.SingleValue);
            CommandOption functionArg = commandLineApplication.Option("-f |--function <function>", "The function to execute for the" + 
                " specified engine. Use --engine <engine> --function help for more information.", CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                                     General Arguments                                                    /
            /***************************************************************************************************************************/
            CommandOption hostnameArg = commandLineApplication.Option("-h |--hostname <hostname>", "(Data/Platform Engine/OPTIONAL) The hostname or IP address " +
                "of the database to operate on or platform to be monitored", CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                                 DataEngine General Arguments                                             /
            /***************************************************************************************************************************/
            CommandOption databaseTypeArg = commandLineApplication.Option("--databasetype <databasetype>", "(DataEngine) The Database Type " + 
                "<MicrosoftSQL | Oracle | SAPHANA | MySQL | MariaDB | PostgreSQL | MongoDB> to work with", CommandOptionType.SingleValue);
            CommandOption databaseNameArg = commandLineApplication.Option("--databasename <databasename>", "(DataEngine) The database name " +
                "to perform operations on", CommandOptionType.SingleValue);
            CommandOption databaseUsernameArg = commandLineApplication.Option("--username <username>", "(DataEngine) A database user " +
                "with the correct permissions", CommandOptionType.SingleValue);
            CommandOption databasePasswordArg = commandLineApplication.Option("--password <password>", "(DataEngine) The password for " +
                "the database user", CommandOptionType.SingleValue);
            CommandOption amountArg = commandLineApplication.Option("--amount <amount>", "(DataEngine/OPTIONAL) The amount of data which will be operated " +
                "on in the database", CommandOptionType.SingleValue);
            CommandOption unitArg = commandLineApplication.Option("--unit <unit>", "(DataEngine/OPTIONAL) The storage unit " +
                "<Bytes | Kilobytes | Megabytes | Gigabytes | Terabytes> of measurement for the amount of data to be operated on",
                CommandOptionType.SingleValue);
            CommandOption numberOfThreadsArg = commandLineApplication.Option("--numberofthreads <numberofthreads>", "(DataEngine/OPTIONAL) The number of parallel " +
                "workers for the operation", CommandOptionType.SingleValue);
            CommandOption schemaTypeArg = commandLineApplication.Option("--schematype <schema>", "(DataEngine/OPTIONAL)The type of schema to create" +
                " <WithIndexes | WithoutIndexes | WithIndexesLOB | WithoutIndexesLOB | MemoryOptimised | MemoryOptimisedWithoutIndexes | " +
                "MemoryOptimisedLOB | MemoryOptimisedWithoutIndexesLOB>", CommandOptionType.SingleValue);
            CommandOption tableAmplificationArg = commandLineApplication.Option("--tableamplification <tableamplification>", "(DataEngine/OPTIONAL) Controls the" +
                " number of tables to create for the schema ", CommandOptionType.SingleValue);
            CommandOption columnWidthArg = commandLineApplication.Option("--columnwidth <columnwidth>", "(DataEngine/OPTIONAL) How wide the HTML column" +
                " should be", CommandOptionType.SingleValue);
            CommandOption folderArg = commandLineApplication.Option("--folder <folder>", "(DataEngine/OPTIONAL) (export) The path to a folder where exported DOES.DataEngine data will be placed" +
                " (add)The path to a folder to holding exported" +
                " Engine-Oil files", CommandOptionType.SingleValue);
            CommandOption randomPercentageArg = commandLineApplication.Option("--randompercentage <randompercentage>", "(DataEngine/OPTIONAL) The percentage of how " +
                "random the data will be", CommandOptionType.SingleValue);
            CommandOption portArg = commandLineApplication.Option("--port <port>", "(DataEngine/OPTIONAL) The port number on which the database is" +
                " available", CommandOptionType.SingleValue);
            CommandOption clearOperationArg = commandLineApplication.Option("--clearoperation <operation>", "(DataEngine/OPTIONAL) The operation to" +
                " use when clearing the database objects <Drop | Truncate>", CommandOptionType.SingleValue);
            CommandOption queryOperationArg = commandLineApplication.Option("--queryoperation <operation>", "(DataEngine/OPTIONAL) The operation to" +
                " use when querying the database objects <LeftOuterJoin | UnionAll>", CommandOptionType.SingleValue);
            CommandOption updateReplaceArg = commandLineApplication.Option("--replace", "(DataEngine/OPTIONAL) Indicates that when updating" +
                " web pages existing pages should be completely replaced", CommandOptionType.NoValue);
            CommandOption testTypeArg = commandLineApplication.Option("--testtype <test>", "(DataEngine/OPTIONAL) The type of test operation" +
               "to perform <Simple | Advanced | Complex>", CommandOptionType.SingleValue);
            CommandOption deferInitialIngestArg = commandLineApplication.Option("--deferinitialingest ", "(DataEngine/OPTIONAL) Indicates that when " +
                "performing test functions there will be no initial data ingest ", CommandOptionType.NoValue);
            CommandOption growthRateArg = commandLineApplication.Option("--growthrate <growthrate>", "(DataEngine/OPTIONAL) Indicates how much (in %) the dataset" +
                " should grow when using test functions", CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                              DataEngine Microsoft SQL Arguments                                          /
            /***************************************************************************************************************************/
            CommandOption namedInstanceArg = commandLineApplication.Option("--namedinstance <namedinstance>", "(DataEngine/OPTIONAL) The named instance for " +
                "Microsoft SQL server deployments", CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                              DataEngine Oracle Arguments                                                 /
            /***************************************************************************************************************************/
            CommandOption useOracleSIDArg = commandLineApplication.Option("--useoraclesid", "(DataEngine/OPTIONAL) Indicates that the Oracle SID should be used " +
                  "instead of the service name", CommandOptionType.NoValue);

            /***************************************************************************************************************************/
            //                                               DataEngine SAP HANA Arguments                                              /
            /***************************************************************************************************************************/
            CommandOption instanceNumberArg = commandLineApplication.Option("--instancenumber <instancenumber>", "(DataEngine/OPTIONAL) The SAP HANA instance " +
                "number in two digin form (00)", CommandOptionType.SingleValue);
            CommandOption percentageColumnsArg = commandLineApplication.Option("--percentagecolumns <percentagecolumns>", "(DataEngine/OPTIONAL) The percentage of" +
                "SAP HANA column tables to create in the schema", CommandOptionType.SingleValue);
            CommandOption percentagePagedTablesArg = commandLineApplication.Option("--percentagepagedtables <percentagepagedtables>", "(DataEngine/OPTIONAL) The " +
                "percentage of SAP HANA paged tables (Native Storage Extension) to create in the schema", CommandOptionType.SingleValue);
            CommandOption percentageWarmExtensionNodeTablesArg = commandLineApplication.Option("--percentagewarmextensionnodetables " +
                "<percentagewarmextensionnodetables>",
                "(DataEngine/OPTIONAL) The percentage of SAP HANA tables to place in an extension node", CommandOptionType.SingleValue);
            CommandOption extensionNodeGroupNameArg = commandLineApplication.Option("--extensionnodegroupname <extensionnodegroupname>",
                "(DataEngine/OPTIONAL) The SAP HANA extension " +
                "node group name to place data in", CommandOptionType.SingleValue);
            CommandOption partitionsArg = commandLineApplication.Option("--partitions <partitions>", "(DataEngine/OPTIONAL) The number of SAP HANA " +
                "table partitions to create", CommandOptionType.SingleValue);
            CommandOption unloadTablesPercentageArg = commandLineApplication.Option("--unloadtablepercentage <unloadtablepercentage>", "(DataEngine/OPTIONAL) " +
                "The percentage of SAP HANA column  tables to unload table partitions to create", CommandOptionType.SingleValue);
            CommandOption setExtensionNodePercentageArg = commandLineApplication.Option("--setextensionnodepercentage <extensionnodepercentage>", "(DataEngine/OPTIONAL)" +
                " The percentage of SAP HANA tables to place in an extension node", CommandOptionType.SingleValue);
            CommandOption setPagedTablesPercentageArg = commandLineApplication.Option("--setpagedtablespercentage <pagedtablespercentage>", "(DataEngine/OPTIONAL) " +
                "The percentage of SAP HANA tables to set as paged loadable (NSE)", CommandOptionType.SingleValue);
            CommandOption setColumnLoadablePercentageArg = commandLineApplication.Option("--setcolumnloadablepercentage <partitions>", "(DataEngine/OPTIONAL)" +
                " The percentage of SAP HANA tables to set as column loadable", CommandOptionType.SingleValue);
            CommandOption setPercentageToPreloadArg = commandLineApplication.Option("--percentagetopreload <preloadtables>", "(DataEngine/OPTIONAL)" +
                " The percentage of SAP HANA columns tables to be as preload", CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                                 DataEngine MySQL Arguments                                               /
            /***************************************************************************************************************************/
            CommandOption mysqlStorageEngineArg = commandLineApplication.Option("--mysqlstorageengine <mystorageengine>", "(DataEngine/OPTIONAL) The MySQL Storage Engine " +
                "to use", CommandOptionType.SingleValue);
            CommandOption ndbTableSpaceArg = commandLineApplication.Option("--ndbtablespace <ndbtablespace>", "(DataEngine/OPTIONAL) The MySQL NDB Cluster" +
                " tablespace name", CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                                 DataEngine MariaDB Arguments                                             /
            /***************************************************************************************************************************/
            CommandOption mariadbStorageEngineArg = commandLineApplication.Option("--mariadbstorageengine <mariastorageengine>", "(DataEngine/OPTIONAL) The MariaDB Storage Engine " +
                "to use", CommandOptionType.SingleValue);
            CommandOption mariadbSetPercentageArg = commandLineApplication.Option("--setpercentage <setpercentage>", "(DataEngine/OPTIONAL) The percentage value of MariaDB tables to " +
                "change the storage engine of", CommandOptionType.SingleValue);
            CommandOption mariaDBChangeObjectPropArg = commandLineApplication.Option("--changeobjectproperties", "(DataEngine/OPTIONAL)Indicates that the object properties " +
                "for MariaDB should be changed", CommandOptionType.NoValue);

            /***************************************************************************************************************************/
            //                                                 DataEngine MongoDB Arguments                                             /
            /***************************************************************************************************************************/
            CommandOption mongoDBDeploymentTypeArg = commandLineApplication.Option("--mongodbdeploymenttype <mongodbdeploymenttype>", "(DataEngine/OPTIONAL) The MongoDB deployment type "
                , CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                        DataEngine, PlatformEngine Logging Arguments                                      /
            /***************************************************************************************************************************/
            CommandOption logDataArg = commandLineApplication.Option("--logdata", "(Data/Analysis/PlatformEngine/OPTIONAL) Indicates that operataional analytics should " +
                "be collected in a database", CommandOptionType.NoValue);
            CommandOption testNameArg = commandLineApplication.Option("--testname <testname>", "(Data/Analysis/Platform Engine/OPTIONAL) The test name for operational analytics", CommandOptionType.SingleValue);
            CommandOption objectNameArg = commandLineApplication.Option("--objectname <objectname>", "(Data/Platform Engine/OPTIONALL) The object name for " +
                "operational analytics", CommandOptionType.SingleValue);
            CommandOption ObjectCategoryArg = commandLineApplication.Option("--ObjectCategory <ObjectCategory>", "(Data/Platform Engine/OPTIONAL)The object" +
                " Category for object grouping when using operational analytics", CommandOptionType.SingleValue);
            CommandOption sequenceArg = commandLineApplication.Option("--sequence <sequence>", "(Data/Platform Engine/OPTIONAL)The sequence or stage on which the " +
                "operational analytics are indicative of", CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                                 PlatformEngine Arguments                                               /
            /***************************************************************************************************************************/
            CommandOption collectionTypeArg = commandLineApplication.Option("--collectiontype <collectiontype>", "(PlatformEngine/OPTIONAL) The type of resource" +
                " collection <PointInTime | Duration | Interval> operation to perform.", CommandOptionType.SingleValue);
            CommandOption durationArg = commandLineApplication.Option("--duration <duration>", "(PlatformEngine/OPTIONAL) The duration to collection platform" +
                " resources for", CommandOptionType.SingleValue);
            CommandOption intervalArg = commandLineApplication.Option("--interval <interval>", "(Analysis/PlatformEngine/OPTIONAL) The interval between triggers for" +
                " platform resources", CommandOptionType.SingleValue);

            /***************************************************************************************************************************/
            //                                                 AnalysisEngine Arguments                                                 /
            /***************************************************************************************************************************/
            CommandOption solutionArg = commandLineApplication.Option("--solution <solution>", "(AnalysisEngine) The solution <MicrosoftSQL | Oracle | SAPHANA |" +
                            " MySQL}> being tested. ", CommandOptionType.SingleValue);
            CommandOption changeRateArg = commandLineApplication.Option("--changerate <changerate>", "(AnalysisEngine) A percentage value indicating the change " +
                "rate", CommandOptionType.SingleValue);
            CommandOption attemptArg = commandLineApplication.Option("--attempt <attempt>", "(AnalysisEngine) The attempt of the test in numerical form"
                , CommandOptionType.SingleValue);
            CommandOption codeRevisionArg = commandLineApplication.Option("--coderevision <coderevision>", "(AnalysisEngine) The revision of code being tested"
                , CommandOptionType.SingleValue);
            CommandOption dataImportStartArg = commandLineApplication.Option("--dataimportstart <dataimportstart>", "(AnalysisEngine) When the data import operation " +
                "started", CommandOptionType.SingleValue);
            CommandOption dataImportEndArg = commandLineApplication.Option("--dataimportend <dataimportend>", "(AnalysisEngine) When the data import operation ended",
                CommandOptionType.SingleValue);
            CommandOption dataChangeStartArg = commandLineApplication.Option("--datachangestart <datachangestart>", "(AnalysisEngine) When the data change " +
                "operation start", CommandOptionType.SingleValue);
            CommandOption dataChangeEndArg = commandLineApplication.Option("--datachangeend <datachangeend>", "(AnalysisEngine) " + 
                "When the data change operation ended", CommandOptionType.SingleValue);
            CommandOption coreJobStartArg = commandLineApplication.Option("--corejobstart <corejobstart>", "(AnalysisEngine) When the core operation " +
                "started", CommandOptionType.SingleValue);
            CommandOption coreJobEndArg = commandLineApplication.Option("--corejobend <corejobend>",
                "(AnalysisEngine) When the core operation ended", CommandOptionType.SingleValue);
            CommandOption otherJobStartArg = commandLineApplication.Option("--otherjobstart <otherjobstart>", "(AnalysisEngine) When an unnamed operation " +
                "started", CommandOptionType.SingleValue);
            CommandOption otherJobEndArg = commandLineApplication.Option("--otherjobend <otherjobend>",
                "(AnalysisEngine) When an unnamed operation ended", CommandOptionType.SingleValue);
            CommandOption sequenceStartArg = commandLineApplication.Option("--sequencestart <sequencestart>",
                "(AnalysisEngine) When the sequence started", CommandOptionType.SingleValue);
            CommandOption sequenceEndArg = commandLineApplication.Option("--sequenceend <sequenceend>",
                "(AnalysisEngine) When the sequence ended", CommandOptionType.SingleValue);

            CommandOption verboseArg = commandLineApplication.Option("-v | --verbose", "Enable verbose output", CommandOptionType.NoValue);
            commandLineApplication.HelpOption("-? | -h | --help");

            #endregion

            commandLineApplication.OnExecute(() =>
            {
                bool validatedForDataEngineExecution = true;

                #region Parameters 
                string _ip = "localhost";
                if (hostnameArg.HasValue()) { _ip = hostnameArg.Value(); }
                Dynamics.Database _dbType = Dynamics.Database.MicrosoftSQL;
                string _dbname = "";
                UInt64 _amount = 0;
                Dynamics.StorageUnit _unit = Dynamics.StorageUnit.Bytes;
                if (engineArg.Value() == Engine.data.ToString())
                {
                    if (databaseTypeArg.HasValue()) { Enum.TryParse(databaseTypeArg.Value(), out _dbType); }
                    else if (functionArg.Value() == nameof(DataEngineFunctions.help)) { validatedForDataEngineExecution = true; }
                    else { Console.WriteLine("Database type must be provided with --databasetype"); validatedForDataEngineExecution = false; }

                    if (databaseNameArg.HasValue()) { _dbname = databaseNameArg.Value(); }
                    else if (functionArg.Value() == nameof(DataEngineFunctions.help)) { validatedForDataEngineExecution = true; }
                    else { Console.WriteLine("Database name must be provided with --databasename"); validatedForDataEngineExecution = false; }
                }

                if (amountArg.HasValue()) { _amount = Convert.ToUInt64(amountArg.Value()); }

                if (unitArg.HasValue()) { Enum.TryParse(unitArg.Value(), out _unit); }

                string _user = null;
                if (databaseUsernameArg.HasValue()) { _user = databaseUsernameArg.Value(); }

                string _password = null;
                if (databasePasswordArg.HasValue()) { _password = databasePasswordArg.Value(); }

                int _numberOfThreads = 1;
                if (numberOfThreadsArg.HasValue()) { _numberOfThreads = Convert.ToInt32(numberOfThreadsArg.Value()); }

                Dynamics.DatabaseSchema _schema = Dynamics.DatabaseSchema.WithIndexesLOB;
                if (schemaTypeArg.HasValue()) { Enum.TryParse(schemaTypeArg.Value(), out _schema); }

                int _tableAmplifier = 8;
                if (tableAmplificationArg.HasValue()) { _tableAmplifier = Convert.ToInt32(tableAmplificationArg.Value()); }

                int _columnWidth = 2147483647;
                if (columnWidthArg.HasValue()) { _columnWidth = Convert.ToInt32(columnWidthArg.Value()); }

                string _folderPath = null;
                if (folderArg.HasValue()) { _folderPath = folderArg.Value(); }

                double _randomPercentage = 30;
                if (randomPercentageArg.HasValue()) { _randomPercentage = Convert.ToDouble(randomPercentageArg.Value()); }

                Dynamics.ClearingType _clearingType = Dynamics.ClearingType.Drop;
                if (clearOperationArg.HasValue()){ Enum.TryParse(clearOperationArg.Value(), out _clearingType); }

                Dynamics.ReadQuery _queryType = Dynamics.ReadQuery.UnionAll;
                if (queryOperationArg.HasValue()) { Enum.TryParse(queryOperationArg.Value(), out _queryType); }

                bool _replaceWebPages = false;
                if (updateReplaceArg.HasValue()) { _replaceWebPages = true; }

                Dynamics.TestType _testType = Dynamics.TestType.Simple;
                if (testTypeArg.HasValue()) { Enum.TryParse(testTypeArg.Value(), out _testType); }

                bool _deferInitialIngest = false;
                if (deferInitialIngestArg.HasValue()) { _deferInitialIngest = true; }

                int _changeRate = 10;
                if (changeRateArg.HasValue()) { _changeRate = Convert.ToInt32(changeRateArg.Value()); }

                double _growthRate = 0;
                if (growthRateArg.HasValue()) { _growthRate = Convert.ToDouble(growthRateArg.Value()); }

                //MS SQL only parameter set 
                string _instanceName = null;
                if (namedInstanceArg.HasValue()) { _instanceName = namedInstanceArg.Value(); }

                //Oracle only parameter set 
                //useOracleSIDArg
                bool _useOracleSID = false;
                if (useOracleSIDArg.HasValue()) { _useOracleSID = true; }

                //SAP HANA only parameter set 
                string _instanceNumber = "00";
                if (instanceNumberArg.HasValue()) { _instanceNumber = instanceNumberArg.Value(); }

                int _percentColumnTables = 80;
                if (percentageColumnsArg.HasValue()) { _percentColumnTables = Convert.ToInt32(percentageColumnsArg.Value()); }

                int _percentPagedTables = 0;
                if (percentagePagedTablesArg.HasValue()) { _percentPagedTables = Convert.ToInt32(percentagePagedTablesArg.Value()); }

                int _percentageWarmExtensionNode = 0;
                if (percentageWarmExtensionNodeTablesArg.HasValue()) { _percentageWarmExtensionNode = Convert.ToInt32(percentageWarmExtensionNodeTablesArg.Value()); }

                string _extensionNodeGroupName = "<my_warm_data>";
                if (extensionNodeGroupNameArg.HasValue()) { _extensionNodeGroupName = extensionNodeGroupNameArg.Value(); }

                int _partitions = -1;
                if (partitionsArg.HasValue()) { _partitions = Convert.ToInt32(partitionsArg.Value()); }

                int _unloadPercentage = 0;
                if (unloadTablesPercentageArg.HasValue()) { _unloadPercentage = Convert.ToInt32(unloadTablesPercentageArg.Value()); }

                int _setExtensionNodePercentage = 0;
                if (setExtensionNodePercentageArg.HasValue()) { _setExtensionNodePercentage = Convert.ToInt32(setExtensionNodePercentageArg.Value()); }

                int _setPagedTablesPercentage = 0;
                if (setPagedTablesPercentageArg.HasValue()) { _setPagedTablesPercentage = Convert.ToInt32(setPagedTablesPercentageArg.Value()); }

                int _setColumnLoadableTablesPercentage = 0;
                if (setColumnLoadablePercentageArg.HasValue()) { _setColumnLoadableTablesPercentage = Convert.ToInt32(setColumnLoadablePercentageArg.Value()); }

                int _percentToPreload = 0;
                if (setPercentageToPreloadArg.HasValue()) { _percentToPreload = Convert.ToInt32(setPercentageToPreloadArg.Value()); }
               
                //MySQL only parameter set 
                Dynamics.MySQLStorageEngine _mysqlEngine = Dynamics.MySQLStorageEngine.InnoDB;
                if (mysqlStorageEngineArg.HasValue()) { Enum.TryParse(mysqlStorageEngineArg.Value(), out _mysqlEngine); }

                string _ndbtablespace = "";
                if (ndbTableSpaceArg.HasValue()) { _ndbtablespace = ndbTableSpaceArg.Value(); }

                //MariaDB only parameter set 
                Dynamics.MariaDBStorageEngine _mariadbEngine = Dynamics.MariaDBStorageEngine.InnoDB;
                if (mariadbStorageEngineArg.HasValue()) { Enum.TryParse(mariadbStorageEngineArg.Value(), out _mariadbEngine); }

                int _setPercentage = 0;
                if (mariadbSetPercentageArg.HasValue()) { _setPercentage = Convert.ToInt32(mariadbSetPercentageArg.Value()); }

                bool _changeMariaDBObjects = false;
                if (mariaDBChangeObjectPropArg.HasValue()) { _changeMariaDBObjects = true; }

                int _portNumber = 0;
                if (portArg.HasValue()) { _portNumber = Convert.ToInt32(portArg.Value()); }

                //MongoDB only parameter set 
                Dynamics.MongoDBDeployment _mongoDBDeployment = Dynamics.MongoDBDeployment.StandAlone;
                if (mongoDBDeploymentTypeArg.HasValue()) { Enum.TryParse(mongoDBDeploymentTypeArg.Value(), out _mongoDBDeployment); }

                //Logging Parameters 
                bool _logData = false;
                if (logDataArg.HasValue()) { _logData = true; }

                string _testname = "";
                if (testNameArg.HasValue()) { _testname = testNameArg.Value(); }

                string _objectName = "";
                if (objectNameArg.HasValue()) { _objectName = objectNameArg.Value(); }

                string _objectCategory = null;
                if (ObjectCategoryArg.HasValue()) { _objectCategory = ObjectCategoryArg.Value(); }

                int _sequence = 1;
                if (sequenceArg.HasValue()) { _sequence = Convert.ToInt32(sequenceArg.Value()); }

                // PlatformEngine Arguments 
                Dynamics.ResourceRetrievalType _retrievalType = Dynamics.ResourceRetrievalType.PointInTime;
                if (collectionTypeArg.HasValue()) { Enum.TryParse(collectionTypeArg.Value(), out _retrievalType); }

                TimeSpan _duration = TimeSpan.FromMinutes(10);
                if (durationArg.HasValue()) { _duration = TimeSpan.Parse(durationArg.Value()); }
            
                TimeSpan _interval = TimeSpan.FromSeconds(5);
                if (intervalArg.HasValue()) { _interval = TimeSpan.Parse(durationArg.Value()); }

                // AnalysisEngine Arguments 

                string _codeRevision = "Unknown";
                if (codeRevisionArg.HasValue()) { _codeRevision = codeRevisionArg.Value(); }

                string _attempt = "1";
                if (attemptArg.HasValue()) { _attempt = attemptArg.Value(); }

                DateTime? _dataImportStart = null;
                if (dataImportStartArg.HasValue()) { _dataImportStart = DateTime.Parse(dataImportStartArg.Value()); }

                DateTime? _dataImportEnd = null;
                if (dataImportEndArg.HasValue()) { _dataImportEnd = DateTime.Parse(dataImportEndArg.Value()); }

                DateTime? _dataChangeStart = null;
                if (dataChangeStartArg.HasValue()) { _dataChangeStart = DateTime.Parse(dataChangeStartArg.Value()); }

                DateTime? _dataChangeEnd = null;
                if (dataChangeEndArg.HasValue()) { _dataChangeEnd = DateTime.Parse(dataChangeEndArg.Value()); }

                DateTime? _coreJobStart = null;
                if (coreJobStartArg.HasValue()) { _coreJobStart = DateTime.Parse(coreJobStartArg.Value()); }

                DateTime? _coreJobEnd = null;
                if (coreJobEndArg.HasValue()) { _coreJobEnd = DateTime.Parse(coreJobEndArg.Value()); }

                DateTime? _otherJobStart = null;
                if (otherJobStartArg.HasValue()) { _otherJobStart = DateTime.Parse(otherJobStartArg.Value()); }

                DateTime? _otherJobEnd = null;
                if (otherJobEndArg.HasValue()) { _otherJobEnd = DateTime.Parse(otherJobEndArg.Value()); }

                DateTime? _sequenceStart = null;
                if (sequenceStartArg.HasValue()) { _sequenceStart = DateTime.Parse(sequenceStartArg.Value()); }

                DateTime? _sequenceEnd = null;
                if (sequenceEndArg.HasValue()) { _sequenceEnd = DateTime.Parse(sequenceEndArg.Value()); }


                #endregion

                if (engineArg.HasValue())
                {
                    if(engineArg.Value() == Engine.data.ToString())
                    {
                        if (validatedForDataEngineExecution)
                        {
                            // DOES.DataEngine Functions are the only ones which will be valid. 
                            switch (functionArg.Value())
                            {
                                case nameof(DataEngineFunctions.add):
                                    operatorObj = new Add(_ip, _dbType, _dbname, _amount, _unit, _user, _password, _useOracleSID,
                                        _numberOfThreads, _schema, _tableAmplifier, _columnWidth, _folderPath,
                                        _randomPercentage, _instanceName, _instanceNumber, _percentColumnTables,
                                        _percentPagedTables, _percentageWarmExtensionNode, _extensionNodeGroupName,
                                        _partitions, _mysqlEngine, _mariadbEngine, _ndbtablespace, _mongoDBDeployment, _portNumber, _logData, _testname, _objectName,
                                        _objectCategory, _sequence);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    _tokenSource = operatorObj.TokenSource;
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.clear):
                                    operatorObj = new Clear(_ip, _dbType, _dbname, _user, _password, _useOracleSID, _tableAmplifier,
                                        _clearingType, _instanceName, _instanceNumber, _portNumber, _mongoDBDeployment);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.export):
                                    operatorObj = new Export(_ip, _dbType, _dbname, _user, _password, _useOracleSID, _folderPath,
                                        _instanceName, _instanceNumber, _schema, _portNumber, _mysqlEngine, _mariadbEngine, _ndbtablespace, _mongoDBDeployment);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    _tokenSource = operatorObj.TokenSource;
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.merge):
                                    operatorObj = new Merge(_ip, _dbType, _dbname, _user, _password, _tableAmplifier,
                                        _unloadPercentage, _setExtensionNodePercentage, _extensionNodeGroupName,
                                        _setPagedTablesPercentage, _setColumnLoadableTablesPercentage,
                                        _percentToPreload, _instanceNumber, _percentColumnTables, _portNumber);
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.remove):
                                    operatorObj = new Remove(_ip, _dbType, _dbname, _amount, _unit, _user, _password, _useOracleSID,
                                           _numberOfThreads, _schema, _tableAmplifier, _columnWidth,
                                           _randomPercentage, _instanceName, _instanceNumber, _percentColumnTables,
                                           _percentPagedTables, _percentageWarmExtensionNode, _extensionNodeGroupName,
                                           _partitions, _mysqlEngine, _mariadbEngine, _ndbtablespace, _mongoDBDeployment, _portNumber, _logData, _testname, _objectName,
                                           _objectCategory, _sequence);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    _tokenSource = operatorObj.TokenSource;
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.search):
                                    operatorObj = new Search(_ip, _dbType, _dbname, _amount, _unit, _user, _password, _useOracleSID,
                                          _numberOfThreads, _schema, _tableAmplifier, _queryType, _columnWidth,
                                          _randomPercentage, _instanceName, _instanceNumber, _percentColumnTables,
                                          _percentPagedTables, _percentageWarmExtensionNode, _extensionNodeGroupName,
                                          _partitions, _mysqlEngine, _mariadbEngine, _ndbtablespace, _mongoDBDeployment, _portNumber, _logData, _testname, _objectName,
                                          _objectCategory, _sequence);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    _tokenSource = operatorObj.TokenSource;
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.test):
                                    operatorObj = new Test(_ip, _dbType, _dbname, _amount, _unit, _user, _password, _useOracleSID, _numberOfThreads, _schema,
                                        _tableAmplifier, _columnWidth, _folderPath, _randomPercentage, _testType, _deferInitialIngest, _changeRate,
                                        _growthRate, _instanceName, _instanceNumber, _percentColumnTables, _percentPagedTables, _percentageWarmExtensionNode, _extensionNodeGroupName,
                                        _partitions, _mysqlEngine, _mariadbEngine, _ndbtablespace, _mongoDBDeployment, _portNumber, _logData, _testname, _objectName, _objectCategory, _sequence);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    _tokenSource = operatorObj.TokenSource;
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.update):
                                    operatorObj = new Update(_ip, _dbType, _dbname, _amount, _unit, _user, _password, _useOracleSID, 
                                           _numberOfThreads, _replaceWebPages,_schema, _tableAmplifier, _columnWidth, _folderPath,
                                           _randomPercentage, _instanceName, _instanceNumber, _percentColumnTables,
                                           _percentPagedTables, _percentageWarmExtensionNode, _extensionNodeGroupName,
                                           _partitions, _mysqlEngine, _mariadbEngine, _ndbtablespace, _mongoDBDeployment, _portNumber, _logData, _testname, _objectName,
                                           _objectCategory, _sequence);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    _tokenSource = operatorObj.TokenSource;
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.write):
                                    operatorObj = new Write(_ip, _dbType, _dbname, _user, _password, _useOracleSID, _numberOfThreads,
                                        _schema, _tableAmplifier, _columnWidth, _randomPercentage, _instanceName,
                                        _instanceNumber, _percentColumnTables, _percentPagedTables, _percentageWarmExtensionNode,
                                        _extensionNodeGroupName, _partitions, _mysqlEngine, _mariadbEngine, _ndbtablespace, _mongoDBDeployment, _portNumber);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    _tokenSource = operatorObj.TokenSource;
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.convert):
                                    operatorObj = new ConvertObj(_ip, _dbType, _dbname, _user, _password,
                                        _tableAmplifier, _mariadbEngine, _portNumber, _setPercentage, _changeMariaDBObjects);
                                    if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                    operatorObj.ExecuteOperation();
                                    break;
                                case nameof(DataEngineFunctions.help):
                                    Console.WriteLine("-----------------------------------------------------------------------------------------------------");
                                    Console.WriteLine("                                             DOES.DataEngine Help                                         ");
                                    Console.WriteLine("-----------------------------------------------------------------------------------------------------");
                                    Console.WriteLine("");
                                    Console.WriteLine("The following functions are accepted by the DOES.DataEngine : ");
                                    Console.WriteLine("");
                                    Console.WriteLine("add            Populates a database schema. Database objects are automatically created.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --amount, --unit, --numberofthreads, --schematype, ");
                                    Console.WriteLine("               --tableamplification, --columnwidth, --folder, --randompercentage, --port, ");
                                    Console.WriteLine("               --namedinstance, --instancenumber, --percentagecolumns, --percentagepagedtables,");
                                    Console.WriteLine("               --percentagewarmextensionnodetables, --extensionnodegroupname, --partitions ,");
                                    Console.WriteLine("               --mysqlstorageengine, --ndbtablespace, --mariadbstorageengine,--logdata, --tesname, ");
                                    Console.WriteLine("               --objectname, --ObjectCategory,--sequence");
                                    Console.WriteLine("");
                                    Console.WriteLine("clear          Clears all database objects through DROP or TRUNCATE operations.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --clearoperation, --tableamplification, --port, ");
                                    Console.WriteLine("               --namedinstance, --instancenumber");
                                    Console.WriteLine("");
                                    Console.WriteLine("export         Exports core WebPage data to local data files for later use.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --folder, --tableamplification, --port, ");
                                    Console.WriteLine("               --namedinstance, --instancenumber");
                                    Console.WriteLine("");
                                    Console.WriteLine("merge          Performs object administration operations on SAP HANA databases.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --tableamplification, --port, --namedinstance, --instancenumber, ");
                                    Console.WriteLine("               --unloadtablepercentage, --setextensionnodepercentage, --setpagedtablespercentage, ");
                                    Console.WriteLine("               --setcolumnloadablepercentage, --percentagetopreload ");
                                    Console.WriteLine("");
                                    Console.WriteLine("remove         Deletes a specified amount of data from database objects.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --amount, --unit, --numberofthreads, --schematype, ");
                                    Console.WriteLine("               --tableamplification, --columnwidth, --port,  --namedinstance, --instancenumber,");
                                    Console.WriteLine("               --percentagecolumns, --percentagepagedtables, --percentagewarmextensionnodetables,");
                                    Console.WriteLine("               --extensionnodegroupname, --partitions ,--mysqlstorageengine, --ndbtablespace, ");
                                    Console.WriteLine("               --mariadbstorageengine, --logdata, --tesname, --objectname, --ObjectCategory,--sequence");
                                    Console.WriteLine("");
                                    Console.WriteLine("search         Performs SELECT query operations on database objects and processes result sets. ");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --amount, --unit, --numberofthreads, --schematype, ");
                                    Console.WriteLine("               --tableamplification, --columnwidth, --port,  --namedinstance, --instancenumber,");
                                    Console.WriteLine("               --percentagecolumns, --percentagepagedtables, --percentagewarmextensionnodetables,");
                                    Console.WriteLine("               --extensionnodegroupname, --partitions ,--mysqlstorageengine, --ndbtablespace,  ");
                                    Console.WriteLine("               --mariadbstorageengine, --logdata,--tesname, --objectname, --ObjectCategory,");
                                    Console.WriteLine("               --sequence, --queryoperation");
                                    Console.WriteLine("");
                                    Console.WriteLine("test           Populates the database up to a specified size and then performs transaction and ");
                                    Console.WriteLine("               analysis operations on object data.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --amount, --unit, --numberofthreads, --schematype, ");
                                    Console.WriteLine("               --tableamplification, --columnwidth, --folder, --randompercentage, --port, ");
                                    Console.WriteLine("               --deferinitialingest, --growthrate, --changerate,");
                                    Console.WriteLine("               --tableamplification, --columnwidth, --folder, --randompercentage, --port, ");
                                    Console.WriteLine("               --namedinstance, --instancenumber, --percentagecolumns, --percentagepagedtables,");
                                    Console.WriteLine("               --percentagewarmextensionnodetables, --extensionnodegroupname, --partitions ,");
                                    Console.WriteLine("               --mysqlstorageengine, --ndbtablespace, --mariadbstorageengine, --logdata,");
                                    Console.WriteLine("               --tesname, --objectname, --ObjectCategory,--sequence");
                                    Console.WriteLine("");
                                    Console.WriteLine("update         Updates data in database objects by either changing a subset of the object properties or ");
                                    Console.WriteLine("               completely replacing the objects data in place.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --amount, --unit, --numberofthreads, --schematype, ");
                                    Console.WriteLine("               --tableamplification, --columnwidth, --port, --namedinstance, --instancenumber,");
                                    Console.WriteLine("               --folder, --replace, ");
                                    Console.WriteLine("               --percentagecolumns, --percentagepagedtables, --percentagewarmextensionnodetables,");
                                    Console.WriteLine("               --extensionnodegroupname, --partitions ,--mysqlstorageengine, --ndbtablespace, ");
                                    Console.WriteLine("               --mariadbstorageengine, --logdata,--tesname, --objectname, --ObjectCategory,--sequence");
                                    Console.WriteLine("");
                                    Console.WriteLine("write          Writes data to a specific database object every 200 micro-seconds until cancelled.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --numberofthreads, --schematype, ");
                                    Console.WriteLine("               --tableamplification, --columnwidth, --folder, --randompercentage, --port, ");
                                    Console.WriteLine("               --namedinstance, --instancenumber, --percentagecolumns, --percentagepagedtables,");
                                    Console.WriteLine("               --percentagewarmextensionnodetables, --extensionnodegroupname, --partitions,");
                                    Console.WriteLine("               --mysqlstorageengine, --ndbtablespace, --mariadbstorageengine,");
                                    Console.WriteLine("");
                                    Console.WriteLine("convert        Converts existing database objects to use a different storage engine.");
                                    Console.WriteLine("               Accepts the following arguments : --hostname, --databasetype, --databasename");
                                    Console.WriteLine("               --username, --password, --tableamplification, --port, --namedinstance, --instancenumber, ");
                                    Console.WriteLine("               --setpercentage, --changeobjectproperties");
                                    Console.WriteLine("");
                                    break;
                                default:
                                    Console.WriteLine("No valid DOES.DataEngine function selected");
                                    break;
                            }
                        }
                    }
                    else if (engineArg.Value() == Engine.platform.ToString())
                    {
                        // PlatformEngine Functions are the only ones which will be valid
                        switch (functionArg.Value())
                        {
                            case nameof(PlatformEngineFunctions.start):
                                operatorObj = new Start(_ip, _retrievalType, _duration, _interval,
                                    _logData, _testname, _objectName, _objectCategory, _sequence);
                                if (verboseArg.HasValue()) { operatorObj.VerboseWriter = true; }
                                operatorObj.ExecuteOperation();
                                break;
                            case nameof(PlatformEngineFunctions.stop):
                                operatorObj = new Stop(_ip, _logData, _testname, _objectName,
                                    _objectCategory, _sequence);
                                operatorObj.ExecuteOperation();
                                break;
                            case nameof(PlatformEngineFunctions.deploy):
                                InstallerHelper.OnCommitted();
                                break;
                            case nameof(PlatformEngineFunctions.remove):
                                InstallerHelper.OnBeforeRollback();
                                break;
                            case nameof(PlatformEngineFunctions.help):
                                Console.WriteLine("-----------------------------------------------------------------------------------------------------");
                                Console.WriteLine("                                         PlatformEngine Help                                         ");
                                Console.WriteLine("-----------------------------------------------------------------------------------------------------");
                                Console.WriteLine("");
                                Console.WriteLine("The following functions are accepted by the DOES.DataEngine : ");
                                Console.WriteLine("");
                                Console.WriteLine("start          Starts monitoring the operating platform.");
                                Console.WriteLine("               Accepts the following arguments : --collectionType, --duration, --interval, ");
                                Console.WriteLine("               --hostname, --logdata, --tesname, --objectname, --ObjectCategory,--sequence");   
                                Console.WriteLine("");
                                Console.WriteLine("stop           Stops and retrieves operating platform monitoring data.");
                                Console.WriteLine("               Accepts the following arguments :   --hostname, --logdata, --tesname, ");
                                Console.WriteLine("               --objectname, --ObjectCategory,--sequence");
                                Console.WriteLine("");
                                break;
                            default:
                                Console.WriteLine("No valid PlatformEngine function selected");
                                break;
                        }
                    }
                    else if (engineArg.Value() == Engine.analysis.ToString())
                    {
                        // AnalysisEngine Functions are the only ones which will be valid.
                        switch (functionArg.Value())
                        {
                            case nameof(AnalysisEngineFunctions.addtest):
                                operatorObj = new AddTest(_testname, _codeRevision, _dbType, 
                                    _amount.ToString(), _changeRate.ToString(), _attempt);
                                operatorObj.ExecuteOperation();
                                break;
                            case nameof(AnalysisEngineFunctions.addsequence):
                                operatorObj = new AddSequence(_testname, _objectName, _sequence, _logData, _dataImportStart,
                                    _dataImportEnd, _dataChangeStart, _dataChangeEnd, _coreJobStart, _coreJobEnd,
                                    _otherJobStart, _otherJobEnd, _sequenceStart, _sequenceEnd);
                                operatorObj.ExecuteOperation();
                                break;
                            case nameof(AnalysisEngineFunctions.help):
                                Console.WriteLine("-----------------------------------------------------------------------------------------------------");
                                Console.WriteLine("                                         AnalysisEngine Help                                         ");
                                Console.WriteLine("-----------------------------------------------------------------------------------------------------");
                                Console.WriteLine("");
                                Console.WriteLine("The following functions are accepted by the DOES.DataEngine : ");
                                Console.WriteLine("");
                                Console.WriteLine("addtest        Adds a new test object to analsysis persistence.");
                                Console.WriteLine("               Accepts the following arguments : --solution, --changerate, --attempt, ");
                                Console.WriteLine("               --coderevision");
                                Console.WriteLine("");
                                Console.WriteLine("addsequence    Adds a new sequence entry to analysis persistence. .");
                                Console.WriteLine("               Accepts the following arguments :   --dataimportstart, --dataimportend, --datachangestart, ");
                                Console.WriteLine("               --datachangeend, --corejobstart,--corejobend, --otherjobstart, --otherjobend,--sequencestart,");
                                Console.WriteLine("               --sequencend, --sequence");
                                Console.WriteLine("");
                                break;
                            default:
                                Console.WriteLine("No valid AnalysisEngine function selected");
                                break;
                        }
                    }
                }
                else
                {
                    Console.WriteLine("No valid engine set. Use one of <data | platform | analysis>");
                }
                return 0;
            });
            commandLineApplication.Execute(args);
        }

        enum Engine
        {
            data,
            platform,
            analysis
        }

        enum DataEngineFunctions
        {
            add, 
            clear, 
            export,
            merge,
            remove, 
            search,
            test, 
            update,
            write, 
            convert, 
            help
        }

        enum PlatformEngineFunctions
        {
            start,
            stop,
            deploy,
            remove,
            help
        }

        enum AnalysisEngineFunctions
        {
            addsequence,
            addtest,
            help
        }     
    }
}
