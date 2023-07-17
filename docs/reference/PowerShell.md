# D.O.E.S PowerShell Command Reference
DOES.PowerShell exposes a number of PowerShell Cmdlets that can be used to perform various functions. 

## Prerequisites 
D.O.E.S.PowerShell works for both Windows and Linux environments.  

To install PowerShell 7.3.4 and above on Windows follow [this](https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell-on-windows?view=powershell-7.3) guide. 

To install PowerShell 7.3.4 and above on Linux follow [this](https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell-on-linux?view=powershell-7.3) guide. 

Before the Cmdlets are available they must be imported into the local PowerShell session. This can be done using the Import-Module Cmdlet after installing D.O.E.S on the relevant operating system. 

*Windows*
`Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'`

*Linux*
`Import-Module /opt/purestorage/does/DOES.PowerShell.dll`

## Add-DataEngine

Populates the database up to the required size. If the tables do not exist, then they will be created using the SchemaType template and any associated vendor customizations.

If the -Folder Parameter is excluded, then the data source will be a Wikipedia, otherwise local files will be used.

Local files can be created using the Export-DataEngine Cmdlet.

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| DatabaseType | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB \| PostgreSQL \| MongoDB | All | Y | MicrosoftSQL |
| Hostname | Specifies the host name where the database is present. Accepts: String | All | N | localhost |
| DatabaseName | Specifies the database name. Accepts: String | All | Y | - |
| UserName | Specifies the name of the user to connect as. If omitted, integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| Password | Specifies the password of the user to connect as. If omitted, integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| Amount | Specifies the amount of data to populate the database with. Accepts: UInt64 | All | Y | - |
| Unit | Specifies the data measurement unit for the Amount parameter. Accepts: Bytes \| Kilobytes \| Megabytes \| Gigabytes \| Terabytes | All | Y | Bytes |
| NumberOfThreads | The number of parallel operations to run the function at. When used in tandem with -Folder, this value must match the number of Engine-Oil files in the folder. Accepts: Int32 | All | N | 1 |
| SchemaType | The schema template to use for creating the database objects. Accepts: WithIndexes \| WithoutIndexes \| WithIndexesLOB \| WithoutIndexesLOB \| MemoryOptimised \| MemoryOptimisedWithoutIndexes \| MemoryOptimisedLOB \| MemoryOptimisedWithoutIndexesLOB | All | N | WithoutIndexesLOB |
| TableAmplification | Controls how many tables will be created and how many will be populated with data. For each increment of this value, 4 (four) more tables will be created. Accepts: Int32 (Max 255) | All | N | 8 |
| ColumnWidth | The maximum width of the HTML column. This value has a knock-on effect on how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647) | All | N | 2147483647 |
| Folder | The folder on the local filesystem which contains exported DataEngine (Engine-Oil) files. Accepts: String | All | N | - |
| RandomPercentage | Specifies in percentage how much of the data inserted into database objects will be randomized. Accepts: Int32 (0-100) | All | N | 60 |
| NamedInstance | The Named Instance for Microsoft SQL server installations. Accepts: String | MicrosoftSQL | N | MSSQLSERVER |
| UseOracleSID | If an Oracle database SID is being used as the DatabaseName, then this switch parameter should be specified. | Oracle | N | False |
| InstanceNumber | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02) | SAPHANA | N | 00 |
| PercentageColumns | Specifies in percentage how many of the tables created in the schema will be of type "column". Accepts: Int32 (0-100) | SAPHANA | N | 80 |
| PercentagePagedTables | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| PercentageWarmExtensionNodeTables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| ExtensionNodeGroupName | Specifies the name of the extension node group for data to be placed in. Accepts: String | SAPHANA | N | my_warm_data|
| Partitions | The number of partitions to be created for tables in the schema template. Accepts: Int32 | SAPHANA | N | -1 |
| MySQLStorageEngine | The storage engine for tables in the schema template. Accepts: InnoDB \| NDB | MySQL | N | InnoDB |
| NDBTableSpace | The table space to use for on-disk data in a MySQL cluster installation. | MySQL | N | - |
| MariaDBStorageEngine | The storage engine for tables in the schema template. Accepts: InnoDB \| ColumnStore \| ROCKSDB \| S3 | MariaDB | N | InnoDB |
| Port | The port number to connect to a database or instance on. Accepts: String | All | N | MicrosoftSQL = 1433, Oracle = 1521, SAP HANA = 3InstanceNumber;13, MySQL = 3306, MariaDB = 3306, PostgreSQL = 5432, MongoDB = 27017 |
| LogData | Specifies if data should be logged to an analytics endpoint. Accepts: Switch Parameter | All | N | - |
| TestName | Specifies the test name to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| ObjectName | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| ObjectCatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| Sequence | The sequence number of the test data to use when persisting data to an analytics endpoint. Accepts: Int32 | All | N | 1 |

## Clear-DataEngine

Drops or truncates the objects in a specified database.

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| DatabaseType | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB \| PostgreSQL \| MongoDB | All | Y | MicrosoftSQL |
| Hostname | Specifies the host name where the database is present. Accepts: String | All | N | localhost |
| DatabaseName | Specifies the database name. Accepts: String | All | Y | - |
| UserName | Specifies the name of the user to connect as. If omitted, integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| Password | Specifies the password of the user to connect as. If omitted, integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| TableAmplification | Controls how many tables will be created and how many will be deleted.  | All            | N        | 8             |

## Convert-DataEngine

Converts tables in the database to use a different storage engine.  

> **Note**: Only currently supported for converting InnoDB tables to S3 for MariaDB.

| Parameter                 | Description                                                                                                                                                   | Parameter Set | Required | Default Value |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | -------- | ------------- |
| DatabaseType              | Specifies the database type. Accepts: MariaDB                                                                                                                | All            | Y        | MariaDB       |
| Hostname                  | Specifies the host name where the database is present. Accepts: String                                                                                       | All            | N        | localhost     |
| DatabaseName              | Specifies the database name. Accepts: String                                                                                                                 | All            | Y        | -             |
| UserName                  | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String                                    | All            | N        | -             |
| Password                  | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String                                | All            | N        | -             |
| SetPercentage             | Specifies the percentage of tables in the database to change. Accepts: UInt64 (0 to 100)                                                                     | All            | N        | 0             |
| ChangeObjectProperties    | Specifies if the objects properties should be changed. Accepts: Switch Parameter                                                                             | All            | N        | -             |
| TableAmplification        | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created.  | All            | N        | 8             |
| MariaDBStorageEngine      | The storage engine for tables in the schema template. Accepts: InnoDB, ColumnStore, ROCKSDB, S3                                                             | MariaDB        | N        | InnoDB        |
| Port                      | The port number to connect to a database or instance on. Accepts: String                                                                                    | All            | N        | MariaDB = 3306 |

## Export-DataEngine

Exports the minimum required data to recreate the schema to highly compressed files for later use. This will create the database objects if they are not found.

> **Note**: This Cmdlet is best used after adding data to a database using Add-DataEngine without the -Folder parameter.

| Parameter                 | Description                                                                                                                                                      | Parameter Set | Required | Default Value   |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | -------- | --------------- |
| DatabaseType              | Specifies the database type. Accepts: MicrosoftSQL, Oracle, SAPHANA, MySQL, MariaDB, PostgreSQL, MongoDB                                                        | All            | Y        | MicrosoftSQL   |
| Hostname                  | Specifies the host name where the database is present. Accepts: String                                                                                          | All            | N        | localhost       |
| DatabaseName              | Specifies the database name. Accepts: String                                                                                                                    | All            | Y        | -               |
| UserName                  | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String                                       | All            | N        | -               |
| Password                  | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String                                   | All            | N        | -               |
| SchemaType                | The schema template to use for creating the database objects. Accepts: WithIndexes, WithoutIndexes, WithIndexesLOB, WithoutIndexesLOB, MemoryOptimised, MemoryOptimisedWithoutIndexes, MemoryOptimisedLOB, MemoryOptimisedWithoutIndexesLOB | All            | N        | WithoutIndexesLOB |
| Folder                    | The folder on the local filesystem where the DataEngine files will be created. Accepts: String                                                                 | All            | Y        | -               |
| NamedInstance             | The Named Instance for Microsoft SQL server installations. Accepts: String                                                                                      | MicrosoftSQL   | N        | MSSQLSERVER     |
| UseOracleSID              | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified.                                                     | Oracle         | N        | False           |
| InstanceNumber            | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02)                                                                             | SAPHANA        | N        | 00              |
| MySQLStorageEngine        | The storage engine for tables in the schema template. Accepts: InnoDB, NDB                                                                                      | MySQL          | N        | InnoDB          |
| NDBTableSpace             | The table space to use for on-disk data in a MySQL cluster installation.                                                                                        | MySQL          | N        | -               |
| MariaDBStorageEngine      | The storage engine for tables in the schema template. Accepts: InnoDB, ColumnStore, ROCKSDB, S3                                                                 | MariaDB        | N        | InnoDB          |
| Port                      | The port number to connect to a database or instance on. Accepts: String                                                                                        | All            | N        | MicrosoftSQL = 1433, Oracle = 1521, SAP HANA = 3<InstanceNumber>13, MySQL = 3306, MariaDB = 3306, PostgreSQL = 5432, MongoDB = 27017 |

## Merge-DataEngine

Performs delta merge operations on SAP HANA databases. Also sets object properties such as warm data storage (NSE, extension nodes) and preload/unload triggers.

> **Note**: This Cmdlet is only for SAP HANA databases.

| Parameter                 | Description                                                                                      | Parameter Set | Required | Default Value |
| ------------------------- | ------------------------------------------------------------------------------------------------ | -------------- | -------- | ------------- |
| DatabaseType              | Specifies the database type. Accepts: SAPHANA                                                   | SAPHANA        | Y        | SAPHANA       |
| Hostname                  | Specifies the host name where the database is present. Accepts: String                          | SAPHANA        | N        | localhost     |
| DatabaseName              | Specifies the database name. Accepts: String                                                    | SAPHANA        | Y        | -             |
| UserName                  | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | SAPHANA        | N        | -             |
| Password                  | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | SAPHANA        | N        | -             |
| TableAmplification        | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created.  | SAPHANA        | N        | 8             |
| InstanceNumber            | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02)             | SAPHANA        | N        | 00            |
| UnloadTablesPercentage    | Specifies in percentageThe information you provided appears to be a set of PowerShell cmdlets related to database operations. | SAPHANA        | N        | 0       |

## New-SequenceStat

This Cmdlet is focused on recording data for analytical purposes. It assumes that any test use case can be subdivided into multiple segments and the beginning and end for each can be recorded for later analysis.

| Parameter          | Description                                                                                                        | Parameter Set | Required | Default Value |
|--------------------|--------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| DataImportStart    | When initial data population began (Pre-Test)                                                                       | All           | N        | -             |
| DataImportEnd      | When initial data population ended (Pre-Test)                                                                       | All           | N        | -             |
| DataChangeStart    | When initial data change began.                                                                                     | All           | N        | -             |
| DataChangeEnd      | When initial data change ended.                                                                                     | All           | N        | -             |
| CoreJobStart       | When core operations started.                                                                                      | All           | N        | -             |
| CoreJobEnd         | When core operations ended.                                                                                        | All           | N        | -             |
| OtherJobStart      | When miscellaneous operations started.                                                                              | All           | N        | -             |
| OtherJobEnd        | When miscellaneous operations ended.                                                                                | All           | N        | -             |
| SequenceStart      | When the entire sequence began.                                                                                     | All           | N        | -             |
| SequenceEnd        | When the entire sequence ended.                                                                                     | All           | N        | -             |
| LogData            | Specifies if data should be logged to an analytics endpoint.                                                       | All           | N        | -             |
| TestName           | Specifies the test name to use when persisting analytics data to an analytics endpoint.                            | All           | Y        | -             |
| ObjectName         | Specifies the name of the object to use when persisting analytics data to an analytics endpoint.                   | All           | Y        | -             |
| Sequence           | The sequence number of the test data to use when persisting data to an analytics endpoint.                         | All           | Y        | 1             |

## New-TestAnalytic

This Cmdlet records additional information about test characteristics.

| Parameter   | Description                                                                                                        | Parameter Set | Required | Default Value |
|-------------|--------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| TestName    | The name of the test.                                                                                              | All           | Y        | -             |
| CodeRevision| The version of software being tested.                                                                              | All           | Y        | -             |
| Solution    | The solution being tested.                                                                                         | All           | N        | -             |
| DataSize    | The size of the data being tested.                                                                                  | All           | N        | -             |
| ChangeRate  | The change rate of the data being tested.                                                                           | All           | N        | -             |
| Attempt     | The attempt of this test being perfomed.                                                                            | All           | N        | -             |

## Remove-DataEngine

Deletes data in database objects up to the amount specified.

If the database objects do not exist they will be created.

| Parameter           | Description                                                                                                        | Parameter Set | Required | Default Value |
|---------------------|--------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| DatabaseType        | Specifies the database type.                                                                                       | All           | Y        | MicrosoftSQL  |
| Hostname            | Specifies the host name where the database is present.                                                             | All           | N        | localhost     |
| DatabaseName        | Specifies the database name.                                                                                        | All           | Y        | -             |
| UserName            | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL).         | All           | N        | -             |
| Password            | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL).     | All           | N        | -             |
| Amount              | Specifies the amount of data to delete from the database.                                                          | All           | Y        | -             |
| Unit                | Specifies the data measurement unit for the Amount parameter.                                                      | All           | Y        | Bytes         |
| NumberOfThreads     | The number of parallel operations to run the function at.                                                          | All           | N        | 1             |
| SchemaType          | The schema template to use for creating the database objects.                                                      | All           | N        | WithoutIndexesLOB |
| TableAmplification  | Controls how many tables will be created and how many will be populated with data.                                 | All           | N        | 8             |
| ColumnWidth | The maximum width of the HTML column. This value has a knock-on effect on how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647) | All | N | 2147483647 |

## Search-DataEngine

Performs SELECT queries and read-only processing of data in database objects. If the database objects do not exist they are created.

| Parameter           | Description                                                                                     | Parameter Set | Required | Default Value     |
|---------------------|-------------------------------------------------------------------------------------------------|---------------|----------|-------------------|
| DatabaseType        | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB \| PostgreSQL \| MongoDB | All           | Y        | MicrosoftSQL     |
| Hostname            | Specifies the host name where the database is present. Accepts: String                            | All           | N        | localhost         |
| DatabaseName        | Specifies the database name. Accepts: String                                                    | All           | Y        | -                 |
| UserName            | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All           | N        | -                 |
| Password            | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All           | N        | -                 |
| Amount              | Specifies the amount of data to populate the database with. Accepts: UInt64                     | All           | Y        | -                 |
| Unit                | Specifies the data measurement unit for the Amount parameter. Accepts: Bytes \| Kilobytes \| Megabytes \| Gigabytes \| Terabytes | All           | Y        | Bytes             |
| NumberOfThreads     | The number of parallel operations to run the function at. Accepts: Int32                         | All           | N        | 1                 |
| SchemaType          | The schema template to use for creating the database objects. Accepts: WithIndexes \| WithoutIndexes \| WithIndexesLOB \| WithoutIndexesLOB \| MemoryOptimised \| MemoryOptimisedWithoutIndexes \| MemoryOptimisedLOB \| MemoryOptimisedWithoutIndexesLOB | All           | N        | WithoutIndexesLOB |
| TableAmplification  | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255) | All           | N        | 8                 |
| ColumnWidth         | The maximum width of the HTML column. This value has a knock-on effect for how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647) | All           | N        | 2147483647        |
| QueryType           | The type of query operation to run on the object data. Accepts: UnionAll \| LeftOuterJoin        | All           | N        | UnionAll          |
| RandomPercentage    | Specifies in percentage how much of the data inserted into database objects will be randomized. Accepts: Int32 (0-100) | All           | N        | 60                |
| NamedInstance       | The Named Instance for Microsoft SQL server installations. Accepts: String                       | MicrosoftSQL  | N        | MSSQLSERVER       |
| UseOracleSID        | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified. | Oracle        | N        | False             |
| InstanceNumber      | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02)             | SAPHANA       | N        | 00                |
| PercentageColumns   | Specifies in percentage how many of the tables created in the schema will be of type "column". Accepts: Int32 (0-100) | SAPHANA       | N        | 80                |
| PercentagePagedTables | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". Accepts: Int32 (0-100) | SAPHANA       | N        | 0                 |
| PercentageWarmExtensionNodeTables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100) | SAPHANA       | N        | 0                 |
| ExtensionNodeGroupName | Specifies the name of the extension node group for data to be placed in. Accepts: String          | SAPHANA       | N        | <my_warm_data>    |
| Partitions          | The number of partitions to be created for tables in the schema template. Accepts: Int32          | SAPHANA       | N        | -1                |
| MySQLStorageEngine  | The storage engine for tables in the schema template. Accepts: InnoDB \| NDB                     | MySQL         | N        | InnoDB            |
| NDBTableSpace       | The table space to use for on-disk data in a MySQL cluster installation.                          | MySQL         | N        | -                 |
| MariaDBStorageEngine | The storage engine for tables in the schema template. Accepts: InnoDB \| ColumnStore \| ROCKSDB \| S3 | MariaDB       | N        | InnoDB            |
| Port                | The port number to connect to a database or instance on. Accepts: String                        | All           | N        | MicrosoftSQL = 1433, Oracle = 1521, SAP HANA = 3<InstanceNumber>13, MySQL = 3306, MariaDB = 3306, PostgreSQL = 5432, MongoDB = 27017 |
| LogData             | Specifies if data should be logged to an analytics endpoint. Accepts: Switch Parameter           | All           | N        | -                 |
| TestName            | Specifies the test name to use when persisting analytics data to an analytics endpoint. Accepts: String | All           | N        | -                 |
| ObjectName          | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. Accepts: String | All           | N        | -                 |
| ObjectCatagory      | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. Accepts: String | All           | N        | -                 |
| Sequence            | The sequence number of the test data to use when persisting data to an analytics endpoint. Accepts: Int32 | All           | N        | 1                 |

## Start-PlatformEngine

Starts a monitoring operation for an Operating Systems resources.

| Parameter      | Description                                                                                     | Parameter Set | Required | Default Value |
|----------------|-------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| Hostname       | The hostname of a system with the PlatformEngine_Client deployed. Accepts: String                | All           | N        | localhost     |
| CollectionType | The type of resource collection to perform. Accepts: PointInTime \| Duration \| UntilNotified    | All           | N        | PointInTime   |
| Duration       | The duration for which Duration Collection Types should run for. Accepts: TimeSpan in the form : HH:MM:SS | All           | N        | 10 minutes (00:10:00) |
| Interval       | The interval between data points in a Duration or UntilNotified CollectionType. Accepts: TimeSpan in the form : HH:MM | All           | N     | 5 Minutes (00:00:05)     |

## Stop-PlatformEngine

When a Duration or UntilNotified operation has been requested using Start-PlatformEngine this command will stop the operation (using UntilNotified) and retrieve the data points.

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| Hostname | The hostname of a system with the PlatformEngine_Client deployed.  Accepts: String | All | N | localhost |
| LogData | Specifies if data should be logged to an analytics endpoint.  Accepts: Switch Parameter | All | N | - |
| TestName | Specifies the test name to use when persisting analytics data to an analytics endpoint.  Accepts: String | All | N | - |
| ObjectName | Specifies the name of the object to use when persisting analytics data to an analytics endpoint.  Accepts: String | All | N | - |
| ObjectCatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint.  Accepts: String | All | N | - |
| Sequence | The sequence number of the test data to use when persisting data to an analytics endpoint.  Accepts: Int32 | All | N | 1 |

## Test-DataEngine

Combines INSERT, UPDATE, DELETE, and SELECT scenarios to create a unified test strategy.

This cmdlet will create the objects, populate them with data, and then perform test scenarios which include vendor-specific operations.
The following test scenarios are currently supported:

### Simple

All Databases: Perform parallel queries using insert, delete, and update style commands.

### Advanced

-Microsoft SQL: Run parallel queries for insert, delete, select, and update style commands. Indexes are reorganized and rebuilt once completed.
-Oracle Database: Run parallel queries for insert, delete, select, and update style commands.
-SAP HANA: Run parallel queries for insert, delete, select, and update style commands. A Delta Merge operation is performed once completed.
-MySQL: Run parallel queries for insert, delete, select, and update style commands. Tables are optimized once the operations are all completed.
-MariaDB: Run parallel queries for insert, delete, select, and update style commands. Tables are optimized once the operations are all completed.
-PostgreSQL: Run parallel queries for insert, delete, select, and update style commands. Table vacuum is performed once all the operations are complete.
-MongoDB: Run parallel queries for insert, delete, select, and update style commands.

### Complex

-Microsoft SQL: Run parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. At regular intervals during the operation, the indexes are reorganized and rebuilt. At the end of the test, a DBCC operation is performed.
-Oracle Database: Run parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row.
-SAP HANA: Run parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. Delta Merge and unload operations are run at regular intervals while the test is running. At the end of the test, a consistency check is run on all tables.
-MySQL: Run parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. The tables are flushed at regular intervals while the test is running. A consistency check is run at the end of the test.
-MariaDB: Run parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. The tables are flushed at regular intervals while the test is running. A consistency check is run at the end of the test.
-PostgreSQL: Run parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. The tables are analyzed at regular intervals while the test is running. A consistency check is run at the end of the test.-
--MongoDB: Run parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row.

| Parameter | Description | Parameter Set | Required | Default Value |
|-----------|-------------|---------------|----------|---------------|
| DatabaseType | Specifies the database type. | All | Y | MicrosoftSQL |
| Hostname | Specifies the host name where the database is present. | All | N | localhost |
| DatabaseName | Specifies the database name. | All | Y | - |
| UserName | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). | All | N | - |
| Password | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). | All | N | - |
| Amount | Specifies the amount of data to populate the database with. | All | Y | - |
| Unit | Specifies the data measurement unit for the Amount parameter. | All | Y | Bytes |
| NumberOfThreads | The number of parallel operations to run the function at. | All | N | 1 |
| SchemaType | The schema template to use for creating the database objects. | All | N | WithoutIndexesLOB |
| TableAmplification | Controls how many tables will be created and how many will be populated with data. | All | N | 8 |
| ColumnWidth | The maximum width of the HTML column. | All | N | 2147483647 |
| TestType | The type of test being performed. | All | N | Simple |
| DeferInitialIngest | If a database is already populated with data then this will skip populating the database and proceed directly to the TestType. | All | N | - |
| ChangeRate | Specifies the percentage of the overall dataset to perform testing operations on. | All | N | 10 |
| GrowthRate | Specifies the percentage of overall dataset growth to allow for during testing. | All | N | 0 |
| Folder | The folder on the local filesystem which contains exported DataEngine (Engine-Oil) files. | All | N | - |
| RandomPercentage | Specifies in percentage how much of the data inserted into database objects will be randomized. | All | N | 60 |
| NamedInstance | The Named Instance for Microsoft SQL server installations. | MicrosoftSQL | N | MSSQLSERVER |
| UseOracleSID | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified. | Oracle | N | False |
| InstanceNumber | The Instance number for SAP HANA installations. | SAPHANA | N | 00 |
| PercentageColumns | Specifies in percentage how many of the tables created in the schema will be of type "column". | SAPHANA | N | 80 |
| PercentagePagedTables | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". | SAPHANA | N | 0 |
| PercentageWarmExtensionNodeTables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. | SAPHANA | N | 0 |
| ExtensionNodeGroupName | Specifies the name of the extension node group for data to be placed in. | SAPHANA | N | <my_warm_data> |
| Partitions | The number of partitions to be created for tables in the schema template. | SAPHANA | N | -1 |
| MySQLStorageEngine | The storage engine for tables in the schema template. | MySQL | N | InnoDB |
| NDBTableSpace | The table space to use for on-disk data in a MySQL cluster installation. | MySQL | N | - |
| MariaDBStorageEngine | The storage engine for tables in the schema template. | MariaDB | N | InnoDB |
| Port | The port number to connect to a database or instance on. | All | N | (see default values) |
| LogData | Specifies if data should be logged to an analytics endpoint. | All | N | - |
| TestName | Specifies the test name to use when persisting analytics data to an analytics endpoint. | All | N | - |
| ObjectName | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. | All | N | - |
| ObjectCatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint.  Accepts: String | All | N | - |
| Sequence | The sequence number of the test data to use when persisting data to an analytics endpoint.  Accepts: Int32 | All | N | 1 |

## Update-DataEngine

Existing data in the database is replaced with new data using update commands.  

| Parameter | Description | Parameter Set | Required | Default Value |
|-----------|-------------|---------------|----------|---------------|
| DatabaseType | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB | All | Y | MicrosoftSQL |
| Hostname | Specifies the host name where the database is present. Accepts: String | All | N | localhost |
| DatabaseName | Specifies the database name. Accepts: String | All | Y | - |
| UserName | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| Password | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| Amount | Specifies the amount of data to populate the database with. Accepts: UInt64 | All | Y | - |
| Unit | Specifies the data measurement unit for the Amount parameter. Accepts: Bytes \| Kilobytes \| Megabytes \| Gigabytes \| Terabytes | All | Y | Bytes |
| NumberOfThreads | The number of parallel operations to run the function at. When used in tandem with -Folder and -Replace this value must match the number of Engine-Oil files in the folder. Accepts: Int32 | All | N | 1 |
| SchemaType | The schema template to use for creating the database objects. Accepts: WithIndexes \| WithoutIndexes \| WithIndexesLOB \| WithoutIndexesLOB \| MemoryOptimised \| MemoryOptimisedWithoutIndexes \| MemoryOptimisedLOB \| MemoryOptimisedWithoutIndexesLOB | All | N | WithoutIndexesLOB |
| TableAmplification | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255) | All | N | 8 |
| ColumnWidth | The maximum width of the HTML column. This value has a knock-on effect for how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647) | All | N | 2147483647 |
| Folder | The folder on the local filesystem which contains exported DataEngine (Engine-Oil) files. Only used when -Replace is specified. Accepts: String | All | N | - |
| Replace | Instead of updating data on a random column basis, entire rows of data are changed out. Accepts: SwitchParameter | All | N | - |
| RandomPercentage | Specifies in percentage how much of the data inserted into database objects will be randomized. Accepts: Int32 (0-100) | All | N | 60 |
| NamedInstance | The Named Instance for Microsoft SQL server installations. Accepts: String | MicrosoftSQL | N | MSSQLSERVER |
| UseOracleSID | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified. | Oracle | N | False |
| InstanceNumber | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02) | SAPHANA | N | 00 |
| PercentageColumns | Specifies in percentage how many of the tables created in the schema will be of type "column". Accepts: Int32 (0-100) | SAPHANA | N | 80 |
| PercentagePagedTables | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| PercentageWarmExtensionNodeTables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| ExtensionNodeGroupName | Specifies the name of the extension node group for data to be placed in. Accepts: String | SAPHANA | N | my_warm_data|
| Partitions | The number of partitions to be created for tables in the schema template. Accepts: Int32 | SAPHANA | N | -1 |
| MySQLStorageEngine | The storage engine for tables in the schema template. Accepts: InnoDB \| NDB | MySQL | N | InnoDB |
| NDBTableSpace | The table space to use for on-disk data in a MySQL cluster installation. | MySQL | N | - |
| MariaDBStorageEngine | The storage engine for tables in the schema template. InnoDB \| ColumnStore \| ROCKSDB \| S3 | MariaDB | N | InnoDB |
| Port | The port number to connect to a database or instance on. Accepts: String | All | N | MicrosoftSQL = 1433 \| Oracle = 1521 \| SAP HANA = 3InstanceNumber;13 \| MySQL = 3306 \| MariaDB = 3306 \| PostgreSQL = 5432 \| MongoDB = 27017 |
| LogData | Specifies if data should be logged to an analytics endpoint. Accepts: Switch Parameter | All | N | - |
| TestName | Specifies the test name to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| ObjectName | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| ObjectCatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| Sequence | The sequence number of the test data to use when persisting data to an analytics endpoint. Accepts: Int32 | All | N | 1 |

## Write-DataEngine

Data is written to a specific table in the database every 200 microseconds.

If the database objects are not present they will be created.

| Parameter                       | Description                                                                                             | Parameter Set | Required | Default Value    |
|---------------------------------|---------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| DatabaseType                    | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB             | All           | Y        | MicrosoftSQL    |
| Hostname                        | Specifies the host name where the database is present. Accepts: String                                  | All           | N        | localhost        |
| DatabaseName                    | Specifies the database name. Accepts: String                                                             | All           | Y        | -                |
| UserName                        | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All           | N        | -                |
| Password                        | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All           | N        | -                |
| NumberOfThreads                 | The number of parallel operations to run the function at. Accepts: Int32                                | All           | N        | 1                |
| SchemaType                      | The schema template to use for creating the database objects. Accepts: WithIndexes \| WithoutIndexes \| WithIndexesLOB \| WithoutIndexesLOB \| MemoryOptimised \| MemoryOptimisedWithoutIndexes \| MemoryOptimisedLOB \| MemoryOptimisedWithoutIndexesLOB | All           | N        | WithoutIndexesLOB |
| TableAmplification              | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255) | All           | N        | 8                |
| ColumnWidth                     | The maximum width of the HTML column. This value has a knock-on effect for how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647) | All           | N        | 2147483647       |
| NamedInstance                   | The Named Instance for Microsoft SQL server installations. Accepts: String                              | MicrosoftSQL  | N        | MSSQLSERVER      |
| UseOracleSID                    | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified. | Oracle        | N        | False            |
| InstanceNumber                  | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02)                      | SAPHANA       | N        | 00               |
| PercentageColumns               | Specifies in percentage how many of the tables created in the schema will be of type "column". Accepts: Int32 (0-100) | SAPHANA       | N        | 80               |
| PercentagePagedTables           | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". Accepts: Int32 (0-100) | SAPHANA       | N        | 0                |
| PercentageWarmExtensionNodeTables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100) | SAPHANA       | N        | 0                |
| ExtensionNodeGroupName          | Specifies the name of the extension node group for data to be placed in. Accepts: String                 | SAPHANA       | N        | <my_warm_data>   |
| Partitions                      | The number of partitions to be created for tables in the schema template. Accepts: Int32                  | SAPHANA       | N        | -1               |
| MySQLStorageEngine              | The storage engine for tables in the schema template. Accepts: InnoDB \| NDB                             | MySQL         | N        | InnoDB           |
| NDBTableSpace                   | The table space to use for on-disk data in a MySQL cluster installation.                                  | MySQL         | N        | -                |
| MariaDBStorageEngine            | The storage engine for tables in the schema template. Accepts: InnoDB \| ColumnStore \| ROCKSDB \| S3    | MariaDB       | N        | InnoDB           |
| Port                            | The port number to connect to a database or instance on. Accepts: String                                 | All           | N        | MicrosoftSQL = 1433 \| Oracle = 1521 \| SAP HANA = 3<InstanceNumber>13 \| MySQL = 3306 \| MariaDB = 3306 \| PostgreSQL = 5432 \| MongoDB = 27017 |
