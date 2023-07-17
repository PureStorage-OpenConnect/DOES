# D.O.E.S Cli Command Reference

D.O.E.S can be accessed through the command line tool "DOES.Cli" on Windows or Linux platforms. 

DOES.Cli is installed at the following locations :

*Microsoft Windows*

`C:\Program Files\Pure Storage\D.O.E.S\DOES.CLl.exe`

*Linux*

`/opt/purestorage/does/DOES.Cli`

The --help argument can be used to retrieve help text.

The use of DOES.Cli follows a pattern of exposing functions through the use of specific engines :

`DOES.Cli --engine <data | platform | analysis> --function <engine function>`

An engine must always be specified. The following engines are provided :

- data - creating , destroying and interacting with test databases and the relevant objects inside them. 
- platform - Interacting with systems where the platformengine_client has been installed. The platformengine_client is included with the core D.O.E.S installer for both Microsoft Windows and Linux. 
- analysis - Functions for adding more content to analytical test data. 

Getting help for which functions are available in each engine can be yielded using the following sytnax :

`DOES.Cli --engine <data | platform | analysis> --function help`

## Data Engine Functions 

### add

Populates the database up to the required size. If the tables do not exist then they will be required using the --schematype template and any associated vendor customizations.

If the --folder Parameter is excluded then the data source will be a Wikipedia, otherwise local files will be used.

Local files can be created using the export function.

| Parameter                 | Description                                                                                                                                                                                                                                                          | Parameter Set | Required | Default Value      |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|--------------------|
| --databasetype            | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB \| PostgreSQL \| MongoDB                                                                                                                                                 | All           | Y        | MicrosoftSQL       |
| --hostname                | Specifies the host name where the database is present. Accepts: String                                                                                                                                                                                                | All           | N        | localhost          |
| --databasename            | Specifies the database name. Accepts: String                                                                                                                                                                                                                          | All           | Y        | -                  |
| --username                | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String                                                                                                                                            | All           | N        | -                  |
| --password                | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String                                                                                                                                            | All           | N        | -                  |
| --amount                  | Specifies the amount of data to populate the database with. Accepts: UInt64                                                                                                                                                                                           | All           | Y        | -                  |
| --unit                    | Specifies the data measurement unit for the --amount parameter. Accepts: Bytes \| Kilobytes \| Megabytes \| Gigabytes \| Terabytes                                                                                                                                  | All           | Y        | Bytes              |
| --numberofthreads         | The number of parallel operations to run the function at. When used in tandem with --folder this value must match the number of Engine-Oil files present in the folder. Accepts: Int32                                                                                   | All           | N        | 1                  |
| --schematype              | The schema template to use for creating the database objects. Accepts: WithIndexes \| WithoutIndexes \| WithIndexesLOB \| WithoutIndexesLOB \| MemoryOptimised \| MemoryOptimisedWithoutIndexes \| MemoryOptimisedLOB \| MemoryOptimisedWithoutIndexesLOB         | All           | N        | WithoutIndexesLOB  |
| --tableamplification      | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255)                                                                                  | All           | N        | 8                  |
| --columnwidth             | The maximum width of the HTML column. This value has a knock-on effect on how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647)                          | All           | N        | 2147483647         |
| --folder                  | The --folder on the local filesystem which contains ex--ported DataEngine (Engine-Oil) files. Accepts: String                                                                                                                                                        | All           | N        | -                  |
| --randompercentage        | Specifies in percentage how much of the data inserted into database objects will be randomized. Accepts: Int32 (0-100)                                                                                                                                               | All           | N        | 60                 |
| --namedinstance           | The Named Instance for Microsoft SQL server installations. Accepts: String                                                                                                                                                                                            | MicrosoftSQL  | N        | MSSQLSERVER        |
| --useoraclesid            | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified.                                                                                                                                                          | Oracle        | N        | False              |
| --instancenumber          | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02)                                                                                                                                                                                  | SAPHANA       | N        | 00                 |
| --percentagecolumns       | Specifies in percentage how many of the tables created in the schema will be of type "column". Accepts: Int32 (0-100)                                                                                                                                                 | SAPHANA       | N        | 80                 |
| --percentagepagedtables   | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". Accepts: Int32 (0-100)                                                                                                                                 | SAPHANA       | N        | 0                  |
| --percentagewarmextensionnodetables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100)                                                                                                                            | SAPHANA       | N        | 0                  |
| --extensionnodegroupname  | Specifies the name of the extension node group for data to be placed in. Accepts: String                                                                                                                                                                              | SAPHANA       | N        | <my_warm_data>     |
| --partitions              | The number of partitions to be created for tables in the schema template. Accepts: Int32                                                                                                                                                                             | SAPHANA       | N        | -1                 |
| --mysqlstorageengine      | The storage engine for tables in the schema template. Accepts: InnoDB \| NDB                                                                                                                                                                                         | MySQL         | N        | InnoDB             |
| --ndbtablespace           | The table space to use for on-disk data in a MySQL cluster installation.                                                                                                                                                                                             | MySQL         | N        | -                  |
| --mariadbstorageengine    | The storage engine for tables in the schema template. Accepts: InnoDB \| ColumnStore \| ROCKSDB \| S3                                                                                                                                                                | MariaDB       | N        | InnoDB             |
| --port                    | The port number to connect to a database or instance on. Accepts: String                                                                                                                                                                                             | All           | N        | MicrosoftSQL = 1433, Oracle = 1521, SAP HANA = 3<--instancenumber>13, MySQL = 3306, MariaDB = 3306, PostgreSQL = 5432, MongoDB = 27017 |
| --logdata                 | Specifies if data should be logged to an analytics endpoint. Accepts: Switch Parameter                                                                                                                                                                               | All           | N        | -                  |
| --testname                | Specifies the test name to use when persisting analytics data to an analytics endpoint. Accepts: String                                                                                                                                                              | All           | N        | -                  |
| --objectname              | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. Accepts: String                                                                                                                                                     | All           | N        | -                  |
| --objectcatagory          | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. Accepts: String                                                                                                                                            | All           | N        | -                  |
| --sequence                | The sequence number of the test data to use when persisting data to an analytics endpoint. Accepts: Int32                                                                                                                                                    | All           | N        | 1                  |

### clear

Drops or truncates the objects in a specified database.

| Parameter             | Description                                                                                                                                               | Parameter Set | Required | Default Value   |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|-----------------|
| --databasetype        | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB \| PostgreSQL \| MongoDB                                         | All           | Y        | MicrosoftSQL    |
| --hostname            | Specifies the host name where the database is present. Accepts: String                                                                                    | All           | N        | localhost       |
| --databasename        | Specifies the database name. Accepts: String                                                                                                              | All           | Y        | -               |
| --username            | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String                                 | All           | N        | -               |
| --password            | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String                             | All           | N        | -               |
| --tableamplification  | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. | All           | N        | 8               |
| --clearoperation      | The operation to use when clearing the object contents. Accepts: DROP \| TRUNCATE                                                                          | All           | N        | DROP            |
| --namedinstance       | The Named Instance for Microsoft SQL server installations. Accepts: String                                                                                 | MicrosoftSQL  | N        | MSSQLSERVER     |
| --useoraclesid        | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified.                                               | Oracle        | N        | False           |
| --instancenumber      | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02)                                                                       | SAPHANA       | N        | 00              |
| --port                | The port number to connect to a database or instance on. Accepts: String                                                                                   | All           | N        | -               |
| --logdata             | Specifies if data should be logged to an analytics endpoint. Accepts: Switch Parameter                                                                    | All           | N        | -               |

### convert

Converts tables in the database to use a different storage engine.

> **Note:** Only currently supported for converting InnoDB tables to S3 for MariaDB.
    Only currently supported for converting InnoDB tables to S3 for MariaDB.

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| --databasetype | Specifies the database type. Accepts: MariaDB | All | Y | MariaDB |
| --hostname | Specifies the host name where the database is present. Accepts: String | All | N | localhost |
| --databasename | Specifies the database name. Accepts: String | All | Y | - |
| --username | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --password | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --setpercentage | Specifies the percentage of tables in the database to change. Accepts: UInt64 (0 to 100) | All | N | 0 |
| --changeobjectproperties | Specifies if the objects properties should be changed. Accepts: Switch Parameter | All | N | - |
| --tableamplification | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255) | All | N | 8 |
| --mariadbstorageengine | The storage engine for tables in the schema template. Accepts: InnoDB, ColumnStore, ROCKSDB, S3 | MariaDB | N | InnoDB |
| --port | The port number to connect to a database or instance on. Accepts: String | All | N | MariaDB = 3306 |

### export

Exports the minimum required data to recreate the schema to highly compressed files for later use.

This will create the database objects if they are not found.

**This function is best used after adding data to a database using the add function without the ---folder parameter.**

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| --databasetype | Specifies the database type. Accepts: MicrosoftSQL, Oracle, SAPHANA, MySQL, MariaDB, PostgreSQL, MongoDB | All | Y | MicrosoftSQL |
| --hostname | Specifies the host name where the database is present. Accepts: String | All | N | localhost |
| --databasename | Specifies the database name. Accepts: String | All | Y | - |
| --username | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --password | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --schematype | The schema template to use for creating the database objects. Accepts: WithIndexes, WithoutIndexes, WithIndexesLOB, WithoutIndexesLOB, MemoryOptimised, MemoryOptimisedWithoutIndexes, MemoryOptimisedLOB, MemoryOptimisedWithoutIndexesLOB | All | N | WithoutIndexesLOB |
| --folder | The --folder on the local filesystem where the DataEngine files will be created. Accepts: String | All | Y | - |
| --namedinstance | The Named Instance for Microsoft SQL server installations. Accepts: String | MicrosoftSQL | N | MSSQLSERVER |
| --useoraclesid | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified. | Oracle | N | False |
| --instancenumber | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02) | SAPHANA | N | 00 |
| --mysqlstorageengine | The storage engine for tables in the schema template. Accepts: InnoDB, NDB | MySQL | N | InnoDB |
| --ndbtablespace | The table space to use for on-disk data in a MySQL cluster installation. | MySQL | N | - |
| --mariadbstorageengine | The storage engine for tables in the schema template. Accepts: InnoDB, ColumnStore, ROCKSDB, S3 | MariaDB | N | InnoDB |
| --port | The port number to connect to a database or instance on. Accepts: String | All | N | MicrosoftSQL = 1433, Oracle = 1521, SAP HANA = 3&lt;--instancenumber&gt;13, MySQL = 3306, MariaDB = 3306, PostgreSQL = 5432, MongoDB = 27017 |

### merge

**This function is only for SAP HANA databases.**

Performs delta merge operations on SAP HANA databases. Also sets object properties such as warm data storage (NSE, extension nodes) and preload/unload triggers.

| Parameter                    | Description                                                                                        | Parameter Set | Required | Default Value |
|------------------------------|----------------------------------------------------------------------------------------------------|----------------|----------|---------------|
| --databasetype               | Specifies the database type. Accepts: SAPHANA                                                      | SAPHANA        | Y        | SAPHANA       |
| --hostname                   | Specifies the host name where the database is present. Accepts: String                             | SAPHANA        | N        | localhost     |
| --databasename               | Specifies the database name. Accepts: String                                                       | SAPHANA        | Y        | -             |
| --username                   | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | SAPHANA        | N        | -             |
| --password                   | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | SAPHANA        | N        | -             |
| --tableamplification         | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255) | SAPHANA | N        | 8             |
| --instancenumber             | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02)                | SAPHANA        | N        | 00            |
| UnloadTablesPercentage       | Specifies in percentage how many tables will be unloaded from memory. Accepts: Int32 (0-100)       | SAPHANA        | N        | 0             |
| --setextensionnodepercentage | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100) | SAPHANA | N        | 0             |
| --extensionnodegroupname     | Specifies the name of the extension node group for data movement. Accepts: String                  | SAPHANA        | N        | <my_warm_data> |
| --setpagedtablespercentage   | The percentage of tables to set as page loadable. Accepts: String                                  | SAPHANA        | N        | 0             |
| --setcolumnloadablepercentage| The percentage of tables to set as column loadable. Accepts: Int32                                 | SAPHANA        | N        | 0             |
| --percentagetopreload        | The percentage of tables to set as preloadable. Accepts: Int32 (0-100)                            | MySQL          | N        | InnoDB        |
| --percentagecolumns          | The percentage of column tables in the database. Accepts: Int32 (0-100)                           | SAPHANA        | N        | 70            |
| --port                       | The port number to connect to a database or instance on. Accepts: String                          | SAPHANA        | N        | SAPHANA =3&lt;--instancenumber&gt;13 |

### remove

Deletes data in database objects up to the amount specified.  

If the database objects do not exist they will be created.  

| Parameter                 | Description                                                                                   | Parameter Set | Required | Default Value   |
|---------------------------|-----------------------------------------------------------------------------------------------|---------------|----------|-----------------|
| --databasetype            | Specifies the database type.                                                                 | All           | Y        | MicrosoftSQL   |
| --hostname                | Specifies the host name where the database is present.                                         | All           | N        | localhost       |
| --databasename            | Specifies the database name.                                                                  | All           | Y        | -               |
| --username                | Specifies the name of the user to connect as. If omitted integrated authentication is used.   | All           | N        | -               |
| --password                | Specifies the password of the user to connect as. If omitted integrated authentication is used.| All           | N        | -               |
| --amount                  | Specifies the amount of data to delete from the database.                                     | All           | Y        | -               |
| --unit                    | Specifies the data measurement unit for the --amount parameter.                               | All           | Y        | Bytes           |
| --numberofthreads         | The number of parallel operations to run the function at.                                     | All           | N        | 1               |
| --schematype              | The schema template to use for creating the database objects.                                 | All           | N        | WithoutIndexesLOB |
| --tableamplification      | Controls how many tables will be created and populated with data.                             | All           | N        | 8               |
| --columnwidth             | The maximum width of the HTML column.                                                          | All           | N        | 2147483647      |
| --randompercentage        | Specifies the percentage of data inserted into database objects that will be randomized.      | All           | N        | 60              |
| --namedinstance           | The Named Instance for Microsoft SQL server installations.                                     | MicrosoftSQL  | N        | MSSQLSERVER     |
| --useoraclesid            | Specifies if an Oracle database SID is being used as the DatabaseName.                        | Oracle        | N        | False           |
| --instancenumber          | The Instance number for SAP HANA installations.                                                | SAPHANA       | N        | 00              |
| --percentagecolumns       | Specifies the percentage of tables created in the schema that will be of type "column".       | SAPHANA       | N        | 80              |
| --percentagepagedtables   | Specifies the percentage of columns tables created in the schema that will be "page loadable".| SAPHANA       | N        | 0               |
| --percentagewarmextensionnodetables | Specifies the percentage of columns tables created in the schema that will be placed in an extension node. | SAPHANA | N | 0    |
| --extensionnodegroupname  | Specifies the name of the extension node group for data to be placed in.                      | SAPHANA       | N        | <my_warm_data>  |
| --partitions              | The number of partitions to be created for tables in the schema template.                     | SAPHANA       | N        | -1              |
| --mysqlstorageengine      | The storage engine for tables in the schema template.                                          | MySQL         | N        | InnoDB          |
| --ndbtablespace           | The table space to use for on-disk data in a MySQL cluster installation.                      | MySQL         | N        | -               |
| --mariadbstorageengine    | The storage engine for tables in the schema template.                                          | MariaDB       | N        | InnoDB          |
| --port                    | The port number to connect to a database or instance on.                                      | All           | N        | MicrosoftSQL = 1433, Oracle = 1521, SAP HANA = 3<--instancenumber>13, MySQL = 3306, MariaDB = 3306, PostgreSQL = 5432, MongoDB = 27017 |
| --logdata                 | Specifies if data should be logged to an analytics endpoint.                                  | All           | N        | -               |
| --testname                | Specifies the test name to use when persisting analytics data to an analytics endpoint.       | All           | N        | -               |
| --objectname              | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. | All        | N        | -               |
| --objectcatagory          | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. | All   | N        | -               |
| --sequence                | The sequence number of the test data to use when persisting data to an analytics endpoint.    | All           | N        | 1               |

### search

Performs SELECT queries and read-only processing of data in database objects.

If the database objects do no exist they are created.

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| --databasetype | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB \| PostgreSQL \| MongoDB | All | Y | MicrosoftSQL |
| --hostname | Specifies the host name where the database is present. Accepts: String | All | N | localhost |
| --databasename | Specifies the database name. Accepts: String | All | Y | - |
| --username | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --password | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --amount | Specifies the amount of data to populate the database with. Accepts: UInt64 | All | Y | - |
| --unit | Specifies the data measurement unit for the --amount parameter. Accepts: Bytes \| Kilobytes \| Megabytes \| Gigabytes \| Terabytes | All | Y | Bytes |
| --numberofthreads | The number of parallel operations to run the function at. Accepts: Int32 | All | N | 1 |
| --schematype | The schema template to use for creating the database objects. Accepts: WithIndexes \| WithoutIndexes \| WithIndexesLOB \| WithoutIndexesLOB \| MemoryOptimised \| MemoryOptimisedWithoutIndexes \| MemoryOptimisedLOB \| MemoryOptimisedWithoutIndexesLOB | All | N | WithoutIndexesLOB |
| --tableamplification | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255) | All | N | 8 |
| --columnwidth | The maximum width of the HTML column. This value has a knock-on effect for how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647) | All | N | 2147483647 |
| --queryoperation | The type of query operation to run on the object data. Accepts: UnionAll \| LeftOuterJoin | All | N | UnionAll |
| --randompercentage | Specifies in percentage how much of the data inserted into database objects will be randomized. Accepts: Int32 (0-100) | All | N | 60 |
| --namedinstance | The Named Instance for Microsoft SQL server installations. Accepts: String | MicrosoftSQL | N | MSSQLSERVER |
| --useoraclesid | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified. | Oracle | N | False |
| --instancenumber | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02) | SAPHANA | N | 00 |
| --percentagecolumns | Specifies in percentage how many of the tables created in the schema will be of type "column". Accepts: Int32 (0-100) | SAPHANA | N | 80 |
| --percentagepagedtables | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| --percentagewarmextensionnodetables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| --extensionnodegroupname | Specifies the name of the extension node group for data to be placed in. Accepts: String | SAPHANA | N | <my_warm_data> |
| --partitions | The number of --partitions to be created for tables in the schema template. Accepts: Int32 | SAPHANA | N | -1 |
| --mysqlstorageengine | The storage engine for tables in the schema template. Accepts: InnoDB \| NDB | MySQL | N | InnoDB |
| --ndbtablespace | The table space to use for on-disk data in a MySQL cluster installation. | MySQL | N | - |
| --mariadbstorageengine | The storage engine for tables in the schema template. InnoDB \| ColumnStore \| ROCKSDB \| S3 | MariaDB | N | InnoDB |
| --port | The port number to connect to a database or instance on. Accepts: String | All | N | MicrosoftSQL = 1433 \| Oracle = 1521 \| SAP HANA = 3<--instancenumber>13 \| MySQL = 3306 \| MariaDB = 3306 \| PostgreSQL = 5432 \| MongoDB = 27017 |
| --logdata | Specifies if data should be logged to an analytics endpoint. Accepts: Switch Parameter | All | N | - |
| --testname | Specifies the test name to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --objectname | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --objectcatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --sequence | The --sequence number of the test data to use when persisting data to an analytics endpoint. Accepts: Int32 | All | N | 1 |

### test

Combines INSERT,UPDATE ,DELETE and SELECTscenarios to create a unified test strategy.

This function will create the objects, populate them with data, and then perform test scenarios which include vendor-specific operations.

The following test scenarios are currently supported:

#### Simple

All Databases: Perform parallel queries using insert, delete, and update style commands.

#### Advanced

Microsoft SQL: Run Parallel queries for insert, delete, select, and update style commands. Indexes are reorganized and rebuilt once completed.

Oracle Database: Run Parallel queries for insert, delete, select, and update style commands.

SAP HANA: Run Parallel queries for insert, delete, select, and update style commands. A Delta Merge operation is performed once completed.

MySQL: Run Parallel queries for insert, delete, select, and update style commands. Tables are optimized once the operations are all completed.

MariaDB: Run Parallel queries for insert, delete, select, and update style commands. Tables are optimized once the operations are all completed.

PostgreSQL: Run Parallel queries for insert, delete, select, and update style commands. Table vacuum is performed once all the operations are complete.

MongoDB: Run Parallel queries for insert, delete, select, and update style commands.

#### Complex

Microsoft SQL: Run Parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. At regular intervals during the operation, the indexes are reorganized and rebuilt. At the end of the test, a DBCC operation is performed.

Oracle Database: Run Parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row.

SAP HANA: Run Parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. Delta Merge and unload operations are run at regular intervals while the test is running. At the end of the testa consistency check is run on all tables.

MySQL: Run Parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. The tables are flushed at regular intervals while the test is running. A consistency check is run at the end of the test.

MariaDB: Run Parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. The tables are flushed at regular intervals while the test is running. A consistency check is run at the end of the test.

PostgreSQL: Run Parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row. The tables are analyzed at regular intervals while the test is running. A consistency check is run at the end of the test.

MongoDB: Run Parallel queries for insert, delete, select, and update style commands. Update scenarios are different as entire rows are replaced instead of specific columns in a row.

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| --databasetype | Specifies the database type. | All | Y | MicrosoftSQL |
| --hostname | Specifies the host name where the database is present. | All | N | localhost |
| --databasename | Specifies the database name. | All | Y | - |
| --username | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). | All | N | - |
| --password | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). | All | N | - |
| --amount | Specifies the amount of data to populate the database with. | All | Y | - |
| --unit | Specifies the data measurement unit for the --amount parameter. | All | Y | Bytes |
| --numberofthreads | The number of paralleloperations to run the function at. | All | N | 1 |
| --schematype | The schema template to use for creating the database objects. | All | N | WithoutIndexesLOB |
| --tableamplification | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. | All | N | 8 |
| --columnwidth | The maximum width of the HTML column. This value has a knock-on effect on how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. | All | N | 2147483647 |
| --testtype | The type of test being performed. | All | N | Simple |
| --deferinitialingest | If a database is already populated with data then this will skip populating the database and proceed directly to the --testtype. | All | N | - |
| --changerate | Specifies the percentage of the overall dataset to performtesting operations on. | All | N | 10 |
| --growthrate | Specifies the percentage of overall dataset growth to allow for during testing. | All | N | 0 |
| --folder | The folder on the local filesystem which contains exported DataEngine (Engine-Oil) files. | All | N | - |
| --randompercentage | Specifies in percentagehow much of the data inserted into database objects will be randomized. | All | N | 60 |
| --namedinstance | The Named Instance for Microsoft SQL server installations. | MicrosoftSQL | N | MSSQLSERVER |
| --useoraclesid | If an Oracle database SID is being used as the DatabaseName thenthis switch parameter should be specified. | Oracle | N | False |
| --instancenumber | The Instance number for SAP HANAinstallations. | SAPHANA | N | 00 |
| --percentagecolumns | Specifies in percentage how many of the tables created in the schema will be of type "column". | SAPHANA | N | 80 |
| --percentagepagedtables | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". | SAPHANA | N | 0 |
| --percentagewarmextensionnodetables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. | SAPHANA | N | 0 |
| --extensionnodegroupname | Specifies the name of the extension node group for data to be placed in. | SAPHANA | N | <my_warm_data> |
| --partitions | The number of --partitions to be created for tables in the schema template. | SAPHANA | N | -1 |
| --mysqlstorageengine | The storage engine for tables in the schema template. | MySQL | N | InnoDB |
| --ndbtablespace | The table space to use for on-disk data in a MySQL cluster installation. | MySQL | N | - |
| --mariadbstorageengine | The storage engine for tables in the schema template. InnoDB \| ColumnStore \| ROCKSDB \| S3 | MariaDB | N | InnoDB |
| --port | The port number to connect to a database or instance on. Accepts: String | All | N | MicrosoftSQL = 1433 \| Oracle = 1521 \| SAP HANA = 3<--instancenumber>13 \| MySQL = 3306 \| MariaDB = 3306 \| PostgreSQL = 5432 \| MongoDB = 27017 |
| --logdata | Specifies if data should be logged to an analytics endpoint. Accepts: Switch Parameter | All | N | - |
| --testname | Specifies the test name to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --objectname | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --objectcatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --sequence | The --sequence number of the test data to use when persisting data to an analytics endpoint. Accepts: Int32 | All | N | 1 |

### update

Existing data in the database is replaced with new data using update commands.

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| --databasetype | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB \| PostgreSQL \| MongoDB | All | Y | MicrosoftSQL |
| --hostname | Specifies the host name where the database is present. Accepts: String | All | N | localhost |
| --databasename | Specifies the database name. Accepts: String | All | Y | - |
| --username | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --password | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --amount | Specifies the amount of data to populate the database with. Accepts: UInt64 | All | Y | - |
| --unit | Specifies the data measurement unit for the --amount parameter. Accepts: Bytes \| Kilobytes \| Megabytes \| Gigabytes \| Terabytes | All | Y | Bytes |
| --numberofthreads | The number of parallel operations to run the function at. When used in tandem with --folder this value must match the number of Engine-Oil files present in the folder. Accepts: Int32 | All | N | 1 |
| --schematype | The schema template to use for creating the database objects. Accepts: WithIndexes \| WithoutIndexes \| WithIndexesLOB \| WithoutIndexesLOB \| MemoryOptimised \| MemoryOptimisedWithoutIndexes \| MemoryOptimisedLOB \| MemoryOptimisedWithoutIndexesLOB | All | N | WithoutIndexesLOB |
| --tableamplification | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255) | All | N | 8 |
| --columnwidth | The maximum width of the HTML column. This value has a knock-on effect for how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647) | All | N | 2147483647 |
| --folder | The folder on the local filesystem which contains exported DataEngine (Engine-Oil) files. Only used when Replace is present. Accepts: String | All | N | - |
| --replace | Instead of updating data on a random column basis, entire rows of data are changed out. Accepts: SwitchParameter | All | N | - |
| --randompercentage | Specifies in percentage how much of the data inserted into database objects will be randomized. Accepts: Int32 (0-100) | All | N | 60 |
| --namedinstance | The Named Instance for Microsoft SQL server installations. Accepts: String | MicrosoftSQL | N | MSSQLSERVER |
| --useoraclesid | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified. | Oracle | N | False |
| --instancenumber | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02) | SAPHANA | N | 00 |
| --percentagecolumns | Specifies in percentage how many of the tables created in the schema will be of type "column". Accepts: Int32 (0-100) | SAPHANA | N | 80 |
| --percentagepagedtables | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| --percentagewarmextensionnodetables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| --extensionnodegroupname | Specifies the name of the extension node group for data to be placed in. Accepts: String | SAPHANA | N | <my_warm_data> |
| --partitions | The number of --partitions to be created for tables in the schema template. Accepts: Int32 | SAPHANA | N | -1 |
| --mysqlstorageengine | The storage engine for tables in the schema template. Accepts: InnoDB \| NDB | MySQL | N | InnoDB |
| --ndbtablespace | The table space to use for on-disk data in a MySQL cluster installation. | MySQL | N | - |
| --mariadbstorageengine | The storage engine for tables in the schema template. Accepts: InnoDB \| ColumnStore \| ROCKSDB \| S3 | MariaDB | N | InnoDB |
| --port | The port number to connect to a database or instance on. Accepts: String | All | N | MicrosoftSQL = 1433, Oracle = 1521, SAP HANA = 3<--instancenumber>13, MySQL = 3306, MariaDB = 3306, PostgreSQL = 5432, MongoDB = 27017 |
| --logdata | Specifies if data should be logged to an analytics endpoint. Accepts: SwitchParameter | All | N | - |
| --testname | Specifies the test name to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --objectname | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --objectcatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. Accepts: String | All | N | - |
| --sequence | The sequence number of the test data to use when persisting data to an analytics endpoint. Accepts: Int32 | All | N | 1 |

### write

Data is written to a specific table in the database every 200 microseconds.

If the database objects are not present they will be created.

| Parameter | Description | Parameter Set | Required | Default Value |
| --- | --- | --- | --- | --- |
| --databasetype | Specifies the database type. Accepts: MicrosoftSQL \| Oracle \| SAPHANA \| MySQL \| MariaDB \| PostgreSQL \| MongoDB | All | Y | MicrosoftSQL |
| --hostname | Specifies the host name where the database is present. Accepts: String | All | N | localhost |
| --databasename | Specifies the database name. Accepts: String | All | Y | - |
| --username | Specifies the name of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --password | Specifies the password of the user to connect as. If omitted integrated authentication is used (MicrosoftSQL). Accepts: String | All | N | - |
| --numberofthreads | The number of parallel operations to run the function at. Accepts: Int32 | All | N | 1 |
| --schematype | The schema template to use for creating the database objects. Accepts: WithIndexes \| WithoutIndexes \| WithIndexesLOB \| WithoutIndexesLOB \| MemoryOptimised \| MemoryOptimisedWithoutIndexes \| MemoryOptimisedLOB \| MemoryOptimisedWithoutIndexesLOB | All | N | WithoutIndexesLOB |
| --tableamplification | Controls how many tables will be created and how many will be populated with data. For each increment of this value 4 (four) more tables will be created. Accepts: Int32 (Max 255) | All | N | 8 |
| --columnwidth | The maximum width of the HTML column. This value has a knock-on effect for how much data will be inserted into each LOB column and how much textual data is used for each characterized schema transaction. Accepts: Int32 (Max 2147483647) | All | N | 2147483647 |
| --namedinstance | The Named Instance for Microsoft SQL server installations. Accepts: String | MicrosoftSQL | N | MSSQLSERVER |
| --useoraclesid | If an Oracle database SID is being used as the DatabaseName then this switch parameter should be specified. | Oracle | N | False |
| --instancenumber | The Instance number for SAP HANA installations. Accepts: 2 digit Int32 (00, 01, 02) | SAPHANA | N | 00 |
| --percentagecolumns | Specifies in percentage how many of the tables created in the schema will be of type "column". Accepts: Int32 (0-100) | SAPHANA | N | 80 |
| --percentagepagedtables | Specifies in percentage how many of the columns tables created in the schema will be of type "page loadable". Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| --percentagewarmextensionnodetables | Specifies in percentage how many of the columns tables created in the schema will be placed in an extension node. Accepts: Int32 (0-100) | SAPHANA | N | 0 |
| --extensionnodegroupname | Specifies the name of the extension node group for data to be placed in. Accepts: String | SAPHANA | N | <my_warm_data> |
| --partitions | The number of partitions to be created for tables in the schema template. Accepts: Int32 | SAPHANA | N | -1 |
| --mysqlstorageengine | The storage engine for tables in the schema template. Accepts: InnoDB \| NDB | MySQL | N | InnoDB |
| --ndbtablespace | The table space to use for on-disk data in a MySQL cluster installation. | MySQL | N | - |
| --mariadbstorageengine | The storage engine for tables in the schema template. InnoDB \| ColumnStore \| ROCKSDB \| S3 | MariaDB | N | InnoDB |
| --port | The port number to connect to a database or instance on. Accepts: String | All | N | MicrosoftSQL = 1433, Oracle = 1521, SAP HANA = 3<--instancenumber>13, MySQL = 3306, MariaDB = 3306, PostgreSQL = 5432, MongoDB = 27017 |

## Platform Engine Functions

### start

Starts a monitoring operation for an Operating Systems resources.

| Parameter     | Description                                           | Parameter Set | Required | Default Value |
|---------------|-------------------------------------------------------|---------------|----------|---------------|
| --hostname    | The --hostname of a system with the PlatformEngine_Client deployed. | All           | N        | localhost     |
| CollectionType | The type of resource collection to perform.            | All           | N        | PointInTime   |
| --duration    | The duration for which Duration Collection Types should run for. | All           | N        | 10 minutes (00:10:00) |
| --interval    | The interval between data points in a Duration or UntilNotified CollectionType. | All           | N        | 5 seconds (00:00:05) |
| --logdata     | Specifies if data should be logged to an analytics endpoint. | All           | N        | -             |
| --testname    | Specifies the test name to use when persisting analytics data to an analytics endpoint. | All           | N        | -             |
| --objectname  | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. | All           | N        | -             |
| --objectcatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. | All           | N        | -             |
| --sequence    | The --sequence number of the test data to use when persisting data to an analytics endpoint. | All           | N        | 1             |

### stop

When a Duration or UntilNotified operation has been requested using that start function, this command will stop the operation (using UntilNotified) and retrieve the data points.

| Parameter     | Description                                           | Parameter Set | Required | Default Value |
|---------------|-------------------------------------------------------|---------------|----------|---------------|
| --hostname    | The --hostname of a system with the PlatformEngine_Client deployed. | All           | N        | localhost     |
| --logdata     | Specifies if data should be logged to an analytics endpoint. | All           | N        | -             |
| --testname    | Specifies the test name to use when persisting analytics data to an analytics endpoint. | All           | N        | -             |
| --objectname  | Specifies the name of the object to use when persisting analytics data to an analytics endpoint. | All           | N        | -             |
| --objectcatagory | Specifies the name of the objects category to use when persisting analytics data to an analytics endpoint. | All           | N        | -             |
| --sequence    | The sequence number of the test data to use when persisting data to an analytics endpoint. | All           | N        | 1             |

## Analysis Engine Functions

### addsequence

This function is focused on recording data for analytical purposes. It assumes that any test use case can be subdivided into multiple segments and the beginning and end for each can be recorded for later analysis.

| Parameter          | Description                                               | Parameter Set | Required | Default Value |
|--------------------|-----------------------------------------------------------|---------------|----------|---------------|
| --dataimportstart  | When initial data population began (Pre-Test)             | All           | N        | -             |
| --dataimportend    | When initial data population ended (Pre-Test)             | All           | N        | -             |
| --datachangestart  | When initial data change began                            | All           | N        | -             |
| --datachangeend    | When initial data change ended                            | All           | N        | -             |
| --corejobstart     | When core operations started                              | All           | N        | -             |
| --corejobend       | When core operations ended                                | All           | N        | -             |
| --otherjobstart    | When miscellaneous operations started                     | All           | N        | -             |
| --otherjobend      | When miscellaneous operations ended                       | All           | N        | -             |
| --sequencestart    | When the entire sequence began                            | All           | N        | -             |
| --sequencend       | When the entire sequence ended                            | All           | N        | -             |
| --logdata          | Specifies if data should be logged to an analytics endpoint | All           | N        | -             |
| --testname         | Specifies the test name to use when persisting analytics data to an analytics endpoint | All           | Y        | -             |
| --objectname       | Specifies the name of the object to use when persisting analytics data to an analytics endpoint | All           | Y        | -             |
| --sequence         | The sequence number of the test data to use when persisting data to an analytics endpoint | All           | Y        | 1             |

### addtest

This function records additional information about test characteristics.

| Parameter       | Description                                               | Parameter Set | Required | Default Value |
|-----------------|-----------------------------------------------------------|---------------|----------|---------------|
| --testname      | The name of the test                                      | All           | Y        | -             |
| --coderevision  | The version of software being tested                      | All           | Y        |               |
| --solution      | The solution being tested                                 | All           | N        | -             |
| --amount        | The size of the data being tested                         | All           | N        | -             |
| --changerate    | The change rate of the data being tested                  | All           | N        | -             |
| --attempt       | The attempt of this test being performed                  | All           | N        | -             |
