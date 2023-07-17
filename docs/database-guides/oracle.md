# Using D.O.E.S with Oracle Database

The D.O.E.S Data Engine interacts with Oracle Database deployments through [PowerShell Cmdlets](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/reference/PowerShell.md) or [D.O.E.S.Cli functions](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/reference/cli.md).


## Supported Schema Templates

| Schema Template  | Description |
| --------------| -------|
| WithIndexes | A schema making use of characterized data (VARCHAR) for large column fields. Additional Nonclustered indexes are created for a range of fields. |
| WithoutIndexes | A schema making use of characterized data (VARCHAR) for large column fields. No additional indexes are created other than primary keys and unique indexes.  |
| WithIndexesLOB | A schema making use of large object (VARBINARY) fields for columns with large data sets. Additional Nonclustered indexes are created for a range of fields. |
| WithoutIndexesLOB | A schema making use of large object (VARBINARY) fields for columns with large data sets. No additional indexes are created other than primary keys and unique indexes. |

## Vendor specific test scenario 

| Test Type  | Vendor Operation |
| --------------| -------|
| Simple |  No vendor operation is performed. |
| Advanced | No vendor operation is performed. |
| Complex | No vendor operation is performed.|

## Examples 

### Add data to database 


This will add data to the database using transactional INSERT statements. The source for web page data is Wikipedia.Username and password authentication is used. 

**PowerShell example**

`Add-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 10 -Unit Gigabytes`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function add --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 10 --unit Gigabytes`

### Export database data to data files for later use


Core web page data is taken from the database and exported to files (with the name Engine-Oil-xxx.tar.gz) for later use. Username and password authentication is used. 

**PowerShell example**

`Export-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Folder $DataFileTargetFolder -Username $Username -Password $Password`

**DOES.Cli example** 

`.\DOES.Cli  --engine export --function add --hostname $Hostname --databasetype Oracle --databasename $Databasename --folder Z:\DataEngineFiles3Export --username $Username --password $Password`

### Add data to database using data files 


This will add data to the database using transactional INSERT statements. The source for web page data is a local folder containing exported data files. Username and password authentication is used. The operation's performance is increased through parallelism. 
*Note the use of  -NumberOfThreads. Value here must match the number of data files for the add operation.*

**PowerShell example**

`Add-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -UserName $User -Password $Password -Folder $DataFileSourceFolder`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function add --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --username $User --password $Password --numberofthreads 8 --folder $DataFileSourceFolder`

### Drop the tables in the database schema 
This will destroy the database tables. Using the table amplification value will assist with how many of the tables are dropped.Username and password authentication is used. 

**PowerShell example**

`Clear-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -TableClearOperation Drop -TableAmplification 8`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function clear --hostname $Hostname --databasetype Oracle --databasename $Databasename --tableamplification 8 --clearoperation Drop`

### Clear all table content in the database schema 


This will destroy all of the content of database tables. Using the table amplification value will assist with how many of the tables are affected.Username and password authentication is used. 

**PowerShell example**

`Clear-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -TableClearOperation Truncate -TableAmplification 8`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function clear --hostname $Hostname --databasetype Oracle --databasename $Databasename --tableamplification 8 --clearoperation Truncate`

### Execute SELECT queries using the LEFT OUTER JOIN search pattern 


Read only queries using a LEFT OUTER JOIN search pattern are run on the tables in the schema. The table amplification parameter is used to decide how many tables are queried. The operation's performance is increased through parallelism.Username and password authentication is used. 
*More data than is already in the database cannot be queried. If the database is smaller than the requested amount , the amount to be updated will be truncated.*

**PowerShell example**

`Search-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 50 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -QueryType LeftOuterJoin`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function search --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation LeftOuterJoin`

### Execute SELECT queries using the UNION ALL search pattern 


Read only queries using a UNION ALL search pattern are run on the tables in the schema. The table amplification parameter is used to decide how many tables are queried. The operation's performance is increased through parallelism.Username and password authentication is used. 
*More data than is already in the database cannot be queried. If the database is smaller than the requested amount , the amount to be queried will be truncated.*

**PowerShell example**

`Search-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 50 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -QueryType UnionAll`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function search --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation UnionAll`

### Change data in place for objects in the database 


Row and column level data is updated in the database schema. Username and password authentication is used. 
*More data than is already in the database cannot be updated. If the database is smaller than the requested amount , the amount to be updated will be truncated.*

**PowerShell example**

`Update-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 50 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function update --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8`

### Change data in place by replacing entire rows with new data in the database


Entire rows are updated in the database schema by replacing it with new data.The source for the new data can be local files with exported data or Wikipedia. Username and password authentication is used.
*More data than is already in the database cannot be updated. If the database is smaller than the requested amount , the amount to be deleted will be truncated.This will likely delete all data in the database.*

**PowerShell example**

`Update-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 50 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -ReplaceWebPages -Folder $DataFileSourceFolder`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function update --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8 --replace --folder $DataFileSourceFolder`

### Delete object data in the database 


Data is deleted from tables in the database schema. Username and password authentication is used. 

**PowerShell example**

`Remove-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 50 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function remove --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8`

### Run a simple test using a 30% change rate 


This will populate the database up to the amount specified and then run the simple test on 30% of the database size. The data source for new data is exported files in a local folder.Username and password authentication is used. 

**PowerShell example**

`Test-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -Testtype Simple -ChangeRate 30`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function test --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 30`

### Run an advanced test using a 30% change rate 


This will populate the database up to the amount specified and then run the advanced test on 30% of the database size. The data source for new data is expoted files in a local folder.Username and password authentication is used. 

**PowerShell example**

`Test-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -Testtype Advanced -ChangeRate 30`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function test --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --numberofthreads 8 --folder $DataFileSourceFolder --testtype Advanced --changerate 30`

### Run a complex test using a 30% change rate 


This will populate the database up to the amount specified and then run the complex test on 30% of the database size. The data source for new data is exported files in a local folder.Username and password authentication is used. 

**PowerShell example**

`Test-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -Testtype Complex -ChangeRate 30`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function test --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --numberofthreads 8 --folder $DataFileSourceFolder --testtype Complex --changerate 30`

### Run a simple test with a 10% change rate and 10% data growth 


This will populate the database up to the amount specified and then run the simple test on 30% of the database size.The database will increase in size by 10%. The data source for new data is exported files in a local folder. Username and password authentication is used. 

**PowerShell example**

`Test-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -Testtype Simple -ChangeRate 10 -Growthrate 10`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function test --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 10 --growthrate 10`

### Run an import operation and then a simple test ensuring no initial data population occurs


The database is populated using the add function or cmdlet and then a test run on it without doing the initial data population. The data source for new data is exported files in a local folder. The schema template to use is specified for both operations along with increased parallelism for performance purposed. Username and password authentication is used. 

**PowerShell example**

`Add-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 256 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexes -TableAmplification 8 -Folder $DataFileSourceFolder`
`Test-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 256 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithIndexes -TableAmplification 8 -Testtype Simple -ChangeRate 10`

**DOES.Cli example** 

`.\DOES.Cli  --engine data --function add --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 256 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder`
`.\DOES.Cli  --engine data --function test --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 256 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexes --testtype Simple --changerate 30`

### Run a simple test with a 10% change rate and collecting the data for later analysis


This will populate the database up to the amount specified and then run the simple test on 10% of the database size. The data source for new data is exported files in a local folder. A test and object name  are specified for logging purposes. Username and password authentication is used. 

**PowerShell example**

`Test-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Username $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithIndexes -TableAmplification 8 -Testtype Simple -ChangeRate 10 -LogData -TestName $TestName -ObjectName $Objectname`

**DOES.Cli example** 

`\DOES.exe --engine data --function test --hostname $Hostname --databasetype Oracle --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexes --testtype Simple --changerate 10 --logdata --testname $TestName --objectname $Objectname`

### Run a simple test with a 10% change rate for 30 sequences and collecting the data for later analysis


The database is populated with one command and then testing is cycled for 30 iterations using a for loop or other iterative function. Data is collected for each sequence once the scenario is completed. Username and password authentication is used. 

**PowerShell example**

`Add-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Folder $DataFileSourceFolder  -LogData -TestName $TestName -ObjectName TestAdvanced_CyclerTrend`
`for($i = 1; $i -le 30;$i++)`
`{`
`Test-DataEngine -DatabaseType Oracle -Hostname $Hostname -DatabaseName $Databasename -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -TableAmplification 8 -Testtype Simple -ChangeRate 10 -DeferInitialWrite -Verbose -LogData -TestName $TestName -ObjectName TestSimple_CyclerTrend -Sequence $i`
`}`

**DOES.Cli example** 

`.\DOES.Cli --engine data --function add --hostname $Hostname --databasetype Oracle --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder`
`for i in {1..30}; do`
`    .\DOES.Cli  --engine data --function test --hostname $Hostname --databasetype Oracle --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 10 --deferinitialingest --logdata --testname $TestName --objectname TestSimple_CyclerTrend --seqence $i`
`;done`
