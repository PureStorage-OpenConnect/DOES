############################################################################################################
#                                                                                                          #
#        This example script perfoms a number of D.O.E.S DataEngine operations on MongoDB databases.       #
#      Where logging is required ensure that a Microsoft SQL server, Oracle, MySQL, MariaDB or             #
#                 PostgreSQL database is available with the access credentials set in                      # 
#           C:\Users\<Username>\AppData\Roaming\Pure Storage\D.O.E.S\Config\Analytics.conf (Windows)       #
#                                   /opt/purestorage/does/config/ (Linux)                                  #
#                                                                                                          #
############################################################################################################


# Set the following variables to run the full test cycle. 
$TestName = ""
$Hostname = ""
$Databasename = ""
$Username = ""
$Password = ""
#Set this variable for the location of the folder with the DataEngine data files. 
$DataFileSourceFolder =  ""
# Set this variable for the target folder where DataEngine data files will be created. 
$DataFileTargetFolder = ""

Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'

############################
#First Test set - NO LOGGING
############################

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose

##Check All ADD capabilities are working - Web
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 4 -SchemaType WithIndexes -TableAmplification 8 -Verbose

#Check All ADD capabilities are working -File
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithoutIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithoutIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose

#Check export capabilities 
Export-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Folder $DataFileTargetFolder -Verbose

#Check search works well 
Search-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 5 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -QueryType LeftOuterJoin -Verbose
Search-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 5 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -QueryType UnionAll -Verbose

#Check delete works 
Remove-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 5 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Verbose
 
#Check if udate works for replace and LOB variants 
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Update-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 1 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Verbose

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Update-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 1 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Verbose

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Update-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 1 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -ReplaceWebPages -Folder $DataFileSourceFolder -Verbose

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Update-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 1 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -ReplaceWebPages -Folder $DataFileSourceFolder -Verbose

############################
#Second Test set - LOGGING
#############################

# Start the platform engine client
Start-PlatformEngine -Hostname $Hostname -CollectionType UntilNotified -Verbose

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose

##Check All ADD capabilities are working - Web
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 4 -SchemaType WithIndexesLOB -TableAmplification 8 -Verbose -LogData -TestName $TestName -ObjectName Add_From_Web_Defaults

##Check All ADD capabilities are working -File
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName Add_From_File_WithIndexes

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithoutIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName Add_From_File_WithoutIndexes

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName Add_From_File_WithIndexesLOB

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithoutIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName Add_From_File_WithOutIndexesLOB

#Check export capabilities 
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 100 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Export-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Folder $DataFiletargetFolder -Verbose
#>
#Check search works well 
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Search-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -QueryType LeftOuterJoin -Verbose -LogData -TestName $TestName -ObjectName Search_LeftOuterJoin
Search-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -QueryType UnionAll -Verbose -LogData -TestName $TestName -ObjectName Search_UnionAll

#Check search works well with LOB
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 128 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Search-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -QueryType LeftOuterJoin -Verbose -LogData -TestName $TestName -ObjectName Search_LeftOuterJoin_LOB
Search-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -QueryType UnionAll -Verbose -LogData -TestName $TestName -ObjectName Search_UnionAll_LOB

#Check delete works 
Remove-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Verbose -LogData -TestName $TestName -ObjectName RemoveData

#Check if udate works for replace and LOB variants 
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Update-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 1 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Verbose -LogData -TestName $TestName -ObjectName Update_WithIndexes

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithoutIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Update-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Verbose -LogData -TestName $TestName -ObjectName Update_WithIndexesLOB

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithIndexes -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Update-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -ReplaceWebPages -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName Update_WithIndexes_And_Replace

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -SchemaType WithoutIndexesLOB -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose
Update-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -ReplaceWebPages -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName Update_WithIndexesLOB_And_Replace

#Run Test Cycles - With Indexes
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithIndexes -TableAmplification 8 -Testtype Simple -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestSimple_WithIndexes

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithIndexes -TableAmplification 8 -Testtype Advanced -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestAdvanced_WithIndexes

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithIndexes -TableAmplification 8 -Testtype Complex -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestComplex_WithIndexes

#Run Test Cycles - Without Indexes
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithoutIndexes -TableAmplification 8 -Testtype Simple -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestSimple_WithoutIndexes

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithoutIndexes -TableAmplification 8 -Testtype Advanced -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestAdvanced_WithoutIndexes

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithoutIndexes -TableAmplification 8 -Testtype Complex -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestComplex_WithoutIndexes

#Run Test Cycles - With Indexes LOB
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithIndexesLOB -TableAmplification 8 -Testtype Simple -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestSimple_WithIndexesLOB

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithIndexesLOB -TableAmplification 8 -Testtype Advanced -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestAdvanced_WithIndexesLOB

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithIndexesLOB -TableAmplification 8 -Testtype Complex -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestComplex_WithIndexesLOB

#Run Test Cycles - Without Indexes LOB
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithoutIndexesLOB -TableAmplification 8 -Testtype Simple -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestSimple_WithoutIndexesLOB

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithoutIndexesLOB -TableAmplification 8 -Testtype Advanced -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestAdvanced_WithoutIndexesLOB

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -SchemaType WithoutIndexesLOB -TableAmplification 8 -Testtype Complex -ChangeRate 10 -Verbose -LogData -TestName $TestName -ObjectName TestComplex_WithoutIndexesLOB


#Final Test to see if defer intiail write is working 
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName TestSimple_DeferInitialCheck
Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -TableAmplification 8 -Testtype Simple -ChangeRate 30 -DeferInitialWrite -Verbose -LogData -TestName $TestName -ObjectName TestSimple_DeferInitialCheck

#Do Cycle Tests with multiple iterations 
Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName TestSimple_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -TableAmplification 8 -Testtype Simple -ChangeRate 10 -DeferInitialWrite -Verbose -LogData -TestName $TestName -ObjectName TestSimple_CyclerTrend -Sequence $i
}

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName TestAdvanced_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -TableAmplification 8 -Testtype Advanced -ChangeRate 10 -DeferInitialWrite -Verbose -LogData -TestName $TestName -ObjectName TestAdvanced_CyclerTrend -Sequence $i
}

Clear-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -TableClearOperation Drop -TableAmplification 8 -Verbose
Add-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 10 -Unit Gigabytes -NumberOfThreads 8 -TableAmplification 8 -Folder $DataFileSourceFolder -Verbose -LogData -TestName $TestName -ObjectName TestComplex_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    Test-DataEngine -DatabaseType MongoDB -Hostname $Hostname -DatabaseName $Databasename -UserName $Username -Password $Password -Amount 3 -Unit Gigabytes -NumberOfThreads 8 -Folder $DataFileSourceFolder -TableAmplification 8 -Testtype Complex -ChangeRate 10 -DeferInitialWrite -Verbose -LogData -TestName $TestName -ObjectName TestComplex_CyclerTrend -Sequence $i
}

Stop-PlatformEngine -Hostname $Hostname -LogData -TestName $TestName -ObjectName "PlatformEngineTest " -Verbose

