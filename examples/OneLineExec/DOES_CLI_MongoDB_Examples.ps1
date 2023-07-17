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
$DataFileSourceFolder = ""
# Set this variable for the target folder where DataEngine data files will be created. 
$DataFileTargetFolder = ""

$DoesExecFolder = 'C:\Program Files\Pure Storage\D.O.E.S'
sl $DoesExecFolder

############################
#First Test set - NO LOGGING
############################
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 


##Check All ADD capabilities are working - Web
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 10 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 


##Check All ADD capabilities are working -File
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithoutIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithoutIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

#Check export capabilities 

.\DOES.Cli.exe --engine data --function export --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --folder Z:\DataEngineFiles3Export --verbose 

#Check search works well 

.\DOES.Cli.exe --engine data --function search --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation LeftOuterJoin
.\DOES.Cli.exe --engine data --function search --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation UnionAll

#Check delete works 
.\DOES.Cli.exe --engine data --function remove --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8
 
#Check if udate works for replace and LOB variants 
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function update --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8


.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function update --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function update --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8 --replace --folder $DataFileSourceFolder 

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function update --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8 --replace --folder $DataFileSourceFolder 

############################
#Second Test set - LOGGING
############################

# start the platform engine client
.\DOES.Cli.exe --engine platform --function start --hostname $Hostname --collectiontype UntilNotified

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 

##Check All ADD capabilities are working - Web
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 5 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --logdata --testname $TestName --objectname Add_From_Web_Defaults

##Check All ADD capabilities are working -File
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --logdata --testname $TestName --objectname Add_From_File_WithIndexes

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithoutIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_WithoutIndexes


.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_WithIndexesLOB


.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithoutIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_WithOutIndexesLOB

#Check export capabilities 
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function export --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --folder $DataFileTargetFolder --verbose 

#Check search works well 
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function search --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation LeftOuterJoin --logdata --testname $TestName --objectname Search_LeftOuterJoin
.\DOES.Cli.exe --engine data --function search --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation UnionAll --logdata --testname $TestName --objectname Search_UnionAll

#Check delete works 
.\DOES.Cli.exe --engine data --function remove --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --logdata --testname $TestName --objectname Delete_SingleTest
 
#Check if udate works for replace and LOB variants 

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function update --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --logdata --testname $TestName --objectname Update_WithIndexes

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function update --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --logdata --testname $TestName --objectname Update_WithIndexesLOB

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function update --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --replace --folder $DataFileSourceFolder --logdata --testname $TestName --objectname Update_WithIndexes_And_Replace

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.Cli.exe --engine data --function update --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --replace --folder $DataFileSourceFolder --logdata --testname $TestName --objectname Update_WithIndexesLOB_And_Replace

#Run Test Cycles - With Indexes
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexes --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_WithIndexes

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexes --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_WithIndexes

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexes --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_WithIndexes

#Run Test Cycles - Without Indexes
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexes --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_WithoutIndexes

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexes --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_WithoutIndexes

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexes --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_WithoutIndexes

#Run Test Cycles - With Indexes LOB
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexesLOB --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_WithIndexesLOB

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexesLOB --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_WithIndexesLOB

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexesLOB --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_WithIndexesLOB

#Run Test Cycles - Without Indexes LOB
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexesLOB --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_WithoutIndexesLOB

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexesLOB --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_WithoutIndexesLOB

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexesLOB --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_WithoutIndexesLOB

#Final Test to see if defer intiail write is working 
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder --logdata --testname $TestName --objectname TestSimple_DeferInitialCheck
.\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedWithoutIndexesLOB --testtype Simple --changerate 50  --deferinitialingest --logdata --testname $TestName --objectname TestSimple_DeferInitialCheck

#Do Cycle Tests with multiple iterations 
.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder --logdata --testname $TestName --objectname TestSimple_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    .\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 50  --deferinitialingest --logdata --testname $TestName --objectname TestSimple_CyclerTrend --seqence $i
}

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder --logdata --testname $TestName --objectname TestAdvanced_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    .\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 50  --deferinitialingest --logdata --testname $TestName --objectname TestAdvanced_CyclerTrend --seqence $i
}

.\DOES.Cli.exe --engine data --function clear --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --tableamplification 8 --cleanoperation Drop 
.\DOES.Cli.exe --engine data --function add --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder --logdata --testname $TestName --objectname TestAdvanced_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    .\DOES.Cli.exe --engine data --function test --hostname $Hostname --databasetype MongoDB --databasename $Databasename --username $Username --password $Password --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 50  --deferinitialingest --logdata --testname $TestName --objectname TestComplex_CyclerTrend --seqence $i
}
# stop the platform engine client
.\DOES.Cli.exe --engine platform --function stop --hostname $Hostname --logdata --testname $TestName --objectname PlatformEngineTest
