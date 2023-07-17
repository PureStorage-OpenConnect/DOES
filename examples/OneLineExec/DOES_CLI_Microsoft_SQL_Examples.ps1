############################################################################################################
#                                                                                                          #
#     This example script perfoms a number of D.O.E.S DataEngine operations on Microsoft SQL databases.    #
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
#$Username = ""
#$Password = ""
#Set this variable for the location of the folder with the DataEngine data files. 
$DataFileSourceFolder = ""
# Set this variable for the target folder where DataEngine data files will be created. 
$DataFileTargetFolder = ""


$DoesExecFolder = 'C:\Program Files\Pure Storage\D.O.E.S\'
sl $DoesExecFolder

############################
#First Test set - NO LOGGING
############################
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 


##Check All ADD capabilities are working - Web
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 10 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 


##Check All ADD capabilities are working -File
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithoutIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithoutIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype MemoryOptimised --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype MemoryOptimisedWithoutIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype MemoryOptimisedLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype MemoryOptimisedWithoutIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 


#Check export capabilities 

.\DOES.exe --engine export --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --folder Z:\DataEngineFiles3Export --verbose 

#Check search works well 

.\DOES.exe --engine data --function search --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation LeftOuterJoin
.\DOES.exe --engine data --function search --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation UnionAll

#Check delete works 
.\DOES.exe --engine data --function remove --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8
 
#Check if udate works for replace and LOB variants 
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function update --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8


.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function update --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function update --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8 --replace --folder $DataFileSourceFolder 

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function update --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8  --numberofthreads 8 --replace --folder $DataFileSourceFolder 

############################
#Second Test set - LOGGING
############################

#Start the platform engine client
.\DOES.exe --engine platform --function start --hostname $Hostname --collectiontype UntilNotified


.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 

##Check All ADD capabilities are working - Web
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 5 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --logdata --testname $TestName --objectname Add_From_Web_Defaults

##Check All ADD capabilities are working -File
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 10 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --logdata --testname $TestName --objectname Add_From_File_WithIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithoutIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_WithoutIndexes


.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_WithIndexesLOB


.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithoutIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_WithOutIndexesLOB


.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype MemoryOptimised --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_MemoryOptimisedWithIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype MemoryOptimisedWithoutIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_MemoryOptimisedWithoutIndexes


.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype MemoryOptimisedLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_MemoryOptimisedLOB


.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype MemoryOptimisedWithoutIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder  --logdata --testname $TestName --objectname Add_From_File_MemoryOptimisedWithoutIndexesLOB

#Check export capabilities 
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function export --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --folder $DataFileTargetFolder --verbose 

#Check search works well 
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function search --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation LeftOuterJoin --logdata --testname $TestName --objectname Search_LeftOuterJoin
.\DOES.exe --engine data --function search --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --queryoperation UnionAll --logdata --testname $TestName --objectname Search_UnionAll

#Check delete works 
.\DOES.exe --engine data --function remove --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --logdata --testname $TestName --objectname Delete_SingleTest
 
#Check if udate works for replace and LOB variants 
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function update --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --logdata --testname $TestName --objectname Update_WithIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function update --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --logdata --testname $TestName --objectname Update_WithIndexesLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function update --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --replace --folder $DataFileSourceFolder --logdata --testname $TestName --objectname Update_WithIndexes_And_Replace

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --schematype WithIndexesLOB --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder 
.\DOES.exe --engine data --function update --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 56 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --replace --folder $DataFileSourceFolder --logdata --testname $TestName --objectname Update_WithIndexesLOB_And_Replace

#Run Test Cycles - With Indexes
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexes --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_WithIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexes --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_WithIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexes --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_WithIndexes

#Run Test Cycles - Without Indexes
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexes --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_WithoutIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexes --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_WithoutIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexes --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_WithoutIndexes

#Run Test Cycles - With Indexes LOB
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexesLOB --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_WithIndexesLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexesLOB --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_WithIndexesLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithIndexesLOB --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_WithIndexesLOB

#Run Test Cycles - Without Indexes LOB
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexesLOB --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_WithoutIndexesLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexesLOB --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_WithoutIndexesLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype WithoutIndexesLOB --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_WithoutIndexesLOB

#Run Test Cycles - MemoryOptimised
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimised --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_MemoryOptimised

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimised --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_MemoryOptimised

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimised --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_MemoryOptimised

#Run Test Cycles - MemoryOptimisedWithoutIndexes
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedWithoutIndexes --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_MemoryOptimisedWithoutIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedWithoutIndexes --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_MemoryOptimisedWithoutIndexes

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedWithoutIndexes --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_MemoryOptimisedWithoutIndexes


#Run Test Cycles - MemoryOptimised LOB
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedLOB --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_MemoryOptimisedLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedLOB --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_MemoryOptimisedLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedLOB --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_MemoryOptimisedLOB

#Run Test Cycles - MemoryOptimisedWithoutIndexes LOB
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedWithoutIndexesLOB --testtype Simple --changerate 50 --logdata --testname $TestName --objectname TestSimple_MemoryOptimisedWithoutIndexesLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedWithoutIndexesLOB --testtype Advanced --changerate 50 --logdata --testname $TestName --objectname TestAdvanced_MemoryOptimisedWithoutIndexesLOB

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedWithoutIndexesLOB --testtype Complex --changerate 50 --logdata --testname $TestName --objectname TestComplex_MemoryOptimisedWithoutIndexesLOB



#Final Test to see if defer intiail write is working 
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder --logdata --testname $TestName --objectname TestSimple_DeferInitialCheck
.\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --schematype MemoryOptimisedWithoutIndexesLOB --testtype Simple --changerate 50  --deferinitialingest --logdata --testname $TestName --objectname TestSimple_DeferInitialCheck

#Do Cycle Tests with multiple iterations 
.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder --logdata --testname $TestName --objectname TestSimple_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    .\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 50  --deferinitialingest --logdata --testname $TestName --objectname TestSimple_CyclerTrend --seqence $i
}

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder --logdata --testname $TestName --objectname TestAdvanced_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    .\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 50  --deferinitialingest --logdata --testname $TestName --objectname TestAdvanced_CyclerTrend --seqence $i
}

.\DOES.exe --engine data --function clear --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --tableamplification 8 --clearoperation Drop 
.\DOES.exe --engine data --function add --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 Folder $DataFileSourceFolder --logdata --testname $TestName --objectname TestAdvanced_CyclerTrend

for($i = 1; $i -le 5;$i++)
{
    .\DOES.exe --engine data --function test --hostname $Hostname --databasetype MicrosoftSQL --databasename $Databasename --amount 128 --unit  Gigabytes --tableamplification 8 --numberofthreads 8 --folder $DataFileSourceFolder --testtype Simple --changerate 50  --deferinitialingest --logdata --testname $TestName --objectname TestComplex_CyclerTrend --seqence $i
}

# stop the platform engine client 
.\DOES.exe --engine platform --function stop --hostname $Hostname --logdata --testname $TestName --objectname PlatformEngineTest
