############################################################################
#       SAP HANA D.O.E.S PowerShell Test Runner Script V 0.1               #
#--------------------------------------------------------------------------#
#             This test cycle script assumes the following :               #
#          1. This is a clean,new HANA instance installation               #
#              DO NOT RUN THIS SCRIPT ON PRODUCTION SYSTEMS                #
#          2. The SYSTEM User is unlocked                                  #
#   Ensure the HANA Client has been installed to allow the functions in    #
#                     this script work as required                         #
############################################################################

############################################################################
#                         Script Wide Parameters                           #
############################################################################

$TestName = ""
$SolutionSizeInGB = "256"

############################################################################
#        All remote client hostnames or IP's should be set here.           #
#       Each client should have D.O.E.S and PowerShell installed.          #
#                 PowerShell remoting should be enabled                    #
#   The source and target location for the DataEngineFiles must be set     #
############################################################################

# Comma seperated list of the clients. No comma needed for a single client. 
[string[]]$Clients = ""
# Where the DataEngine Files reside prior to being organized and copied
$DataEngineFileSourceLocation = ""
# Where the DataEngine Files will reside after being organized and copied
$DataEngineFileTargetLocation = ""
# The folder prefix wherin the DataEngine files will reside per DOES instance
$DataEngineFileFolderPrefix = "DataEngineFiles"
$PSConfigurationName = "PowerShell.7.3.4"

############################################################################
#        All database server hostnames or IP's should be set here.         #
############################################################################

# Comma seperate list of the database systems. No comma needed for a single 
# system
[string[]]$DatabaseSystems = ""
$Ports = [System.Collections.ArrayList]@()

############################################################################
#                      Database Specific Parameters                        #  
#          A database admin user must exist for this script to use         #
#                            for configuration.                            #
#    These parameters will control how large the database becomes with     #
#    warm data management (NSE) and how many tenant databases to create.   #    
#                  The system user password must be set.                   #            
############################################################################

$DatabaseAdminUserName = "SYSTEM"
$DatabaseAdminPassword = ""
$SAPHANAInstanceNumber = "00"
$DatabaseTestUserPrefix = "User"
$DatabaseTestUserPassword = "Password"
$TenantPrefix = "SH"
$PerctageColumntables = 100
$DatabaseSizeMuliplier = 2
$NumberOfTenants = 1
$NumberOfUsersPerDatabase = 16
$ChangeRate = 30
$TableCount = 8
$ThreadCountPerRunner = 2
$ThreadCountPerRunnerReadScale = 8
$PercentageRandom = 30
$SchemaTemplate = "WithoutIndexes"

############################################################################
#                           Credential Parameters                          #
#   The username and password for PowerShell remoting must be set here     # 
#     WinRM needs to be setup on the remote systems set in $Clients        # 
#  WinRM can be setup on each client system using the following commands : #
#                         Quicksetup : winrm quickconfig                   #
#              Set trusted hosts for allowed connections :                 #
#      Set-Item WSMan:\localhost\Client\TrustedHosts -Value '*'            #
############################################################################

$RemoteWindowsUser = ""
$RemoteWindowsPassword = "" | ConvertTo-SecureString -AsPlainText -Force
$WinRMCredentials = New-Object -TypeName System.Management.Automation.PSCredential `
-ArgumentList $RemoteWindowsUser,$RemoteWindowsPassword

############################################################################
#                     SAP HANA Configuration Parameters                    #
#        These properties will be setup during database configuration      #
############################################################################

$DataBackupBufferSize = "4096"
$LogBackupBufferSize = "4096"
$LogBackupIntervalMode = "service"
$LogBackupSize = "64"
$NumCompletionQueues = "8" 
$NumberSubmitQueues = "8"
$CatalogBackup = "/hana/backup/data"
$DataBackup = "/hana/backup/data"
$LogBackup = "/hana/backup/log"
$LogBufferCount = "128"
$LogBufferSize = "16384"
$LogFormatSetpSize = "16384"
$LogSegmentSize = "128"
# Adjusting the buffer size and buffer percentage parameters ensures more read IO will occur
$NSEBufferSize = "32768"
$NSEBufferPercentage = "5" 
[STRING]$LogPreformatSegmentCount = [System.Math]::Floor((([INT]$LogSegmentSize / 1024) `
* [INT]$SolutionSize) / $DatabaseSystems.Count) / [INT]$NumberOfTenants

Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'

###########################################################################
#           DropDatabaseTables will remove any DataEngine tables           #
############################################################################
Function DropDatabaseTables()
{
    Param( 
        $TableCount
    )
    Write-Host "Dropping database tables" -BackgroundColor White -ForegroundColor Black
    $commandset = {
                        param($Database, $Username, $Password, $Hostname, $TableAmplification, $Port)
                                    Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                                    Clear-DataEngine -DatabaseType SAPHANA -Hostname $Hostname `
                                    -DatabaseName $Database -UserName $Username -Password $Password `
                                    -TableClearOperation Drop -TableAmplification $TableAmplification `
                                    -Port $Port -Verbose
                  }
    
    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    $DatabaseSystemNumber = 0
    $DatabaseSystemsMax = ($DatabaseSystems.Count - 1)

    for($TenantNumber = 1; $TenantNumber -le $NumberOfTenants;$TenantNumber++)
    {
        $Tenant = $TenantPrefix + $TenantNumber 
        for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
        {
            $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
            Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials -ConfigurationName $PSConfigurationName `
            -ScriptBlock $commandset -ArgumentList $Tenant, $DatabaseUser, $DatabaseTestUserPassword, `
            $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $Ports[$TenantNumber - 1] -AsJob

            $TestClientNumber++;;$DatabaseSystemNumber++

            if(!($TestClientNumber -le $TestClientMax)) { $TestClientNumber = 0 }
            if(!($DatabaseSystemNumber -le $DatabaseSystemsMax)) { $DatabaseSystemNumber = 0 } 
        }
    }

    $completed = $false
    while(!($completed))
    {
        $RunningJobs = Get-Job -State Running
        if($RunningJobs.Count -eq 0)
        {
            $completed = $true
        }
        else
        {
            Start-Sleep -Seconds 5
        }
    }
}

############################################################################
#           DeltaMerge will  force a delta marge accross any               #
#      DataEngine tables and unload a specific percent of the them         #
############################################################################
Function DeltaMerge()
{
    Param( 
        $TableCount,
        $PercentToUnload
    )
    Write-Host "Running delta merge" -BackgroundColor White -ForegroundColor Black
    $commandset = {
                        param($Database, $Username, $Password, $Hostname, $TableAmplification, $Port,
                         $PercentToUnload)
                                    Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                                    Merge-DataEngine -DatabaseType SAPHANA -Hostname $Hostname `
                                    -DatabaseName $Database -UserName $Username -Password $Password `
                                    -TableAmplification $TableAmplification  -UnloadTablesPercentage $PercentToUnload `
                                    -Port $Port
                  }
    
    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    $DatabaseSystemNumber = 0
    $DatabaseSystemsMax = ($DatabaseSystems.Count - 1)

    for($TenantNumber = 1; $TenantNumber -le $NumberOfTenants;$TenantNumber++)
    {
        $Tenant = $TenantPrefix + $TenantNumber 
        for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
        {
            $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
            Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials -ConfigurationName $PSConfigurationName `
            -ScriptBlock $commandset -ArgumentList $Tenant, $DatabaseUser, $DatabaseTestUserPassword, `
            $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $Ports[$TenantNumber - 1], $PercentToUnload -AsJob

            $TestClientNumber++;;$DatabaseSystemNumber++

            if(!($TestClientNumber -le $TestClientMax)) { $TestClientNumber = 0 }
            if(!($DatabaseSystemNumber -le $DatabaseSystemsMax)) { $DatabaseSystemNumber = 0 } 
        }
    }

    $completed = $false
    while(!($completed))
    {
        $RunningJobs = Get-Job -State Running
        if($RunningJobs.Count -eq 0)
        {
            $completed = $true
            Write-Verbose "All Completed  for DropAllDatabaseTables"
        }
        else
        {
            Start-Sleep -Seconds 5
        }
    }
}

############################################################################
#                PopulateDatabase will populate the database               #
############################################################################
Function PopulateDatabase()
{
    Param(
        $ThreadCount,
        $TableCount,
        $DataSizePerUser,
        $DataSizeUnit, 
        $PercentColumnTables,
        $PercentPagedTables,
        [Parameter(Mandatory=$false)]
        [Switch]$RotateDataFolder,
        $ObjectCategory, 
        $Sequence
    )
    Write-Host "Running initial database population" -BackgroundColor White -ForegroundColor Black
    $DataFolderNumber
    if($RotateDataFolder){ $DataFolderNumber = 1} else {$DataFolderNumber = 0}

    $commandset = {
        param($Database, $Username, $Password, $Hostname, $TableAmplification, $Port, $DataSize, $DataSizeUnit,
                $DataFolder, $ThreadCount, $SchemaType, $ColumnTables, $PagedTables, $PercentageRandomVal,
                $TestName, $ObjectCategory, $Sequence)
                    Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                    $ObjectName = $ObjectCategory + "_" + $Database + "_" + $Username
                    Add-DataEngine -DatabaseType SAPHANA -DatabaseName $Database -UserName $Username `
                    -Password $Password -Hostname $Hostname -Amount $DataSize -Unit $DataSizeUnit -Folder $DataFolder `
                    -RandomPercentage $PercentageRandomVal -NumberOfThreads $ThreadCount -SchemaType $SchemaType -PercentageColumns $ColumnTables `
                    -TableAmplification $TableAmplification -PercentagePagedTables $PagedTables -Port $Port -LogData `
                    -TestName $TestName -ObjectName $ObjectName -ObjectCategory $ObjectCategory -Sequence $Sequence
    }

    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    $DatabaseSystemNumber = 0
    $DatabaseSystemsMax = ($DatabaseSystems.Count - 1)
    $DataFolder = $DataEngineFileTargetLocation + $DataEngineFileFolderPrefix

    for($TenantNumber = 1; $TenantNumber -le $NumberOfTenants;$TenantNumber++)
    {
        $Tenant = $TenantPrefix + $TenantNumber 
        for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
        {
            $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
            if($DataFolderNumber -ne 0){$DataFolder = $DataEngineFileTargetLocation + $DataEngineFileFolderPrefix + $DataFolderNumber}
            Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials -ConfigurationName $PSConfigurationName `
            -ScriptBlock $commandset -ArgumentList $Tenant, $DatabaseUser, $DatabaseTestUserPassword, `
            $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $Ports[$TenantNumber - 1], $DataSizePerUser, `
            $DataSizeUnit, $DataFolder, $ThreadCount, $SchemaTemplate, $PercentColumnTables, $PercentPagedTables, $PercentageRandom, `
            $TestName, $ObjectCategory, $Sequence -AsJob

            $TestClientNumber++;$DatabaseSystemNumber++;

            if(!($TestClientNumber -le $TestClientMax)) 
            { 
                $TestClientNumber = 0 
                if($DataFolderNumber -ne 0)
                {if(!($DataFolderNumber -ne $NumberOfFoldersPerTestClient))
                {$DataFolderNumber++}}
            }
            if(!($DatabaseSystemNumber -le $DatabaseSystemsMax)) { $DatabaseSystemNumber = 0 } 
        }
    }
    $completed = $false
    while(!($completed))
    {
        $RunningJobs = Get-Job -State Running
        if($RunningJobs.Count -eq 0)
        {
            $completed = $true
        }
        else {
            Start-Sleep -seconds 5
        }
    }
}

############################################################################
#         PerformDataEngineTest executes the DataEngine test function      #
############################################################################
Function PerformDataEngineTest()
{
    Param(
        $ThreadCount,
        $TableCount,
        $DataSizePerUser,
        $DataSizeUnit,
        $ChangePercentage,
        $TestType,
        [Parameter(Mandatory=$false)]
        [Switch]$RotateDataFolder,
        [Parameter(Mandatory=$false)]
        [Switch]$DeferInitialWrite,
        $ObjectCategory, 
        $Sequence
    )
    Write-Host "Running DataEngine test function" -BackgroundColor White -ForegroundColor Black
    $DataFolderNumber
    if($RotateDataFolder){ $DataFolderNumber = 1} else {$DataFolderNumber = 0}

    if(!($DeferInitialWrite))
    {
        $commandset = {
            param($Database, $Username, $Password, $Hostname, $TableAmplification, $Port, $DataSize, $DataSizeUnit,
                    $ChangePercentage, $TestType, $DataFolder, $ThreadCount, $PercentageRandomVal, $SchemaType, `
                    $TestName, $ObjectCategory, $Sequence)
                        Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                        $ObjectName = $ObjectCategory + "_" + $Database + "_" + $Username
                        Test-DataEngine -DatabaseType SAPHANA -Hostname $Hostname -DatabaseName $Database `
                        -UserName $Username -Password $Password -Amount $DataSize -Unit $DataSizeUnit `
                        -NumberOfThreads $ThreadCount -Folder $DataFolder -RandomPercentage $PercentageRandomVal -SchemaType WithoutIndexes `
                        -TableAmplification $TableAmplification -Testtype $TestType -ChangeRate $ChangePercentage `
                        -Port $Port -SchemaType $SchemaType -LogData `
                        -TestName $TestName -ObjectName $ObjectName -ObjectCategory $ObjectCategory -Sequence $Sequence
          }
      }
      else
      {
        $commandset = {
            param($Database, $Username, $Password, $Hostname, $TableAmplification, $Port, $DataSize, $DataSizeUnit,
                   $ChangePercentage, $TestType, $DataFolder, $ThreadCount, $PercentageRandomVal, $SchemaType, `
                   $TestName, $ObjectCategory, $Sequence)
                        Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                        $ObjectName = $ObjectCategory + "_" + $Database + "_" + $Username
                        Test-DataEngine -DatabaseType SAPHANA -Hostname $Hostname -DatabaseName $Database `
                        -UserName $Username -Password $Password -Amount $DataSize -Unit $DataSizeUnit `
                        -NumberOfThreads $ThreadCount -Folder $DataFolder -RandomPercentage $PercentageRandomVal -SchemaType WithoutIndexes `
                        -TableAmplification $TableAmplification -Testtype $TestType -ChangeRate $ChangePercentage `
                        -Port $Port -DeferInitialWrite -SchemaType $SchemaType -LogData `
                        -TestName $TestName -ObjectName $ObjectName -ObjectCategory $ObjectCategory -Sequence $Sequence
          }
      }



    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    $DatabaseSystemNumber = 0
    $DatabaseSystemsMax = ($DatabaseSystems.Count - 1)
    $DataFolder = $DataEngineFileTargetLocation + $DataEngineFileFolderPrefix

    for($TenantNumber = 1; $TenantNumber -le $NumberOfTenants;$TenantNumber++)
    {
        $Tenant = $TenantPrefix + $TenantNumber 
        for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
        {
            $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
            if($DataFolderNumber -ne 0){$DataFolder = $DataEngineFileTargetLocation + $DataEngineFileFolderPrefix + $DataFolderNumber}
            Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials -ConfigurationName $PSConfigurationName `
            -ScriptBlock $commandset -ArgumentList $Tenant, $DatabaseUser, $DatabaseTestUserPassword, `
            $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $Ports[$TenantNumber - 1], $DataSizePerUser, `
            $DataSizeUnit, $ChangePercentage, $TestType, $DataFolder, $ThreadCount, $PercentageRandom, $SchemaTemplate, `
            $TestName, $ObjectCategory, $Sequence -AsJob

            $TestClientNumber++;$DatabaseSystemNumber++;

            if(!($TestClientNumber -le $TestClientMax)) 
            { 
                $TestClientNumber = 0 
                if($DataFolderNumber -ne 0)
                {if(!($DataFolderNumber -eq $NumberOfFoldersPerTestClient))
                {$DataFolderNumber++}}
            }
            if(!($DatabaseSystemNumber -le $DatabaseSystemsMax)) { $DatabaseSystemNumber = 0 } 
        }
        
    }

    $completed = $false
    while(!($completed))
    {
        $RunningJobs = Get-Job -State Running
        if($RunningJobs.Count -eq 0)
        {
            $completed = $true
        }
        else {
            Start-Sleep -seconds 5
        }
    }
}

############################################################################
#       PerformDataEngineOLAPTest executes the DataEngine OLAP function    #
#                       using the UnionAll  set operator                   #
############################################################################
Function PerformDataEngineOLAPTest()
{
    Param(
        $ThreadCount,
        $TableCount,
        $DataSizePerUser,
        $DataSizeUnit,
        $ObjectCategory, 
        $Sequence
    )
    Write-Host "Running DataEngine read query function" -BackgroundColor White -ForegroundColor Black
    $commandset = {
        param($Database, $Username, $Password, $Hostname, $TableAmplification, $Port, $DataSize, $DataSizeUnit,
                $ThreadCount, $TestName, $ObjectCategory, $Sequence)
                    Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                    $ObjectName = $ObjectCategory + "_" + $Database + "_" + $Username
                    Search-DataEngine -DatabaseType SAPHANA -Hostname $Hostname -DatabaseName $Database `
                    -UserName $Username -Password $Password -Amount $DataSize -Unit $DataSizeUnit `
                    -NumberOfThreads $ThreadCount -TableAmplification $TableAmplification -QueryType UnionAll `
                    -Port $Port -LogData `
                    -TestName $TestName -ObjectName $ObjectName -ObjectCategory $ObjectCategory -Sequence $Sequence
    }


    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    $DatabaseSystemNumber = 0
    $DatabaseSystemsMax = ($DatabaseSystems.Count - 1)

    for($TenantNumber = 1; $TenantNumber -le $NumberOfTenants;$TenantNumber++)
    {
        $Tenant = $TenantPrefix + $TenantNumber 
        for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
        {
            $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
            Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials -ConfigurationName $PSConfigurationName `
            -ScriptBlock $commandset -ArgumentList $Tenant, $DatabaseUser, $DatabaseTestUserPassword, `
            $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $Ports[$TenantNumber - 1], $DataSizePerUser, `
            $DataSizeUnit, $ThreadCount, $TestName, $ObjectCategory, $Sequence -AsJob

            $TestClientNumber++;$DatabaseSystemNumber++;

            if(!($TestClientNumber -le $TestClientMax))  { $TestClientNumber = 0 }
            if(!($DatabaseSystemNumber -le $DatabaseSystemsMax)) { $DatabaseSystemNumber = 0 } 
        }
    }

    $completed = $false
    while(!($completed))
    {
        $RunningJobs = Get-Job -State Running
        if($RunningJobs.Count -eq 0)
        {
            $completed = $true
        }
        else
        {
            Start-Sleep -Seconds 5
        }
    }
}

############################################################################
#     CreateDatabasesAndUsers connects to the database instance, drops     #
#      any existing databases with the same name, and then creates a       #
#   new databases. Users using the DatabaseUserPrefix are created in this  #
#                              new database                                #
############################################################################
Function CreateDatabasesAndUsers()
{
    Write-Host "Creating databases and users" -BackgroundColor White -ForegroundColor Black
    $hdbConnectionStringSystemDB = "Driver={HDBODBC};ServerNode=" + $DatabaseSystems[0] + ":3" `
    + $SAPHANAInstanceNumber + "13;UID=" + $DatabaseAdminUserName + ";PWD=" + $DatabaseAdminPassword +";"
    
    for($Tenant = 1; $Tenant -le $NumberOfTenants; $Tenant++)
    {
        # Destroy all existing Tenants
        $hdbDestroyTenant = "ALTER SYSTEM STOP DATABASE " + $TenantPrefix + $Tenant.ToString() + " IMMEDIATE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbDestroyTenant
        $hdbDestroyTenant = "DROP DATABASE " + $TenantPrefix + $Tenant.ToString() + ";"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbDestroyTenant
    }

    for($Tenant = 1; $Tenant -le $NumberOfTenants; $Tenant++)
    {
            # Create New Tenants
        $hdbCreateTenant = "CREATE DATABASE " + $TenantPrefix + $Tenant.ToString() `
        + " AT LOCATION '" + $DatabaseSystems[0].Split('.')[0] + "' SYSTEM USER PASSWORD " + $DatabaseAdminPassword + ";"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCreateTenant
        # Add Indexservers for Scale Out
        for($TestSystemCount = 1; $TestSystemCount -lt $DatabaseSystems.Count;$TestSystemCount++)
        {
            $hdbAddIndexServer = "ALTER DATABASE " + $TenantPrefix + $Tenant.ToString() + " ADD 'indexserver' " + 
            "AT LOCATION '" + $DatabaseSystems[$TestSystemCount].Split('.')[0] + "';"
            Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbAddIndexServer
        }
    }
    
    for($Tenant = 1; $Tenant -le $NumberOfTenants; $Tenant++) 
    {
        if($Tenant -eq 1)
        {
            $Port = 41
        }
        else 
        {
            $Port = $Port + 3
        }
        $hdbConnectionStringTenantDB = "Driver={HDBODBC};ServerNode=" + $DatabaseSystems[0] `
        + ":3" + $SAPHANAInstanceNumber + $Port.ToString() + ";UID=" + $DatabaseAdminUserName + ";PWD=" + $DatabaseAdminPassword +";"
        $CreateNewUsers = "CREATE ROLE DOES_ROLE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        $CreateNewUsers = "GRANT CREATE SCHEMA TO DOES_ROLE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        $CreateNewUsers = "GRANT DEVELOPMENT TO DOES_ROLE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        $CreateNewUsers = "GRANT EXPORT TO DOES_ROLE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        $CreateNewUsers = "GRANT IMPORT TO DOES_ROLE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        $CreateNewUsers = "GRANT MONITORING TO DOES_ROLE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        $CreateNewUsers = "GRANT BACKUP OPERATOR TO DOES_ROLE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        $CreateNewUsers = "GRANT CATALOG READ TO DOES_ROLE;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
        {
            $CreateNewUsers = "CREATE USER " + $DatabaseTestUserPrefix + $UserNumber.ToString() + " PASSWORD " + $DatabaseTestUserPassword + " NO FORCE_FIRST_PASSWORD_CHANGE;"
            Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
            $CreateNewUsers = "GRANT DOES_ROLE TO " + $DatabaseTestUserPrefix + $UserNumber.ToString() + ";"
            Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $CreateNewUsers
        }
    }
}

############################################################################
#           Set the NSE buffer cache to a specified limit                  #
############################################################################
Function SetNSEBufferCacheSize()
{
    Write-Host "Setting native storage extension buffer cache" -BackgroundColor White -ForegroundColor Black
    for($Tenant = 1; $Tenant -le $NumberOfTenants; $Tenant++) 
    {
        if($Tenant -eq 1)
        {
            $Port = 41
        }
        else 
        {
            $Port = $Port + 3
        }
        $hdbConnectionStringTenantDB = "Driver={HDBODBC};ServerNode=" + $DatabaseSystems[0] `
        + ":3" + $SAPHANAInstanceNumber + $Port.ToString() + ";UID=" + $DatabaseAdminUserName + ";PWD=" + $DatabaseAdminPassword +";"
        $hdbCommandConfigureBufferMB = "ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'system') SET ('buffer_cache_cs','max_size') = '" + $NSEBufferSize + "' with reconfigure;"
        $hdbCommandBufferNSEPC = "ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'system') SET ('buffer_cache_cs','max_size_rel') = '" + $NSEBufferPercentage + "' with reconfigure;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $hdbCommandConfigureBufferMB
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $hdbCommandBufferNSEPC
    }
}

############################################################################
#                Run a savepoint on the SAP HANA database                  #
############################################################################
Function RunSavePoint()
{
    Write-Host "Running save point" -BackgroundColor White -ForegroundColor Black
    for($Tenant = 1; $Tenant -le $NumberOfTenants; $Tenant++) 
    {
        if($Tenant -eq 1)
        {
            $Port = 41
        }
        else 
        {
            $Port = $Port + 3
        }
        $hdbConnectionStringTenantDB = "Driver={HDBODBC};ServerNode=" + $DatabaseSystems[0] `
        + ":3" + $SAPHANAInstanceNumber + $Port.ToString() + ";UID=" + $DatabaseAdminUserName + ";PWD=" + $DatabaseAdminPassword +";"
        $RunSavePoint = "ALTER SYSTEM SAVEPOINT;"
        Get-ODBCData -hanaConnectionString $hdbConnectionStringTenantDB -hdbsql $RunSavePoint
    }
}

############################################################################
#      Supporting function for the HANA connections in this script         #
#                Ensure the SAP HANA client is installed                   #
#              https://tools.hana.ondemand.com/#hanatools                  #
############################################################################
function Get-ODBCData() 
{
    Param($hanaConnectionString,
    $hdbsql)

    $Conn = New-Object System.Data.Odbc.OdbcConnection($hanaCOnnectionString)
    $Conn.open()
    $readcmd = New-Object System.Data.Odbc.OdbcCommand($hdbsql,$Conn)
    $readcmd.CommandTimeout = '6000'
    $da = New-Object System.Data.Odbc.OdbcDataAdapter($readcmd)
    $dt = New-Object System.Data.DataTable
    [void]$da.fill($dt)
    $Conn.close()
    return $dt
}

############################################################################
#             Sets up the source data files on each D.O.E.S client.        #
#  Takes all of the files in a single folder location and then copies them #
#          to isolated folders for use by each DataEngine instance         #
############################################################################
Function SetupDataEngineClientFolders()
{
    Write-Host "Setting up client DataEngine folders" -BackgroundColor White -ForegroundColor Black
    Param(
        [Parameter(Mandatory=$true)]
        $DataFileTemplatePath,
        [Parameter(Mandatory=$true)]
        [INT]$NumberOfFolders,
        [Parameter(Mandatory=$true)]
        $TargetLocation,
        [Parameter(Mandatory=$true)]
        $FolderPrefix
        )

    $commandset = {
                       param($DataFileTemplatePath, [INT]$NumberOfFolders, $TargetLocation, $FolderPrefix)
                       $DataEngineSourceFolders = Get-ChildItem $TargetLocation | Where-Object Name -Like "$FolderPrefix*" `
                       | Where-Object FullName -NE $DataFileTemplatePath
                       if([string]::IsNullOrWhiteSpace($DataEngineSourceFolders) -eq $false)
                       {
                            foreach($Folder in $DataEngineSourceFolders)
                            {
                                Remove-Item -Path $Folder.FullName -Recurse
                            }
                       }
                        [System.Collections.Queue]$DataFiles = Get-ChildItem -Path $DataFileTemplatePath | Where-Object Name -Like Engine.Oil*
                        [INT]$TotalFiles = $DataFiles.Count
                        $FilesPerFolder = $TotalFiles / $NumberOfFolders
                        $FilesPerFolder = [math]::Truncate($FilesPerFolder)
                                
                        for($FolderNum = 1; $FolderNum -le $NumberOfFolders; $FolderNum++)
                        {
                            $TargetFolderForCopy = $TargetLocation + $FolderPrefix + $FolderNum
                            New-Item -ItemType Directory -Path $TargetFolderForCopy
                                
                            for($FilesInIsolation = 1; $FilesInIsolation -le $FilesPerFolder;$FilesInIsolation++)
                            {
                                $SourceFileToCopy = $DataFiles.Dequeue()
                                Copy-Item -Path $SourceFileToCopy.FullName -Destination $TargetFolderForCopy 
                            }
                        }
                  }
    
    foreach($client in $Clients)
    {
        Invoke-Command -ComputerName $client -Credential $WinRMCredentials -ConfigurationName $PSSessionConfigurationName `
            -ScriptBlock $commandset -ArgumentList $DataFileTemplatePath, $NumberOfFolders, $TargetLocation, $FolderPrefix -AsJob
    }


    $completed = $false
    while(!($completed))
    {
        $RunningJobs = Get-Job -State Running
        if($RunningJobs.Count -eq 0)
        {
            $completed = $true
        }
        else
        {
            Start-Sleep -Seconds 5
        }
    }
}

############################################################################
#             Set SAP HANA instance level configuration parameters         #
############################################################################
Function ConfigureHANAPerformanceOptimization()
{
    Write-Host "Optimizing SAP HANA system wide configuration" -BackgroundColor White -ForegroundColor Black
    $hdbConnectionStringSystemDB = "Driver={HDBODBC};ServerNode=" + $DatabaseSystems[0] + ":3" `
    + $SAPHANAInstanceNumber + "13;UID=" + $DatabaseAdminUserName + ";PWD=" + $DatabaseAdminPassword +";"

    $hdbCommandDataBackupBufferSize = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('backup','data_backup_buffer_size') = '" + $DataBackupBufferSize + "' WITH RECONFIGURE"
    $hdbCommandLogBackupBufferSize = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('backup','log_backup_buffer_size') = '" + $LogBackupBufferSize + "' WITH RECONFIGURE"
    $hdbCommandLogBackupIntervalMode = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('backup','log_backup_interval_mode') = '" + $LogBackupIntervalMode + "' WITH RECONFIGURE"
    $hdbCommandLogBackupSize = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('backup','max_log_backup_size') = '" + $LogBackupSize + "' WITH RECONFIGURE"
    $hdbCommandNumCompletionQueues = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('fileio','num_completion_queues') = '" + $NumCompletionQueues + "' WITH RECONFIGURE"
    $hdbCommandNumberSubmitQueues = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('fileio','num_submit_queues') = '" + $NumberSubmitQueues + "' WITH RECONFIGURE"
    $hdbCommandCatalogBackup = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('persistence','basepath_catalogbackup') = '" + $CatalogBackup + "' WITH RECONFIGURE"
    $hdbCommandDataBackup = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('persistence','basepath_databackup') = '" + $DataBackup + "' WITH RECONFIGURE"
    $hdbCommandLogBackup = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('persistence','basepath_logbackup') = '" + $LogBackup + "' WITH RECONFIGURE"
    $hdbCommandLogBufferCount = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('persistence','log_buffer_count') = '" + $LogBufferCount + "' WITH RECONFIGURE"
    $hdbCommandLogBufferSize = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('persistence','log_buffer_size_kb') = '" + $LogBufferSize + "' WITH RECONFIGURE"
    $hdbCommandLogFormatSetpSize = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('persistence','log_format_step_size_kb') = '" + $LogFormatSetpSize + "' WITH RECONFIGURE"
    $hdbCommandLogSegmentSize = "ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('persistence','log_segment_size_mb') = '" + $LogSegmentSize + "' WITH RECONFIGURE"
    $hdbCommandLogPreformatSegmentCount ="ALTER SYSTEM ALTER CONFIGURATION ('global.ini','SYSTEM') SET ('persistence','log_preformat_segment_count') = '" + $LogPreformatSegmentCount + "' WITH RECONFIGURE"

    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandDataBackupBufferSize
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogBackupBufferSize
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogBackupIntervalMode
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogBackupSize
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandNumCompletionQueues
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandNumberSubmitQueues
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandCatalogBackup
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandDataBackup
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogBackup
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogBufferCount
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogBufferSize
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogFormatSetpSize
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogSegmentSize
    Get-ODBCData -hanaConnectionString $hdbConnectionStringSystemDB -hdbsql $hdbCommandLogPreformatSegmentCount
}

############################################################################
#   Populates the database and then runs the Simple DataEngine OLTP test   #
#                 Uses the WithoutIndexes schema template                  #
#                Runs for a single interation/sequence                     #
############################################################################
Function RunnerOLTP()
{
    [string[]]$TestTypes = "Simple"
    foreach($Test in $TestTypes)
    {
        [FLOAT]$DataSizePerUser = [INT]$SolutionSizeInGB / ($NumberOfUsersPerDatabase * $NumberOfTenants)
        $ObjectDefinition = "OLTP" + "_" + $NumberOfTenants.ToString() + "_Tenants_" + $NumberOfUsersPerDatabase.ToString() + `
        "_Users_" + $TableCount + "_TableCount_" + $ThreadCountPerRunner + "_Threads_" + $PercentageRandom + "_PercentRandom"
        for($Sequence = 1; $Sequence -le 1; $Sequence++)
        {
            if($Sequence -eq 1)
            {
                SetupDataEngineClientFolders -DataFileTemplatePath $DataEngineFileSourceLocation -NumberOfFolders $NumberOfFoldersPerTestClient `
                -TargetLocation $DataEngineFileTargetLocation -FolderPrefix $DataEngineFileFolderPrefix
                CreateDatabasesAndUsers 
                for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
                PopulateDatabase -ThreadCount $ThreadCountPerRunner -TableCount $TableCount -DataSizePerUser $DataSizePerUser -DataSizeUnit Gigabytes  `
                -SchemaType $SchemaTemplate -PercentColumnTables $PerctageColumntables -PercentPagedTables 0 -RotateDataFolder `
                -ObjectCategory ($ObjectDefinition = "Populate_" + $ObjectDefinition) -Sequence $Sequence 
                RunSavePoint
                for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                                                                       -ObjectName ($ObjectDefinition = "Populate_" + $DatabaseSystems[$dbs] + "_" + `
                                                                       $ObjectDefinition) -Sequence $Sequence }
                for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }                                                      
                PerformDataEngineTest -ThreadCount $ThreadCountPerRunner -TableCount $TableCount -DataSizePerUser $DataSizePerUser -DataSizeUnit Gigabytes  `
                -ChangePercentage $ChangeRate -TestType $Test -RotateDataFolder -DeferInitialWrite
                for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                    -ObjectName ($ObjectDefinition = $DatabaseSystems[$dbs] + "_" + $ObjectDefinition) -Sequence $Sequence }
            }
            else
            {
                for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
                PerformDataEngineTest -ThreadCount $ThreadCountPerRunner -TableCount $TableCount -DataSizePerUser $DataSizePerUser -DataSizeUnit Gigabytes -`
                -ChangePercentage $ChangeRate -TestType $Test -RotateDataFolder -DeferInitialWrite `
                -ObjectCategory $ObjectDefinition -Sequence $Sequence
                for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                    -ObjectName ($ObjectDefinition = $DatabaseSystems[$dbs] + "_" + $ObjectDefinition) -Sequence $Sequence }
            }
        }
    }
}

############################################################################
#        Populates the database, sets all of it to use warm data (NSE)     #
#               and then runs the DataEngine Query Test                    # 
#     Runs for 5 interations/sequences. Tables are unloaded from memory    #
#                 at the end of each interation/sequence                   #
############################################################################
Function RunnerOLAP_NSE()
{
    [FLOAT]$DataSizePerUser = [INT]$SolutionSizeInGB / ($NumberOfUsersPerDatabase * $NumberOfTenants)
    $ObjectDefinition = "OLAP_NSE" + "_" + $NumberOfTenants.ToString() + "_Tenants_" + $NumberOfUsersPerDatabase.ToString() + `
        "_Users_" + $TableCount + "_TableCount_" + $ThreadCountPerRunner + "_Threads_" + $PercentageRandom + "_PercentRandom"
    for($Sequence = 1; $Sequence -le 1; $Sequence++)
    {
        if($Sequence -eq 1)
        {
            SetupDataEngineClientFolders -DataFileTemplatePath $DataEngineFileSourceLocation -NumberOfFolders $NumberOfFoldersPerTestClient `
            -TargetLocation $DataEngineFileTargetLocation -FolderPrefix $DataEngineFileFolderPrefix
            CreateDatabasesAndUsers
            RunSavePoint
            SetNSEBufferCacheSize
            
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
            # Increases the overall size of the database by the scale factor DataSizeMulitplier
            for($i = 1; $i -le $DatabaseSizeMuliplier; $i++)
            {
                PopulateDatabase -ThreadCount $ThreadCountPerRunner -TableCount $TableCount -DataSizePerUser $DataSizePerUser -DataSizeUnit Gigabytes  `
                -SchemaType $SchemaTemplate -PercentColumnTables 100 -PercentPagedTables 100 -RotateDataFolder `
                -ObjectCategory ($ObjectDefinition = "Populate_" + $ObjectDefinition) -Sequence $Sequence 
                RunSavePoint
                DeltaMerge -TableCount $TableCount -PercentToUnload 100 
            }
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                -ObjectName ($ObjectDefinition = "Populate_" + $DatabaseSystems[$dbs] + "_" + `
                $ObjectDefinition) -Sequence $Sequence }
        }
        
        
        # This increases the rate at which each individual schema is qeried
        [INT]$AdjustedThreadCount = [INT]$ThreadCountPerRunner * $ThreadCountPerRunnerReadScale
        # The operation will continue until half of the database has been queried
        [FLOAT]$AdjustedDataSizePerUser = $DataSizePerUser /2
        for($QuerySequence = 1;$QuerySequence -le 5; $QuerySequence++)
        {
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
            PerformDataEngineOLAPTest -ThreadCount $AdjustedThreadCount -TableCount $TableCount -DataSizePerUser $AdjustedDataSizePerUser -DataSizeUnit Gigabytes `
            -ObjectCategory $ObjectDefinition -Sequence $Sequence 
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                -ObjectName ($ObjectDefinition = $DatabaseSystems[$dbs] + "_" + $ObjectDefinition) -Sequence $Sequence }
            DeltaMerge -TableCount $TableCount -PercentToUnload 100
        }
    }
}

############################################################################
#     Populates the database, sets all of it to use hot data (In-Memory)   #
#               and then runs the DataEngine Query Test                    # 
#                Runs for 5 interations/sequences                          #
############################################################################
Function RunnerOLAP_InMemory()
{
    [FLOAT]$DataSizePerUser = [INT]$SolutionSizeInGB / ($NumberOfUsersPerDatabase * $NumberOfTenants)
    $ObjectDefinition = "OLAP_InMemory" + "_" + $NumberOfTenants.ToString() + "_Tenants_" + $NumberOfUsersPerDatabase.ToString() + `
        "_Users_" + $TableCount + "_TableCount_" + $ThreadCountPerRunner + "_Threads_" + $PercentageRandom + "_PercentRandom"
    for($Sequence = 1; $Sequence -le 1; $Sequence++)
    {
        if($Sequence -eq 1)
        {
            SetupDataEngineWriterFolders -DataFilePath C:\DataEngineFiles -NumberOfWriters $NumberOfFoldersPerTestClient -TargetLocation C:\DataEngineFiles
            CreateDatabasesAndUsers
            RunSavePoint
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
            for($i = 1; $i -le 1; $i++)
            {
                PopulateDatabase -ThreadCount $ThreadCountPerRunner -TableCount $TableCount -DataSizePerUser $DataSizePerUser -DataSizeUnit Gigabytes `
                -SchemaType $SchemaTemplate -PercentColumnTables 100 -RotateDataFolder `
                -ObjectCategory ($ObjectDefinition = "Populate_" + $ObjectDefinition) -Sequence $Sequence 
                DeltaMerge -TableCount $TableCount -PercentToUnload 0
                RunSavePoint
            }
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                -ObjectName ($ObjectDefinition = "Populate_" + $DatabaseSystems[$dbs] + "_" + `
                $ObjectDefinition) -Sequence $Sequence }
            RunSavePoint
            Start-Sleep -Seconds 30
        }

         # This increases the rate at which each individual schema is qeried
         [INT]$AdjustedThreadCount = [INT]$ThreadCountPerRunner * $ThreadCountPerRunnerReadScale
         # The operation will continue until half of the database has been queried
         [FLOAT]$AdjustedDataSizePerUser = $DataSizePerUser /2
        for($QuerySequence = 1;$QuerySequence -le 5; $QuerySequence++)
        {
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
            PerformDataEngineOLAPTest -ThreadCount $AdjustedThreadCount -TableCount $TableCount -DataSizePerUser $AdjustedDataSizePerUser -DataSizeUnit Gigabytes `
            -ObjectCategory $ObjectDefinition -Sequence $QuerySequence
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                -ObjectName ($ObjectDefinition = $DatabaseSystems[$dbs] + "_" + $ObjectDefinition) -Sequence $QuerySequence }
        }
    }
}

############################################################################
#               This is where execution will take place.                   #
############################################################################


$Ports.Clear()
[INT]$NumberOfFoldersPerTestClient = ($NumberOfUsersPerDatabase * $NumberOfTenants) / $Clients.Count
for($Tenant = 1; $Tenant -le $NumberOfTenants; $Tenant++) {if($Tenant -eq 1) `
{$Port = 41}else {$Port = $Port + 3}$Ports.Add($Port) }
# Initially configures the instance level parameters.
ConfigureHANAPerformanceOptimization
# Runs the OLTP Test
RunnerOLTP
# Runs the NSE Test using the read query test
RunnerOLAP_NSE
# Runs the In-Memory read query test
RunnerOLAP_InMemory
 


