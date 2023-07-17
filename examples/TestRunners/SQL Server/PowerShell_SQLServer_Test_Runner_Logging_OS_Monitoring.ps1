############################################################################
#       Microsoft SQL D.O.E.S PowerShell Test Runner Script V 0.1          #
#--------------------------------------------------------------------------#
#   Ensure Install-Module -Name SqlServer has been run to ensure this      #
#                       function works as required                         #
############################################################################

############################################################################
#                         Script Wide Parameters                           #
############################################################################

$TestName = "MicrosoftSQL_OSMonitoring"
$SolutionSizeInGB = "128"

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
[string[]]$DatabaseSystems =""
[int]$PrimaryReplicaIndex = 0
$PortTarget = "1433"

############################################################################
#                      Database Specific Parameters                        #  
#            A database user with all priviledges must exist               #
############################################################################

$DatabaseUsername = ""
$DatabasePassword = ""
$DatabaseName = ""
$DatabaseTestUserPrefix = "user"
$DatabaseTestUserPassword = "Password"
$NumberOfUsersPerDatabase = 6
$ChangeRate = 10
$TableCount = 15
$ThreadCountPerRunner = 8
$ThreadCountPerRunnerReadScale = 1
$PercentageRandom = 30
$SchemaTemplate = "WithIndexes"

############################################################################
#                           Credential Parameters                          #
#   The username and password for PowerShell remoting must be set here     # 
#     WinRM needs to be setup on the remote systems set in $Clients        # 
#  WinRM can be setup on each client system using the following commands : #
#                         Quicksetup : winrm quickconfig                   #
#              Set trusted hosts for allowed connections :                 #
#        Set-Item WSMan:\localhost\Client\TrustedHosts -Value '*'          #
############################################################################

$RemoteWindowsUser = ""
$RemoteWindowsPassword = "" | ConvertTo-SecureString -AsPlainText -Force
$WinRMCredentials = New-Object -TypeName System.Management.Automation.PSCredential `
-ArgumentList $RemoteWindowsUser,$RemoteWindowsPassword

Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'

############################################################################
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
                                    Clear-DataEngine -DatabaseType MicrosoftSQL -Hostname $Hostname `
                                    -DatabaseName $Database -UserName $Username -Password $Password `
                                    -TableClearOperation Drop -TableAmplification $TableAmplification `
                                    -Port $Port -Verbose
                  }
    
    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    # Only the primary can execute data change commands
    $DatabaseSystemNumber = $PrimaryReplicaIndex
    for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
    {
        $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
        Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials -ConfigurationName $PSConfigurationName `
        -ScriptBlock $commandset -ArgumentList $DatabaseName, $DatabaseUser, $DatabaseTestUserPassword, `
        $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $PortTarget -AsJob
        $TestClientNumber++;
        if(!($TestClientNumber -le $TestClientMax)) { $TestClientNumber = 0 }
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
#                PopulateDatabase will populate the database               #
############################################################################
Function PopulateDatabase()
{
    Param(
        $ThreadCount,
        $TableCount,
        $DataSizePerUser,
        $DataSizeUnit,
        [Parameter(Mandatory=$false)]
        [Switch]$RotateDataFolder,
        $ObjectCategory, 
        $Sequence
    )
    Write-Host "Running initial database population" -BackgroundColor White -ForegroundColor Black
    $DataFolderNumber
    if($RotateDataFolder){ $DataFolderNumber = 1} else {$DataFolderNumber = 0}

    $commandset = {
        param($Database, $Username, $Password, $Hostname, $TableAmplification, $DataSize, $DataSizeUnit,
                $DataFolder, $ThreadCount, $SchemaType, $PercentageRandomVal, `
                $TestName, $ObjectCategory, $Sequence)
                    Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                    $ObjectName = $ObjectCategory + "_" + $Database + "_" + $Username
                    Add-DataEngine -DatabaseType MicrosoftSQL -DatabaseName $Database -UserName $Username `
                    -Password $Password -Hostname $Hostname -Amount $DataSize -Unit $DataSizeUnit -Folder $DataFolder `
                    -RandomPercentage $PercentageRandomVal -NumberOfThreads $ThreadCount -SchemaType $SchemaType `
                    -TableAmplification $TableAmplification -LogData `
                    -TestName $TestName -ObjectName $ObjectName -ObjectCategory $ObjectCategory -Sequence $Sequence
    }

    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    # Only the primary can execute data change commands
    $DatabaseSystemNumber = $PrimaryReplicaIndex
    $DataFolder = $DataEngineFileTargetLocation + $DataEngineFileFolderPrefix

    for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
    {
        $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
        if($DataFolderNumber -ne 0){$DataFolder = $DataEngineFileTargetLocation + $DataEngineFileFolderPrefix + $DataFolderNumber}
        Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials `
        -ConfigurationName $PSConfigurationName `
        -ScriptBlock $commandset -ArgumentList $DatabaseName, $DatabaseUser, $DatabaseTestUserPassword, `
        $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $DataSizePerUser, `
        $DataSizeUnit, $DataFolder, $ThreadCount, $SchemaTemplate, $PercentageRandom, $TestName, $ObjectCategory, $Sequence -AsJob
        $TestClientNumber++;
        if(!($TestClientNumber -le $TestClientMax)) 
        { 
            $TestClientNumber = 0 
            if($DataFolderNumber -ne 0)
            {if(!($DataFolderNumber -ne $NumberOfFoldersPerTestClient))
            {$DataFolderNumber++}}
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
            param($Database, $Username, $Password, $Hostname, $TableAmplification, $DataSize, $DataSizeUnit,
                    $ChangePercentage, $TestType, $DataFolder, $ThreadCount,
                    $PercentageRandomVal, $SchemaType, `
                    $TestName, $ObjectCategory, $Sequence)
                        Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                        $ObjectName = $ObjectCategory + "_" + $Database + "_" + $Username
                        Test-DataEngine -DatabaseType MicrosoftSQL -Hostname $Hostname -DatabaseName $Database `
                        -UserName $Username -Password $Password -Amount $DataSize -Unit $DataSizeUnit   `
                        -NumberOfThreads $ThreadCount -Folder $DataFolder -RandomPercentage $PercentageRandomVal `
                        -SchemaType $SchemaType -TableAmplification $TableAmplification -Testtype $TestType `
                        -ChangeRate $ChangePercentage -LogData `
                        -TestName $TestName -ObjectName $ObjectName -ObjectCategory $ObjectCategory -Sequence $Sequence
          }
      }
      else
      {
        $commandset = {
            param($Database, $Username, $Password, $Hostname, $TableAmplification, $DataSize, $DataSizeUnit,
                  $ChangePercentage, $TestType, $DataFolder, $ThreadCount,
                    $PercentageRandomVal, $SchemaType, `
                    $TestName, $ObjectCategory, $Sequence)
                        Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                        $ObjectName = $ObjectCategory + "_" + $Database + "_" + $Username
                        Test-DataEngine -DatabaseType MicrosoftSQL -Hostname $Hostname -DatabaseName $Database `
                        -UserName $Username -Password $Password -Amount $DataSize -Unit $DataSizeUnit `
                        -NumberOfThreads $ThreadCount -Folder $DataFolder -RandomPercentage $PercentageRandomVal `
                        -SchemaType $SchemaType -TableAmplification $TableAmplification -Testtype $TestType `
                        -ChangeRate $ChangePercentage -DeferInitialWrite -LogData `
                        -TestName $TestName -ObjectName $ObjectName -ObjectCategory $ObjectCategory -Sequence $Sequence
          }
      }

    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    # Only the primary can execute data change commands
    $DatabaseSystemNumber = $PrimaryReplicaIndex
    $DataFolder = $FolderPrefix

    for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
    {
        $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
        if($DataFolderNumber -ne 0){$DataFolder = $DataEngineFileTargetLocation + $DataEngineFileFolderPrefix + $DataFolderNumber}
        Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials -ConfigurationName $PSConfigurationName `
        -ScriptBlock $commandset -ArgumentList $DatabaseName, $DatabaseUser, $DatabaseTestUserPassword, `
        $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $DataSizePerUser, $DataSizeUnit, $ChangePercentage, `
        $TestType, $DataFolder, $ThreadCount, $PercentageRandom, $SchemaTemplate, $TestName, $ObjectCategory, $Sequence -AsJob

        $TestClientNumber++;

        if(!($TestClientNumber -le $TestClientMax)) 
        { 
            $TestClientNumber = 0 
            if($DataFolderNumber -ne 0)
            {if(!($DataFolderNumber -eq $NumberOfFoldersPerTestClient))
            {$DataFolderNumber++}}
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
        param($Database, $Username, $Password, $Hostname, $TableAmplification, $DataSize, $DataSizeUnit,
               $ThreadCount, $TestName, $ObjectCategory, $Sequence)
                    Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.PowerShell.dll'
                    $ObjectName = $ObjectCategory + "_" + $Database + "_" + $Username
                    Search-DataEngine -DatabaseType MicrosoftSQL -Hostname $Hostname -DatabaseName $Database `
                    -UserName $Username -Password $Password -Amount $DataSize -Unit $DataSizeUnit `
                    -NumberOfThreads $ThreadCount -TableAmplification $TableAmplification -QueryType UnionAll -LogData `
                    -TestName $TestName -ObjectName $ObjectName -ObjectCategory $ObjectCategory -Sequence $Sequence
    }

    $TestClientNumber = 0
    $TestClientMax = ($Clients.Count -1)
    # This will rotate read requests over all replicas
    $DatabaseSystemNumber = 0
    $DatabaseSystemsMax = ($DatabaseSystems.Count - 1)

    for($UserNumber = 1; $Usernumber -le $NumberOfUsersPerDatabase;$UserNumber++)
    {
        $DatabaseUser = $DatabaseTestUserPrefix + $UserNumber
        Invoke-Command -ComputerName $Clients[$TestClientNumber] -Credential $WinRMCredentials `
        -ConfigurationName $PSConfigurationName `
        -ScriptBlock $commandset -ArgumentList $DatabaseName, $DatabaseUser, $DatabaseTestUserPassword, `
        $DatabaseSystems[$DatabaseSystemNumber], $TableCount, $DataSizePerUser, `
        $DataSizeUnit, $ThreadCount, $TestName, $ObjectCategory, $Sequence -AsJob

        $TestClientNumber++;$DatabaseSystemNumber++;

        if(!($TestClientNumber -le $TestClientMax))  { $TestClientNumber = 0 }
        if(!($DatabaseSystemNumber -le $DatabaseSystemsMax)) { $DatabaseSystemNumber = 0 } 
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
#             Sets up the source data files on each D.O.E.S client.        #
#  Takes all of the files in a single folder location and then copies them #
#          to isolated folders for use by each DataEngine instance         #
############################################################################
Function SetupDataEngineClientFolders()
{
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
    Write-Host "Setting up client DataEngine folders" -BackgroundColor White -ForegroundColor Black
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
#     CreateUsersAndSchemas connects to the database instance, drops       #
#      any existing schemas with the same name, and then creates a         #
#   new databases. Users using the DatabaseUserPrefix are created in this  #
#                              new database                                #
#   Ensure Install-Module -Name SqlServer has been run to ensure this      #
#                       function works as required                         #
############################################################################
Function CreateUsersAndSchemas()
{

    for($UserCount = 1; $UserCount -le $NumberOfUsersPerDatabase; $UserCount++)
    {   
        try {
            ##Destroy schema
            $dropSchema = "DROP SCHEMA IF EXISTS " + $DatabaseTestUserPrefix + $UserCount.ToString() + "_schema;"
            Invoke-Sqlcmd -ServerInstance $DatabaseSystems[$PrimaryReplicaIndex] -Database $DatabaseName `
            -Username $DatabaseUsername -Password $DatabasePassword -TrustServerCertificate -Query $dropSchema
        } 
        catch {
            Write-Host "An Error Occured dropping the schema " $DatabaseTestUserPrefix $UserCount.ToString()"_schema" -ForegroundColor RED
        }
    }

    for($UserCount = 1; $UserCount -le $NumberOfUsersPerDatabase; $UserCount++)
    {   
        try {
            ##Destroy user
            $dropUser = "DROP USER IF EXISTS " + $DatabaseTestUserPrefix + $UserCount.ToString() + "c;"
            Invoke-Sqlcmd -ServerInstance $DatabaseSystems[$PrimaryReplicaIndex] -Database $DatabaseName `
            -Username $DatabaseUsername -Password $DatabasePassword -TrustServerCertificate -Query $dropUser
        } 
        catch {
            Write-Host "An Error Occured dropping the user " $UserPrefix $UserCount.ToString()"c" -ForegroundColor RED
        }
    }

    for($UserCount = 1; $UserCount -le $NumberOfUsersPerDatabase; $UserCount++)
    {   
        try {
            ##Destroy login
            $dropLogin = "DROP LOGIN " + $DatabaseTestUserPrefix.ToString() + $UserCount.ToString() + ";"
            Invoke-Sqlcmd -ServerInstance $DatabaseSystems[$PrimaryReplicaIndex] -Database $DatabaseName `
            -Username $DatabaseUsername -Password $DatabasePassword -TrustServerCertificate -Query $dropLogin
        } 
        catch {
            Write-Host "An Error Occured dropping the login "  $DatabaseTestUserPrefix.ToString() $UserCount.ToString() -ForegroundColor RED
        }
    }

    for($UserCount = 1; $UserCount -le $NumberOfUsersPerDatabase; $UserCount++)
    {   
        try {
            ##Create login
            $createLogin = "CREATE LOGIN " + $DatabaseTestUserPrefix + $UserCount.ToString() + " WITH PASSWORD = '" + $DatabaseTestUserPassword  + "';"
            Invoke-Sqlcmd -ServerInstance $DatabaseSystems[$PrimaryReplicaIndex] -Database $DatabaseName `
            -Username $DatabaseUsername -Password $DatabasePassword -TrustServerCertificate -Query $createLogin
        } 
        catch {
            Write-Host "An Error Occured creating the login " $DatabaseTestUserPrefix $UserCount.ToString()  -ForegroundColor RED
        }
    }

    for($UserCount = 1; $UserCount -le $NumberOfUsersPerDatabase; $UserCount++)
    {   
        try {
            ##Create schema
            $createSchema = "CREATE SCHEMA " + $DatabaseTestUserPrefix + $UserCount.ToString() + "_schema;"
            Invoke-Sqlcmd -ServerInstance $DatabaseSystems[$PrimaryReplicaIndex] -Database $DatabaseName `
            -Username $DatabaseUsername -Password $DatabasePassword -TrustServerCertificate -Query $createSchema
        } 
        catch {
            Write-Host "An Error Occured creating the schema "$DatabaseTestUserPrefix $UserCount.ToString()"_schema" -ForegroundColor RED
        }
    }

    for($UserCount = 1; $UserCount -le $NumberOfUsersPerDatabase; $UserCount++)
    {   
        try {
            ##Create User
            $createUser = "CREATE USER " + $DatabaseTestUserPrefix + $UserCount.ToString() + "c FOR LOGIN " + $DatabaseTestUserPrefix + $UserCount.ToString() + `
            " WITH DEFAULT_SCHEMA =" + $DatabaseTestUserPrefix + $UserCount.ToString() + "_schema;"
            Invoke-Sqlcmd -ServerInstance $DatabaseSystems[$PrimaryReplicaIndex] -Database $DatabaseName `
            -Username $DatabaseUsername -Password $DatabasePassword -TrustServerCertificate -Query $createUser
        } 
        catch {
            Write-Host "An Error Occured creating the user " $DatabaseTestUserPrefix $UserCount.ToString()"c" -ForegroundColor RED
        }
    }

    for($UserCount = 1; $UserCount -le $NumberOfUsersPerDatabase; $UserCount++)
    {   
        try {
            ##Alter Authorization On User
            $AlterAuthorization = "ALTER AUTHORIZATION ON SCHEMA::"+ $DatabaseTestUserPrefix + $UserCount.ToString() + "_schema to " + $DatabaseTestUserPrefix + $UserCount.ToString() + "c;"
            Invoke-Sqlcmd -ServerInstance $DatabaseSystems[$PrimaryReplicaIndex] -Database $DatabaseName `
            -Username $DatabaseUsername -Password $DatabasePassword -TrustServerCertificate -Query $AlterAuthorization
        } 
        catch {
            Write-Host "An Error Occured altering authorization on the schema " $DatabaseTestUserPrefix $UserCount.ToString()"_schema" -ForegroundColor RED
        }
    }

    for($UserCount = 1; $UserCount -le $NumberOfUsersPerDatabase; $UserCount++)
    {   
        try {
            ##Grant permissions to user
            $GrantPermissions = "GRANT CREATE SYNONYM, CREATE TABLE, CREATE TYPE, CREATE VIEW, ALTER, CONTROL, SELECT, INSERT, UPDATE, DELETE TO "`
             + $DatabaseTestUserPrefix + $UserCount.ToString() + "c;"
            Invoke-Sqlcmd -ServerInstance $DatabaseSystems[$PrimaryReplicaIndex] -Database $DatabaseName `
            -Username $DatabaseUsername -Password $DatabasePassword -TrustServerCertificate -Query $GrantPermissions
        } 
        catch {
            Write-Host "An Error Occured Granting permissions to  " $DatabaseTestUserPrefix $UserCount.ToString()"c" -ForegroundColor RED
        }
    }
}

############################################################################
#   Populates the database and then runs the Simple DataEngine OLTP test   #
#                Runs for a single interation/sequence                     #
############################################################################
Function RunnerOLTP()
{
    [string[]]$TestTypes = "Simple"
    foreach($Test in $TestTypes)
    {
        [FLOAT]$DataSizePerUser = [INT]$SolutionSizeInGB / $NumberOfUsersPerDatabase
        $ObjectDefinition = "OLTP" + "_" + $NumberOfUsersPerDatabase.ToString() + `
        "_Users_" + $TableCount + "_TableCount_" + $ThreadCountPerRunner + "_Threads_" + $PercentageRandom + "_PercentRandom"
        for($Sequence = 1; $Sequence -le 1; $Sequence++)
        {
            if($Sequence -eq 1)
            {
                DropDatabaseTables -TableCount $TableCount
                CreateUsersAndSchemas
                SetupDataEngineClientFolders -DataFileTemplatePath $DataEngineFileSourceLocation -NumberOfFolders $NumberOfFoldersPerTestClient `
                -TargetLocation $DataEngineFileTargetLocation -FolderPrefix $DataEngineFileFolderPrefix
                for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
                PopulateDatabase -ThreadCount $ThreadCountPerRunner -TableCount $TableCount -DataSizePerUser $DataSizePerUser -DataSizeUnit Gigabytes  `
                -RotateDataFolder -ObjectCategory ("Populate_" + $ObjectDefinition) -Sequence $Sequence 
                for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                    -ObjectName ("Populate_" + $DatabaseSystems[$dbs] + "_" + `
                    $ObjectDefinition) -Sequence $Sequence }
            }
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
            PerformDataEngineTest -ThreadCount $ThreadCountPerRunner -TableCount $TableCount -DataSizePerUser $DataSizePerUser -DataSizeUnit Gigabytes `
            -ChangePercentage $ChangeRate -TestType $Test -RotateDataFolder -DeferInitialWrite  -ObjectCategory $ObjectDefinition -Sequence $Sequence
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                -ObjectName ($DatabaseSystems[$dbs] + "_" + $ObjectDefinition) -Sequence $Sequence }
        }
    }
}

############################################################################
#    Populates the database and then runs the DataEngine Query Test        # 
#                     Runs for 5 interations/sequences.                    #
############################################################################
Function RunnerOLAP()
{
    [FLOAT]$DataSizePerUser = [INT]$SolutionSizeInGB / $NumberOfUsersPerDatabase
    $ObjectDefinition = "OLAP" + "_" + $NumberOfUsersPerDatabase.ToString() + `
    "_Users_" + $TableCount + "_TableCount_" + $ThreadCountPerRunner + "_Threads_" + $PercentageRandom + "_PercentRandom"
    for($Sequence = 1; $Sequence -le 1; $Sequence++)
    {
        if($Sequence -eq 1)
        {
            DropDatabaseTables -TableCount $TableCount
            CreateUsersAndSchemas
            SetupDataEngineClientFolders -DataFileTemplatePath $DataEngineFileSourceLocation -NumberOfFolders $NumberOfFoldersPerTestClient `
            -TargetLocation $DataEngineFileTargetLocation -FolderPrefix $DataEngineFileFolderPrefix
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Start-PlatformEngine -Hostname $DatabaseSystems[$dbs] -CollectionType UntilNotified }
            PopulateDatabase -ThreadCount $ThreadCountPerRunner -TableCount $TableCount -DataSizePerUser $DataSizePerUser -DataSizeUnit Gigabytes  `
            -RotateDataFolder -ObjectCategory ("Populate_" + $ObjectDefinition) -Sequence $Sequence    
            for($dbs = 0; $dbs -lt $DatabaseSystems.Count;$dbs++) {Stop-PlatformEngine -Hostname $DatabaseSystems[$dbs] -LogData -TestName $TestName `
                -ObjectName ("Populate_" + $DatabaseSystems[$dbs] + "_" + `
                $ObjectDefinition) -Sequence $Sequence }    
        }
        Start-Sleep -Seconds 60
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
                -ObjectName ($DatabaseSystems[$dbs] + "_" + $ObjectDefinition) -Sequence $QuerySequence }
        }
    }
}

############################################################################
#               This is where execution will take place.                   #
############################################################################

[INT]$NumberOfFoldersPerTestClient = $NumberOfUsersPerDatabase / $Clients.Count
# Runs the OLTP Test
RunnerOLTP
# Runs the OLAP Test
RunnerOLAP