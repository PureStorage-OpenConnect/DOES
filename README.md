# Introduction 
The Diverse Object Evaluation Suite (D.O.E.S) is a set of tools targeted at evaluating a number of databases using a single easy to use command interface through PowerShell Cmdlets or a command line terminal

- Microsoft SQL Server 
- Oracle Database 
- SAP HANA 
- MySQL
- MariaDB
- PostgreSQL
- MongoDB

**This version of D.O.E.S is only available to Pure Storage employees.** 

# Getting Started
To install the latest version of D.O.E.S simple download the Microsoft Windows installer from the build artefacts directory :

https://github.com/PureStorage-OpenConnect/D.O.E.S/raw/master/Build%20Artefacts/DOES_Installer.msi

To correctly operate this set of tools on any system PowerShell Core must be installed.

PowerShell 7.2.1 or above is recommended when using D.O.E.S. 

To get started using D.O.E.S execute the installer and then open the PowerShell Core command window , then run the following command :

```powershell
Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES_Powershell.dll'
```

The following Cmdlet's will now be available :

| Name              | Function                                                     |
| ----------------- | ------------------------------------------------------------ |
| Add-DataEngine    | Inserts web page data into the relevant database objects. Can use either wikipedia or data files as the source for new data. |
| Clear-DataEngine  | Drops or truncates database objects.                         |
| Export-DataEngine | Extracts base web page data from the database and exports them to data files for reuse. |
| Merge-DataEngine  | SAP HANA function used to perfom delta-merge operations and set object characteristics. |
| New-SequenceStat  | Creates a new analytic to identify how long various test operations have taken. |
| New-TestObject    | Creates a new Test object entry for further analytics to be created against. |
| Remove-DataEngine | Deletes data from database objects.                          |
| Search-DataEngine | Queries data in database objects.                            |
| Test-DataEngine   | Runs a range of test options which encompass create , insert, update, delete , search and vendor specific behaviour. |
| Update-DataEngine | Updates data in database objects.                            |
| Write-DataEngine  | Writes a single entry to a database table every 100ms until the operation is stopped. |

