# Diverse Object Evaluation Suite (D.O.E.S)
The Diverse Object Evaluation Suite (D.O.E.S) is a cross platform (Windows and Linux) set of tools that provide the following abilities:
- Dynamically create database objects (Tables, Indexes , Sequences , Triggers, etc).
- Populate database objects with different forms of data.
- Change the data in database objects through removal and change-in-place mechanics. 
- Alter database objects to create vendor specific work patterns. 
- Perform read only operations on database objects. 
- Conduct non-industry-standard test scenarios. 
- Monitor a single or multiple operating platforms. 
- Collate database and platform monitoring data and present analysis in the form of trends or benchmarking. 

D.O.E.S works with existing terminals such as Command Prompt (Windows), Powershell (Windows and Linux), and Bash (Linux) for detailed information on the suite the see the [overview](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/Overview.md). 
# Installation Guide

## PowerShell
To correctly operate D.O.E.S using PowerShell, [PowerShell](https://github.com/PowerShell/PowerShell) must be installed.
PowerShell 7.3.4 or above is recommended.

## Linux 
Depending on OS and revision the RPM package may require that the libopenssl1 package be installed. This can be done by executing the following

`rpm -ivh https://rpmfind.net/linux/opensuse/distribution/leap/15.4/repo/oss/x86_64/libopenssl1_0_0-1.0.2p-3.49.1.x86_64.rpm` 

## Installation
*NOTE: The packages distributed in this repository do not contain the full functionality set out in the code for SAP HANA and Oracle Database.*
*This is because the Oracle Database ([Oracle.ManagedDataAccess.dll](https://www.nuget.org/packages/Oracle.ManagedDataAccess)) and [SAP HANA client libraries](https://tools.hana.ondemand.com/#hanatools) have not been included due to licensing restrictions. To make this functionality avalabile the toolset will need to be built with the relevant libraries included.*

*The PlatformEngine.Client is not included in the packages distributed in this repository*

D.O.E.S. can be downloaded and installed for any of the following platforms:
|Platform|Installation package|
|--------- |------------------------- |
|Windows (x64)| [DOES.Setup.Public.msi](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/build/ms-windows/DOES.Setup.Public.msi)
|RHEL 7,8,9, Centos 7, Rocky Linux 8.5+, Fedora 32+, SLES 12+, SLES 15+, openSUSE Leap 15+|[purestorage_does_public-3.3.4-1.x86_64.rpm](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/build/rpm/purestorage_does_public-3.3.4-1.x86_64.rpm)

## Getting Started 
To interface with and use the functions provided by D.O.E.S either the [PowerShell Cmdlet](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/reference/PowerShell.md) or [D.O.E.S.Cli functions](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/reference/cli.md) command line interfaces can be used. 

### DOES.PowerShell
To get started using D.O.E.S, execute the installer and then open the PowerShell command interface. 
To make the DOES.PowerShell commands available for use, execute the following command:

**Microsoft Windows** 
`Import-Module 'C:\Program Files\Pure Storage\D.O.E.S\DOES.Powershell.dll'`

**Linux** 
`Import-Module /opt/purestorage/does/DOES.Powershell.dll`

To see the list of availabile PowerShell CmdLets execute the following command: 
`Get-Command -Module DOES.PowerShell`

### DOES.Cli
In Microsoft Windows DOES.Cli can be run by executing the following command
`(C:\Program Files\Pure Storage\D.O.E.S\DOES.Cli.exe)`

Using the RPM package in Linux will automatically add the path to DOES.Cli to the environment variables
To run DOES.Cli execute the following command
`DOES.Cli`

# User Guides
The following topics guide you on how to use D.O.E.S. with different database types:
- [Using D.O.E.S. with Microsoft SQL Server](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/database-guides/ms-sql.md)
- [Using D.O.E.S with Oracle Database](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/database-guides/oracle.md)
- [Using D.O.E.S with SAP HANA](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/database-guides/saphana.md)
- [Using D.O.E.S with MySQL](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/database-guides/mysql.md)
- [Using D.O.E.S with MariaDB](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/database-guides/mariadb.md)
- [Using D.O.E.S with PostgreSQL ](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/database-guides/pgsql.md)
- [Using D.O.E.S with MongoDB](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/database-guides/mongodb.md)

# Command Reference Guide
The following pages contain a complete list of commands for Powershell and CLI for reference: 
- [D.O.E.S Powershell Command Reference](https://github.com/PureStorage-OpenConnect/DOES/tree/3.3.4/docs/reference)
- [D.O.E.S CLI Command Reference](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/reference/cli.md)

# Usage Examples

This repository includes two different usage scenario [examples](https://github.com/PureStorage-OpenConnect/DOES/tree/3.3.4/examples) for D.O.E.S with various databases. 
These examples are all written in PowerShell. 

[The first set of examples](https://github.com/PureStorage-OpenConnect/DOES/tree/3.3.4/examples/OneLineExec) are focused on how a single instance of D.O.E.S can be used to interact with a database in a number of ways. 

[The second set of examples](https://github.com/PureStorage-OpenConnect/DOES/tree/3.3.4/examples/TestRunners) are focused on how multiple instances of D.O.E.S can be coordinated on different systems using PowerShell remoting to run Online Transaction Processing (OLTP) or Online Analytical Processing (OLAP) workloads. 

Several pre-created Data Engine files are includes in [the examples folder](https://github.com/PureStorage-OpenConnect/DOES/tree/3.3.4/examples/DataEngineFiles). 