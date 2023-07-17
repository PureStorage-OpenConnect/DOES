# D.O.E.S Overview
The Diverse Object Evaluation Suite (D.O.E.S) is a set of tools used to provide insight into how databases and operating platforms behave under different user defined scenarios. 

D.O.E.S can be divided into three (3) distinct components :

- The Data Engine -  Interacts dynamically with databases by creating , populating , altering and deleting database objects. 
- The Platform Engine - Interacts with operating systems to gather resource usage information. 
- The Analysis Engine - Gathers and persists behavioral trends collected from the DataEngine and PlatformEngine components. Also creates commonality between tests run on databases and the associated resource consumption for the operating platform. 

![Intersecting Engines](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Intersect.jpeg)

The components are accessed through command line controls inside [a command line utility (DOES.Cli)](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/reference/cli.md) or exposed [PowerShell Cmdlet's](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/reference/PowerShell.md). All components are cross platform supporting Microsoft Windows and Linux operating systems. 

## D.O.E.S Data Engine 
Almost everything with the DataEngine is performed with the intent of working on a user specified amount of data.

Each database vendor will have its own set of **schema templates**.
A schema template allows for further customization of how the objects in the database are created and if they will use any vendor specific functional capabilities. 

There is an important Data Engine concept to know about - **Table Amplification**.
Table Amplification allows for a level of granular control as to how many tables will be created in a schema. A **schema template** is then used to manufacture data to the Table Amplification specification. 
This allows for flexibility in multiple areas :
- Databases in a few very dense/deep tables/objects
- Databases spread across many tables/objects in a shallow manner.
- Performing operations (delete , updates , new data , etc) on a specific subset of database objects

### Architecture 
The foundation of all Data Engine workings is a web page. 
The following sections will highlight how a schema in a database is populated.

#### Step 1. Data Retrieval 
A web request sent to retrieve a random Wikipedia page. This web requests returns data in the form of a URL, HTML and Web Page Headers. 

![](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Data_Step1.jpeg) 

#### Step 2. Data Preparation 
##### Catalog and check 
1. The URL of the returned web page is cataloged as a unique key. 
2. A SHA1 hash of the web page URL is created. Adds More unique data, but also a way to do integrity checking. 

![Data retrieval](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Data_Step2.jpeg)

##### Generate encoding from HTML and amplify using table amplification 
The Table amplification value is a constant through the lifecycle of an operation. 
Depending on the schema template selected data is manufactured in one of two methods:

![Encoding generation](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Data_Step2_2.jpeg)

- **Binary Encoding (LOB Schema's)**
A database large object (LOB) is typically a field which can hold large amounts of data in character or binary form. 
Using the various encodings (Unicode, ASCII, IBM-Latin-1 and UTF32) a number of binary values are computed. From these values a corresponding Base64 string value is created. 

- **Characterization**
In a normal schema type all string data is set to use Unicode. 

![Data model](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Data_Step2_3.jpeg)

##### Randomization 
For any of the schema types data is randomized up to a percentage of total data. This allows for flexibility with regards to how compression and deduplication will behave. 

##### Data Size 
All of this data is then computed to establish how data could be inserted from this web page. 

#### Step 3. Data Creation and Manipulation 
##### Create objects
The database objects are always checked if they exist first , and created if any are missing. The schema template and table amplification are used to manufacture these objects. 
> Minimum number of tables is six (6).
> Maximum number of tables per schema is one thousand and thirty (1030).

![Create objects](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Data_Step3.jpeg)

##### Insert Data using transactional isolation 
Each web page is inserted as a part of a transaction. This will include all the tables in the schema. The data mapping to database object looks like the following :
- URL, HTML, Computed Lengths and statistics -> WebPages
- Encoded HTML Data -> Encoding table(s)
- Web Page Headers -> WebPageHeaders
- Uniqueness is enforced using the web page URL. 

Data will be populated until the data engine is satisfied the amount of data which has been inserted is the same as the amount of data requested

![Alt text](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Data_Step3_1.jpeg)

##### Using data files instead of web requests 
It is possible to create data files with the minimum required data to completely recreate a database. These can be created after using using the add function or cmdlet by using the equivalent export function or cmdlet.

![Data files](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Data_Step3_Files.jpeg)

##### Database operations 
With each web page being successfully inserted into the database a PageID is assigned as a primary key. This property can be used to traverse the database for different operations. For the search function or cmdlet , update function or cmdlet and delete function and cmdlet a list of PageID's is retrieved and then actions are taken against them. 

##### Test Scenarios 
The DataEngine can also be used to perform various test scenarios. This achieved through the use of the test function or cmdlet. 
There are three (3) test types : Simple , Advanced and Complex. Each test is designed combine operations on data (new, changing and deleting) with additional read operations and vendor platform operations.  

##### Scaling and Performance
There are a number of factors to consider when attempting to increase scale and performance :

###### Scaling for a single D.O.E.S command/instance  
A single data engine command will only work against a single instance for the user provided. Additional scale can be achieved by increasing the number of parallel operations created for the function(-- numberofthreads) or cmdlet (-NumberOfThreads). This will eventually encounter limits for how much data can be inserted into a single schema at once. 

###### Scaling using multiple users 
Once the limit for a single command/instance has been reached it is possible to create additional users and execute D.O.E.S command/instances against them. This can be done programmatically or manually. PowerShell remoting, Ansible or Salt are some automation frameworks that this could be done from.
> If user a local folder with Engine-Oil files it is important that each user points to their own files. This is good practice to ensure duplication in the database does not occur.

###### Additional Considerations
The DataEngine interacts with databases using client-server architecture. As such here are some areas to consider when running D.O.E.S DataEngine commands :
- CPU of the client system
- Network capabilities between client (D.O.E.S interface) and server (Database)

## D.O.E.S Platform Engine
The PlatformEngine is a way in which the operating platform of systems being evaluated can be monitored to identify bottlenecks and observe component behavior. A client service agent is installed on the relevant system and a function or cmdlet is then used to interact with it use REST API calls. 

### Architecture 
The Platform Engine is divided into two sections :

- The PlatformEngine_Client runs as a service in both Microsoft Windows and Linux. The service exposes a REST API on port 53637. 
- Client requests from the DOES.PowerShell command set or DOES.Cli function list. 

### How to use 
#### Connecting to service agents and sending monitoring requests (Start monitoring) 
The request will include a **CollectionType** instruction. There are three (3) collection types :
- **Point-In-Time** - Respond with what the operating platforms resource usage is for a single point in time when the request is recieved. 
- **Duration** - Monitor the operating platform for a set period of time. 
- **UntilNotified** - Monitor the operating platform until a connection is made to stop the monitoring operation.

> The interval of monitoring data points can also be set through the use of an Interval.

![Platform Engine Start](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Platform_Go.jpg)

#### Connecting to service agents and sending sending monitoring requests (Stop Monitoring) 
When using a **Duration** or **UntilNotified** monitoring request it is necessary to connect to the client and stop the monitoring (UntilNotified) and then retrieve the data points.
(**Duration only** : only once the monitoring operation is complete for the duration).

![Platform Engine Stop](https://github.com/PureStorage-OpenConnect/DOES/blob/3.3.4/docs/images/Platform_End.jpg)

Once the data points are retrieved they can be persisted to the Analsys Engine. 

## D.O.E.S Analysis Engine
The AnalysisEngine unifies the PlatformEngine and DataEngine by providing for a common way in which to collect and analyze data. At the completion of DataEngine or PlatformEngine operations it is possible to persist analytics collected to a relational database. 

### Architecture 
The AnalysisEngine assumes that any scenario is constructed using the following  methodology :

#### Test 
The high level description of what is being evaluated. The same test can be run twice as long as it has a different code revision or attempt. 

#### Object 
A component which is being evaluated. An object can only be apart of one test. Multiple objects can be created for the same component using an **Object Catagory** (Multiple Objects with the same Object Catagory and Test can have their metrics combined into a single value).

##### Sequence 
A value indicating the point in time being evaluated. For scenarios which repeat the same test in an iterative loop this value serves as a delimiter between data points being collected. **An object can have multiple Sequences.**

##### Metric 
This is the analytic collected from the DataEngine or PlatformEngine. It can take a the form of a rolled up final report , a trend to analyze or a more granular element of the scenario.  

The following Metrics are available :

- **SequenceData** - Can be used to collect the point in time at which  a scenario has been started and stopped. Very useful for establishing the effort a scenario will take (using historical data points)
- **Interim DataEngine Analytics** - The trend of how a Data Engine operation behaved at each 1 second interval. 
- **Interim DataEngine Thread Analytics** - The trend of how each Data Engine parallel operation behaved at each 1 second interval. 
- **DataEngine Thread Analytics** - The final result for the operation and how each parallel operation behaved in it. 
- **DataEngine Rolled Up Analytics**- The final result for the operation. 
- **Data Engine Final Reports** - A textual recording of the final report provided at the end of any DataEngine operation. 
- **Windows Resource Analytics** - A trend of how a Microsoft Windows system with the PlatformEngine_Client behaved while a scenario was running. 
- **Linux Resource Analytics**  - A trend of how a Linux system with the PlatformEngine_Client behaved while a scenario was running. 

