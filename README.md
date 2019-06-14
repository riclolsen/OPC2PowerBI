# OPC2PowerBI

This tool allows to connect to OPC UA and DA servers, poll for data and subscribe to events and write data in real time to MS PowerBI using the OData Feed data source.

It is simple to configure, the opc2powerbi.conf file is self explained, it must be put in the same folder as the exe file.

After configuring OPC servers and tags, create a new OData Feed data source in PowerBI and type http://127.0.0.1:8080/odata in the URL field.

Data refreshs must be forced manually or configured as periodic (feature that will be available soon for PowerBI).

The code is written in C# and it uses the h-OPC library for C#.
Requires the .NET fremework 4.6 or later.

Executable binaries are available for download in the Releases section.

Need any help? Create an issue here or contact me.
Here is my LinkedIn contact: https://www.linkedin.com/in/ricardo-olsen/.

Example of config file:

    #port service_name
    8080, odata
	
	# OPC SERVERS

	#  OPC_UA_URL,                   READ_INTERVAL_IN_SECONDS,  SERVER_NAME
	opc.tcp://opcuaserver.com:48484, 10,                        Server1

	# OPC TAGS TO READ FROM THE SERVER

	# OPC_TAG_PATH                        ,TYPE      ,SUBSCRIBE ,TAG(must be unique)
	ns=1;s=Countries.US.Queens.Latitude   ,Double    ,Y         ,US.Queens.Latitude                
	ns=1;s=Countries.US.Queens.Longitude  ,Double    ,N         ,US.Queens.Longitude    
	# ... repeat for more servers
 