# OPC2PowerBI

This tool allows to connect to OPC UA and DA servers, poll for data and subscribe to events and write data in real time to MS PowerBI using the OData Feed data source.

It is simple to configure, the opc2powerbi.conf file is self explained, it must be put in the same folder as the exe file.

After configuring OPC servers and tags, create a new OData Feed data source in PowerBI and type http://127.0.0.1:8080/odata in the URL field.

Data refreshs must be forced manually. As far as I know OData data source does not support periodic updates (DirectQuery) in PowerBI.

The code is written in C# and it uses the h-OPC library for C#.

_Warning: The h-opc library is unmantained legacy and is known to have problems to enable OPC connections using certificates_.

Requires the .NET 6.0 runtime.

Executable binaries are available for download in the Releases section.

Need any help? Create an issue here or contact me.
Here is my LinkedIn contact: https://www.linkedin.com/in/ricardo-olsen/.

Example of config file:

    #port service_name
    8080, odata
	
	# OPC SERVERS

	#  OPC_UA_URL,                   READ_INTERVAL_IN_SECONDS,  SERVER_NAME, CERTIFICATE_FILE_PATH, CERTIFICATE_PASSWORD
	opc.tcp://opcuaserver.com:48484, 10,                        Server1,     certfile.cer,          password

	# OPC TAGS TO READ FROM THE SERVER

	# OPC_TAG_PATH                        ,TYPE(opt) ,SUBSCRIBE ,TAG(must be unique)
	ns=1;s=Countries.US.Queens.Latitude   ,    ,Y         ,US.Queens.Latitude                
	ns=1;s=Countries.US.Queens.Longitude  ,    ,N         ,US.Queens.Longitude    
	# ... repeat for more servers

    #  OPC_DA_URL,                           READ_INTERVAL_IN_SECONDS,  SERVER_NAME,
	opcda://127.0.0.1/Prosys.OPC.Simulation, 20,                        Server5
	# OPC_TAG_PATH                        ,TYPE(opt) ,SUBSCRIBE ,TAG(must be unique)
	Random.PsFloat1                       ,float     ,Y         ,
	Random.PsInteger1                     ,VT_I4     ,N         ,
	Random.PsBool1                        ,bool      ,N         ,
	Random.PsState1                       ,state     ,N         ,
	Random.PsDateTime1                    ,DateTime  ,N         ,

