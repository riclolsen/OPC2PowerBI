/*
OPC2PowerBI
OPC UA / DA --> Power BI (OData v4 JSON)

	Copyright 2019-2023 - Ricardo L. Olsen
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

using Hylasoft.Opc.Ua;
using Hylasoft.Opc.Da;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Text;
using System.Web;

namespace OPC2PowerBI
{
    class Program
    {
        static public string Version = "OPC2PowerBI Version 0.5 - Copyright 2019-2023 - Ricardo L. Olsen";
        static public string ConfigFile = "opc2powerbi.conf";
        static public bool logevent = true;
        static public bool logread = true;
        static public bool logcommand = true;

        // static public ConcurrentQueue<OPC_Value> OPCDataQueue = new ConcurrentQueue<OPC_Value>();
        static public ConcurrentDictionary<string, OPC_Value> MapValues = new ConcurrentDictionary<string, OPC_Value>();

        public const int bufSize = 8 * 1024;

        public class State
        {
            public byte[] buffer = new byte[bufSize];
        }

        public struct OPC_entry
        {
            public string opc_server_name;
            public string opc_path;
            public string tag;
            public string opc_type;
            public bool subscribe;
        }

        public struct OPC_server
        {
            public string opc_url;
            public string opc_server_name;
            public string certificate_file;
            public string certificate_password;
            public List<OPC_entry> entries;
            public int read_period;
            public int is_opc_ua;
        }

        public struct OPC_Value
        {
            public OPC_entry opc_entry;
            public double double_value;
            public string string_value;
            public bool bool_value;
            public DateTime sourceTimestamp;
            public DateTime serverTimestamp;
            public Hylasoft.Opc.Common.Quality quality;
        }

        static void ProcessUa(String URI, List<OPC_entry> entries, int readperiod, string appname, string certfile, string cert_password)
        {
            CultureInfo ci = new CultureInfo("en-US");
            Thread.CurrentThread.CurrentCulture = ci;
            Thread.CurrentThread.CurrentUICulture = ci;

            do
            {
                try
                {
                    var options = new UaClientOptions
                    {
                        SessionTimeout = 60000,
                        SessionName = "OPC2PowerBI client (h-opc)",
                        ApplicationName = appname
                    };
                    if (certfile != "")
                    {
                        if (File.Exists(certfile))
                        {
                            options.ApplicationCertificate = 
                                new System.Security.Cryptography.X509Certificates.X509Certificate2(
                                        certfile, 
                                        cert_password, 
                                        System.Security.Cryptography.X509Certificates.X509KeyStorageFlags.MachineKeySet
                                        );
                        }
                        else
                            Console.WriteLine("Certificate file not found: " + certfile);
                    }

                    using (var client = new UaClient(new Uri(URI), options))
                    {
                        Console.WriteLine("Connecting UA " + URI);
                        client.Connect();
                                         
                        Console.WriteLine("UA " + URI + " " + client.Status) ;

                        foreach (OPC_entry entry in entries)
                        {
                            if (!entry.subscribe)
                                continue;

                            string stype;
                            if (entry.opc_type == "")
                                stype = client.GetDataType(entry.opc_path).ToString().ToLower();
                            else
                                stype = entry.opc_type.ToLower();

                            switch (stype)
                            {
                                case "bool":
                                case "boolean":
                                case "system.boolean":
                                    {
                                        client.Monitor<Boolean>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            bool bval = readEvent.Value;
                                            string sval = bval.ToString().ToLower();
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = readEvent.Value ? 1.0 : 0.0,
                                                bool_value = readEvent.Value,
                                                string_value = sval,
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + sval);
                                        });
                                    }
                                    break;
                                case "float":
                                case "single":
                                case "system.single":
                                    {
                                        client.Monitor<Single>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = readEvent.Value,
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")));
                                        });
                                    }
                                    break;
                                case "double":
                                case "system.double":
                                    {
                                        client.Monitor<Double>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = readEvent.Value,
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")));
                                        });
                                    }
                                    break;
                                case "system.decimal":
                                    {
                                        client.Monitor<Decimal>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            Decimal dval = readEvent.Value;
                                            string sval = dval.ToString("G", CultureInfo.CreateSpecificCulture("en-US"));
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + sval);
                                        });
                                    }
                                    break;
                                case "byte":
                                case "system.byte":
                                    {
                                        client.Monitor<Byte>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "sbyte":
                                case "system.sbyte":
                                    {
                                        client.Monitor<SByte>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "int16":
                                case "system.int16":
                                    {
                                        client.Monitor<Int16>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "uint16":
                                case "system.uint16":
                                    {
                                        client.Monitor<UInt16>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "integer":
                                case "int32":
                                case "system.int32":
                                    {
                                        client.Monitor<Int32>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "statuscode":
                                case "uint32":
                                case "system.uint32":
                                    {
                                        client.Monitor<UInt32>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "int64":
                                case "system.int64":
                                    {
                                        client.Monitor<Int64>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "uint64":
                                case "system.uint64":
                                    {
                                        client.Monitor<UInt64>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "expandednodeid":
                                case "guid":
                                case "nodeid":
                                case "qualifiedname":
                                case "localizedtext":
                                case "string":
                                case "system.string":
                                case "xmlelement":
                                    {
                                        client.Monitor<string>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            double dblval = 0.0;
                                            try
                                            {
                                                dblval = Convert.ToDouble(readEvent.Value);
                                            }
                                            catch (Exception)
                                            {
                                                dblval = 0.0;
                                            }
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = dblval,
                                                bool_value = dblval != 0,
                                                string_value = readEvent.Value,
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "time":
                                case "date":
                                case "datetime":
                                case "system.datetime":
                                    {
                                        client.Monitor<DateTime>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value.Ticks),
                                                bool_value = false,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value.ToString());
                                        });
                                    }
                                    break;
                            }
                        }

                        do
                        {
                            foreach (OPC_entry entry in entries)
                            {
                                string stype;
                                if (entry.opc_type == "")
                                    // this function fails for opc DA returning only "System.Int16", so plese define the type manually
                                    stype = client.GetDataType(entry.opc_path).ToString().ToLower();
                                else
                                    stype = entry.opc_type.ToLower();

                                switch (stype)
                                {
                                    case "bool":
                                    case "boolean":
                                    case "system.boolean":
                                        {
                                            var task = client.ReadAsync<Boolean>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = task.Result.Value ? 1.0 : 0.0;
                                            ov.bool_value = task.Result.Value;
                                            ov.string_value = task.Result.Value.ToString().ToLower();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "float":
                                    case "single":
                                    case "system.single":
                                        {
                                            var task = client.ReadAsync<Single>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = task.Result.Value;
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US"));
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "double":
                                    case "system.double":
                                        {
                                            var task = client.ReadAsync<Double>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = task.Result.Value;
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US"));
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "system.decimal":
                                        {
                                            var task = client.ReadAsync<Decimal>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US"));
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "byte":
                                    case "system.byte":
                                        {
                                            var task = client.ReadAsync<Byte>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "sbyte":
                                    case "system.sbyte":
                                        {
                                            var task = client.ReadAsync<SByte>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "int16":
                                    case "system.int16":
                                        {
                                            var task = client.ReadAsync<Int16>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "uint16":
                                    case "system.uint16":
                                        {
                                            var task = client.ReadAsync<UInt16>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "integer":
                                    case "int32":
                                    case "system.int32":
                                        {
                                            var task = client.ReadAsync<Int32>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "statuscode":
                                    case "uint32":
                                    case "system.uint32":
                                        {
                                            var task = client.ReadAsync<UInt32>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "int64":
                                    case "system.int64":
                                        {
                                            var task = client.ReadAsync<Int64>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "uint64":
                                    case "system.uint64":
                                        {
                                            var task = client.ReadAsync<UInt64>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "expandednodeid":
                                    case "guid":
                                    case "nodeid":
                                    case "qualifiedname":
                                    case "localizedtext":
                                    case "string":
                                    case "system.string":
                                    case "xmlelement":
                                        {
                                            var task = client.ReadAsync<string>(entry.opc_path);
                                            task.Wait();
                                            double dblval = 0.0;
                                            try
                                            {
                                                dblval = Convert.ToDouble(task.Result.Value);
                                            }
                                            catch (Exception)
                                            {
                                                dblval = 0.0;
                                            }
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = dblval;
                                            ov.bool_value = dblval != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "time":
                                    case "date":
                                    case "datetime":
                                    case "system.datetime":
                                        {
                                            var task = client.ReadAsync<DateTime>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            DateTime dt = task.Result.Value;
                                            ov.double_value = Convert.ToDouble(task.Result.Value.Ticks);
                                            ov.bool_value = false;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    default:
                                        if (logread) Console.WriteLine("READ UNSUPPORTED TYPE: " + appname + " " + entry.opc_path + " " + stype);
                                        break;
                                }
                            }

                            System.Threading.Thread.Sleep(1000 * readperiod);

                        } while (true);
                    }
                }
                catch (Exception e)
                {
                    // EXCEPTION HANDLER
                    Console.WriteLine("Exception UA " + appname);
                    Console.WriteLine(e);
                    System.Threading.Thread.Sleep(15000);
                }
            } while (true);
        }

        static void ProcessDa(String URI, List<OPC_entry> entries, int readperiod, string appname)
        {
            CultureInfo ci = new CultureInfo("en-US");
            Thread.CurrentThread.CurrentCulture = ci;
            Thread.CurrentThread.CurrentUICulture = ci;

            do
            {
                try
                {
                    using (var client = new DaClient(new Uri(URI)))
                    {
                        client.Connect();
                        Console.WriteLine("Connect DA " + URI);

                        foreach (OPC_entry entry in entries)
                        {
                            if (!entry.subscribe)
                                continue;
                            string stype;
                            if (entry.opc_type == "")
                                // this function fails for opc DA returning only "System.Int16", so plese define the type manually
                                stype = client.GetDataType(entry.opc_path).ToString().ToLower();
                            else
                                stype = entry.opc_type.ToLower();

                            switch (stype)
                            {
                                case "vt_bool":
                                case "bool":
                                case "system.boolean":
                                    {
                                        client.Monitor<Boolean>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            bool bval = readEvent.Value;
                                            string sval = bval.ToString().ToLower();
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = readEvent.Value ? 1.0 : 0.0,
                                                bool_value = readEvent.Value,
                                                string_value = sval,
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + sval);
                                        });
                                    }
                                    break;
                                case "vt_r4":
                                case "single":
                                case "system.single":
                                    {
                                        client.Monitor<Single>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = readEvent.Value,
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")));
                                        });
                                    }
                                    break;
                                case "vt_r8":
                                case "float":
                                case "double":
                                case "system.double":
                                    {
                                        client.Monitor<Double>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = readEvent.Value,
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US")));
                                        });
                                    }
                                    break;
                                case "vt_i1":
                                case "byte":
                                case "system.byte":
                                    {
                                        client.Monitor<Byte>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "vt_i2":
                                case "int16":
                                case "system.int16":
                                    {
                                        client.Monitor<Int16>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "state":
                                case "vt_i4":
                                case "int32":
                                case "integer":
                                case "system.int32":
                                    {
                                        client.Monitor<Int32>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value),
                                                bool_value = readEvent.Value != 0,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "vt_bstr":
                                case "string":
                                case "system.string":
                                    {
                                        client.Monitor<string>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            double dblval = 0.0;
                                            try
                                            {
                                                dblval = Convert.ToDouble(readEvent.Value);
                                            }
                                            catch (Exception)
                                            {
                                                dblval = 0.0;
                                            }
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = dblval,
                                                bool_value = dblval != 0,
                                                string_value = readEvent.Value,
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value);
                                        });
                                    }
                                    break;
                                case "vt_date":
                                case "date":
                                case "datetime":
                                case "system.datetime":
                                    {
                                        client.Monitor<DateTime>(entry.opc_path, (readEvent, unsubscribe) =>
                                        {
                                            OPC_Value ov = new OPC_Value()
                                            {
                                                opc_entry = entry,
                                                double_value = Convert.ToDouble(readEvent.Value.Ticks),
                                                bool_value = false,
                                                string_value = readEvent.Value.ToString(),
                                                sourceTimestamp = readEvent.SourceTimestamp,
                                                serverTimestamp = readEvent.ServerTimestamp,
                                                quality = readEvent.Quality
                                            };
                                            MapValues[entry.tag] = ov;
                                            if (logevent) Console.WriteLine("EVENT " + appname + " " + entry.opc_path + " " + entry.tag + " " + readEvent.Value.ToString());
                                        });
                                    }
                                    break;
                            }
                        }

                        do
                        {
                            foreach (OPC_entry entry in entries)
                            {
                                string stype;
                                if (entry.opc_type == "")
                                    // this function fails for opc DA returning only "System.Int16", so plese define the type manually
                                    stype = client.GetDataType(entry.opc_path).ToString().ToLower();
                                else
                                    stype = entry.opc_type.ToLower();

                                //double val = 0;
                                //string sval = "";
                                //Hylasoft.Opc.Common.Quality quality = Hylasoft.Opc.Common.Quality.Bad;

                                switch (stype)
                                {
                                    case "vt_bool":
                                    case "bool":
                                    case "system.boolean":
                                        {
                                            var task = client.ReadAsync<Boolean>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = task.Result.Value ? 1.0 : 0.0;
                                            ov.bool_value = task.Result.Value;
                                            ov.string_value = task.Result.Value.ToString().ToLower();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "vt_r4":
                                    case "system.single":
                                        {
                                            var task = client.ReadAsync<Single>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = task.Result.Value;
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US"));
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "vt_r8":
                                    case "float":
                                    case "system.double":
                                        {
                                            var task = client.ReadAsync<Double>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = task.Result.Value;
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString("G", CultureInfo.CreateSpecificCulture("en-US"));
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "vt_i1":
                                    case "byte":
                                    case "system.byte":
                                        {
                                            var task = client.ReadAsync<SByte>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "vt_i2":
                                    case "int16":
                                    case "system.int16":
                                        {
                                            var task = client.ReadAsync<Int16>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "state":
                                    case "vt_i4":
                                    case "int32":
                                    case "integer":
                                    case "system.int32":
                                        {
                                            var task = client.ReadAsync<Int32>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = Convert.ToDouble(task.Result.Value);
                                            ov.bool_value = task.Result.Value != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "string":
                                        {
                                            var task = client.ReadAsync<string>(entry.opc_path);
                                            task.Wait();
                                            double dblval = 0.0;
                                            try
                                            {
                                                dblval = Convert.ToDouble(task.Result.Value);
                                            }
                                            catch (Exception)
                                            {
                                                dblval = 0.0;
                                            }
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            ov.double_value = dblval;
                                            ov.bool_value = dblval != 0;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    case "vt_date":
                                    case "date":
                                    case "datetime":
                                    case "system.datetime":
                                        {
                                            var task = client.ReadAsync<DateTime>(entry.opc_path);
                                            task.Wait();
                                            OPC_Value ov = new OPC_Value();
                                            ov.opc_entry = entry;
                                            DateTime dt = task.Result.Value;
                                            ov.double_value = Convert.ToDouble(task.Result.Value.Ticks);
                                            ov.bool_value = false;
                                            ov.string_value = task.Result.Value.ToString();
                                            ov.sourceTimestamp = task.Result.SourceTimestamp;
                                            ov.serverTimestamp = task.Result.ServerTimestamp;
                                            ov.quality = task.Result.Quality;
                                            MapValues[entry.tag] = ov;
                                            if (logread) Console.WriteLine("READ  " + appname + " " + entry.opc_path + " " + entry.tag + " " + ov.string_value);
                                        }
                                        break;
                                    default:
                                        if (logread) Console.WriteLine("READ UNSUPPORTED TYPE: " + appname + " " + entry.opc_path + " " + stype);
                                        break;
                                }

                            }

                            System.Threading.Thread.Sleep(1000 * readperiod);

                        } while (true);
                    }
                }
                catch (Exception e)
                {
                    // EXCEPTION HANDLER
                    Console.WriteLine("Exception DA " + appname);
                    Console.WriteLine(e.ToString().Substring(0, e.ToString().IndexOf(Environment.NewLine)));
                    System.Threading.Thread.Sleep(3000);
                }
            } while (true);
        }
        
        public class WebServer
        {
            private readonly HttpListener _listener = new HttpListener();
            private readonly Func<HttpListenerRequest, string> _responderMethod;

            public WebServer(IReadOnlyCollection<string> prefixes, Func<HttpListenerRequest, string> method)
            {
                if (!HttpListener.IsSupported)
                {
                    throw new NotSupportedException("Needs Windows XP SP2, Server 2003 or later.");
                }

                // URI prefixes are required eg: "http://localhost:8080/test/"
                if (prefixes == null || prefixes.Count == 0)
                {
                    throw new ArgumentException("URI prefixes are required");
                }

                if (method == null)
                {
                    throw new ArgumentException("responder method required");
                }

                foreach (var s in prefixes)
                {
                    _listener.Prefixes.Add(s);
                }

                _responderMethod = method;
                _listener.Start();
            }

            public WebServer(Func<HttpListenerRequest, string> method, params string[] prefixes)
               : this(prefixes, method)
            {
            }

            public void Run()
            {
                ThreadPool.QueueUserWorkItem(o =>
                {
                    Console.WriteLine("Webserver running...");
                    try
                    {
                        while (_listener.IsListening)
                        {
                            ThreadPool.QueueUserWorkItem(c =>
                            {
                                var ctx = c as HttpListenerContext;
                                try
                                {
                                    if (ctx == null)
                                    {
                                        return;
                                    }

                                    var rstr = _responderMethod(ctx.Request);
                                    var buf = Encoding.UTF8.GetBytes(rstr);

                                    if ( rstr.IndexOf("<?xml") == 0 )
                                    {
                                        ctx.Response.ContentType = "application/xml;";
                                        ctx.Response.Headers.Add("Access-Control-Allow-Origin: *");
                                        ctx.Response.Headers.Add("OData-Version: 4.0");
                                        ctx.Response.Headers.Add("Cache-Control: no-cache");
                                        ctx.Response.Headers.Add("Expires: -1");
                                        ctx.Response.Headers.Add("Pragma: no-cache");
                                    }
                                    else
                                    {
                                        ctx.Response.ContentType = "application/json; odata.metadata=minimal";
                                        ctx.Response.Headers.Add("Access-Control-Allow-Origin: *");
                                        ctx.Response.Headers.Add("OData-Version: 4.0");
                                        ctx.Response.Headers.Add("Cache-Control: no-cache");
                                        ctx.Response.Headers.Add("Expires: -1");
                                        ctx.Response.Headers.Add("Pragma: no-cache");
                                    }


                                    ctx.Response.ContentLength64 = buf.Length;
                                    ctx.Response.OutputStream.Write(buf, 0, buf.Length);
                                }
                                catch
                                {
                                    // ignored
                                }
                                finally
                                {
                                    // always close the stream
                                    if (ctx != null)
                                    {
                                        ctx.Response.OutputStream.Close();
                                    }
                                }
                            }, _listener.GetContext());
                        }
                    }
                    catch (Exception ex)
                    {
                        // ignored
                    }
                });
            }

            public void Stop()
            {
                _listener.Stop();
                _listener.Close();
            }
        }

        public static string SendResponse(HttpListenerRequest request)
        {
            Console.WriteLine("");
            Console.WriteLine(request.Url);
            Console.WriteLine("");
            string resp = "";

            if (request.RawUrl.Contains("$metadata"))
            {
                resp = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>" +
                       "<edmx:Edmx Version=\"4.0\" xmlns:edmx=\"http://docs.oasis-open.org/odata/ns/edmx\">" +
                       "<edmx:DataServices>" +
                       "<Schema Namespace=\"OPC2PowerBI.OData.Service.OPC2PowerBI.Models\" xmlns=\"http://docs.oasis-open.org/odata/ns/edm\">" +
                       "<EntityType Name=\"OPCValue\">" +
                           "<Key>" +
                               "<PropertyRef Name=\"Tag\"/>" +
                           "</Key>" +
                           "<Property Name=\"Tag\" Type=\"Edm.String\" Nullable=\"false\"/>" +
                           "<Property Name=\"Quality\" Type=\"Edm.String\"/>" +
                           "<Property Name=\"StringValue\" Type=\"Edm.String\"/>" +
                           "<Property Name=\"BoolValue\" Type=\"Edm.Boolean\"/>" +
                           "<Property Name=\"DoubleValue\" Type=\"Edm.Double\"/>" +
                           "<Property Name=\"ServerTimestamp\" Type=\"Edm.DateTimeOffset\"/>" +
                           "<Property Name=\"SourceTimestamp\" Type=\"Edm.DateTimeOffset\"/>" +
                            "</EntityType>" +

                       "<EntityContainer Name=\"Container\">" +
                       "<EntitySet Name=\"OPCValues\" EntityType=\"OPC2PowerBI.OData.Service.OPC2PowerBI.Models.OPCValue\">" +
                           "<NavigationPropertyBinding Path=\"Values\" Target=\"OPCValues\"/>" +
                       "</EntitySet>" +
                       "</EntityContainer>" +

                       "</Schema>" +
                       "</edmx:DataServices>" +
                       "</edmx:Edmx>";
            }
            else
            {
                // string resp = "{\"@odata.context\":\"https://services.odata.org/TripPinRESTierService/(S(onqmbi4ocvpglsqesqg1x2bs))/$metadata#People\",\"value\":[";
                resp = "{\"@odata.context\":\""+ request.Url.GetLeftPart(UriPartial.Path) + "/$metadata#OPCValues\",\"value\":[";

                int cnt = 0;
                foreach (KeyValuePair<string, OPC_Value> entry in MapValues)
                {
                    if (cnt != 0)
                        resp += ",";

                    string servert = entry.Value.serverTimestamp.ToString("o");
                    if (servert == "0001-01-01T00:00:00.0000000")
                        servert = "\"ServerTimestamp\":null";
                    else
                        servert = "\"ServerTimestamp\":\"" + servert + "\"";

                    string sourcet = entry.Value.sourceTimestamp.ToString("o");
                    if (sourcet == "0001-01-01T00:00:00.0000000")
                        sourcet = "\"SourceTimestamp\":null";
                    else
                        sourcet = "\"SourceTimestamp\":\"" + sourcet + "\"";

                    // do something with entry.Value or entry.Key
                    resp += "{\"Tag\":\"" + entry.Key + "\"," +
                              "\"DoubleValue\":" + entry.Value.double_value + "," +
                              "\"BoolValue\":" + entry.Value.bool_value.ToString().ToLower() + "," +
                              "\"StringValue\":\"" + HttpUtility.JavaScriptStringEncode(entry.Value.string_value) + "\"," +
                              servert + "," +
                              sourcet + "," +
                              "\"Quality\":\"" + entry.Value.quality + "\"" +
                              "}";
                    cnt++;
                }
                resp += "]}";
            }
            
            return string.Format("{0}", resp);
        }

        static void Main(string[] args)
        {
            CultureInfo ci = new CultureInfo("en-US");
            Thread.CurrentThread.CurrentCulture = ci;
            Thread.CurrentThread.CurrentUICulture = ci;
            string servicename = "";
            int port = 8080;

            Console.WriteLine(Version);

            List<OPC_server> servers = new List<OPC_server>();

            // Read file using StreamReader. Reads file line by line  
            using (StreamReader file = new StreamReader(ConfigFile))
            {
                int counter = 0;
                string ln;
                int cnt_entries = -1;
                int cnt_servers = -1;

                while ((ln = file.ReadLine()) != null)
                {
                    counter++;
                    if (ln.Trim() == "")
                        continue;

                    var result = ln.Split(',');

                    if (result[0][0] == '#' || result.Count() == 0) // comment or empty line
                        continue;
                    if ( result.Count() == 2 && servicename == "")
                    {
                        port = System.Convert.ToInt32(result[0].Trim());
                        servicename = result[1].Trim();
                        continue;
                    }

                    if (result[0].ToLower().Contains("opc.tcp://") && result.Count() >= 3)
                    { // new opc ua server
                        Console.WriteLine("NEW UA SERVER");
                        cnt_entries = -1;
                        cnt_servers++;
                        string certfile = "", certpasswd = "";
                        if (result.Count() >= 4)
                            certfile = result[3].Trim();
                        if (result.Count() >= 5)
                            certpasswd = result[4].Trim();
                        OPC_server opcserv = new OPC_server
                        {
                            opc_server_name = (result[2].Trim() == "") ? result[0].Trim() : result[2].Trim(),
                            opc_url = result[0].Trim(),
                            certificate_file = certfile,
                            certificate_password = certpasswd,
                            read_period = System.Convert.ToInt32(result[1].Trim()),
                            is_opc_ua = 1,
                            entries = new List<OPC_entry>()
                        };
                        servers.Add(opcserv);
                    }
                    else
                    if (result[0].ToLower().Contains("opcda://") && result.Count() >= 3)
                    { // new opc da server
                        Console.WriteLine("NEW DA SERVER");
                        cnt_entries = -1;
                        cnt_servers++;
                        OPC_server opcserv = new OPC_server
                        {
                            opc_server_name = (result[2].Trim() == "") ? result[0].Trim() : result[2].Trim(),
                            opc_url = result[0].Trim(),
                            read_period = System.Convert.ToInt32(result[1].Trim()),
                            is_opc_ua = 0,
                            entries = new List<OPC_entry>()
                        };
                        servers.Add(opcserv);
                    }
                    else
                    if (result.Count() >= 4)
                    { // must be a tag entry
                        Console.WriteLine("NEW TAG");
                        cnt_entries++;
                        OPC_entry opcentry = new OPC_entry()
                        {
                            opc_server_name = servers[cnt_servers].opc_server_name,
                            opc_path = result[0].Trim(),
                            opc_type = result[1].Trim(),
                            subscribe = result[2].Trim() == "Y" ? true : false,
                            tag = result[3].Trim() == "" ? result[0].Trim() : result[3].Trim(),
                        };
                        servers[cnt_servers].entries.Add(opcentry);
                    }
                    else
                    {
                        Console.WriteLine("Invalid config line: " + counter);
                    }

                    Console.WriteLine(ln);
                }
                file.Close();
                Console.WriteLine($"Config file has {counter} lines.");
            }

            //          Console.ReadKey();

            foreach (OPC_server srv in servers)
            {
                if (srv.is_opc_ua != 0)
                {
                    Thread t = new Thread(() => ProcessUa(srv.opc_url, srv.entries, srv.read_period,srv.opc_server_name, srv.certificate_file, srv.certificate_password));
                    t.Start();
                }
                else
                {
                    Thread t = new Thread(() => ProcessDa(srv.opc_url, srv.entries, srv.read_period, srv.opc_server_name));
                    t.Start();
                }
            }

            var ws = new WebServer(SendResponse, new string[] { "http://127.0.0.1:" + port + "/" + servicename + "/" });
            ws.Run();
            Console.WriteLine("OPC2PowerBI webserver running. Press a key to quit.");
            Console.ReadKey();
            ws.Stop();
            // Console.ReadKey();
        }

    }
}


