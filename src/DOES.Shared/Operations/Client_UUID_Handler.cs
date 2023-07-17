using System;
using System.IO;
using System.Runtime.InteropServices;

namespace DOES.Shared.Operations
{
    public class Client_UUID_Handler
    {
        public static string Check_For_Client_UUID()
        {
            string uuidFileName = "unique.conf";
            string uuid = string.Empty;
            string uuidfilepath = string.Empty;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == true)
            {
                uuidfilepath = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData) + @"\Pure Storage\D.O.E.S\Config\" + uuidFileName;
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == true)
            {
                //Then its a linux platform
                uuidfilepath = @"/opt/purestorage/does/config/" + uuidFileName;
            }

            // Then its a windows platform
            if (File.Exists(uuidfilepath))
            {
                //read the file
                using (var file = new FileStream(uuidfilepath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    using (var configReader = new StreamReader(file))
                    {
                        string line;
                        while ((line = configReader.ReadLine()) != null)
                        {
                            if (IsGuid(line))
                            {
                                uuid = line;
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                Guid myuuid = Guid.NewGuid();
                uuid = myuuid.ToString();
                //create the file with a new UUID
                FileStream fs = new FileStream(uuidfilepath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                StreamWriter writer = new StreamWriter(fs);
                writer.WriteLine(uuid);
                writer.Close();
            }
            return uuid;
        }

        public static bool IsGuid(string value)
        {
            Guid x;
            return Guid.TryParse(value, out x);
        }
    }
}
