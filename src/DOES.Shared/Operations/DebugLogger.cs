using System;
using System.IO;
using System.Runtime.InteropServices;

namespace DOES.Shared.Operations
{
   public static class DebugLogger
    {
        public static void LogMessages(File file, string logText)
        {
            string logFilePath = string.Empty;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == true)
            {
                logFilePath = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData) + @"\Pure Storage\D.O.E.S\Logs\" + getFileName(file);
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == true)
            {
                //Then its a linux platform
                logFilePath = @"/opt/purestorage/does/log/" + getFileName(file);
            }
            DateTime logTime = DateTime.Now;
            FileStream fs = new FileStream(logFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            StreamWriter writer = new StreamWriter(fs);
            writer.WriteLine(logText);
            writer.Close();
        }

        public enum File
        {
            PlatformEngine,
            Core,
            DataEngine
        };

        public static string getFileName(File file)
        {
            string filename = null;

            if(file == File.Core)
            {
                filename = "Core.log";
            }
            else if(file == File.DataEngine)
            {
                filename = "DataEngine.log";
            }
            else if (file == File.PlatformEngine)
            {
                filename = "PlatformEngine.log";
            }
            return filename;
        }
    }
}
