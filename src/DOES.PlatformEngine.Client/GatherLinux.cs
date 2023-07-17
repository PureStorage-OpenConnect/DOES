using System;
using System.Diagnostics;
using DOES.Shared.Resources;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using DOES.Shared.Debug;

namespace DOES.PlatformEngine.Client
{
    public class GatherLinux
    {
        public static LinuxResource GatherLinuxResourceDataPoint(MessageQueue queue)
        {
            string cpuTop = "top  -n 3 -b | grep Cpu";
            string cpuMHz = "lscpu | grep MHz";

            string procMemTotal = "cat /proc/meminfo | grep MemTotal:";
            string procMemFree = "cat /proc/meminfo | grep MemFree:";
            string procMemActive = "cat /proc/meminfo | grep Active:";

            string diskSD = "iostat -d 2 3 -y | grep sd";
            string diskNVME = "iostat -d 2 3 -y | grep nvme";
            string diskDM = "iostat -d 2 3 -y | grep dm-";

            string getCPUTopData = cpuTop.Bash();

            double sdTPS = 0;
            double sdKBRead = 0;
            double sdKBWrite = 0;
            double sdKBReadPS = 0;
            double sdKBWritePS = 0;
            double nvmeTPS = 0;
            double nvmeKBRead = 0;
            double nvmeKBWrite = 0;
            double nvmeKBReadPS = 0;
            double nvmeKBWritePS = 0;
            double dmTPS = 0;
            double dmKBRead = 0;
            double dmKBWrite = 0; ;
            double dmKBReadPS = 0;
            double dmKBWritePS = 0;

            try
            {
                //Standard SCSI Disks
                string getDiskIsolatedData = "";
                getDiskIsolatedData = getDiskIsolatedData + diskSD.Bash();

                string[] splitdiskData = getDiskIsolatedData.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.None);
                List<double[]> finalUncomputedStatsSD = new List<double[]>();

                for (int i = 1; i < splitdiskData.Length; i++)
                {
                    string[] splitDiskStats = splitdiskData[i].Split(new char[0]);
                    double[] interimDiskStats = new double[5];
                    int diskStatPos = 0;
                    for (int statsCount = 1; statsCount < splitDiskStats.Length; statsCount++)
                    {
                        if (splitDiskStats[statsCount] != "")
                        {
                            interimDiskStats[diskStatPos] = Convert.ToDouble(splitDiskStats[statsCount]);
                            diskStatPos++;
                        }
                    }
                    finalUncomputedStatsSD.Add(interimDiskStats);
                }

                for (int i = 0; i < finalUncomputedStatsSD.Count; i++)
                {
                    sdTPS += (finalUncomputedStatsSD[i][0]);
                    sdKBReadPS += (finalUncomputedStatsSD[i][1]);
                    sdKBWritePS += (finalUncomputedStatsSD[i][2]);
                    sdKBRead += (finalUncomputedStatsSD[i][3]);
                    sdKBWrite += (finalUncomputedStatsSD[i][4]);
                }

                sdTPS = sdTPS / 3;
                sdKBReadPS = sdKBReadPS / 3;
                sdKBWritePS = sdKBWritePS / 3;
                sdKBRead = sdKBRead / 3;
                sdKBWrite = sdKBWrite / 3;


                //NVMe Disks and Devices
                getDiskIsolatedData = "";
                getDiskIsolatedData = getDiskIsolatedData + diskNVME.Bash();

                splitdiskData = getDiskIsolatedData.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.None);
                List<double[]> finalUncomputedStatsNVME = new List<double[]>();

                for (int i = 1; i < splitdiskData.Length; i++)
                {
                    string[] splitDiskStats = splitdiskData[i].Split(new char[0]);
                    double[] interimDiskStats = new double[5];
                    int diskStatPos = 0;
                    for (int statsCount = 1; statsCount < splitDiskStats.Length; statsCount++)
                    {
                        if (splitDiskStats[statsCount] != "")
                        {
                            interimDiskStats[diskStatPos] = Convert.ToDouble(splitDiskStats[statsCount]);
                            diskStatPos++;
                        }
                    }
                    finalUncomputedStatsNVME.Add(interimDiskStats);
                }

                for (int i = 0; i < finalUncomputedStatsNVME.Count; i++)
                {
                    nvmeTPS += (finalUncomputedStatsNVME[i][0]);
                    nvmeKBReadPS += (finalUncomputedStatsNVME[i][1]);
                    nvmeKBWritePS += (finalUncomputedStatsNVME[i][2]);
                    nvmeKBRead += (finalUncomputedStatsNVME[i][3]);
                    nvmeKBWrite += (finalUncomputedStatsNVME[i][4]);
                }

                nvmeTPS = nvmeTPS / 3;
                nvmeKBReadPS = nvmeKBReadPS / 3;
                nvmeKBWritePS = nvmeKBWritePS / 3;
                nvmeKBRead = nvmeKBRead / 3;
                nvmeKBWrite = nvmeKBWrite / 3;

                //Device Mapper devices
                getDiskIsolatedData = "";
                getDiskIsolatedData = getDiskIsolatedData + diskDM.Bash();

                splitdiskData = getDiskIsolatedData.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.None);
                List<double[]> finalUncomputedStatsDM = new List<double[]>();

                for (int i = 1; i < splitdiskData.Length; i++)
                {
                    string[] splitDiskStats = splitdiskData[i].Split(new char[0]);
                    double[] interimDiskStats = new double[5];
                    int diskStatPos = 0;
                    for (int statsCount = 1; statsCount < splitDiskStats.Length; statsCount++)
                    {
                        if (splitDiskStats[statsCount] != "")
                        {
                            interimDiskStats[diskStatPos] = Convert.ToDouble(splitDiskStats[statsCount]);
                            diskStatPos++;
                        }
                    }
                    finalUncomputedStatsDM.Add(interimDiskStats);
                }

                for (int i = 0; i < finalUncomputedStatsDM.Count; i++)
                {
                    dmTPS += (finalUncomputedStatsDM[i][0]);
                    dmKBReadPS += (finalUncomputedStatsDM[i][1]);
                    dmKBWritePS += (finalUncomputedStatsDM[i][2]);
                    dmKBRead += (finalUncomputedStatsDM[i][3]);
                    dmKBWrite += (finalUncomputedStatsDM[i][4]);
                }

                dmTPS = dmTPS / 3;
                dmKBReadPS = dmKBReadPS / 3;
                dmKBWritePS = dmKBWritePS / 3;
                dmKBRead = dmKBRead / 3;
                dmKBWrite = dmKBWrite / 3;

                string getCPUMHGZTopData = cpuMHz.Bash();
                string getMemoryTotalData = procMemTotal.Bash();
                string getMemoryFreeData = procMemFree.Bash();
                string getMemoryActive = procMemActive.Bash();

                string[] splitcpuTopData = getCPUTopData.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.None);

                Regex CPUIDlePattern = new Regex(@"(\w+.\w id)");
                Regex CPUIdleWAPattern = new Regex(@"(\w+.\w wa)");
                Match CPUIDmatch = CPUIDlePattern.Match(splitcpuTopData[2]);
                Match CPUIDWAmatch = CPUIdleWAPattern.Match(splitcpuTopData[2]);
                string[] splitNumber = CPUIDmatch.ToString().Split(' ');
                double CPUIDle = Convert.ToDouble(splitNumber[0]);
                splitNumber = CPUIDWAmatch.ToString().Split(' ');
                double CPUIDLEwa = Convert.ToDouble(splitNumber[0]);
                double CPULoad = Math.Round(100.00 - (CPUIDle + CPUIDLEwa), 3);


                ///TotalPhysicalMemory, UsedPhysicalMemory and FreePhysicalMemory
                Regex UsedMemoryPattern = new Regex(@"(\d+)");
                Regex FreeMemoryPattern = new Regex(@"(\d+)");
                Regex TotalMemoryPattern = new Regex(@"(\d+)");
                Match UsedMemMatch = UsedMemoryPattern.Match(getMemoryActive);
                Match FreeMemMatch = FreeMemoryPattern.Match(getMemoryFreeData);
                Match TotalMemMatch = TotalMemoryPattern.Match(getMemoryTotalData);

                Int64 UsedMemory = Convert.ToInt64(UsedMemMatch.ToString()) * 1024;
                Int64 FreeMemory = Convert.ToInt64(FreeMemMatch.ToString()) * 1024;
                Int64 TotalMemory = Convert.ToInt64(TotalMemMatch.ToString()) * 1024;

                //CurrentClockSpeed 
                Regex CurrentClockPattern = new Regex(@"(\d\d\d\d.\d\d)");
                Match CurrentClockMatch = CurrentClockPattern.Match(getCPUMHGZTopData);
                string CurrentClockSpeed = Convert.ToString(CurrentClockMatch);

                LinuxResource lr = new LinuxResource(CPULoad, TotalMemory, UsedMemory, FreeMemory, Convert.ToDouble(CurrentClockSpeed), sdTPS, sdKBRead, sdKBWrite,
                            sdKBReadPS, sdKBWritePS, nvmeTPS, nvmeKBRead, nvmeKBWrite,
                            nvmeKBReadPS, nvmeKBWritePS, dmTPS, dmKBRead, dmKBWrite, dmKBWritePS, dmKBReadPS, DateTime.Now);
                return lr;
            }
            catch (Exception ex)
            {
                queue.AddMessage(new Message(DateTime.Now, ex.Message, Message.MessageType.Error));
                return null;
            }
        }
    }

    public static class ShellHelper
    {
        public static string Bash(this string cmd)
        {
            var escapedArgs = cmd.Replace("\"", "\\\"");

            var process = new Process()
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"{escapedArgs}\"",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,

                }
            };
            //This line is great for debugging from an IDE but terrible for running production
            //process.StartInfo.EnvironmentVariables.Add("TERM", "xterm");
            process.Start();
            string result = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            return result;
        }
    }
}
