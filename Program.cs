using System;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace StockAnalysis
{
    public class Program
    {        
        // check reference types everywhere...
        static void Main(string[] args)
        {
            // args[0] = yesterdayRawDataFile
            // args[1] = todaysRawDataFile
            // args[2] = outputDirectory
            // args[3] = criteriaSetFile
            // args[4] = numOfThreads
            Stopwatch stopwatch = new Stopwatch();
            string outputDirectoryPath;
            string yesterdaysRawDataFilePath;
            string todaysRawDataFilePath;
            string criteriaSetFilePath;
            int numOfThreads;
            bool boolDummy = false;
            DateTime today = DateTime.Today;
            long totalMemoryUsed;
            long ramUsed;
            
            if (args.Length != 5)
            {
                Console.WriteLine("Program requires 3 command line arguments to run.");
                Console.WriteLine("1) Filepath to yesterdays data");
                Console.WriteLine("2) Filepath to todays data");
                Console.WriteLine("3) Output directory filepath");
                Console.WriteLine("4) Filepath to criteria set file");
                Console.WriteLine("5) Number of Threads (int)");
                return;
            }
            
            stopwatch.Start();
            
            yesterdaysRawDataFilePath = args[0];
            todaysRawDataFilePath = args[1];
            outputDirectoryPath = args[2];
            criteriaSetFilePath = args[3];
            numOfThreads = Convert.ToInt16(args[4]);
            
            /*
            yesterdaysRawDataFilePath = @"C:\Users\Lev\Documents\K-State\Fall2018\CIS625\Homework\FinalProject\100K_Holden\2018_11_30_09_25_39\File 0.csv";
            todaysRawDataFilePath = @"C:\Users\Lev\Documents\K-State\Fall2018\CIS625\Homework\FinalProject\100K_Holden\2018_11_30_09_25_39\File 1.csv";
            outputDirectoryPath = @"C:\temp";
            criteriaSetFilePath = @"C:\Users\Lev\Documents\K-State\Fall2018\CIS625\Homework\FinalProject\All_Criteria_Sets.txt";
            numOfThreads = 4;
            */
            Console.WriteLine("Here are the cmd args.");
            Console.WriteLine("Yesterdays file path: " + yesterdaysRawDataFilePath);
            Console.WriteLine("Todays file path: " + todaysRawDataFilePath);
            Console.WriteLine("Output directory path: " + outputDirectoryPath);
            Console.WriteLine("Criteria set fiel path:" + criteriaSetFilePath);
            Console.WriteLine("Number of cores allocated: " + numOfThreads);            

            Global.outputDirectory = Path.Combine(outputDirectoryPath, today.ToString("yyyy_MM_dd"));
            Directory.CreateDirectory(Global.outputDirectory);

            Global.threadPool.Initalize(numOfThreads);
            Global.frp.Initialize(Global.threadPool.NumOfThreads);            
            Global.criteriaParser.SetFilePath(@criteriaSetFilePath); // TODO: Get path to criteria set file from cmd.
            Global.criteriaParser.Execute(ref boolDummy);

            if (Global.criteriaParser.crossesCount > 0)
            {
                Global.GetRangeCreatorFilePath(@yesterdaysRawDataFilePath); // TODO: Get path data directory, yesterday file here
                Global.frp.Execute(ref boolDummy);
                Global.mainThreadWait.WaitOne();
                Global.chunkProcCreator.yesterdaysData = true;
                Global.chunkProcCreator.Execute(ref boolDummy);
                Global.mainThreadWait.WaitOne();
                Global.parsedRawData = new List<RawDataChunk>();
                Global.frp = new FileRangeCreator();
                Global.frp.Initialize(Global.threadPool.NumOfThreads);
                Global.frp.firstChunk = true;
            }
            
            Global.GetRangeCreatorFilePath(@todaysRawDataFilePath); // TODO: Get path data directory
            Global.frp.Execute(ref boolDummy);
            Global.mainThreadWait.WaitOne();
            Global.chunkProcCreator.yesterdaysData = false;
            Global.chunkProcCreator.Execute(ref boolDummy);
            Global.mainThreadWait.WaitOne();
            Global.postAggContextCreator.Execute(ref boolDummy);
            Global.mainThreadWait.WaitOne();

            KillAllThreads(numOfThreads);
            stopwatch.Stop();
            ramUsed = Process.GetCurrentProcess().WorkingSet64;
            totalMemoryUsed = GC.GetTotalMemory(true);

            Console.WriteLine("RunTime, " + stopwatch.Elapsed.TotalMilliseconds.ToString() + " ms");
            Console.WriteLine("PhysicalMem1, " + ramUsed);
            Console.WriteLine("PhysicalMem2, " + totalMemoryUsed);

            //Console.WriteLine("Press 'Enter' to Exit!");
            //Console.ReadLine();
        }

        private static void KillAllThreads(int numOfThreads)
        {
            for(int i = 0; i < numOfThreads; i++)
            {
                Global.threadPool.Terminate();
            }
        }

        internal static void WriteToConsole(string text)
        {
            Console.WriteLine(text);
        }
    }

    internal static class Global
    {
        internal static string outputDirectory { get; set; }
        internal static AutoResetEvent mainThreadWait = new AutoResetEvent(false);
        internal static PostAggContextCreator postAggContextCreator = new PostAggContextCreator();
        internal static ChunkProcCreator chunkProcCreator = new ChunkProcCreator();
        internal static int globalCriteriaSetWorkCount = 0;
        internal static List<RawDataChunk> parsedRawData = new List<RawDataChunk>();
        internal static ThreadPool threadPool = new ThreadPool();
        internal static List<CriteriaSetObj> criteriaSets = new List<CriteriaSetObj>();
        internal static CriteriaSetParser criteriaParser = new CriteriaSetParser();
        internal static FileRangeCreator frp = new FileRangeCreator();

        internal static void CombineStockDataEntryChunk(RawDataChunk chunkDataParsed)
        {
            lock (parsedRawData)
            {
                parsedRawData.Add(chunkDataParsed);
            }
        }

        internal static void GetRangeCreatorFilePath(string filname)
        {
            frp.SetFilePath(filname);
        } 
        
        internal static void AddCriteriaSet(CriteriaSetObj cso)
        {
            criteriaSets.Add(cso);
        }
    }
}
