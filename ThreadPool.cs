using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace StockAnalysis
{
    internal class ThreadPool
    {
        internal static AutoResetEvent wakeUp = new AutoResetEvent(false);
        Queue<ThreadProcRequest> chunkQueue;
        bool terminateFlag;

        public ThreadPool() // flag may have to be 'ref' variable 
        {
            chunkQueue = new Queue<ThreadProcRequest>();
            terminateFlag = false;
        }

        public int NumOfThreads { get; private set; }

        internal void Initalize(int numOfThreads)
        {
            // create event
            NumOfThreads = numOfThreads;
            for(int i = 0; i < numOfThreads; i++)
            {
                Thread t = new Thread(() => ThreadProc(this));
                t.Name = "Thread_" + i;
                t.Start();
            }
        }

        internal void PutQueue(ThreadProcRequest tpr)
        {
            lock(chunkQueue)
            {
                chunkQueue.Enqueue(tpr);                
            }
            wakeUp.Set();
        }

        internal void Terminate()
        {
            terminateFlag = true;
            wakeUp.Set(); // might need loop to call Set for each thread
        }

        internal static void ThreadProc(ThreadPool tp)
        {
            bool destroyAfterExecute;
            while(true)
            {
                wakeUp.WaitOne();

                if(tp.terminateFlag == true)
                {
                    break; // Thread.Current will break out of while loop
                }

                while(true) // Thread.Current 'pop' from queue while available
                {
                    ThreadProcRequest cpc;
                    lock (tp.chunkQueue)
                    {
                        if (tp.chunkQueue.Count > 0)
                        {
                            cpc = tp.chunkQueue.Dequeue();
                        }
                        else
                        {
                            break;
                        }
                    }
                    destroyAfterExecute = true;
                    cpc.Execute(ref destroyAfterExecute);
                    if(destroyAfterExecute == true)
                    {
                        cpc = null;
                        // dispose of cpc
                    }
                }
            }
        }

    }
}
