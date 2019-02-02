using System;
using System.Collections.Generic;
using System.Text;

namespace StockAnalysis
{
    internal class ChunkProcCreator: ThreadProcRequest
    {
        internal bool yesterdaysData { get; set; }

        public override void Execute(ref bool destory)
        {
            ChunkProcContext contextProcessor;
            int numOfCriteriaSets = Global.criteriaSets.Count;
            int numOfRawDataChunks = Global.parsedRawData.Count;

            if (yesterdaysData)
            {
                for (int i = 0; i < numOfCriteriaSets; i++)
                {
                    if (Global.criteriaSets[i].postAggObj.comparison.ToString().ToUpper().Equals(CriteriaConstants.CROSSES_THRESHOLD_SPECIFIER))
                    {
                        for (int j = 0; j < numOfRawDataChunks; j++)
                        {
                            contextProcessor = new ChunkProcContext(i, j, yesterdaysData);
                            Global.threadPool.PutQueue(contextProcessor);
                        }
                    }
                }
            }
            else
            {
                for (int i = 0; i < numOfCriteriaSets; i++)
                {
                    for (int j = 0; j < numOfRawDataChunks; j++)
                    {
                        contextProcessor = new ChunkProcContext(i, j, yesterdaysData);
                        Global.threadPool.PutQueue(contextProcessor);
                    }
                }
            }
        }

    }
}
