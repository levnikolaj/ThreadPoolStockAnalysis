using System;
using System.Collections.Generic;
using System.Text;

namespace StockAnalysis
{
    internal class PostAggContextCreator: ThreadProcRequest
    {
        // go through all criteria sets and create PostAggContextProc
        public override void Execute(ref bool destory)
        {
            PostAggContextProc postAggContext;
            int numOfCriteriaObj = Global.criteriaSets.Count;

            for(int i = 0; i < numOfCriteriaObj; i++)
            {
                postAggContext = new PostAggContextProc(i);
                Global.threadPool.PutQueue(postAggContext);
            }
        }
    }
}
