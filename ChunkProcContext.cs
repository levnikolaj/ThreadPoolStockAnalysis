using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace StockAnalysis
{
    internal class ChunkProcContext: ThreadProcRequest
    {
        private static int chunkCount;
        private int criteriaIndex;
        private int dataIndex;
        private bool yesterdayData;

        public ChunkProcContext(int criteriaListIndex, int rawDataListIndex, bool dataFrom)
        {
            Interlocked.Add(ref chunkCount, 1);
            criteriaIndex = criteriaListIndex;
            dataIndex = rawDataListIndex;
            yesterdayData = dataFrom;
        }

        public override void Execute(ref bool destroyAfterExecute)
        {
            int outstadindChunkContexts;
            string key;
            CriteriaSetObj workingCriteria = Global.criteriaSets[criteriaIndex];
            RawDataChunk workingData = Global.parsedRawData[dataIndex];
            PreAggSpecs myPreSpecs = workingCriteria.preAggObj;
            AggSpecs myAggSpecs = workingCriteria.aggSpecsObj;
            PostAggSpecs myPostSpecs = workingCriteria.postAggObj;
            uint rawDataRowId;

            for(int i = 0; i < workingData.chunkDataParsed.Count; i++)
            {
                StockDataEntry currStockEntry = workingData.chunkDataParsed[i];
                if(DataEntryWithinFilter(myPreSpecs, currStockEntry))
                {
                    rawDataRowId = ((uint)dataIndex << 16) | ((uint)i); 
                    key = myAggSpecs.CreateAggKeyWithAggValues(currStockEntry);
                    workingCriteria.AddValueToDictionary(yesterdayData, key, currStockEntry.precentageSharesHeld, currStockEntry.sharesHeld, currStockEntry.value, rawDataRowId);
                }
            }

            outstadindChunkContexts = Interlocked.Decrement(ref chunkCount);
            if(outstadindChunkContexts == 0)
            {
                Global.mainThreadWait.Set();
            }
        }       

        private bool DataEntryWithinFilter(PreAggSpecs spec, StockDataEntry data)
        {
            if(spec.type != StockType.Undefined)
            {
                if(!CheckStockType(spec.type, data.stockType, spec.stockTypeCompare))
                {
                    return false;
                }
            }

            if(spec.direction != StockDirection.Undefined)
            {
                if(!CheckDirectionType(spec.direction, data.direction, spec.directionCompare))
                {
                    return false;
                }
            }

            for(int i = 0; i < spec.holderCountry.Count; i++)
            {
                if(CheckHolderCountryType(spec.holderCountry[i], data.holderCountry, spec.countryCompare))
                {
                    return true;
                }
            }

            return false;
        }

        private bool CheckHolderCountryType(string filterCountry, string dataCountry, Comparison compareBy)
        {
            switch(compareBy)
            {
                case Comparison.IN:
                    {
                        if(filterCountry.Equals(dataCountry))
                        {
                            return true;
                        }
                        break;
                    }
                case Comparison.Equal:
                    {
                        if (filterCountry.Equals(dataCountry))
                        {
                            return true;
                        }
                        break;
                    }
                case Comparison.NotEqual:
                    {
                        if(!filterCountry.Equals(dataCountry))
                        {
                            return true;
                        }
                        break;
                    }
            }

            return false;
        }

        private bool CheckDirectionType(StockDirection filterDirection, string dataDirection, Comparison compareBy)
        {
            switch (compareBy)
            {
                case Comparison.Equal:
                    {
                        if(filterDirection.ToString().Equals(dataDirection))
                        {
                            return true;
                        }
                        break;
                    }
                case Comparison.NotEqual:
                    {
                        if (!filterDirection.ToString().Equals(dataDirection))
                        {
                            return true;
                        }
                        break;
                    }
                case Comparison.IN:
                    {
                        if (filterDirection.ToString().Equals(dataDirection))
                        {
                            return true;
                        }
                        break;
                    }
                default:
                    {
                        break;
                    }
            }

            return false;
        }

        private bool CheckStockType(StockType filterType, string dataType, Comparison compareBy)
        {
            switch(compareBy)
            {
                case Comparison.Equal:
                    {
                        if(filterType.ToString().Equals(dataType))
                        {
                            return true;
                        }
                        break;
                    }
                case Comparison.NotEqual:
                    {
                        if(!filterType.ToString().Equals(dataType))
                        {
                            return true;
                        }
                        break;
                    }
                case Comparison.IN:
                    {
                        if (filterType.ToString().Equals(dataType))
                        {
                            return true;
                        }
                        break;
                    }
                default:
                    {
                        break;
                    }
            }

            return false;
        }
    }    
}
