using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.IO;
using System.Text.RegularExpressions;

namespace StockAnalysis
{
    internal class PostAggContextProc: ThreadProcRequest
    {
        private const string CRITERIA_FILTER_DETAILS_FILE_NAME = "CriteriaFilteredDetail.csv";
        private const string CRITERIA_FILTER_FILE_NAME = "CriteriaFiltered.csv";
        private const string CRITERA_THRESHOLD_FILE_NAME = "CriteriaThresholdOutput.csv";
        private const string FILTER_FILE_HEADING = "Aggregation Key,sharesheld,percentagesharesheld,value";
        private StreamWriter criteriaFilterDetailsWriter;
        private StreamWriter criteriaFilterWriter;
        private StreamWriter criteriaThresholdWriter;
        private static int postAggContextCount = 0;
        private int m_CriteriaIndex;
        private StringBuilder aggKeyContainer = new StringBuilder(); // these will the keys that will be part of one of the output files.
        //private OutputFileWriter fileWriter;
        private string writeDirectory;

        public PostAggContextProc(int criteriaObjIndex)
        {
            m_CriteriaIndex = criteriaObjIndex;
            Interlocked.Add(ref postAggContextCount, 1);
            //fileWriter = new OutputFileWriter(outputDirectoryPath);
        }

        public override void Execute(ref bool destory)
        {
            string correctedCriteriaName;
            int outstandingPostAggContext;
            CriteriaSetObj aggCriteriaObj = Global.criteriaSets[m_CriteriaIndex];
            ThresholdComparison typeOfComparison = aggCriteriaObj.postAggObj.comparison;
            correctedCriteriaName = Regex.Replace(aggCriteriaObj.criteriaSetName, "[^a-zA-Z0-9]", "");
            writeDirectory = Path.Combine(Global.outputDirectory, correctedCriteriaName);
            Directory.CreateDirectory(writeDirectory);
            criteriaFilterDetailsWriter = new StreamWriter(Path.Combine(writeDirectory, CRITERIA_FILTER_DETAILS_FILE_NAME));
            criteriaFilterWriter = new StreamWriter(Path.Combine(writeDirectory, CRITERIA_FILTER_FILE_NAME));
            criteriaThresholdWriter = new StreamWriter(Path.Combine(writeDirectory, CRITERA_THRESHOLD_FILE_NAME));

            criteriaFilterWriter.WriteLine(FILTER_FILE_HEADING);
            // TODO: write header lines for details and threshold files

            switch (typeOfComparison)
            {
                case ThresholdComparison.Undefined:
                    {
                        // shouldn't EVER come in here, checks before this in CriteriaObj should prevent this.
                        break;
                    }
                case ThresholdComparison.Max:
                    {
                        // TODO: call associated method.
                        break;
                    }
                case ThresholdComparison.Crosses:
                    {
                        CrossesYesterdayValueComparison(aggCriteriaObj, typeOfComparison);
                        break;
                    }
                case ThresholdComparison.GreaterThan:
                case ThresholdComparison.GreaterThanOrEqualTo:
                case ThresholdComparison.LessThan:
                case ThresholdComparison.LessThanOrEqualTo:
                    {
                        ValueOfColumnComparison(aggCriteriaObj, typeOfComparison);
                        break;
                    }
                default:
                    {
                        // TODO: check criteria set text file he gave us with all criteria sets.
                        break;
                    }
            }

            outstandingPostAggContext = Interlocked.Decrement(ref postAggContextCount);
            if(outstandingPostAggContext == 0)
            {
                Global.mainThreadWait.Set();
            }
        }

        private void CrossesYesterdayValueComparison(CriteriaSetObj criteriaObj, ThresholdComparison compare)
        {
            int rawDataChunkIndex;
            int stockDataIndex;
            double columnValue = 0.0, thresholdCrossed = 0.0;
            ThresholdComparison comparison = compare;
            CriteriaSetObj criteria = criteriaObj;
            ThresholdColumn crossesColumn = criteria.postAggObj.thresholdColumn;
            IReadOnlyList<double> thresholds = criteria.postAggObj.GetRefToThresholdValues();
            IReadOnlyDictionary<string, AggValues> todaysParsedDataDictionary = criteria.GetRefToAggResults();
            IReadOnlyDictionary<string, AggValues> yesterdaysParsedDataDictionary = criteria.GetRefToYesterdayAggResults();
            AggValues todaysAgg;
            AggValues yestedayAgg;
            bool keyExistsYesterday, todayGreaterYesterday;

            if (crossesColumn == ThresholdColumn.Undefiend)
            {
                // TODO: do something else
                return;
            }

            WriteThresholdHeader(criteria);
            
            foreach(string key in todaysParsedDataDictionary.Keys)
            {
                todaysParsedDataDictionary.TryGetValue(key, out todaysAgg); // should always find something
                keyExistsYesterday = yesterdaysParsedDataDictionary.TryGetValue(key, out yestedayAgg);
                if(!keyExistsYesterday)
                {
                    yestedayAgg = new AggValues(); // all values are 0 then.                    
                }

                switch (crossesColumn)
                {
                    case ThresholdColumn.PrecentageSharesHeld:
                        {
                            columnValue = todaysAgg.percentageSharesHeld;
                            if(columnValue > yestedayAgg.percentageSharesHeld)
                            {
                                todayGreaterYesterday = true;
                            }
                            else
                            {
                                todayGreaterYesterday = false;
                            }
                            thresholdCrossed = PreciseThresholdCrossed(todayGreaterYesterday, columnValue, thresholds);
                            break;
                        }
                    case ThresholdColumn.SharesHeld:
                        {
                            columnValue = todaysAgg.sharesHeld;
                            if (columnValue > yestedayAgg.sharesHeld)
                            {
                                todayGreaterYesterday = true;
                            }
                            else
                            {
                                todayGreaterYesterday = false;
                            }
                            thresholdCrossed = PreciseThresholdCrossed(todayGreaterYesterday, columnValue, thresholds);
                            break;
                        }
                    case ThresholdColumn.Value:
                        {
                            columnValue = todaysAgg.value;
                            if (columnValue > yestedayAgg.value)
                            {
                                todayGreaterYesterday = true;
                            }
                            else
                            {
                                todayGreaterYesterday = false;
                            }
                            thresholdCrossed = PreciseThresholdCrossed(todayGreaterYesterday, columnValue, thresholds);
                            break;
                        }
                    default:
                        {
                            break;
                        }
                }

                // TODO: write to files
                criteriaFilterWriter.WriteLine(GetAggValuesCommaSeparatedString(key, todaysAgg));

                IReadOnlyList<uint> unaggregatedDictEntry = todaysAgg.GetRefToRowsInChunk();
                for (int i = 0; i < unaggregatedDictEntry.Count; i++)
                {
                    rawDataChunkIndex = (int)unaggregatedDictEntry[i] >> 16;
                    stockDataIndex = (int)unaggregatedDictEntry[i] & 0xffff;
                    StockDataEntry sde = Global.parsedRawData[rawDataChunkIndex].chunkDataParsed[stockDataIndex];
                    criteriaFilterDetailsWriter.WriteLine(GetCommaSeparatedStockDataEntryWithKey(sde, key));
                }

                criteriaThresholdWriter.WriteLine(GetCommaSeparatedThresholdEntry(key, columnValue, thresholdCrossed));
            }

            criteriaFilterWriter.Dispose();
            criteriaFilterDetailsWriter.Dispose();
            criteriaThresholdWriter.Dispose();
        }

        private double PreciseThresholdCrossed(bool todayGreater, double todaysColumnValue, IReadOnlyList<double> thresholds)
        {
            double lastThresholdCrossed = 0.0;
            if(todayGreater)
            {
                // lowest to highest threshold
                for(int i = 0; i < thresholds.Count; i++)
                {
                    if(todaysColumnValue > thresholds[i])
                    {
                        lastThresholdCrossed = thresholds[i];
                    }
                    else
                    {
                        return lastThresholdCrossed;
                    }
                }
            }
            else
            {
                // highest to lowest threshold
                for(int i = thresholds.Count - 1; i >= 0; i--)
                {
                    if(todaysColumnValue < thresholds[i])
                    {
                        lastThresholdCrossed = thresholds[i];
                    }
                    else
                    {
                        return lastThresholdCrossed;
                    }
                }
            }

            return lastThresholdCrossed;
        }

        private void ValueOfColumnComparison(CriteriaSetObj criteriaObj, ThresholdComparison compare)
        {
            int rawDataChunkIndex;
            int stockDataIndex;
            ThresholdComparison comparison = compare;
            CriteriaSetObj criteria = criteriaObj;
            ThresholdColumn column = criteria.postAggObj.thresholdColumn;
            IReadOnlyDictionary<string, AggValues> readOnlyAggResults = criteria.GetRefToAggResults();
            AggValues aggValue;
            IReadOnlyList<double> thresholdValues = criteria.postAggObj.GetRefToThresholdValues(); // should only contain 1 value.
            double thresholdValue = thresholdValues[0]; // should be only 1 value here'
            double columnValue = 0.0;
            bool thresholdCheck = false;

            if(column == ThresholdColumn.Undefiend)
            {
                // TODO: Do something if it is undefined, shouldn't be.
                return;
            }

            WriteThresholdHeader(criteria);

            foreach (string key in readOnlyAggResults.Keys)
            {
                readOnlyAggResults.TryGetValue(key, out aggValue);

                switch(column)
                {
                    case ThresholdColumn.PrecentageSharesHeld:
                        {
                            columnValue = aggValue.percentageSharesHeld;
                            thresholdCheck = ValuePassesComparison(columnValue, thresholdValue, comparison);                            
                            break;
                        }
                    case ThresholdColumn.SharesHeld:
                        {
                            columnValue = aggValue.sharesHeld;
                            thresholdCheck = ValuePassesComparison(columnValue, thresholdValue, comparison);
                            break;
                        }
                    case ThresholdColumn.Value:
                        {
                            columnValue = aggValue.value;
                            thresholdCheck = ValuePassesComparison(columnValue, thresholdValue, comparison);
                            break;
                        }
                    default:
                        {
                            break;
                        }
                }

                if (thresholdCheck)
                {
                    criteriaFilterWriter.WriteLine(GetAggValuesCommaSeparatedString(key, aggValue));

                    IReadOnlyList<uint> unaggregatedDictEntry = aggValue.GetRefToRowsInChunk();
                    for (int i = 0; i < unaggregatedDictEntry.Count; i++)
                    {
                        rawDataChunkIndex = (int)unaggregatedDictEntry[i] >> 16;
                        stockDataIndex = (int)unaggregatedDictEntry[i] & 0xffff;
                        StockDataEntry sde = Global.parsedRawData[rawDataChunkIndex].chunkDataParsed[stockDataIndex];
                        criteriaFilterDetailsWriter.WriteLine(GetCommaSeparatedStockDataEntryWithKey(sde, key));
                    }
                    criteriaThresholdWriter.WriteLine(GetCommaSeparatedThresholdEntry(key, columnValue, thresholdValue));
                }
            }

            criteriaFilterWriter.Dispose();
            criteriaFilterDetailsWriter.Dispose();
            criteriaThresholdWriter.Dispose();
        }

        private string GetCommaSeparatedThresholdEntry(string key, double columnValue, double thresholdValue)
        {
            return key + "," + Convert.ToString(columnValue) + "," + Convert.ToString(thresholdValue);
        }

        private void WriteThresholdHeader(CriteriaSetObj criteriaSet)
        {
            // TODO: create constants
            criteriaThresholdWriter.WriteLine("Set," + criteriaSet.criteriaSetName);
            criteriaThresholdWriter.WriteLine("Date," + DateTime.Today.ToString("MM/dd/yyyy"));

            if(criteriaSet.aggSpecsObj.stockCode)
            {
                criteriaThresholdWriter.WriteLine("Agg Key Column," + CriteriaConstants.STOCK_CODE);
            }
            if(criteriaSet.aggSpecsObj.stockType)
            {
                criteriaThresholdWriter.WriteLine("Agg Key Column," + CriteriaConstants.STOCK_TYPE);
            }
            if(criteriaSet.aggSpecsObj.holderId)
            {
                criteriaThresholdWriter.WriteLine("Agg Key Column," + CriteriaConstants.HOLDER_ID);
            }
            if(criteriaSet.aggSpecsObj.direction)
            {
                criteriaThresholdWriter.WriteLine("Agg Key Column," + CriteriaConstants.DIRECTION);
            }

            criteriaThresholdWriter.WriteLine();
            criteriaThresholdWriter.WriteLine("Agg Key,Column Value,Threshold Crossed");
        }

        private string GetCommaSeparatedStockDataEntryWithKey(StockDataEntry sde, string key)
        {
            return sde.stockCode + "," + sde.stockType + "," + sde.holderId + "," + sde.holderCountry + "," + Convert.ToString(sde.sharesHeld) + "," + Convert.ToString(sde.precentageSharesHeld) + "," + sde.direction + "," + Convert.ToString(sde.value) + "," + key;       
        }

        private string GetAggValuesCommaSeparatedString(string key, AggValues agg)
        {
            return key + "," + Convert.ToString(agg.percentageSharesHeld) + "," + Convert.ToString(agg.sharesHeld) + "," + Convert.ToString(agg.value);
        }

        private bool ValuePassesComparison(double columnValue, double thresholdValue, ThresholdComparison tc)
        {
            switch(tc)
            {
                case ThresholdComparison.GreaterThan:
                    {
                        if (columnValue > thresholdValue)
                        {
                            return true;
                        }
                        break;
                    }
                case ThresholdComparison.GreaterThanOrEqualTo:
                    {
                        if (columnValue >= thresholdValue)
                        {
                            return true;
                        }
                        break;
                    }
                case ThresholdComparison.LessThan:
                    {
                        if (columnValue < thresholdValue)
                        {
                            return true;
                        }
                        break;
                    }
                case ThresholdComparison.LessThanOrEqualTo:
                    {
                        if(columnValue <= thresholdValue)
                        {
                            return true;
                        }
                        break;
                    }
            }
            return false;
        }


    }
}
