using System;
using System.Collections.Generic;
using System.Text;

namespace StockAnalysis
{
    internal class OutputFileWriter: ThreadProcRequest
    {
        internal CriteriaPostAggFiltered postAggFiltered;
        internal CriteriaPostAggFilteredDetails postAggDetails;
        internal CriteriaPostAggThreshold postAggThreshold;
        private string outputDir;

        public OutputFileWriter(string directory)
        {
            outputDir = directory;
            postAggFiltered = new CriteriaPostAggFiltered();
            postAggDetails = new CriteriaPostAggFilteredDetails();
            postAggThreshold = new CriteriaPostAggThreshold();
        }

        public override void Execute(ref bool destory)
        {
            // TODO: write to directory the 3 files   
        }
    }

    internal class CriteriaPostAggFiltered
    {
        private Dictionary<string, AggValues> thresholdRelevantEntries = new Dictionary<string, AggValues>();

        internal void AddToOutputEntry(string key, AggValues values)
        {
            thresholdRelevantEntries.Add(key, values);
        }

        internal IReadOnlyDictionary<string, AggValues> GetRefToFilteredDictionary()
        {
            return thresholdRelevantEntries;
        }
    }

    internal class CriteriaPostAggFilteredDetails
    {
        private List<StockDataEntry> dividedSummedStockData = new List<StockDataEntry>();
        
        internal void AddStockEntry(StockDataEntry sde)
        {
            dividedSummedStockData.Add(sde);
        }

        internal IReadOnlyList<StockDataEntry> GetReferenceToStockEntryList()
        {
            return dividedSummedStockData;
        }
    }

    internal class CriteriaPostAggThreshold
    {
        private string criteriaSetName;
        internal DateTime todaysDate { get; private set; } = DateTime.Today; // TODO: Check format of Today
        private string commaSeparatedAggKeyColumns;
        private string postAggColumn;
        private List<ThresholdAggOutputValues> outputThresholdEntries = new List<ThresholdAggOutputValues>();
        
        internal void AddCriteriaSetName(string name)
        {
            criteriaSetName = name;
        }

        internal string GetTodaysDate()
        {
            return todaysDate.ToString("MM/dd/yyyy");
        }

        internal void AddCommaSeparatedAggKeyColumns(string commaSeparatedColumns)
        {
            commaSeparatedAggKeyColumns = commaSeparatedColumns;
        }

        internal void AddThresholdOutputValue(string aggKey, double columnValue, double thresholdCrossed)
        {
            ThresholdAggOutputValues taov = new ThresholdAggOutputValues(aggKey, columnValue, thresholdCrossed);
            outputThresholdEntries.Add(taov);
        }

        internal IReadOnlyList<ThresholdAggOutputValues> GetRefToOutputThresholdList()
        {
            return outputThresholdEntries;
        }
    }

    internal class ThresholdAggOutputValues
    {
        internal string aggKey { get; private set; }
        internal double columnValue { get; private set; }
        internal double crossedThreshold { get; private set; }

        internal ThresholdAggOutputValues(string key, double column, double threshold)
        {
            aggKey = key;
            columnValue = column;
            crossedThreshold = threshold;
        }
    }
}
