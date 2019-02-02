using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace StockAnalysis
{
    public static class CriteriaConstants
    {
        public const string HOLDER_COUNTRY                      = "holdercountry";
        public const string STOCK_TYPE                          = "stocktype";
        public const string DIRECTION                           = "direction";
        public const string PRECENT_SHARES_HELD                 = "percentagesharesheld";
        public const string SHARES_HELD                         = "sharesheld";
        public const string VALUE                               = "value";
        public const string STOCK_CODE                          = "stockcode";
        public const string HOLDER_ID                           = "holderid";
        public const string IN_MATCH_SPECIFIER                  = "IN";        
        public const string MAX_THRESHOLD_SPECIFIER             = "MAX";
        public const string CROSSES_THRESHOLD_SPECIFIER         = "CROSSES";
        public const string EQUAL_MATCH_SPECIFIER               = "=";
        public const string NOT_EQUAL_MATCH_SPECIFIER           = "<>";
        public const string GREATER_THAN_COMPARER               = ">";
        public const string GREATER_THAN_EQUAL_TO_COMPARERE     = ">=";
        public const string LESS_THAN_COMPARER                  = "<";
        public const string LESS_THAN_OR_EQUAL_COMPARER         = "<=";
    }

    // All enums have 'Undefined' because it will be the default value for enum definition.
    internal enum StockType {Undefined, Common, Preferred};
    internal enum StockDirection {Undefined, Short, Long};
    internal enum ThresholdColumn {Undefiend, PrecentageSharesHeld, SharesHeld, Value};
    internal enum ThresholdComparison {Undefined, GreaterThan, GreaterThanOrEqualTo, LessThan, LessThanOrEqualTo, Equal, NotEqual, Crosses, Max};
    internal enum Comparison {Undefined, GreaterThan, GreaterThanOrEqualTo, LessThan, LessThanOrEqualTo, Equal, NotEqual, IN};
    internal enum AssignTo {Undefined, HolderCountry, Direction, StockType};
    //********************************************************************************************************************************
    internal class CriteriaSetObj
    {
        internal string criteriaSetName { get; private set; }
        private int localProcCtxCount = 0; // TODO: Use this value to know when CriteriaSetObj Pre-Agg and Agg functionality is finished.
        internal PreAggSpecs preAggObj = new PreAggSpecs();
        internal AggSpecs aggSpecsObj = new AggSpecs();
        internal PostAggSpecs postAggObj = new PostAggSpecs();
        // TKeys for the Dictionaries will be a string comprised of stockcode, stocktype, holderid, direction
        private object dictionaryLock = new object(); // used to lock dictionarys when aggregating
        private Dictionary<string, AggValues> aggResults = new Dictionary<string, AggValues>();
        // TODO: only need yesterday if CROSSES keyword is found. Create a function that will initialize this object.
        // TODO: make crossed case
        private Dictionary<string, AggValues> yesterdayAggResults = new Dictionary<string, AggValues>();

        internal bool SetCriteriaName(string name)
        {
            if (criteriaSetName == null)
            {
                criteriaSetName = name;
                return true;
            }
            return false;
        }
        
        // Hopefully this is the correct way to give access to this dictionary as read-only.
        internal IReadOnlyDictionary<string, AggValues> GetRefToAggResults()
        {
            return aggResults;
        }

        internal IReadOnlyDictionary<string, AggValues> GetRefToYesterdayAggResults()
        {
            return yesterdayAggResults;
        }

        internal void AddValueToDictionary(bool yesterdayData, string key, double percentageSharesHeld, double sharesHeld, double value, uint rawDataRowId)
        {
            Dictionary<string, AggValues> sumDictionary;
            AggValues aggTemp;

            if(yesterdayData)
            {
                sumDictionary = yesterdayAggResults;
            }
            else
            {
                sumDictionary = aggResults;
            }

            lock (dictionaryLock)
            {
                if (sumDictionary.TryGetValue(key, out aggTemp))
                {
                    // TODO: make sure reference is the same, and that dictionary updates.
                    aggTemp.AddToValue(value);
                    aggTemp.AddToSharesHeld(sharesHeld);
                    aggTemp.AddToPercentSharesHeld(percentageSharesHeld);
                    aggTemp.AddRowChunkReference(rawDataRowId);
                }
                else
                {
                    aggTemp = new AggValues();
                    aggTemp.AddToValue(value);
                    aggTemp.AddToSharesHeld(sharesHeld);
                    aggTemp.AddToPercentSharesHeld(percentageSharesHeld);
                    aggTemp.AddRowChunkReference(rawDataRowId);
                    sumDictionary.Add(key, aggTemp);
                }
            }            
        }

        internal bool ValidateParsingCompletion()
        {
            if(preAggObj.preAggSpecificationsSatisfied && aggSpecsObj.aggSpecificationsSatisfied && postAggObj.postAggSpecificationsSatisfied)
            {
                return true;
            }
            return false;
        }
    }

    //********************************************************************************************************************************
    internal class AggValues
    {
        internal double percentageSharesHeld { get; private set; }
        internal double sharesHeld { get; private set; }
        internal double value { get; private set; }
        private List<uint> rowInChunkEntries = new List<uint>(); //first 16 bits is chunk index, second 16 bits is stock entry

        internal IReadOnlyList<uint> GetRefToRowsInChunk()
        {
            return rowInChunkEntries;
        }

        internal AggValues()
        {
            percentageSharesHeld = 0;
            sharesHeld = 0;
            value = 0;
        }

        internal void AddRowChunkReference(uint rowChunkEntry)
        {
            rowInChunkEntries.Add(rowChunkEntry);
        }

        internal void AddToPercentSharesHeld(double morePercSharesHeld)
        {
            percentageSharesHeld += morePercSharesHeld;
        }

        internal void AddToSharesHeld(double moreSharesHeld)
        {
            sharesHeld += moreSharesHeld;
        }

        internal void AddToValue(double moreValue)
        {
            value += moreValue;
        }
    }

    //********************************************************************************************************************************
    internal class PreAggSpecs
    {
        private enum PreAggProgress {HaveNothing, HaveName, HaveComparisonType, HaveComparisonValue};
        private PreAggProgress progress = PreAggProgress.HaveNothing;
        internal bool preAggSpecificationsSatisfied = false;
        internal AssignTo currentAssignment;
        // The values used for the Pre-Aggregation filtering
        internal Comparison stockTypeCompare;
        internal Comparison directionCompare;
        internal Comparison countryCompare;
        internal StockType type;
        internal StockDirection direction;
        internal List<string> holderCountry = new List<string>();        

        // TODO: return bool
        // This relates to the line that goes with '@'.
        internal void ChangeCurrentAssignee(string newAssignment)
        {
            switch(newAssignment)
            {
                case CriteriaConstants.HOLDER_COUNTRY:
                    {
                        currentAssignment = AssignTo.HolderCountry;
                        break;
                    }
                case CriteriaConstants.STOCK_TYPE:
                    {
                        currentAssignment = AssignTo.StockType;
                        break;
                    }
                case CriteriaConstants.DIRECTION:
                    {
                        currentAssignment = AssignTo.Direction;
                        break;
                    }
                default:
                    {                        
                        break;
                    }
            }

            if(progress == PreAggProgress.HaveNothing)
            {
                progress = PreAggProgress.HaveName;
            }
        }

        // This relates to the line that goes with '^'.
        internal void AddComparisonType(string specifier)
        {
            switch(specifier)
            {
                case CriteriaConstants.IN_MATCH_SPECIFIER:
                    {
                        switch(currentAssignment)
                        {
                            case AssignTo.Undefined:
                                {
                                    // TODO: return bool
                                    break;
                                }
                            case AssignTo.Direction:
                                {
                                    directionCompare = Comparison.IN;
                                    break;
                                }
                            case AssignTo.HolderCountry:
                                {
                                    countryCompare = Comparison.IN;
                                    break;
                                }
                            case AssignTo.StockType:
                                {
                                    stockTypeCompare = Comparison.IN;
                                    break;
                                }
                        }
                        break;
                    }
                case CriteriaConstants.EQUAL_MATCH_SPECIFIER:
                    {
                        switch (currentAssignment)
                        {
                            case AssignTo.Undefined:
                                {
                                    // TODO: return bool
                                    break;
                                }
                            case AssignTo.Direction:
                                {
                                    directionCompare = Comparison.Equal;
                                    break;
                                }
                            case AssignTo.HolderCountry:
                                {
                                    countryCompare = Comparison.Equal;
                                    break;
                                }
                            case AssignTo.StockType:
                                {
                                    stockTypeCompare = Comparison.Equal;
                                    break;
                                }
                        }
                        break;
                    }
                case CriteriaConstants.NOT_EQUAL_MATCH_SPECIFIER:
                    {
                        switch (currentAssignment)
                        {
                            case AssignTo.Undefined:
                                {
                                    // TODO: return bool
                                    break;
                                }
                            case AssignTo.Direction:
                                {
                                    directionCompare = Comparison.NotEqual;
                                    break;
                                }
                            case AssignTo.HolderCountry:
                                {
                                    countryCompare = Comparison.NotEqual;
                                    break;
                                }
                            case AssignTo.StockType:
                                {
                                    stockTypeCompare = Comparison.NotEqual;
                                    break;
                                }
                        }
                        break;
                    }
                default:
                    {
                        break;
                    }
            }
            if (progress == PreAggProgress.HaveName)
            {
                progress = PreAggProgress.HaveComparisonType;
            }
        }

        // This relates to the line that goes with '#'.
        internal void AddComparisonValue(string value)
        {
            switch (currentAssignment)
            {
                case AssignTo.Undefined:
                    {
                        // TODO: Return bool
                        break;
                    }
                case AssignTo.Direction:
                    {
                        if (string.Compare(value, StockDirection.Long.ToString().ToLower()) == 0)
                        {
                            direction = StockDirection.Long;
                        }
                        else
                        {
                            direction = StockDirection.Short;
                        }
                        break;
                    }
                case AssignTo.HolderCountry:
                    {
                        holderCountry.Add(value);
                        break;
                    }
                case AssignTo.StockType:
                    {
                        if (string.Compare(value, StockType.Common.ToString().ToLower()) == 0)
                        {
                            type = StockType.Common;
                        }
                        else
                        {
                            type = StockType.Preferred;
                        }
                        break;
                    }
                default:
                    {
                        break;
                    }
            }

            if (progress == PreAggProgress.HaveComparisonType)
            {
                progress = PreAggProgress.HaveComparisonValue;
                preAggSpecificationsSatisfied = true;
            }
        }
    }

    //********************************************************************************************************************************
    internal class AggSpecs
    {
        private enum AggProgress {HaveNothing, HaveAggKey, HaveSumColumn};
        private AggProgress progress = AggProgress.HaveNothing;
        internal bool aggSpecificationsSatisfied = false;
        // Values used for the AggKey
        internal bool stockType { get; private set; } = false;
        internal bool direction { get; private set; } = false;
        internal bool stockCode { get; private set; } = false;
        internal bool holderId { get; private set; } = false;
        // Sumation values
        private bool precentageSharesHeld = false;
        private bool sharesHeld = false;
        private bool value = false;

        internal string CreateAggKeyWithAggValues(StockDataEntry data)
        {
            // TODO: May not be good to create 'new' AggValues, might de-reference.
            StringBuilder sb = new StringBuilder();
            // AggKey
            if (stockCode)
            {
                sb.Append(data.stockCode);
            }
            if(stockType)
            {
                if(sb.Length != 0)
                {
                    sb.Append("~");
                }
                sb.Append(data.stockType);
            }
            if(holderId)
            {
                if (sb.Length != 0)
                {
                    sb.Append("~");
                }
                sb.Append(data.holderId);
            }
            if(direction)
            {
                if (sb.Length != 0)
                {
                    sb.Append("~");
                }
                sb.Append(data.direction);
            }

            return sb.ToString();
        }

        // TODO: return bool
        // This relates to the line that goes with '*'.
        internal void SetAggregationKey(string value)
        {
            switch(value)
            {
                case CriteriaConstants.STOCK_CODE:
                    {
                        stockCode = true;
                        break;
                    }
                case CriteriaConstants.HOLDER_ID:
                    {
                        holderId = true;
                        break;
                    }
                case CriteriaConstants.STOCK_TYPE:
                    {
                        stockType = true;
                        break;
                    }
                case CriteriaConstants.DIRECTION:
                    {
                        direction = true;
                        break;
                    }
                default:
                    {
                        break;
                    }

            }

            if(progress == AggProgress.HaveNothing)
            {
                progress = AggProgress.HaveAggKey;
            }
        }

        // This relates to the line that goes with '+'.
        internal void SetColumnSummations(string columnName)
        {
            switch (columnName)
            {
                case CriteriaConstants.PRECENT_SHARES_HELD:
                    {
                        precentageSharesHeld = true;
                        break;
                    }
                case CriteriaConstants.SHARES_HELD:
                    {
                        sharesHeld = true;
                        break;
                    }
                case CriteriaConstants.VALUE:
                    {
                        value = true;
                        break;
                    }
                default:
                    {
                        break;
                    }
            }

            if (progress == AggProgress.HaveAggKey)
            {
                progress = AggProgress.HaveSumColumn;
                aggSpecificationsSatisfied = true;
            }
        }
    }
    //********************************************************************************************************************************
    internal class PostAggSpecs
    {
        private enum PostAggProgress {HaveNothing, HaveThresholdColumn, HaveThresholdComparison, HaveValue};
        private PostAggProgress progress = PostAggProgress.HaveNothing;
        internal bool postAggSpecificationsSatisfied = false;
        internal ThresholdColumn thresholdColumn { get; private set; } // If 'threshold' is 'Value' then need to store what the 'Comparison' will be
        private List<double> thresholdValues = new List<double>();
        internal ThresholdComparison comparison { get; private set; }

        internal IReadOnlyList<double> GetRefToThresholdValues()
        {
            return thresholdValues;
        }
        // This relates to the line that goes with '#'.
        internal void AddThresholdValue(double value)
        {
            thresholdValues.Add(value);
            if(progress == PostAggProgress.HaveThresholdComparison)
            {
                progress = PostAggProgress.HaveValue;
                postAggSpecificationsSatisfied = true;
            }
        }

        // This relates to the line that goes with '$'.
        internal void SetThresholdColumn(string column)
        {
            switch(column)
            {
                case CriteriaConstants.PRECENT_SHARES_HELD:
                    {
                        thresholdColumn = ThresholdColumn.PrecentageSharesHeld;
                        break;
                    }
                case CriteriaConstants.SHARES_HELD:
                    {
                        thresholdColumn = ThresholdColumn.SharesHeld;
                        break;
                    }
                case CriteriaConstants.VALUE:
                    {
                        thresholdColumn = ThresholdColumn.Value;
                        break;
                    }
                default:
                    {
                        break;
                    }
            }

            if(progress == PostAggProgress.HaveNothing)
            {
                progress = PostAggProgress.HaveThresholdColumn;
            }
        }

        // This relates to the line that goes with '&'.
        internal void SetThresholdComparison(string compareValue)
        {
            switch(compareValue)
            {
                case CriteriaConstants.MAX_THRESHOLD_SPECIFIER:
                    {
                        comparison = ThresholdComparison.Max;
                        break;
                    }
                case CriteriaConstants.CROSSES_THRESHOLD_SPECIFIER:
                    {
                        comparison = ThresholdComparison.Crosses;
                        break;
                    }
                case CriteriaConstants.GREATER_THAN_COMPARER:
                    {
                        comparison = ThresholdComparison.GreaterThan;
                        break;
                    }
                case CriteriaConstants.GREATER_THAN_EQUAL_TO_COMPARERE:
                    {
                        comparison = ThresholdComparison.GreaterThanOrEqualTo;
                        break;
                    }
                case CriteriaConstants.LESS_THAN_COMPARER:
                    {
                        comparison = ThresholdComparison.LessThan;
                        break;
                    }
                case CriteriaConstants.LESS_THAN_OR_EQUAL_COMPARER:
                    {
                        comparison = ThresholdComparison.LessThanOrEqualTo;
                        break;
                    }
                default:
                    {
                        break;
                    }
            }

            if (progress == PostAggProgress.HaveThresholdColumn)
            {
                progress = PostAggProgress.HaveThresholdComparison;
            }
        }
    }
    //********************************************************************************************************************************
}
