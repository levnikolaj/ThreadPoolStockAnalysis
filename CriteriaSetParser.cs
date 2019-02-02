using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace StockAnalysis
{    
    internal class CriteriaSetParser: ThreadProcRequest
    {
        private const string COMMENT_LINE = "--";
        private const string NEW_CRITERIA_SET_START = "!";
        private const string PRE_AGG_COLUMN = "@";
        private const string COMPARISON_INDICATOR = "^";        
        private const string COMPARISON_VALUE_IDENTIFIER = "#";
        private const string AGG_KEY_COLUMN = "*";
        private const string AGG_SUM_COLUMN = "+";
        private const string POST_AGG_COMPARISON_SPECIFIER = "$";
        private const string COMPARISON_SPECIFIER = "&";

        private enum WorkingState { PreAgg, Aggregation, PostAgg };
        private WorkingState state = WorkingState.PreAgg;
        private StreamReader criteriaSetReader;
        private string criteriaSetFilePath;
        private long sizeOfCriteriaFile;

        public int crossesCount { get; private set; } = 0;

        internal void SetFilePath(string filename)
        {
            criteriaSetFilePath = filename;
        }

        // This execute method parses the entire CriteriaSet.txt in one go. Since it is a small file, one thread should be able to do it.
        public override void Execute(ref bool destory)
        {
            bool criteriaObjCheck; 
            string criteriaLine;
            string lineSpecifier;
            CriteriaSetObj newCriteria = new CriteriaSetObj();
            // TODO: set the completedParsing field to true when done with a criteria set.

            
            using (criteriaSetReader = new StreamReader(criteriaSetFilePath))
            {
                sizeOfCriteriaFile = criteriaSetReader.BaseStream.Length;

                while ((criteriaLine = criteriaSetReader.ReadLine()) != null)
                {
                    if(criteriaLine.Length == 0) // Empty line
                    {
                        if(newCriteria.ValidateParsingCompletion())
                        {
                            Global.AddCriteriaSet(newCriteria);
                            newCriteria = new CriteriaSetObj();
                            state = WorkingState.PreAgg;
                        }
                    }                        
                    else if (string.Compare(COMMENT_LINE, criteriaLine.Substring(0, 2)) != 0) // Skip if line is a comment
                    {
                        lineSpecifier = criteriaLine.Substring(0, 1);
                        switch (lineSpecifier)
                        {
                            case NEW_CRITERIA_SET_START:
                                {
                                    criteriaObjCheck = newCriteria.SetCriteriaName(criteriaLine.Substring(1));
                                    if(!criteriaObjCheck)
                                    {
                                        Program.WriteToConsole("CriteriaSetParser: More than one name associated with CriteriaSet.");
                                    }
                                    break;
                                }
                            case PRE_AGG_COLUMN:
                                {
                                    newCriteria.preAggObj.ChangeCurrentAssignee(criteriaLine.Substring(1));
                                    break;
                                }
                            case COMPARISON_INDICATOR:
                                {
                                    newCriteria.preAggObj.AddComparisonType(criteriaLine.Substring(1));
                                    break;
                                }
                            case COMPARISON_VALUE_IDENTIFIER:
                                {
                                    if(state == WorkingState.PreAgg)
                                    {
                                        newCriteria.preAggObj.AddComparisonValue(criteriaLine.Substring(1));
                                    }
                                    else if(state == WorkingState.PostAgg)
                                    {
                                        // TODO: check this, if it removes commas and converts to double.
                                        string temp = criteriaLine.Substring(1).Replace(",", "");
                                        newCriteria.postAggObj.AddThresholdValue(Convert.ToDouble(temp));
                                    }
                                    else
                                    {
                                        // TODO: State did not change from Aggregation. File error.
                                    }
                                    break;
                                }
                            case AGG_KEY_COLUMN:
                                {
                                    if(state == WorkingState.PreAgg)
                                    {
                                        state = WorkingState.Aggregation;
                                    }
                                    newCriteria.aggSpecsObj.SetAggregationKey(criteriaLine.Substring(1));
                                    break;
                                }
                            case AGG_SUM_COLUMN:
                                {
                                    newCriteria.aggSpecsObj.SetColumnSummations(criteriaLine.Substring(1));
                                    break;
                                }
                            case POST_AGG_COMPARISON_SPECIFIER:
                                {
                                    if(state == WorkingState.Aggregation)
                                    {
                                        state = WorkingState.PostAgg;
                                    }
                                    newCriteria.postAggObj.SetThresholdColumn(criteriaLine.Substring(1));
                                    break;
                                }
                            case COMPARISON_SPECIFIER:
                                {
                                    if(CriteriaConstants.CROSSES_THRESHOLD_SPECIFIER.Equals(criteriaLine.Substring(1)))
                                    {
                                        crossesCount++;
                                    }
                                    newCriteria.postAggObj.SetThresholdComparison(criteriaLine.Substring(1));
                                    break;
                                }
                            default:
                                {
                                    break;
                                }
                        }
                    }
                }

                if (newCriteria.ValidateParsingCompletion())
                {
                    Global.AddCriteriaSet(newCriteria);
                }
            }
                       
        }
    }
}
