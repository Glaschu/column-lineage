using System.Collections.Generic; // Added for List
// Removed using ColumnLineageCore.Helpers;

namespace ColumnLineageCore.Helpers // Corrected namespace
{
    public enum SourceType { Table, CTE, Subquery, View } // Added View

    public class SourceInfo // Changed internal to public
    {
        public string Name { get; } // Real name of the table/CTE or Alias of the subquery
        public SourceType Type { get; }
        // Stores the output structure of a subquery
        public List<OutputColumn>? SubqueryOutputColumns { get; }

        // Constructor for Table/CTE
        public SourceInfo(string name, SourceType type)
        {
            Name = name;
            Type = type;
            SubqueryOutputColumns = null;
        }

        // Constructor for Subquery or View (which also has defined output columns)
        public SourceInfo(string aliasOrName, List<OutputColumn> outputColumns, SourceType type = SourceType.Subquery) // Default to Subquery for backward compatibility
        {
            // Ensure type is valid for this constructor
            if (type != SourceType.Subquery && type != SourceType.View)
            {
                throw new ArgumentException("This constructor is only for Subquery or View source types.", nameof(type));
            }

            Name = aliasOrName;
            Type = type;
            SubqueryOutputColumns = outputColumns ?? throw new ArgumentNullException(nameof(outputColumns));
        }

        // Overload for View specifically, if needed for clarity, though the above handles it.
        // public SourceInfo(string viewName, List<OutputColumn> outputColumns)
        //     : this(viewName, outputColumns, SourceType.View) { }
    }
}
