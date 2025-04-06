using ColumnLineageCore.Model;

namespace ColumnLineageCore.Helpers // Corrected namespace
{
    /// <summary>
    /// Represents information about a single output column from a query or subquery,
    /// including its name and its ultimate source node (if traceable).
    /// </summary>
    public class OutputColumn // Changed internal to public
    {
        /// <summary>
        /// The name of the column in the output of the query/subquery.
        /// </summary>
        public string OutputName { get; }

        /// <summary>
        /// The node representing the ultimate source of this output column (e.g., a base table column).
        /// Can be null if the source is untraceable (e.g., literal, complex expression without alias).
        /// </summary>
        public ColumnNode? SourceNode { get; }

        public OutputColumn(string outputName, ColumnNode? sourceNode)
        {
            OutputName = outputName;
            SourceNode = sourceNode;
        }
    }
}
