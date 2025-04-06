using ColumnLineageCore.Model;

namespace ColumnLineageCore.Json
{
    /// <summary>
    /// Represents a node in the JSON graph output.
    /// </summary>
    public class JsonNode
    {
        /// <summary>
        /// Unique identifier for the node (e.g., "Table1.col1" or "col1").
        /// </summary>
        public string? Id { get; set; } // Made nullable

        /// <summary>
        /// Display label for the node (typically the column name).
        /// </summary>
        public string? Label { get; set; } // Made nullable

        /// <summary>
        /// The source table or CTE name, if applicable.
        /// </summary>
        public string? Source { get; set; }

        // Parameterless constructor for deserialization if needed later
        public JsonNode() { }

        public JsonNode(ColumnNode node)
        {
            Id = node.Id;
            Label = node.Name;
            Source = node.SourceName;
        }
    }
}
