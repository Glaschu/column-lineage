using ColumnLineageCore.Model;

namespace ColumnLineageCore.Json
{
    /// <summary>
    /// Represents an edge in the JSON graph output.
    /// </summary>
    public class JsonEdge
    {
        /// <summary>
        /// The ID of the source node.
        /// </summary>
        public string? Source { get; set; } // Made nullable

        /// <summary>
        /// The ID of the target node.
        /// </summary>
        public string? Target { get; set; } // Made nullable

        // Parameterless constructor for deserialization if needed later
        public JsonEdge() { }

        public JsonEdge(LineageEdge edge)
        {
            Source = edge.SourceNodeId;
            Target = edge.TargetNodeId;
        }
    }
}
