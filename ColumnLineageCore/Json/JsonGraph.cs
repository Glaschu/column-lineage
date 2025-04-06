using System.Collections.Generic;

namespace ColumnLineageCore.Json
{
    /// <summary>
    /// Represents the overall graph structure for JSON serialization.
    /// </summary>
    public class JsonGraph
    {
        public List<JsonNode> Nodes { get; set; } = new List<JsonNode>();
        public List<JsonEdge> Edges { get; set; } = new List<JsonEdge>();
    }
}
