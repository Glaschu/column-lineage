using ColumnLineageCore.Model;
using ColumnLineageCore.Json; // Added for JSON classes
using System.Collections.Generic;
using System.Linq; // Added for LINQ
using System.Text.Json; // Added for JSON serialization

namespace ColumnLineageCore
{
    /// <summary>
    /// Represents the result of the column lineage analysis, containing the graph structure.
    /// </summary>
    public class LineageResult
    {
        /// <summary>
        /// Gets the set of all unique column nodes identified in the analysis.
        /// Using HashSet for efficient lookups and uniqueness.
        /// </summary>
        public HashSet<ColumnNode> Nodes { get; }

        /// <summary>
        /// Gets the set of all unique lineage edges (relationships) identified.
        /// Using HashSet for efficient lookups and uniqueness.
        /// </summary>
        public HashSet<LineageEdge> Edges { get; }

        /// <summary>
        /// Gets the list of parsing errors encountered.
        /// </summary>
        public IList<Microsoft.SqlServer.TransactSql.ScriptDom.ParseError> Errors { get; set; } // Made public set for simplicity

        public LineageResult()
        {
            Nodes = new HashSet<ColumnNode>();
            Edges = new HashSet<LineageEdge>();
            Errors = new List<Microsoft.SqlServer.TransactSql.ScriptDom.ParseError>(); // Initialize error list
        }

        /// <summary>
        /// Constructor to initialize with results and errors.
        /// </summary>
        public LineageResult(List<ColumnNode> nodes, List<LineageEdge> edges, IList<Microsoft.SqlServer.TransactSql.ScriptDom.ParseError> errors)
        {
            Nodes = new HashSet<ColumnNode>(nodes ?? Enumerable.Empty<ColumnNode>());
            Edges = new HashSet<LineageEdge>(edges ?? Enumerable.Empty<LineageEdge>());
            Errors = errors ?? new List<Microsoft.SqlServer.TransactSql.ScriptDom.ParseError>();
        }

        /// <summary>
        /// Helper method to add a node, ensuring uniqueness.
        /// </summary>
        public void AddNode(ColumnNode node)
        {
            Nodes.Add(node);
        }

        /// <summary>
        /// Helper method to add an edge, ensuring uniqueness.
        /// Also adds the source and target nodes if they don't exist.
        /// </summary>
        public void AddEdge(ColumnNode source, ColumnNode target)
        {
            if (source == null || target == null) return; // Or throw exception

            AddNode(source);
            AddNode(target);
            Edges.Add(new LineageEdge(source.Id, target.Id));
        }

        /// <summary>
        /// Serializes the lineage graph to a JSON string.
        /// </summary>
        /// <param name="indented">Whether to format the JSON output with indentation.</param>
        /// <returns>A JSON string representing the graph.</returns>
        public string ToJson(bool indented = false)
        {
            var jsonGraph = new JsonGraph
            {
                Nodes = this.Nodes.Select(n => new JsonNode(n)).ToList(),
                Edges = this.Edges.Select(e => new JsonEdge(e)).ToList()
            };

            var options = new JsonSerializerOptions
            {
                WriteIndented = indented,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase // Use camelCase for JSON properties
            };

            return JsonSerializer.Serialize(jsonGraph, options);
        }
    }
}
