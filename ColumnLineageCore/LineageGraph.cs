using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Model;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ColumnLineageCore
{
    /// <summary>
    /// Concrete implementation of ILineageGraph. Manages nodes and edges, ensuring uniqueness.
    /// </summary>
    public class LineageGraph : ILineageGraph
    {
        // Use dictionaries for efficient lookup and uniqueness enforcement by node/edge ID.
        private readonly Dictionary<string, ColumnNode> _nodes = new Dictionary<string, ColumnNode>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, LineageEdge> _edges = new Dictionary<string, LineageEdge>(StringComparer.OrdinalIgnoreCase);

        public IEnumerable<ColumnNode> Nodes => _nodes.Values;
        public IEnumerable<LineageEdge> Edges => _edges.Values;

        public ColumnNode AddNode(ColumnNode node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (!_nodes.TryGetValue(node.Id, out var existingNode))
            {
                _nodes[node.Id] = node;
                System.Diagnostics.Debug.WriteLine($"[LineageGraph] Added Node: {node.Id}");
                return node;
            }
            // Optional: Could verify if existingNode properties match node properties if needed.
            return existingNode;
        }

        public LineageEdge AddEdge(ColumnNode source, ColumnNode target)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (target == null) throw new ArgumentNullException(nameof(target));

            // Ensure nodes exist in the graph first (AddNode handles duplicates)
            // Ensure nodes exist in the graph first (AddNode handles duplicates)
            var actualSource = AddNode(source);
            var actualTarget = AddNode(target);

            // Create the edge using node IDs
            var edge = new LineageEdge(actualSource.Id, actualTarget.Id);
            // Create a unique key for the dictionary
            string edgeKey = GetEdgeId(actualSource.Id, actualTarget.Id);

            if (!_edges.TryGetValue(edgeKey, out var existingEdge))
            {
                _edges[edgeKey] = edge;
                 System.Diagnostics.Debug.WriteLine($"[LineageGraph] Added Edge: {edgeKey}");
                return edge;
            }
            return existingEdge;
        }

        /// <summary>
        /// Helper method to create a consistent ID for an edge based on its source and target.
        /// </summary>
        private string GetEdgeId(string sourceId, string targetId)
        {
            // Simple concatenation, ensure separator prevents ambiguity (e.g., "A.B" -> "C" vs "A" -> "B.C")
            return $"{sourceId}:::->:::{targetId}";
        }
    }
}
