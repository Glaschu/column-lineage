using ColumnLineageCore.Model;

namespace ColumnLineageCore.Interfaces
{
    /// <summary>
    /// Defines the contract for managing the lineage graph (nodes and edges).
    /// Implementations are responsible for ensuring node/edge uniqueness.
    /// </summary>
    public interface ILineageGraph
    {
        /// <summary>
        /// Adds a column node to the graph if it doesn't already exist.
        /// </summary>
        /// <param name="node">The column node to add.</param>
        /// <returns>The added or existing node.</returns>
        ColumnNode AddNode(ColumnNode node);

        /// <summary>
        /// Adds a lineage edge between two nodes if it doesn't already exist.
        /// Ensures both source and target nodes exist in the graph before adding the edge.
        /// </summary>
        /// <param name="source">The source node.</param>
        /// <param name="target">The target node.</param>
        /// <returns>The added or existing edge.</returns>
        LineageEdge AddEdge(ColumnNode source, ColumnNode target);

        /// <summary>
        /// Gets the complete collection of nodes in the graph.
        /// </summary>
        IEnumerable<ColumnNode> Nodes { get; }

        /// <summary>
        /// Gets the complete collection of edges in the graph.
        /// </summary>
        IEnumerable<LineageEdge> Edges { get; }

        // Potentially add methods for finding nodes/edges later if needed
    }
}
