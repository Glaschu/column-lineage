using System;
using System.Diagnostics;

namespace ColumnLineageCore.Model
{
    /// <summary>
    /// Represents a directed edge in the lineage graph, indicating data flow
    /// from a source column node to a target column node.
    /// </summary>
    [DebuggerDisplay("{SourceNodeId} -> {TargetNodeId}")]
    public class LineageEdge : IEquatable<LineageEdge>
    {
        /// <summary>
        /// The identifier of the source column node.
        /// </summary>
        public string SourceNodeId { get; }

        /// <summary>
        /// The identifier of the target column node.
        /// </summary>
        public string TargetNodeId { get; }

        public LineageEdge(string sourceNodeId, string targetNodeId)
        {
            SourceNodeId = sourceNodeId ?? throw new ArgumentNullException(nameof(sourceNodeId));
            TargetNodeId = targetNodeId ?? throw new ArgumentNullException(nameof(targetNodeId));
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as LineageEdge);
        }

        public bool Equals(LineageEdge? other)
        {
            return other != null &&
                   SourceNodeId == other.SourceNodeId &&
                   TargetNodeId == other.TargetNodeId;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(SourceNodeId, TargetNodeId);
        }

        public static bool operator ==(LineageEdge? left, LineageEdge? right)
        {
            if (ReferenceEquals(left, right)) return true;
            if (left is null || right is null) return false;
            return left.Equals(right);
        }

        public static bool operator !=(LineageEdge? left, LineageEdge? right)
        {
            return !(left == right);
        }
    }
}
