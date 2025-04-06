using System;
using System.Diagnostics;

namespace ColumnLineageCore.Model
{
    /// <summary>
    /// Represents a column in the lineage graph.
    /// Can be a source column (table.column) or a target column (result set column).
    /// </summary>
    [DebuggerDisplay("{FullName}")]
    public class ColumnNode : IEquatable<ColumnNode>
    {
        /// <summary>
        /// Unique identifier for the node (e.g., TableName.ColumnName or just ColumnName for result sets).
        /// </summary>
        public string Id { get; }

        /// <summary>
        /// The simple name of the column (e.g., "col1").
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The name of the table or alias this column belongs to (optional, null for result set columns without explicit source).
        /// </summary>
        public string? SourceName { get; }

        /// <summary>
        /// Full name for display/identification (e.g., "Table1.col1" or "col1").
        /// </summary>
        public string FullName => SourceName != null ? $"{SourceName}.{Name}" : Name;

        public ColumnNode(string name, string? sourceName = null)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            SourceName = sourceName;
            Id = FullName; // Use FullName as the initial unique ID. May need refinement later.
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as ColumnNode);
        }

        public bool Equals(ColumnNode? other)
        {
            return other != null && Id == other.Id;
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }

        public static bool operator ==(ColumnNode? left, ColumnNode? right)
        {
            if (ReferenceEquals(left, right)) return true;
            if (left is null || right is null) return false;
            return left.Equals(right);
        }

        public static bool operator !=(ColumnNode? left, ColumnNode? right)
        {
            return !(left == right);
        }
    }
}
