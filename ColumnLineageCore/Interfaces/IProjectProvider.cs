using System.Collections.Generic;

namespace ColumnLineageCore.Interfaces
{
    /// <summary>
    /// Defines a contract for discovering and parsing SQL Server Database Projects (SSDT).
    /// </summary>
    public interface IProjectProvider
    {
        /// <summary>
        /// Discovers all SQL objects in an SSDT project.
        /// </summary>
        /// <param name="projectPath">Path to the SSDT project file (.sqlproj) or project directory.</param>
        /// <returns>A collection of discovered SQL objects.</returns>
        IEnumerable<SqlObjectDefinition> DiscoverSqlObjects(string projectPath);
    }

    /// <summary>
    /// Represents a SQL object (table, view, procedure, function) discovered in an SSDT project.
    /// </summary>
    public class SqlObjectDefinition
    {
        public string Name { get; set; } = string.Empty;
        public string Schema { get; set; } = "dbo";
        public string FullName => $"[{Schema}].[{Name}]";
        public SqlObjectType Type { get; set; }
        public string FilePath { get; set; } = string.Empty;
        public string SqlContent { get; set; } = string.Empty;
        public List<string> Dependencies { get; set; } = new();
        public List<string> Parameters { get; set; } = new();
        public List<string> Columns { get; set; } = new();
        public Dictionary<string, object> Metadata { get; set; } = new();
        
        // For OpenLineage export
        public string DatasetNamespace { get; set; } = string.Empty;
        public string DatasetName => FullName;
        public List<SqlColumnDefinition> ColumnDefinitions { get; set; } = new();
    }

    /// <summary>
    /// Represents a column definition for OpenLineage export.
    /// </summary>
    public class SqlColumnDefinition
    {
        public string Name { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public Dictionary<string, object> Tags { get; set; } = new();
    }

    /// <summary>
    /// Types of SQL objects that can be discovered.
    /// </summary>
    public enum SqlObjectType
    {
        Table,
        View,
        StoredProcedure,
        Function,
        Schema
    }
}
