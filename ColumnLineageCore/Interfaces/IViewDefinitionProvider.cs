namespace ColumnLineageCore.Interfaces
{
    /// <summary>
    /// Defines a contract for retrieving the SQL definition text of a database view.
    /// </summary>
    public interface IViewDefinitionProvider
    {
        /// <summary>
        /// Attempts to get the SQL definition for a view.
        /// </summary>
        /// <param name="viewName">The name of the view (potentially schema-qualified).</param>
        /// <param name="viewDefinition">The SQL text of the CREATE VIEW statement if found; otherwise, null.</param>
        /// <returns>True if the view definition was found, false otherwise.</returns>
        bool TryGetViewDefinition(string viewName, out string? viewDefinition);
    }
}
