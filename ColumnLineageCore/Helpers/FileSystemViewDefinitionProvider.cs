using ColumnLineageCore.Interfaces;
using System.IO;

namespace ColumnLineageCore.Helpers
{
    /// <summary>
    /// Provides view definitions by reading .sql files from a specified directory.
    /// Assumes filename matches view name (e.g., MyView.sql for view MyView).
    /// </summary>
    public class FileSystemViewDefinitionProvider : IViewDefinitionProvider
    {
        private readonly string _viewDirectory;

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="viewDirectory">The directory containing .sql files for view definitions. Defaults to './views'.</param>
        public FileSystemViewDefinitionProvider(string viewDirectory = "views")
        {
            // Consider making the base path configurable or relative to the analyzed script?
            // For now, relative to the current execution directory.
            _viewDirectory = Path.GetFullPath(viewDirectory);
            System.Diagnostics.Debug.WriteLine($"[ViewProvider] Using view directory: {_viewDirectory}");
        }

        public bool TryGetViewDefinition(string viewName, out string? viewDefinition)
        {
            viewDefinition = null;
            if (string.IsNullOrWhiteSpace(viewName)) return false;

            // Basic handling: Assume viewName is just the name, no schema prefix yet.
            // TODO: Handle schema-qualified names (e.g., "dbo.MyView") by potentially looking in subdirs or adjusting filename lookup.
            string fileName = $"{viewName}.sql";
            string filePath = Path.Combine(_viewDirectory, fileName);

            System.Diagnostics.Debug.WriteLine($"[ViewProvider] Attempting to find view definition at: {filePath}");

            if (File.Exists(filePath))
            {
                try
                {
                    viewDefinition = File.ReadAllText(filePath);
                    System.Diagnostics.Debug.WriteLine($"[ViewProvider] Found definition for view '{viewName}'.");
                    return true;
                }
                catch (IOException ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[ViewProvider] Error reading view definition file '{filePath}': {ex.Message}");
                    return false;
                }
            }
            else
            {
                System.Diagnostics.Debug.WriteLine($"[ViewProvider] View definition file not found for '{viewName}'.");
                return false;
            }
        }
    }
}
