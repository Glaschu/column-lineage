using ColumnLineageCore.Interfaces;
using System.Collections.Generic;

namespace ColumnLineageCore.Helpers
{
    /// <summary>
    /// Provides view and object definitions from an SSDT project's discovered SQL objects.
    /// </summary>
    public class SsdtViewDefinitionProvider : IViewDefinitionProvider
    {
        private readonly Dictionary<string, string> _objectDefinitions;

        public SsdtViewDefinitionProvider(IEnumerable<SqlObjectDefinition> sqlObjects)
        {
            _objectDefinitions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var obj in sqlObjects)
            {
                // Store with various key formats for flexible lookup
                var keys = new[]
                {
                    obj.Name,                    // ObjectName
                    obj.FullName,               // [Schema].[ObjectName]
                    $"{obj.Schema}.{obj.Name}", // Schema.ObjectName
                    $"[{obj.Schema}].[{obj.Name}]" // [Schema].[ObjectName]
                };

                foreach (var key in keys)
                {
                    _objectDefinitions[key] = obj.SqlContent;
                }
            }

            System.Diagnostics.Debug.WriteLine($"[SsdtViewDefinitionProvider] Loaded {sqlObjects.Count()} SQL objects for lookup");
        }

        public bool TryGetViewDefinition(string viewName, out string? viewDefinition)
        {
            viewDefinition = null;
            
            if (string.IsNullOrWhiteSpace(viewName)) 
                return false;

            // Try exact match first
            if (_objectDefinitions.TryGetValue(viewName, out viewDefinition))
            {
                return true;
            }

            // Try with different casing and bracket combinations
            var variations = new[]
            {
                viewName,
                viewName.Trim('[', ']'),
                $"[{viewName.Trim('[', ']')}]",
                $"dbo.{viewName.Trim('[', ']')}",
                $"[dbo].[{viewName.Trim('[', ']')}]"
            };

            foreach (var variation in variations)
            {
                if (_objectDefinitions.TryGetValue(variation, out viewDefinition))
                {
                    System.Diagnostics.Debug.WriteLine($"[SsdtViewDefinitionProvider] Found definition for '{viewName}' using variation '{variation}'");
                    return true;
                }
            }

            System.Diagnostics.Debug.WriteLine($"[SsdtViewDefinitionProvider] No definition found for view '{viewName}'");
            return false;
        }

        /// <summary>
        /// Gets all available object names for debugging/diagnostics.
        /// </summary>
        public IEnumerable<string> GetAvailableObjectNames()
        {
            return _objectDefinitions.Keys;
        }
    }
}
