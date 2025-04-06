using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Helpers;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes NamedTableReference fragments (references to tables, views, or CTEs).
    /// Updates the processing context's source map.
    /// </summary>
    public class NamedTableReferenceProcessor : ITableReferenceProcessor<NamedTableReference>
    {
        public void Process(NamedTableReference tableRef, IProcessingContext context)
        {
            ArgumentNullException.ThrowIfNull(tableRef);
            ArgumentNullException.ThrowIfNull(context);

            // SchemaObject can be null for things like VariableTableReference, but should exist for NamedTableReference
            string? sourceName = tableRef.SchemaObject?.BaseIdentifier?.Value;
            if (string.IsNullOrWhiteSpace(sourceName))
            {
                // This shouldn't typically happen for a valid NamedTableReference, but handle defensively.
                System.Diagnostics.Debug.WriteLine($"Warning: NamedTableReference encountered without a valid base identifier."); // Removed .Dump()
                return;
            }

            // Use the alias if provided, otherwise use the source name itself as the key in the map.
            string aliasOrName = tableRef.Alias?.Value ?? sourceName;

            // Check if this name corresponds to a CTE available in the current scope.
            if (context.TryResolveCte(sourceName, out CteInfo? cteInfo))
            {
                // It's a CTE reference
                var sourceInfo = new SourceInfo(sourceName, SourceType.CTE); // Use real CTE name in SourceInfo
                context.CurrentSourceMap[aliasOrName] = sourceInfo;
                System.Diagnostics.Debug.WriteLine($"[Processor] Added CTE source '{sourceName}' with alias/key '{aliasOrName}' to source map.");

                // Ensure the intermediate nodes for the CTE's output columns exist in the graph
                // (This might be redundant if CteScopeProcessor already added them, but safe to ensure)
                if (cteInfo != null && cteInfo.IsProcessed) // Only add nodes if CTE was successfully processed
                {
                    foreach (var outputColName in cteInfo.OutputColumnSources.Keys)
                    {
                        // Add the intermediate node (e.g., "MyCTE.ColumnA")
                        context.Graph.AddNode(new Model.ColumnNode(outputColName, cteInfo.Name));
                    }
                }
            }
            else
            {
                // It's assumed to be a base table/view reference
                var sourceInfo = new SourceInfo(sourceName, SourceType.Table); // Use table name in SourceInfo
                context.CurrentSourceMap[aliasOrName] = sourceInfo;
                System.Diagnostics.Debug.WriteLine($"[Processor] Added Table source '{sourceName}' with alias/key '{aliasOrName}' to source map.");
                // We don't know the table's columns here, so nodes are added when columns are referenced (e.g., in SelectElementProcessor)
            }
        }
    }
}
