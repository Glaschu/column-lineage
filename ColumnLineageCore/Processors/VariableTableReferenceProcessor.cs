using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Model;
using ColumnLineageCore.Helpers;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes variable table references like @MyTable (table variables).
    /// Table variables are declared with DECLARE @var TABLE(...) and used in queries.
    /// </summary>
    public class VariableTableReferenceProcessor : ITableReferenceProcessor<VariableTableReference>
    {
        public void Process(VariableTableReference varRef, IProcessingContext context)
        {
            if (varRef == null || context == null) return;
            
            // Extract the variable name
            string variableName = varRef.Variable?.Name ?? "unknown_variable";
            System.Diagnostics.Debug.WriteLine($"[Processor] Processing table variable reference: {variableName}");
            
            // Extract the table alias (if any) or use the variable name
            string tableAlias = varRef.Alias?.Value ?? variableName;
            
            // Register the variable as a table source
            // Note: We treat table variables as regular tables for lineage purposes
            var sourceInfo = new SourceInfo(variableName, SourceType.Table);
            context.CurrentSourceMap[tableAlias] = sourceInfo;
            
            System.Diagnostics.Debug.WriteLine($"[Processor] Added table variable '{variableName}' with alias '{tableAlias}' to source map.");
            
            // Note: The actual column structure of table variables would need to be tracked
            // from DECLARE statements, which is beyond the current scope.
            // For lineage purposes, we treat them as opaque tables where columns
            // will be resolved when actually referenced in SELECT clauses.
        }
    }
}
