using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Model;
using ColumnLineageCore.Helpers;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes DELETE statements to track column lineage.
    /// DELETE statements can reference columns in WHERE clauses and FROM clauses (for multi-table deletes).
    /// </summary>
    public class DeleteStatementProcessor : IStatementProcessor<DeleteStatement>
    {
        public void Process(DeleteStatement deleteStmt, IProcessingContext context)
        {
            if (deleteStmt == null) throw new ArgumentNullException(nameof(deleteStmt));
            if (context == null) throw new ArgumentNullException(nameof(context));

            System.Diagnostics.Debug.WriteLine($"[Processor] Processing DELETE statement on target: {GetTableName(deleteStmt.DeleteSpecification.Target)}");

            // Store the original source map to restore after processing
            var originalSourceMap = context.CurrentSourceMap;
            var scopedSourceMap = new Dictionary<string, SourceInfo>(originalSourceMap, StringComparer.OrdinalIgnoreCase);
            context.CurrentSourceMap = scopedSourceMap;

            try
            {
                // Process the target table (what's being deleted from)
                ProcessDeleteTarget(deleteStmt.DeleteSpecification, context);

                // Process FROM clause if present (for multi-table DELETE with JOINs)
                if (deleteStmt.DeleteSpecification.FromClause != null)
                {
                    foreach (var tableReference in deleteStmt.DeleteSpecification.FromClause.TableReferences)
                    {
                        ProcessTableReference(tableReference, context);
                    }
                }

                // Process WHERE clause if present
                if (deleteStmt.DeleteSpecification.WhereClause != null)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] TODO: Process DELETE WHERE clause for column references...");
                    // For now, we skip WHERE clause processing since it doesn't directly affect lineage
                    // but it could reference columns that create implicit dependencies
                }

                // Note: DELETE statements don't typically produce output columns for lineage tracking
                // They consume/reference input columns but don't generate new ones
                // However, they can affect data flow if tracked for complete data movement analysis
            }
            finally
            {
                // Restore the original source map
                context.CurrentSourceMap = originalSourceMap;
            }
        }

        private void ProcessDeleteTarget(DeleteSpecification deleteSpec, IProcessingContext context)
        {
            if (deleteSpec.Target != null)
            {
                string targetTableName = GetTableName(deleteSpec.Target);
                System.Diagnostics.Debug.WriteLine($"[Processor] DELETE target table: {targetTableName}");
                
                // Add the target table to the source map for potential column references
                var sourceInfo = new SourceInfo(targetTableName, SourceType.Table);
                string aliasOrName = GetTableAlias(deleteSpec.Target) ?? targetTableName;
                context.CurrentSourceMap[aliasOrName] = sourceInfo;
            }
        }

        private void ProcessTableReference(TableReference tableReference, IProcessingContext context)
        {
            try
            {
                if (tableReference is NamedTableReference namedRef)
                {
                    context.ProcessorFactory.GetProcessor(namedRef).Process(namedRef, context);
                }
                else if (tableReference is QueryDerivedTable derivedRef)
                {
                    context.ProcessorFactory.GetProcessor(derivedRef).Process(derivedRef, context);
                }
                else if (tableReference is JoinTableReference joinRef)
                {
                    context.ProcessorFactory.GetProcessor(joinRef).Process(joinRef, context);
                }
                else if (tableReference is PivotedTableReference pivotRef)
                {
                    context.ProcessorFactory.GetProcessor(pivotRef).Process(pivotRef, context);
                }
                else if (tableReference is UnpivotedTableReference unpivotRef)
                {
                    context.ProcessorFactory.GetProcessor(unpivotRef).Process(unpivotRef, context);
                }
                else if (tableReference is VariableTableReference varRef)
                {
                    context.ProcessorFactory.GetProcessor(varRef).Process(varRef, context);
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported TableReference type in DELETE FROM clause: {tableReference.GetType().Name}");
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[Processor] Error processing TableReference in DELETE: {ex.Message} - Type: {tableReference.GetType().Name}");
            }
        }

        private string GetTableName(TableReference tableRef)
        {
            return tableRef switch
            {
                NamedTableReference namedTable => GetSchemaObjectName(namedTable.SchemaObject),
                QueryDerivedTable queryTable => queryTable.Alias?.Value ?? "derived_table",
                _ => "unknown_table"
            };
        }

        private string GetTableAlias(TableReference tableRef)
        {
            return tableRef switch
            {
                NamedTableReference namedTable => namedTable.Alias?.Value,
                QueryDerivedTable queryTable => queryTable.Alias?.Value,
                _ => null
            };
        }

        private string GetSchemaObjectName(SchemaObjectName schemaObject)
        {
            if (schemaObject?.Identifiers?.Count > 0)
            {
                return schemaObject.Identifiers[schemaObject.Identifiers.Count - 1].Value;
            }
            return "unknown_object";
        }
    }
}
