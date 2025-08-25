using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Model;
using ColumnLineageCore.Helpers;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes UNPIVOT table references to track column transformations in UNPIVOT operations.
    /// UNPIVOT transforms columns into rows, which is the inverse of PIVOT.
    /// </summary>
    public class UnpivotedTableReferenceProcessor : ITableReferenceProcessor<UnpivotedTableReference>
    {
        public void Process(UnpivotedTableReference unpivotRef, IProcessingContext context)
        {
            if (unpivotRef == null || context == null) return;
            
            // First, process the source table reference using the processor pattern
            if (unpivotRef.TableReference is NamedTableReference namedTableRef)
            {
                var processor = context.ProcessorFactory.GetProcessor(namedTableRef);
                processor.Process(namedTableRef, context);
            }
            else if (unpivotRef.TableReference is QueryDerivedTable queryTableRef)
            {
                var processor = context.ProcessorFactory.GetProcessor(queryTableRef);
                processor.Process(queryTableRef, context);
            }
            else if (unpivotRef.TableReference is JoinTableReference joinTableRef)
            {
                var processor = context.ProcessorFactory.GetProcessor(joinTableRef);
                processor.Process(joinTableRef, context);
            }

            // Extract the source table alias or name
            string sourceTableName = GetTableName(unpivotRef.TableReference);
            
            // Extract the value column name (what the unpivoted values will be called)
            string valueColumnName = GetIdentifierName(unpivotRef.ValueColumn);
            
            // Extract the pivot column name (what the source column names will be called)
            string pivotColumnName = GetIdentifierName(unpivotRef.PivotColumn);
            
            // Process the IN columns - these are the source columns being unpivoted
            if (unpivotRef.InColumns != null)
            {
                foreach (var inColumn in unpivotRef.InColumns)
                {
                    string sourceColumnName = GetColumnName(inColumn);
                    
                    // Create lineage from each source column to the value column
                    var sourceNode = new ColumnNode(sourceColumnName, sourceTableName);
                    var valueNode = new ColumnNode(valueColumnName, sourceTableName);
                    
                    context.Graph.AddEdge(sourceNode, valueNode);
                    
                    // The pivot column gets the column name as a value, but we track the lineage
                    // Note: This is more about structure than data values, so we track the transformation
                }
            }

            // Register the unpivot table alias in the source map
            string unpivotTableAlias = unpivotRef.Alias?.Value ?? $"{sourceTableName}_unpivot";
            var sourceInfo = new SourceInfo(unpivotTableAlias, SourceType.Table);
            
            // Add the output columns to the source map
            context.CurrentSourceMap[valueColumnName] = sourceInfo;
            context.CurrentSourceMap[pivotColumnName] = sourceInfo;
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

        private string GetSchemaObjectName(SchemaObjectName schemaObject)
        {
            if (schemaObject?.Identifiers?.Count > 0)
            {
                return schemaObject.Identifiers[schemaObject.Identifiers.Count - 1].Value;
            }
            return "unknown_object";
        }

        private string GetColumnName(ColumnReferenceExpression column)
        {
            if (column?.MultiPartIdentifier?.Identifiers?.Count > 0)
            {
                return column.MultiPartIdentifier.Identifiers[column.MultiPartIdentifier.Identifiers.Count - 1].Value;
            }
            return "unknown_column";
        }

        private string GetIdentifierName(Identifier identifier)
        {
            return identifier?.Value ?? "unknown_identifier";
        }
    }
}
