using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Model;
using ColumnLineageCore.Helpers;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes PIVOT table references to track column transformations in PIVOT operations.
    /// </summary>
    public class PivotedTableReferenceProcessor : ITableReferenceProcessor<PivotedTableReference>
    {
        public void Process(PivotedTableReference pivotRef, IProcessingContext context)
        {
            if (pivotRef == null || context == null) return;
            
            // First, process the source table reference using the processor pattern
            if (pivotRef.TableReference is NamedTableReference namedTableRef)
            {
                var processor = context.ProcessorFactory.GetProcessor(namedTableRef);
                processor.Process(namedTableRef, context);
            }
            else if (pivotRef.TableReference is QueryDerivedTable queryTableRef)
            {
                var processor = context.ProcessorFactory.GetProcessor(queryTableRef);
                processor.Process(queryTableRef, context);
            }
            else if (pivotRef.TableReference is JoinTableReference joinTableRef)
            {
                var processor = context.ProcessorFactory.GetProcessor(joinTableRef);
                processor.Process(joinTableRef, context);
            }

            // Extract the source table alias or name
            string sourceTableName = GetTableName(pivotRef.TableReference);
            
            // Extract the PIVOT column (the FOR column)
            string pivotColumnName = GetColumnName(pivotRef.PivotColumn);
            
            // Extract the value column(s) being aggregated
            List<string> valueColumnNames = new List<string>();
            if (pivotRef.ValueColumns != null)
            {
                foreach (var valueCol in pivotRef.ValueColumns)
                {
                    valueColumnNames.Add(GetColumnName(valueCol));
                }
            }

            // Process the IN columns - these become the new output columns
            if (pivotRef.InColumns != null)
            {
                foreach (var inColumn in pivotRef.InColumns)
                {
                    string pivotValueName = GetColumnName(inColumn);
                    
                    // Create lineage from source value columns to each pivoted output column
                    foreach (string valueColumnName in valueColumnNames)
                    {
                        var sourceNode = new ColumnNode(valueColumnName, sourceTableName);
                        var targetNode = new ColumnNode(pivotValueName, sourceTableName);
                        
                        context.Graph.AddEdge(sourceNode, targetNode);
                    }
                }
            }

            // Register the pivot table alias in the source map
            string pivotTableAlias = pivotRef.Alias?.Value ?? $"{sourceTableName}_pivot";
            var sourceInfo = new SourceInfo(pivotTableAlias, SourceType.Table);
            
            // Add all the output columns to the source map
            if (pivotRef.InColumns != null)
            {
                foreach (var inColumn in pivotRef.InColumns)
                {
                    string columnName = GetColumnName(inColumn);
                    context.CurrentSourceMap[columnName] = sourceInfo;
                }
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

        private string GetSchemaObjectName(SchemaObjectName schemaObject)
        {
            if (schemaObject?.Identifiers?.Count > 0)
            {
                return schemaObject.Identifiers[schemaObject.Identifiers.Count - 1].Value;
            }
            return "unknown_object";
        }

        private string GetColumnName(TSqlFragment columnFragment)
        {
            return columnFragment switch
            {
                ColumnReferenceExpression colRef => GetMultiPartIdentifierName(colRef.MultiPartIdentifier),
                IdentifierOrValueExpression idValue => idValue.Identifier?.Value ?? idValue.ValueExpression?.ToString() ?? "unknown_column",
                Identifier id => id.Value,
                _ => columnFragment?.ToString() ?? "unknown_column"
            };
        }

        private string GetMultiPartIdentifierName(MultiPartIdentifier identifier)
        {
            if (identifier?.Identifiers?.Count > 0)
            {
                return identifier.Identifiers[identifier.Identifiers.Count - 1].Value;
            }
            return "unknown_identifier";
        }
    }
}
