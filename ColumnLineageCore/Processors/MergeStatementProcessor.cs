using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Helpers;
using ColumnLineageCore.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes MergeStatement fragments.
    /// Handles MERGE operations with WHEN MATCHED, WHEN NOT MATCHED BY TARGET, and WHEN NOT MATCHED BY SOURCE clauses.
    /// Tracks lineage from source expressions to target columns in UPDATE and INSERT operations.
    /// </summary>
    public class MergeStatementProcessor : IStatementProcessor<MergeStatement>
    {
        public void Process(MergeStatement statement, IProcessingContext context)
        {
            if (statement?.MergeSpecification?.Target == null) return; // Invalid MERGE statement

            string? targetTableName = (statement.MergeSpecification.Target as NamedTableReference)?.SchemaObject?.BaseIdentifier?.Value;
            if (string.IsNullOrEmpty(targetTableName))
            {
                System.Diagnostics.Debug.WriteLine("[Processor] Warning: Could not determine target table name for MERGE statement.");
                return;
            }

            System.Diagnostics.Debug.WriteLine($"[Processor] Processing MERGE INTO {targetTableName}...");

            // --- Manage Source Map Scope ---
            var originalSourceMap = context.CurrentSourceMap;
            var scopedSourceMap = new Dictionary<string, SourceInfo>(originalSourceMap, StringComparer.OrdinalIgnoreCase);
            context.CurrentSourceMap = scopedSourceMap;

            // Add the target table to the source map
            scopedSourceMap[targetTableName] = new SourceInfo(targetTableName, SourceType.Table);
            System.Diagnostics.Debug.WriteLine($"[Processor] Added target table '{targetTableName}' to local source map.");

            try
            {
                // --- Process USING Clause (Source) ---
                if (statement.MergeSpecification.TableReference != null)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Processing MERGE USING clause...");
                    ProcessTableReference(statement.MergeSpecification.TableReference, context);
                }

                // --- Process ON Clause (Join Condition) ---
                if (statement.MergeSpecification.SearchCondition != null)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Processing MERGE ON condition...");
                    // Could analyze join condition for lineage if needed
                }

                // --- Process WHEN Clauses ---
                if (statement.MergeSpecification.ActionClauses != null)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Processing MERGE WHEN clauses...");
                    foreach (var actionClause in statement.MergeSpecification.ActionClauses)
                    {
                        ProcessMergeActionClause(actionClause, targetTableName, context);
                    }
                }
            }
            finally
            {
                // Restore original source map
                context.CurrentSourceMap = originalSourceMap;
            }
        }

        /// <summary>
        /// Processes a single MERGE action clause (WHEN MATCHED, WHEN NOT MATCHED, etc.).
        /// </summary>
        private void ProcessMergeActionClause(MergeActionClause actionClause, string targetTableName, IProcessingContext context)
        {
            System.Diagnostics.Debug.WriteLine($"[Processor] Processing MERGE action clause type: {actionClause.GetType().Name}");

            // Try to access the Action property through reflection or direct property access
            // Since the exact type hierarchy is unclear, let's use a more generic approach
            
            // Check if this is an update action
            var actionProperty = actionClause.GetType().GetProperty("Action");
            if (actionProperty != null)
            {
                var action = actionProperty.GetValue(actionClause);
                
                if (action is UpdateMergeAction updateAction)
                {
                    ProcessUpdateMergeAction(updateAction, targetTableName, context);
                }
                else if (action is InsertMergeAction insertAction)
                {
                    ProcessInsertMergeAction(insertAction, targetTableName, context);
                }
                else if (action is DeleteMergeAction)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] DELETE action in MERGE - no lineage to track.");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported MERGE action type: {action?.GetType().Name}");
                }
            }
            else
            {
                System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not find Action property on {actionClause.GetType().Name}");
            }
        }

        /// <summary>
        /// Processes UPDATE action within MERGE statement.
        /// </summary>
        private void ProcessUpdateMergeAction(UpdateMergeAction updateAction, string targetTableName, IProcessingContext context)
        {
            System.Diagnostics.Debug.WriteLine("[Processor] Processing MERGE UPDATE action...");

            if (updateAction.SetClauses != null)
            {
                foreach (var setClause in updateAction.SetClauses)
                {
                    if (setClause is AssignmentSetClause assignment)
                    {
                        ProcessAssignmentSetClause(assignment, targetTableName, context);
                    }
                }
            }
        }

        /// <summary>
        /// Processes INSERT action within MERGE statement.
        /// </summary>
        private void ProcessInsertMergeAction(InsertMergeAction insertAction, string targetTableName, IProcessingContext context)
        {
            System.Diagnostics.Debug.WriteLine("[Processor] Processing MERGE INSERT action...");

            // Get target columns
            var targetColumns = insertAction.Columns?.Select(c => c.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value)
                .Where(name => name != null).ToList();

            // Get source values
            if (insertAction.Source is ValuesInsertSource valuesSource && valuesSource.RowValues != null)
            {
                foreach (var rowValue in valuesSource.RowValues)
                {
                    if (rowValue.ColumnValues != null && targetColumns != null)
                    {
                        for (int i = 0; i < Math.Min(targetColumns.Count, rowValue.ColumnValues.Count); i++)
                        {
                            string targetColName = targetColumns[i]!;
                            var sourceExpression = rowValue.ColumnValues[i];

                            ProcessMergeInsertColumnMapping(sourceExpression, targetColName, targetTableName, context);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Processes a single assignment clause in MERGE UPDATE (SET TargetCol = SourceExpr).
        /// </summary>
        private void ProcessAssignmentSetClause(AssignmentSetClause assignment, string targetTableName, IProcessingContext context)
        {
            if (assignment.Column?.MultiPartIdentifier?.Identifiers == null || !assignment.Column.MultiPartIdentifier.Identifiers.Any())
            {
                System.Diagnostics.Debug.WriteLine("[Processor] Warning: Skipping MERGE SET clause with invalid target column.");
                return;
            }

            string targetColumnName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;
            var targetNode = new ColumnNode(targetColumnName, targetTableName);
            context.Graph.AddNode(targetNode);

            System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing MERGE SET {targetNode.Id} = ...");

            if (assignment.NewValue == null) return;

            // Find column references within the source expression
            var columnFinder = new ColumnReferenceFinder();
            assignment.NewValue.Accept(columnFinder);

            if (!columnFinder.ColumnReferences.Any())
            {
                System.Diagnostics.Debug.WriteLine($"[Processor] No source column references found for MERGE SET {targetNode.Id}.");
                return;
            }

            // Resolve each source column reference and add edge
            foreach (var sourceColRef in columnFinder.ColumnReferences)
            {
                ResolveSourceAndAddEdge(sourceColRef, targetNode, context);
            }
        }

        /// <summary>
        /// Processes column mapping in MERGE INSERT operation.
        /// </summary>
        private void ProcessMergeInsertColumnMapping(ScalarExpression sourceExpression, string targetColumnName, string targetTableName, IProcessingContext context)
        {
            var targetNode = new ColumnNode(targetColumnName, targetTableName);
            context.Graph.AddNode(targetNode);

            System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing MERGE INSERT {targetNode.Id} = ...");

            // Find column references within the source expression
            var columnFinder = new ColumnReferenceFinder();
            sourceExpression.Accept(columnFinder);

            if (!columnFinder.ColumnReferences.Any())
            {
                System.Diagnostics.Debug.WriteLine($"[Processor] No source column references found for MERGE INSERT {targetNode.Id}.");
                return;
            }

            // Resolve each source column reference and add edge
            foreach (var sourceColRef in columnFinder.ColumnReferences)
            {
                ResolveSourceAndAddEdge(sourceColRef, targetNode, context);
            }
        }

        /// <summary>
        /// Helper to resolve a source ColumnReferenceExpression and add lineage edge to target.
        /// </summary>
        private void ResolveSourceAndAddEdge(ColumnReferenceExpression sourceColRef, ColumnNode targetNode, IProcessingContext context)
        {
            var parts = sourceColRef.MultiPartIdentifier?.Identifiers;
            if (parts == null || !parts.Any()) return;

            string sourceColumnName = parts.Last().Value;
            string? sourceIdentifier = parts.Count > 1 ? parts[parts.Count - 2].Value : null;

            SourceInfo? sourceInfo = null;
            string? resolvedSourceIdentifier = null;

            if (sourceIdentifier != null)
            {
                // Qualified column
                if (context.CurrentSourceMap.TryGetValue(sourceIdentifier, out sourceInfo))
                {
                    resolvedSourceIdentifier = sourceIdentifier;
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not resolve source identifier '{sourceIdentifier}' in MERGE context.");
                    return;
                }
            }
            else
            {
                // Unqualified column - try to resolve
                var candidateSources = context.CurrentSourceMap.Values
                    .Where(si => si.Type == SourceType.Table || si.Type == SourceType.CTE)
                    .ToList();

                if (candidateSources.Count == 1)
                {
                    sourceInfo = candidateSources.First();
                    resolvedSourceIdentifier = sourceInfo.Name;
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Ambiguous or unresolvable source for column '{sourceColumnName}' in MERGE context.");
                    return;
                }
            }

            if (sourceInfo != null && resolvedSourceIdentifier != null)
            {
                var sourceNode = new ColumnNode(sourceColumnName, sourceInfo.Name);
                context.Graph.AddNode(sourceNode);
                context.Graph.AddEdge(sourceNode, targetNode);
                System.Diagnostics.Debug.WriteLine($"[Processor] Added MERGE lineage edge: {sourceNode.Id} -> {targetNode.Id}");
            }
        }

        /// <summary>
        /// Helper to process table references (similar to other processors).
        /// </summary>
        private void ProcessTableReference(TableReference tableRef, IProcessingContext context)
        {
            // Use the processor factory to handle different table reference types
            if (tableRef is NamedTableReference namedTableRef)
            {
                context.ProcessorFactory.GetProcessor(namedTableRef).Process(namedTableRef, context);
            }
            else if (tableRef is JoinTableReference joinTableRef)
            {
                context.ProcessorFactory.GetProcessor(joinTableRef).Process(joinTableRef, context);
            }
            else if (tableRef is QueryDerivedTable queryDerivedTable)
            {
                context.ProcessorFactory.GetProcessor(queryDerivedTable).Process(queryDerivedTable, context);
            }
            else if (tableRef is PivotedTableReference pivotTableRef)
            {
                context.ProcessorFactory.GetProcessor(pivotTableRef).Process(pivotTableRef, context);
            }
            else if (tableRef is UnpivotedTableReference unpivotTableRef)
            {
                context.ProcessorFactory.GetProcessor(unpivotTableRef).Process(unpivotTableRef, context);
            }
            else if (tableRef is VariableTableReference varTableRef)
            {
                context.ProcessorFactory.GetProcessor(varTableRef).Process(varTableRef, context);
            }
            else
            {
                System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported table reference type in MERGE USING: {tableRef.GetType().Name}");
            }
        }
    }
}
