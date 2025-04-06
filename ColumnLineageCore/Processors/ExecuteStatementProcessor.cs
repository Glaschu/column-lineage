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
    /// Processes ExecuteStatement fragments to trace lineage for procedure parameters and result sets.
    /// </summary>
    public class ExecuteStatementProcessor : IStatementProcessor<ExecuteStatement>
    {
        private readonly IAstProvider _astProvider;

        public ExecuteStatementProcessor(IAstProvider astProvider)
        {
            _astProvider = astProvider ?? throw new ArgumentNullException(nameof(astProvider));
        }

        public void Process(ExecuteStatement statement, IProcessingContext context)
        {
            // Add temporary logging to identify the runtime type - Using Console.WriteLine for visibility
            Console.WriteLine($"[DEBUG] ExecuteSpecification Runtime Type: {statement.ExecuteSpecification?.GetType().FullName}");

            ExecuteSpecification? execSpec = statement.ExecuteSpecification; // Define execSpec early
            if (execSpec?.ExecutableEntity == null) return;

            // string? procedureName = null;
            // SchemaObjectName? schemaObjectName = null;
            IList<ExecuteParameter> execParameters = new List<ExecuteParameter>();

            /* // Temporarily commenting out problematic block due to ScriptDom v170 issues
            // Check the type of the entity being executed using 'is'
            if (execSpec.ExecutableEntity is ProcedureReference procRef)
            {
                schemaObjectName = procRef.Name;
                procedureName = schemaObjectName?.Identifiers?.LastOrDefault()?.Value;
                System.Diagnostics.Debug.WriteLine($"[Processor] Identified ProcedureReference: {procedureName}.");

                // Now that we know it's a procedure, try accessing Parameters on execSpec
                if (execSpec.Parameters != null) // Check execSpec for Parameters
                {
                    execParameters = execSpec.Parameters;
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Warning: execSpec.Parameters is null when ExecutableEntity is ProcedureReference.");
                }
            }
            else if (execSpec.ExecutableEntity is FunctionCall funcCall)
            {
                schemaObjectName = new SchemaObjectName();
                if (funcCall.FunctionName != null) {
                    schemaObjectName.Identifiers.Add(funcCall.FunctionName);
                }
                procedureName = schemaObjectName?.Identifiers?.LastOrDefault()?.Value;
                System.Diagnostics.Debug.WriteLine($"[Processor] Identified FunctionCall: {procedureName}.");

                // Try accessing Parameters on execSpec for functions too
                if (execSpec.Parameters != null) // Check execSpec for Parameters
                {
                    // Note: This might still fail if funcCall parameters are different type
                    // execParameters = execSpec.Parameters; // Potential type mismatch
                    System.Diagnostics.Debug.WriteLine($"[Processor] Found execSpec.Parameters for FunctionCall. Type check needed if assigning.");
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Warning: execSpec.Parameters is null when ExecutableEntity is FunctionCall.");
                }
            }
            else if (execSpec.ExecutableEntity is VariableReference varRef)
            {
                procedureName = varRef.Name;
                System.Diagnostics.Debug.WriteLine($"[Processor] Identified VariableReference execution: {procedureName}.");
                // Try accessing Parameters on execSpec for variables
                if (execSpec.Parameters != null) // Check execSpec for Parameters
                {
                    execParameters = execSpec.Parameters;
                }
                 else
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Warning: execSpec.Parameters is null when ExecutableEntity is VariableReference.");
                }
            }
            else
            {
                string executedElementType = execSpec.ExecutableEntity.GetType().Name;
                System.Diagnostics.Debug.WriteLine($"[Processor] Warning: ExecuteStatementProcessor encountered an unhandled ExecutableEntity type: {executedElementType}.");
                return;
            }
            */ // End of commented out block

            // Check if we successfully identified a procedure or function name
            // Since the block above is commented, procedureName will likely be null.
            // Add a temporary return or default assignment to avoid downstream errors for now.
             System.Diagnostics.Debug.WriteLine("[Processor] ExecuteStatement processing temporarily skipped due to commented-out logic.");
             return; // Exit early for now

            // The following code is unreachable due to the return statement above
            /*
            if (string.IsNullOrEmpty(procedureName))
            {
                 // Log based on the type we actually encountered
                 // execSpec is now guaranteed to be non-null here if we reached this point
                 string entityTypeName = execSpec.ExecutableEntity.GetType().FullName ?? "Unknown";
                 System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not determine procedure name from executable entity (Type: {entityTypeName}). Parameter and output lineage skipped.");
                 return; // Cannot proceed without a name
            }

            // Use the full schema object name if available for more accurate lookups later
            string fullProcedureNameForLookup = schemaObjectName?.ToString() ?? procedureName;
            System.Diagnostics.Debug.WriteLine($"[Processor] Processing EXECUTE {fullProcedureNameForLookup}...");

            // --- Parameter Lineage (Existing Logic - slightly adjusted) ---

            if (!context.ViewProvider.TryGetViewDefinition(procedureName, out string? procDefinitionSql) || procDefinitionSql == null)
            {
                System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not find definition for procedure '{procedureName}'. Parameter and output lineage skipped.");
                return; // Cannot process parameters or output without definition
            }

            // Attempt to parse parameters even if definition exists
            var expectedParameters = ParameterExtractor.ExtractProcedureParameters(procDefinitionSql);
            if (expectedParameters != null)
            {
                // ProcessParameterLineage(execParameters, expectedParameters, procedureName, context); // Keep commented
            }
            else
            {
                System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not parse parameters for procedure '{procedureName}'. Parameter lineage skipped.");
                // Continue to attempt output analysis
            }


            // --- Procedure Output Analysis (New Logic) ---
            // Keep commented
            try
            {
                // Parse the procedure definition itself
                TSqlFragment? procedureAst = _astProvider.Parse(procDefinitionSql, out IList<ParseError> procParseErrors);

                if (procParseErrors.Any())
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Parse errors encountered in definition of procedure '{procedureName}'. Output analysis may be inaccurate.");
                    // Continue if possible, AST might still be partially usable
                }

                if (procedureAst != null)
                {
                    // Extract output columns using a helper method
                    List<OutputColumn> outputColumns = ExtractOutputColumns(procedureAst, context);

                    if (outputColumns.Any())
                    {
                        // Register the found output columns in the context
                        context.RegisterProcedureOutput(procedureName, outputColumns);
                        System.Diagnostics.Debug.WriteLine($"[Processor] Successfully registered {outputColumns.Count} output columns for {procedureName}.");
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine($"[Processor] No output columns (SELECT statements) found or extracted for procedure {procedureName}.");
                    }
                }
                else
                {
                     System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Parsing procedure definition for '{procedureName}' returned null AST. Output analysis skipped.");
                }
            }
            catch (Exception ex)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Error during output analysis for procedure '{procedureName}': {ex.Message}");
                 // Decide if this should halt further processing or just be logged
            }
            */
        } // End of Process method

        /// <summary>
        /// Handles lineage tracing for procedure parameters (input and output).
        /// </summary>
        private void ProcessParameterLineage(IList<ExecuteParameter> execParameters, IList<ProcedureParameter> expectedParameters, string procedureName, IProcessingContext context)
        {
             if (execParameters.Count > expectedParameters.Count)
             {
                  System.Diagnostics.Debug.WriteLine($"[Processor] Warning: More parameters provided in EXEC {procedureName} ({execParameters.Count}) than defined ({expectedParameters.Count}). Matching by order might be incorrect.");
             }

             // Simple matching by order for now. Could be enhanced to match by name.
             for (int i = 0; i < Math.Min(execParameters.Count, expectedParameters.Count); i++)
             {
                 var execParam = execParameters[i];
                 var defParam = expectedParameters[i];

                 // Skip if parameter name is missing in definition (shouldn't happen with valid parse)
                 if (defParam.VariableName?.Value == null) continue;

                 string procParamName = defParam.VariableName.Value;
                 // Parameter node belongs to the procedure scope
                 var procParamNode = context.Graph.AddNode(new ColumnNode(procParamName, procedureName));

                 if (execParam.IsOutput)
                 {
                     // Lineage for OUTPUT parameters: Proc Param -> Variable
                     if (defParam.Modifier == ParameterModifier.Output || defParam.Modifier == ParameterModifier.ReadOnly) // Check if defined as OUTPUT/READONLY
                     {
                         if (execParam.Variable?.Name != null)
                         {
                             string outputVariableName = execParam.Variable.Name;
                             // Variable node exists in the calling scope (null table name)
                             var variableNode = context.Graph.AddNode(new ColumnNode(outputVariableName, null));
                             context.Graph.AddEdge(procParamNode, variableNode);
                             System.Diagnostics.Debug.WriteLine($"[Processor] Added OUTPUT parameter lineage edge: {procParamNode.Id} -> {variableNode.Id}");
                         }
                     }
                     else
                     {
                          System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Parameter '{procParamName}' used as OUTPUT in EXEC but not defined as OUTPUT/READONLY in {procedureName}.");
                     }
                 }
                 else // Input parameter
                 {
                     // Lineage for INPUT parameters: Source Column(s) -> Proc Param
                     if (execParam.ParameterValue != null)
                     {
                         var columnFinder = new ColumnReferenceFinder();
                         execParam.ParameterValue.Accept(columnFinder);

                         if (!columnFinder.ColumnReferences.Any() && execParam.ParameterValue is VariableReference execVarRef)
                         {
                             // Handle direct variable reference as input parameter source
                             var directSourceNode = context.Graph.AddNode(new ColumnNode(execVarRef.Name, null));
                             context.Graph.AddEdge(directSourceNode, procParamNode);
                             System.Diagnostics.Debug.WriteLine($"[Processor] Added INPUT parameter (variable) lineage edge: {directSourceNode.Id} -> {procParamNode.Id}");
                         }
                         else
                         {
                             // Handle column references within expressions
                             foreach (var sourceColRef in columnFinder.ColumnReferences)
                             {
                                 // Resolve the source of the column reference in the current context
                                 SelectScalarExpressionProcessor.ResolveColumnReferenceSource(sourceColRef, context, procParamName + "_input_source", out _, out var directSource);
                                 if (directSource != null)
                                 {
                                     // Ensure source node exists (might already be added by other processors)
                                     directSource = context.Graph.AddNode(directSource);
                                     context.Graph.AddEdge(directSource, procParamNode);
                                     System.Diagnostics.Debug.WriteLine($"[Processor] Added INPUT parameter (column ref) lineage edge: {directSource.Id} -> {procParamNode.Id}");
                                 }
                                 else
                                 {
                                      System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not resolve source for input parameter '{procParamName}' from column reference '{sourceColRef.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value}'.");
                                 }
                             }
                         }
                     }
                 }
             }
        }

        /// <summary>
        /// Analyzes the AST of a procedure to find its output columns (typically from the last SELECT).
        /// </summary>
        private List<OutputColumn> ExtractOutputColumns(TSqlFragment procedureAst, IProcessingContext context)
        {
            System.Diagnostics.Debug.WriteLine($"[Processor] Attempting to extract output columns from procedure AST...");
            // TODO: Implement the visitor logic to find the last SELECT statement
            // TODO: Use a temporary context to analyze the SELECT statement's QueryExpression
            // TODO: Adapt the resulting OutputColumns to represent procedure output

            var finder = new ProcedureOutputFinder(context); // Pass context for factory access
            procedureAst.Accept(finder);

            if (finder.LastSelectQueryExpression != null)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Found candidate SELECT query expression: {finder.LastSelectQueryExpression.GetType().Name}");
                 // Analyze this query expression in isolation
                 return AnalyzeProcedureQuery(finder.LastSelectQueryExpression, context);
            }
            else
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] No SELECT statement found in procedure AST.");
                 return new List<OutputColumn>(); // No output columns found
            }
        }

        /// <summary>
        /// Analyzes a QueryExpression found within a procedure in an isolated context.
        /// </summary>
        private List<OutputColumn> AnalyzeProcedureQuery(QueryExpression queryExpression, IProcessingContext originalContext)
        {
            // Create a temporary, isolated context for analyzing the procedure's internal query
            // It needs its own graph and potentially copies some state, but crucially uses the *same* factory and providers.
            var tempGraph = new LineageGraph(); // Discarded after analysis
            var tempCache = new Dictionary<string, LineageResult>(StringComparer.OrdinalIgnoreCase); // Isolated cache
            // Note: We don't pass initial CTE scope from original context, assume proc is self-contained for now.
            var tempContext = new ProcessingContext(tempGraph, originalContext.ProcessorFactory, originalContext.ViewProvider, tempCache);
            tempContext.IsSubquery = true; // Treat internal proc analysis like a subquery to avoid polluting main graph directly if processors aren't careful

            List<OutputColumn> procedureOutputs = new List<OutputColumn>();

            try
            {
                // Use the processor factory from the *original* context to get the right processor
                if (queryExpression is QuerySpecification querySpec)
                {
                    var processor = originalContext.ProcessorFactory.GetProcessor(querySpec);
                    if (processor is IQueryExpressionProcessor<QuerySpecification> queryProcessor)
                    {
                        // Process the query using the temporary context
                        procedureOutputs = queryProcessor.ProcessQuery(querySpec, tempContext);
                    }
                }
                else if (queryExpression is BinaryQueryExpression binaryQuery)
                {
                     var processor = originalContext.ProcessorFactory.GetProcessor(binaryQuery);
                     if (processor is IQueryExpressionProcessor<BinaryQueryExpression> queryProcessor)
                     {
                          procedureOutputs = queryProcessor.ProcessQuery(binaryQuery, tempContext);
                     }
                }
                // Add other QueryExpression types if needed

                 System.Diagnostics.Debug.WriteLine($"[Processor] Isolated analysis produced {procedureOutputs.Count} raw output columns.");

                // TODO: Adapt output columns - set TableName to procedure name?
                // For now, return them as-is, assuming ProcessQuery provides usable OutputColumn objects.
                // Adaptation might be needed depending on how ProcessQuery populates SourceNode.
            }
            catch (Exception ex)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Error during isolated analysis of procedure query: {ex.Message}");
                 // Return empty list on error
                 return new List<OutputColumn>();
            }

            return procedureOutputs;
        }


        // --- Helper Visitor ---

        /// <summary>
        /// Visitor to find the last SelectStatement within a procedure's AST.
        /// </summary>
        private class ProcedureOutputFinder : TSqlFragmentVisitor
        {
            private readonly IProcessingContext _context; // Needed for factory
            public QueryExpression? LastSelectQueryExpression { get; private set; } = null;

            public ProcedureOutputFinder(IProcessingContext context)
            {
                 _context = context;
            }

            // We are interested in top-level SELECTs within the procedure body
            public override void Visit(SelectStatement node)
            {
                 System.Diagnostics.Debug.WriteLine($"[ProcedureOutputFinder] Visiting SelectStatement...");
                 if (node.QueryExpression != null)
                 {
                      LastSelectQueryExpression = node.QueryExpression; // Keep track of the latest one found
                      System.Diagnostics.Debug.WriteLine($"[ProcedureOutputFinder] Found potential output QueryExpression: {LastSelectQueryExpression.GetType().Name}");
                 }
                 base.Visit(node); // Continue traversal in case of nested selects? (Probably not needed for last select)
            }

            // Optional: Prevent descending into certain structures if needed
            // public override void Visit(ProcedureStatementBody node) { base.Visit(node); }
            // public override void Visit(StatementList node) { base.Visit(node); }
        }

        // --- End Helper Visitor ---


         private bool SourceProvidesColumn(SourceInfo sourceInfo, string columnName, IProcessingContext context)
        {
            switch (sourceInfo.Type)
            {
                case SourceType.Table: return true;
                case SourceType.CTE:
                    if (context.TryResolveCte(sourceInfo.Name, out var cteInfo) && cteInfo != null && cteInfo.IsProcessed)
                        return cteInfo.OutputColumnSources.ContainsKey(columnName);
                    return false;
                case SourceType.Subquery:
                    return sourceInfo.SubqueryOutputColumns?.Any(c => c.OutputName.Equals(columnName, StringComparison.OrdinalIgnoreCase)) ?? false;
                default: return false;
            }
        }

        private void ProcessTableReference(TableReference tableReference, IProcessingContext context)
        {
            try
            {
                if (tableReference is NamedTableReference namedRef) context.ProcessorFactory.GetProcessor(namedRef).Process(namedRef, context);
                else if (tableReference is QueryDerivedTable derivedRef) context.ProcessorFactory.GetProcessor(derivedRef).Process(derivedRef, context);
                else if (tableReference is JoinTableReference joinRef) context.ProcessorFactory.GetProcessor(joinRef).Process(joinRef, context);
                else System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported TableReference type in EXEC context: {tableReference.GetType().Name}");
            }
            catch (Exception ex)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Error processing TableReference in EXEC context: {ex.Message} - Type: {tableReference.GetType().Name}");
            }
        }
    } // End of ExecuteStatementProcessor class
} // End of namespace
