using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Helpers;
using ColumnLineageCore.Model;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ColumnLineageCore
{
    /// <summary>
    /// Concrete implementation of IProcessingContext. Manages shared state during analysis.
    /// </summary>
    public class ProcessingContext : IProcessingContext
    {
        public ILineageGraph Graph { get; }
        public IProcessorFactory ProcessorFactory { get; }

        // Stack to manage nested CTE scopes (innermost scope is last in the list)
        private readonly Stack<IDictionary<string, CteInfo>> _cteScopeStack = new Stack<IDictionary<string, CteInfo>>();

        // Stack to manage source maps for nested queries (e.g., derived tables)
        // TODO: Decide if this needs to be a stack or if a single level is sufficient for now.
        // Starting with a single level for simplicity. Processors might need to manage pushing/popping if stack is needed.
        public IDictionary<string, SourceInfo> CurrentSourceMap { get; set; } = new Dictionary<string, SourceInfo>(StringComparer.OrdinalIgnoreCase);

        public bool IsProcessingCteDefinition { get; set; } = false;
        public CteInfo? CteInfoToPopulate { get; set; } = null;
        public bool IsSubquery { get; set; } = false;
        public Dictionary<string, List<string>>? ColumnAvailabilityMap { get; set; } = null;
        public string? IntoClauseTarget { get; set; } = null;
        public IViewDefinitionProvider ViewProvider { get; } // Added
        public IDictionary<string, LineageResult> AnalysisCache { get; } // Added

        // Dictionary to store registered procedure outputs
        // Key: Procedure Name (string, case-insensitive), Value: List of OutputColumn
        private readonly Dictionary<string, List<OutputColumn>> _procedureOutputs = new Dictionary<string, List<OutputColumn>>(StringComparer.OrdinalIgnoreCase);


        /// <summary>
        /// Initializes a new instance of the ProcessingContext.
        /// </summary>
        /// <param name="graph">The lineage graph instance.</param>
        /// <param name="processorFactory">The processor factory instance.</param>
        /// <param name="viewProvider">The view definition provider instance.</param> // Added
        /// <param name="analysisCache">The shared cache for view/proc analysis results.</param> // Added
        /// <param name="initialCteScope">Optional initial (e.g., script-level) CTE scope.</param>
        public ProcessingContext(
            ILineageGraph graph,
            IProcessorFactory processorFactory,
            IViewDefinitionProvider viewProvider, // Added
            IDictionary<string, LineageResult> analysisCache, // Added
            IDictionary<string, CteInfo>? initialCteScope = null)
        {
            Graph = graph ?? throw new ArgumentNullException(nameof(graph));
            ProcessorFactory = processorFactory ?? throw new ArgumentNullException(nameof(processorFactory));
            ViewProvider = viewProvider ?? throw new ArgumentNullException(nameof(viewProvider)); // Added
            AnalysisCache = analysisCache ?? throw new ArgumentNullException(nameof(analysisCache)); // Added

            // Push the global/empty scope first
            _cteScopeStack.Push(initialCteScope ?? new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase));
        }

        public void PushCteScope(IDictionary<string, CteInfo> ctesInScope)
        {
            if (ctesInScope == null) throw new ArgumentNullException(nameof(ctesInScope));
            // Push the new scope onto the stack. Resolution will check this first.
            _cteScopeStack.Push(ctesInScope);
             System.Diagnostics.Debug.WriteLine($"[Context] Pushed CTE Scope with {ctesInScope.Count} CTEs. Stack depth: {_cteScopeStack.Count}");
        }

        public void PopCteScope()
        {
            if (_cteScopeStack.Count <= 1) // Prevent popping the initial base scope
            {
                throw new InvalidOperationException("Cannot pop the base CTE scope.");
            }
            var poppedScope = _cteScopeStack.Pop();
             System.Diagnostics.Debug.WriteLine($"[Context] Popped CTE Scope with {poppedScope.Count} CTEs. Stack depth: {_cteScopeStack.Count}");
        }

        public bool TryResolveCte(string cteName, out CteInfo? cteInfo)
        {
            if (string.IsNullOrWhiteSpace(cteName))
            {
                cteInfo = null;
                return false;
            }

            // Iterate through the stack from top (innermost scope) downwards
            foreach (var scope in _cteScopeStack)
            {
                if (scope.TryGetValue(cteName, out cteInfo))
                {
                     System.Diagnostics.Debug.WriteLine($"[Context] Resolved CTE '{cteName}' in scope.");
                    return true; // Found in this scope
                }
            }

             System.Diagnostics.Debug.WriteLine($"[Context] Failed to resolve CTE '{cteName}'.");
            cteInfo = null; // Not found in any scope
            return false;
        }

        // --- Procedure Output Management ---

        public void RegisterProcedureOutput(string procedureName, List<OutputColumn> outputColumns)
        {
            if (string.IsNullOrWhiteSpace(procedureName))
            {
                throw new ArgumentException("Procedure name cannot be null or whitespace.", nameof(procedureName));
            }
            if (outputColumns == null)
            {
                throw new ArgumentNullException(nameof(outputColumns));
            }

            // Overwrite if already exists, assuming the latest execution defines the output
            _procedureOutputs[procedureName] = outputColumns;
            System.Diagnostics.Debug.WriteLine($"[Context] Registered {outputColumns.Count} output columns for procedure '{procedureName}'.");
        }

        public bool TryGetProcedureOutput(string procedureName, out List<OutputColumn>? outputColumns)
        {
            if (string.IsNullOrWhiteSpace(procedureName))
            {
                outputColumns = null;
                return false;
            }

            bool found = _procedureOutputs.TryGetValue(procedureName, out outputColumns);
            if (found)
            {
                System.Diagnostics.Debug.WriteLine($"[Context] Retrieved output columns for procedure '{procedureName}'.");
            }
            else
            {
                System.Diagnostics.Debug.WriteLine($"[Context] No output columns found registered for procedure '{procedureName}'.");
            }
            return found;
        }

        // --- Potential methods for managing source map scope ---
        // public void PushSourceMapScope() { /* Create new map, potentially copy/link outer? */ }
        // public void PopSourceMapScope() { /* Restore previous map */ }
    }
}
