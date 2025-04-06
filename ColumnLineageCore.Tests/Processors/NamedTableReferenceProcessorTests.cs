using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Processors;
using ColumnLineageCore.Helpers;
using ColumnLineageCore.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace ColumnLineageCore.Tests.Processors
{
    // --- Mock ProcessingContext for Tests ---
    public class MockProcessingContext : IProcessingContext
    {
        public ILineageGraph Graph { get; }
        public IProcessorFactory ProcessorFactory { get; } // Can be null/stub if not used by processor under test
        public IDictionary<string, SourceInfo> CurrentSourceMap { get; set; } = new Dictionary<string, SourceInfo>(StringComparer.OrdinalIgnoreCase);
        public bool IsProcessingCteDefinition { get; set; }
        public CteInfo? CteInfoToPopulate { get; set; }
        public bool IsSubquery { get; set; }
        public Dictionary<string, List<string>>? ColumnAvailabilityMap { get; set; } // Added
        public string? IntoClauseTarget { get; set; } // Added
        public IViewDefinitionProvider ViewProvider { get; } // Added missing member
        public IDictionary<string, LineageResult> AnalysisCache { get; } // Added missing member


        private readonly Dictionary<string, CteInfo> _availableCtes = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase);

        public MockProcessingContext(ILineageGraph? graph = null, IProcessorFactory? factory = null, IViewDefinitionProvider? viewProvider = null)
        {
            Graph = graph ?? new StubLineageGraph(); // Use stub graph if none provided
            ProcessorFactory = factory ?? new StubProcessorFactory(); // Use stub factory
            ViewProvider = viewProvider ?? new StubViewDefinitionProvider(); // Initialize ViewProvider
            AnalysisCache = new Dictionary<string, LineageResult>(); // Initialize Cache
        }

        // Simulate adding CTEs to the context for resolution testing
        public void AddAvailableCte(CteInfo cte)
        {
            _availableCtes[cte.Name] = cte;
        }

        // --- Interface Methods ---
        public void PushCteScope(IDictionary<string, CteInfo> ctesInScope) { /* Not needed for this processor's tests */ }
        public void PopCteScope() { /* Not needed for this processor's tests */ }

        public bool TryResolveCte(string cteName, out CteInfo? cteInfo)
        {
            return _availableCtes.TryGetValue(cteName, out cteInfo);
        }

        // --- Added Missing Interface Members ---
        public void RegisterProcedureOutput(string procedureName, List<OutputColumn> outputColumns)
        {
            // Dummy implementation for mock - does nothing
            System.Diagnostics.Debug.WriteLine($"[MockContext] RegisterProcedureOutput called for {procedureName} with {outputColumns?.Count ?? 0} columns.");
        }

        public bool TryGetProcedureOutput(string procedureName, out List<OutputColumn>? outputColumns)
        {
            // Dummy implementation for mock - always returns false
            outputColumns = null;
            System.Diagnostics.Debug.WriteLine($"[MockContext] TryGetProcedureOutput called for {procedureName}. Returning false.");
            return false;
        }
    }

    // --- Minimal Stub for IViewDefinitionProvider ---
    public class StubViewDefinitionProvider : IViewDefinitionProvider
    {
        public bool TryGetViewDefinition(string viewName, out string? viewDefinitionSql)
        {
            viewDefinitionSql = null;
            return false; // Always return false for the stub
        }
    }


    // --- Test Class ---
    [TestClass]
    public class NamedTableReferenceProcessorTests
    {
        private NamedTableReferenceProcessor _processor = null!;
        private MockProcessingContext _mockContext = null!;
        private StubLineageGraph _stubGraph = null!;

        [TestInitialize]
        public void TestInitialize()
        {
            _processor = new NamedTableReferenceProcessor();
            _stubGraph = new StubLineageGraph();
            _mockContext = new MockProcessingContext(_stubGraph); // Use stub graph
        }

        // Helper to create NamedTableReference
        private NamedTableReference CreateTableRef(string name, string? alias = null)
        {
            var schemaObject = new SchemaObjectName();
            schemaObject.Identifiers.Add(new Identifier { Value = name }); // Assuming single-part name for simplicity
            var tableRef = new NamedTableReference
            {
                SchemaObject = schemaObject
            };
            if (alias != null)
            {
                tableRef.Alias = new Identifier { Value = alias };
            }
            return tableRef;
        }

        [TestMethod]
        public void Process_SimpleTable_AddsTableToSourceMap()
        {
            // Arrange
            var tableRef = CreateTableRef("TableA");

            // Act
            _processor.Process(tableRef, _mockContext);

            // Assert
            Assert.AreEqual(1, _mockContext.CurrentSourceMap.Count);
            Assert.IsTrue(_mockContext.CurrentSourceMap.ContainsKey("TableA"));
            var sourceInfo = _mockContext.CurrentSourceMap["TableA"];
            Assert.AreEqual("TableA", sourceInfo.Name);
            Assert.AreEqual(SourceType.Table, sourceInfo.Type);
        }

        [TestMethod]
        public void Process_TableWithAlias_AddsAliasedTableToSourceMap()
        {
            // Arrange
            var tableRef = CreateTableRef("TableA", "T1");

            // Act
            _processor.Process(tableRef, _mockContext);

            // Assert
            Assert.AreEqual(1, _mockContext.CurrentSourceMap.Count);
            Assert.IsTrue(_mockContext.CurrentSourceMap.ContainsKey("T1"));
            Assert.IsFalse(_mockContext.CurrentSourceMap.ContainsKey("TableA")); // Should use alias as key
            var sourceInfo = _mockContext.CurrentSourceMap["T1"];
            Assert.AreEqual("TableA", sourceInfo.Name); // Name is still the original table name
            Assert.AreEqual(SourceType.Table, sourceInfo.Type);
        }

        [TestMethod]
        public void Process_CteReference_AddsCteToSourceMap()
        {
            // Arrange
            var cteInfo = new CteInfo("MyCTE", new DummyQueryExpression()) { IsProcessed = true };
            cteInfo.OutputColumnSources.Add("Col1", new ColumnNode("SourceCol", "SourceTable")); // Add dummy source
            _mockContext.AddAvailableCte(cteInfo);
            var tableRef = CreateTableRef("MyCTE");

            // Act
            _processor.Process(tableRef, _mockContext);

            // Assert
            Assert.AreEqual(1, _mockContext.CurrentSourceMap.Count);
            Assert.IsTrue(_mockContext.CurrentSourceMap.ContainsKey("MyCTE"));
            var sourceInfo = _mockContext.CurrentSourceMap["MyCTE"];
            Assert.AreEqual("MyCTE", sourceInfo.Name);
            Assert.AreEqual(SourceType.CTE, sourceInfo.Type);
            // Check if intermediate node was added to graph
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "MyCTE.Col1"));
        }

        [TestMethod]
        public void Process_CteReferenceWithAlias_AddsAliasedCteToSourceMap()
        {
            // Arrange
            var cteInfo = new CteInfo("MyCTE", new DummyQueryExpression()) { IsProcessed = true };
             cteInfo.OutputColumnSources.Add("Col1", new ColumnNode("SourceCol", "SourceTable"));
            _mockContext.AddAvailableCte(cteInfo);
            var tableRef = CreateTableRef("MyCTE", "M");

            // Act
            _processor.Process(tableRef, _mockContext);

            // Assert
            Assert.AreEqual(1, _mockContext.CurrentSourceMap.Count);
            Assert.IsTrue(_mockContext.CurrentSourceMap.ContainsKey("M"));
            Assert.IsFalse(_mockContext.CurrentSourceMap.ContainsKey("MyCTE")); // Should use alias
            var sourceInfo = _mockContext.CurrentSourceMap["M"];
            Assert.AreEqual("MyCTE", sourceInfo.Name); // Name is still the original CTE name
            Assert.AreEqual(SourceType.CTE, sourceInfo.Type);
             // Check if intermediate node was added to graph
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "MyCTE.Col1"));
        }

         [TestMethod]
        public void Process_UnprocessedCteReference_AddsCteToSourceMapButNotNodes()
        {
            // Arrange
            var cteInfo = new CteInfo("MyCTE", new DummyQueryExpression()) { IsProcessed = false }; // Not processed
            _mockContext.AddAvailableCte(cteInfo);
            var tableRef = CreateTableRef("MyCTE");

            // Act
            _processor.Process(tableRef, _mockContext);

            // Assert
            Assert.AreEqual(1, _mockContext.CurrentSourceMap.Count);
            Assert.IsTrue(_mockContext.CurrentSourceMap.ContainsKey("MyCTE"));
            var sourceInfo = _mockContext.CurrentSourceMap["MyCTE"];
            Assert.AreEqual("MyCTE", sourceInfo.Name);
            Assert.AreEqual(SourceType.CTE, sourceInfo.Type);
            // Check that intermediate node was NOT added to graph
            Assert.IsFalse(_stubGraph.AddedNodes.Any(n => n.Id.StartsWith("MyCTE.")));
        }


        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Process_NullTableRef_ThrowsArgumentNullException()
        {
            // Act
            _processor.Process(null!, _mockContext);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Process_NullContext_ThrowsArgumentNullException()
        {
            // Arrange
            var tableRef = CreateTableRef("TableA");

            // Act
             _processor.Process(tableRef, null!);
        }

        [TestMethod]
        public void Process_TempTableReference_AddsTableToSourceMap()
        {
            // Arrange
            var tableRef = CreateTableRef("#temp");

            // Act
            _processor.Process(tableRef, _mockContext);

            // Assert
            Assert.AreEqual(1, _mockContext.CurrentSourceMap.Count);
            Assert.IsTrue(_mockContext.CurrentSourceMap.ContainsKey("#temp"));
            var sourceInfo = _mockContext.CurrentSourceMap["#temp"];
            Assert.AreEqual("#temp", sourceInfo.Name);
            Assert.AreEqual(SourceType.Table, sourceInfo.Type); // Currently treated as a regular table
        }
    }
}
