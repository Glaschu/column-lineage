using ColumnLineageCore;
using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Helpers;
using ColumnLineageCore.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom; // Required for dummy QueryExpression

namespace ColumnLineageCore.Tests
{
    // --- Stubs/Mocks for Dependencies ---

    public class StubLineageGraph : ILineageGraph
    {
        public List<ColumnNode> AddedNodes { get; } = new List<ColumnNode>();
        public List<LineageEdge> AddedEdges { get; } = new List<LineageEdge>();

        public IEnumerable<ColumnNode> Nodes => AddedNodes;
        public IEnumerable<LineageEdge> Edges => AddedEdges;

        public ColumnNode AddNode(ColumnNode node)
        {
            if (!AddedNodes.Contains(node)) AddedNodes.Add(node);
            return node;
        }

        public LineageEdge AddEdge(ColumnNode source, ColumnNode target)
        {
            var edge = new LineageEdge(source.Id, target.Id);
            // Simple add for stub purposes, doesn't check duplicates like real one
            AddedEdges.Add(edge);
            return edge;
        }
    }

    public class StubProcessorFactory : IProcessorFactory
    {
        // Not needed for current context tests, but required by interface
        public ISqlFragmentProcessor<TFragment> GetProcessor<TFragment>(TFragment? fragment = null) where TFragment : TSqlFragment
        {
            throw new NotImplementedException("Stub GetProcessor should not be called in these tests.");
        }
    }

    // Dummy QueryExpression for CteInfo
    public class DummyQueryExpression : QueryExpression { public override void Accept(TSqlFragmentVisitor visitor) { } public override void AcceptChildren(TSqlFragmentVisitor visitor) { } }


    // --- Test Class ---

    [TestClass]
    public class ProcessingContextTests
    {
        private StubLineageGraph _stubGraph = null!;
        private StubProcessorFactory _stubFactory = null!;
        private StubViewDefinitionProvider _stubViewProvider = null!; // Added
        private ProcessingContext _context = null!;
        private CteInfo _cteA = null!;
        private CteInfo _cteB = null!;
        private CteInfo _cteC = null!;

        [TestInitialize]
        public void TestInitialize()
        {
            _stubGraph = new StubLineageGraph();
            _stubFactory = new StubProcessorFactory();
            _stubViewProvider = new StubViewDefinitionProvider(); // Added
            // Provide all required arguments to the constructor
            _context = new ProcessingContext(_stubGraph, _stubFactory, _stubViewProvider, new Dictionary<string, LineageResult>());

            _cteA = new CteInfo("CTE_A", new DummyQueryExpression());
            _cteB = new CteInfo("CTE_B", new DummyQueryExpression());
            _cteC = new CteInfo("CTE_C", new DummyQueryExpression()); // For a different scope
        }

        [TestMethod]
        public void Constructor_InitializesProperties()
        {
            // Assert
            Assert.AreSame(_stubGraph, _context.Graph);
            Assert.AreSame(_stubFactory, _context.ProcessorFactory);
            Assert.IsNotNull(_context.CurrentSourceMap);
            Assert.IsFalse(_context.IsProcessingCteDefinition);
            Assert.IsNull(_context.CteInfoToPopulate);
            Assert.IsFalse(_context.IsSubquery);
        }

        [TestMethod]
        public void TryResolveCte_EmptyContext_ReturnsFalse()
        {
            // Act
            bool result = _context.TryResolveCte("NonExistent", out var resolvedCte);

            // Assert
            Assert.IsFalse(result);
            Assert.IsNull(resolvedCte);
        }

        [TestMethod]
        public void PushCteScope_TryResolveCte_ResolvesCteInCurrentScope()
        {
            // Arrange
            var scope1 = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase) { { _cteA.Name, _cteA } };
            _context.PushCteScope(scope1);

            // Act
            bool result = _context.TryResolveCte("CTE_A", out var resolvedCte);

            // Assert
            Assert.IsTrue(result);
            Assert.AreSame(_cteA, resolvedCte);
        }

        [TestMethod]
        public void PushCteScope_MultipleScopes_ResolvesInnermostCte()
        {
            // Arrange
            var scope1 = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase) { { _cteA.Name, _cteA } };
             // CTE_A shadowed in scope 2
            var scope2 = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase) { { _cteA.Name, _cteB }, { _cteC.Name, _cteC } };
            _context.PushCteScope(scope1);
            _context.PushCteScope(scope2);

            // Act
            bool resultA = _context.TryResolveCte("CTE_A", out var resolvedCteA);
            bool resultC = _context.TryResolveCte("CTE_C", out var resolvedCteC);

            // Assert
            Assert.IsTrue(resultA);
            Assert.AreSame(_cteB, resolvedCteA, "Should resolve CTE_A from the inner scope (scope2)"); // _cteB shadows _cteA
            Assert.IsTrue(resultC);
            Assert.AreSame(_cteC, resolvedCteC, "Should resolve CTE_C from the inner scope (scope2)");
        }

        [TestMethod]
        public void PopCteScope_ResolvesOuterScopeCte()
        {
            // Arrange
            var scope1 = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase) { { _cteA.Name, _cteA } };
            var scope2 = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase) { { _cteB.Name, _cteB } }; // Doesn't contain CTE_A
            _context.PushCteScope(scope1);
            _context.PushCteScope(scope2);

            // Act
            _context.PopCteScope(); // Pop scope2
            bool result = _context.TryResolveCte("CTE_A", out var resolvedCte);

            // Assert
            Assert.IsTrue(result);
            Assert.AreSame(_cteA, resolvedCte, "Should resolve CTE_A from the outer scope (scope1) after pop.");
        }

         [TestMethod]
        public void TryResolveCte_NotInInnerScopeButInOutermost_ResolvesCorrectly()
        {
            // Arrange
            var initialScope = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase) { { _cteA.Name, _cteA } };
            // Provide all required arguments to the constructor, including initial scope
            var contextWithInitial = new ProcessingContext(_stubGraph, _stubFactory, _stubViewProvider, new Dictionary<string, LineageResult>(), initialScope);
            var scope2 = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase) { { _cteB.Name, _cteB } };
            contextWithInitial.PushCteScope(scope2);

            // Act
            bool result = contextWithInitial.TryResolveCte("CTE_A", out var resolvedCte);

            // Assert
            Assert.IsTrue(result);
            Assert.AreSame(_cteA, resolvedCte, "Should resolve CTE_A from the initial scope.");
        }


        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void PopCteScope_OnBaseScope_ThrowsInvalidOperationException()
        {
            // Act
            _context.PopCteScope(); // Try to pop the initial empty scope
        }

         [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void PushCteScope_NullScope_ThrowsArgumentNullException()
        {
            // Act
            _context.PushCteScope(null!);
        }

        [TestMethod]
        public void FlagsAndProperties_CanBeSetAndGet()
        {
            // Arrange
            var sourceMap = new Dictionary<string, SourceInfo>();
            var cteToPopulate = new CteInfo("POPULATE", new DummyQueryExpression());

            // Act
            _context.IsProcessingCteDefinition = true;
            _context.CteInfoToPopulate = cteToPopulate;
            _context.IsSubquery = true;
            _context.CurrentSourceMap = sourceMap;


            // Assert
            Assert.IsTrue(_context.IsProcessingCteDefinition);
            Assert.AreSame(cteToPopulate, _context.CteInfoToPopulate);
            Assert.IsTrue(_context.IsSubquery);
            Assert.AreSame(sourceMap, _context.CurrentSourceMap);
        }
    }
}
