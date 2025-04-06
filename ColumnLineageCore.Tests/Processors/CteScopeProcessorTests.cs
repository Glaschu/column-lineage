using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Processors; // Needed for DependencyNotMetException and CteScopeProcessor
using ColumnLineageCore.Helpers;
using ColumnLineageCore.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace ColumnLineageCore.Tests.Processors
{
    [TestClass]
    public class CteScopeProcessorTests
    {
        private CteScopeProcessor _processor = null!;
        private MockProcessingContext _mockContext = null!;
        private MockProcessorFactory _mockFactory = null!;
        private StubLineageGraph _stubGraph = null!;
        private MockQuerySpecificationProcessor _mockQuerySpecProcessor = null!; // Use the enhanced mock

        [TestInitialize]
        public void TestInitialize()
        {
            _processor = new CteScopeProcessor();
            _mockFactory = new MockProcessorFactory();
            _stubGraph = new StubLineageGraph();
            _mockContext = new MockProcessingContext(_stubGraph, _mockFactory);

            // Register mock query processor
            _mockQuerySpecProcessor = new MockQuerySpecificationProcessor();
            _mockFactory.RegisteredProcessors[typeof(QuerySpecification)] = _mockQuerySpecProcessor;
            // Register other query types if needed for complex CTE definitions
        }

        // Helper to create CommonTableExpression
        private CommonTableExpression CreateCTE(string name, QueryExpression query)
        {
            return new CommonTableExpression
            {
                ExpressionName = new Identifier { Value = name },
                QueryExpression = query
            };
        }

        // Helper to create simple QuerySpecification
         private QuerySpecification CreateSimpleQuerySpec(string colName, string sourceTable)
         {
             var querySpec = new QuerySpecification();
             var colRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = sourceTable }, new Identifier { Value = colName } } } };
             querySpec.SelectElements.Add(new SelectScalarExpression { Expression = colRef });
             querySpec.FromClause = new FromClause { TableReferences = { new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = sourceTable } } } } } };
             return querySpec;
         }


        [TestMethod]
        public void ProcessCteScope_SingleSimpleCte_ProcessesAndPushesScope()
        {
            // Arrange
            var query = CreateSimpleQuerySpec("Col1", "TableA");
            var cteDef = CreateCTE("CTE1", query);
            var cteList = new List<CommonTableExpression> { cteDef };

            // Mock the query processor behavior
            var expectedOutput = new List<OutputColumn> { new OutputColumn("Col1", new ColumnNode("Col1", "TableA")) };
            _mockQuerySpecProcessor.ProcessQueryAction = (qs, ctx) => {
                Assert.IsTrue(ctx.IsProcessingCteDefinition);
                Assert.AreEqual("CTE1", ctx.CteInfoToPopulate?.Name);
                // Simulate populating CteInfo (normally done by SelectScalarProcessor)
                ctx.CteInfoToPopulate!.OutputColumnSources["Col1"] = new ColumnNode("Col1", "TableA");
                return expectedOutput;
            };

            // Act
            var processedCtes = _processor.ProcessCteScope(cteList, _mockContext);

            // Assert
            Assert.AreEqual(1, processedCtes.Count);
            Assert.IsTrue(processedCtes.ContainsKey("CTE1"));
            var processedCteInfo = processedCtes["CTE1"];
            Assert.IsTrue(processedCteInfo.IsProcessed);
            Assert.AreEqual(1, processedCteInfo.OutputColumnSources.Count);
            Assert.AreEqual("TableA.Col1", processedCteInfo.OutputColumnSources["Col1"].Id);

            // Verify context stack was pushed (indirectly verified by TryResolveCte in other tests)
            // The mock context doesn't fully simulate the stack, so direct check is difficult here.
            // Rely on the fact that processedCtes dictionary is correct.
        }

        [TestMethod]
        public void ProcessCteScope_DependentCtesInOrder_ProcessesCorrectly()
        {
            // Arrange
            var query1 = CreateSimpleQuerySpec("ColA", "TableA"); // CTE1 depends on TableA
            var cteDef1 = CreateCTE("CTE1", query1);

            // CTE2 depends on CTE1
            var query2 = new QuerySpecification();
            var colRef2 = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "CTE1" }, new Identifier { Value = "ColA" } } } };
            query2.SelectElements.Add(new SelectScalarExpression { Expression = colRef2, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "ColB" } } });
            query2.FromClause = new FromClause { TableReferences = { new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "CTE1" } } } } } };
            var cteDef2 = CreateCTE("CTE2", query2);

            var cteList = new List<CommonTableExpression> { cteDef1, cteDef2 };

            // Mock processor behavior
            _mockQuerySpecProcessor.ProcessQueryAction = (qs, ctx) => {
                var cteName = ctx.CteInfoToPopulate!.Name;
                if (cteName == "CTE1") {
                    ctx.CteInfoToPopulate!.OutputColumnSources["ColA"] = new ColumnNode("ColA", "TableA");
                    return new List<OutputColumn> { new OutputColumn("ColA", new ColumnNode("ColA", "TableA")) };
                } else if (cteName == "CTE2") {
                    // Simulate populating CteInfo based on assumed dependency resolution
                    // The CteScopeProcessor already checked dependencies before calling this.
                    // We need the ultimate source from CTE1, which the test setup knows is TableA.ColA
                    var sourceNode = new ColumnNode("ColA", "TableA"); // Simulate resolved source
                    ctx.CteInfoToPopulate!.OutputColumnSources["ColB"] = sourceNode;
                    return new List<OutputColumn> { new OutputColumn("ColB", sourceNode) };
                }
                Assert.Fail("Unexpected CTE processed");
                return new List<OutputColumn>();
            };

            // Act
            var processedCtes = _processor.ProcessCteScope(cteList, _mockContext);

            // Assert
            Assert.AreEqual(2, processedCtes.Count);
            Assert.IsTrue(processedCtes["CTE1"].IsProcessed);
            Assert.IsTrue(processedCtes["CTE2"].IsProcessed);
            Assert.AreEqual("TableA.ColA", processedCtes["CTE2"].OutputColumnSources["ColB"].Id);
        }

        [TestMethod]
        public void ProcessCteScope_DependentCtesOutOfOrder_ProcessesCorrectlyWithMultiplePasses()
        {
            // Arrange - Define CTE2 before CTE1
             var query1 = CreateSimpleQuerySpec("ColA", "TableA");
             var cteDef1 = CreateCTE("CTE1", query1);
             var query2 = new QuerySpecification(); // CTE2 depends on CTE1
             var colRef2 = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "CTE1" }, new Identifier { Value = "ColA" } } } };
             query2.SelectElements.Add(new SelectScalarExpression { Expression = colRef2, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "ColB" } } });
             query2.FromClause = new FromClause { TableReferences = { new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "CTE1" } } } } } };
             var cteDef2 = CreateCTE("CTE2", query2);
             var cteList = new List<CommonTableExpression> { cteDef2, cteDef1 }; // Out of order

             int cte1ProcessAttempts = 0;
             int cte2ProcessAttempts = 0;

             // Mock processor behavior with dependency check
             _mockQuerySpecProcessor.ProcessQueryAction = (qs, ctx) => {
                 var cteName = ctx.CteInfoToPopulate!.Name;
                 if (cteName == "CTE1") {
                     cte1ProcessAttempts++;
                     ctx.CteInfoToPopulate!.OutputColumnSources["ColA"] = new ColumnNode("ColA", "TableA");
                     return new List<OutputColumn> { new OutputColumn("ColA", new ColumnNode("ColA", "TableA")) };
                 } else if (cteName == "CTE2") {
                     cte2ProcessAttempts++;
                     // Simulate populating CteInfo based on assumed dependency resolution
                     // The CteScopeProcessor already checked dependencies before calling this.
                     var sourceNode = new ColumnNode("ColA", "TableA"); // Simulate resolved source
                     ctx.CteInfoToPopulate!.OutputColumnSources["ColB"] = sourceNode;
                     return new List<OutputColumn> { new OutputColumn("ColB", sourceNode) };
                 }
                 Assert.Fail("Unexpected CTE processed");
                 return new List<OutputColumn>();
             };

            // Act
            var processedCtes = _processor.ProcessCteScope(cteList, _mockContext);

            // Assert
            Assert.AreEqual(2, processedCtes.Count, "Both CTEs should be processed.");
            Assert.IsTrue(processedCtes["CTE1"].IsProcessed);
            Assert.IsTrue(processedCtes["CTE2"].IsProcessed);
            Assert.AreEqual("TableA.ColA", processedCtes["CTE2"].OutputColumnSources["ColB"].Id);
            Assert.AreEqual(1, cte1ProcessAttempts, "CTE1 should be processed once.");
            Assert.AreEqual(1, cte2ProcessAttempts, "CTE2 should be successfully processed once (in the second pass)."); // Corrected assertion
        }

         [TestMethod]
        public void ProcessCteScope_CyclicDependency_StopsProcessingAndLogs()
        {
             // Arrange - CTE1 -> CTE2 -> CTE1
             var query1 = new QuerySpecification(); // CTE1 depends on CTE2
             var colRef1 = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "CTE2" }, new Identifier { Value = "ColB" } } } };
             query1.SelectElements.Add(new SelectScalarExpression { Expression = colRef1, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "ColA" } } });
             query1.FromClause = new FromClause { TableReferences = { new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "CTE2" } } } } } };
             var cteDef1 = CreateCTE("CTE1", query1);

             var query2 = new QuerySpecification(); // CTE2 depends on CTE1
             var colRef2 = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "CTE1" }, new Identifier { Value = "ColA" } } } };
             query2.SelectElements.Add(new SelectScalarExpression { Expression = colRef2, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "ColB" } } });
             query2.FromClause = new FromClause { TableReferences = { new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "CTE1" } } } } } };
             var cteDef2 = CreateCTE("CTE2", query2);

             var cteList = new List<CommonTableExpression> { cteDef1, cteDef2 };

             // Mock processor behavior with dependency check
             _mockQuerySpecProcessor.ProcessQueryAction = (qs, ctx) => {
                 var cteName = ctx.CteInfoToPopulate!.Name;
                 // In the cyclic test, the CteScopeProcessor's CheckDependencies should prevent
                 // this mock action from ever being called successfully.
                 // If it *is* called, it means CheckDependencies failed, so the test should fail here.
                 Assert.Fail($"Processor action for {cteName} should not be called in cyclic dependency test.");
                 return new List<OutputColumn>();
             };

            // Act
            var processedCtes = _processor.ProcessCteScope(cteList, _mockContext);

            // Assert
             Assert.AreEqual(0, processedCtes.Count, "No CTEs should be successfully processed due to cycle.");
             // Cycle detection is verified by the InvalidOperationException thrown by TopologicalSort
        }

        // --- Tests for Internal Logic (using reflection or making helpers internal/public for testing) ---

        [TestMethod]
        public void BuildDependencyGraph_SimpleDependencies_BuildsCorrectGraph()
        {
            // Arrange
            var query1 = CreateSimpleQuerySpec("ColA", "TableA"); // CTE1 depends on TableA
            var cteDef1 = CreateCTE("CTE1", query1);

            var query2 = new QuerySpecification(); // CTE2 depends on CTE1
            var colRef2 = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "CTE1" }, new Identifier { Value = "ColA" } } } };
            query2.SelectElements.Add(new SelectScalarExpression { Expression = colRef2 });
            query2.FromClause = new FromClause { TableReferences = { new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "CTE1" } } } } } };
            var cteDef2 = CreateCTE("CTE2", query2);

            var query3 = CreateSimpleQuerySpec("ColC", "TableC"); // CTE3 depends on TableC
            var cteDef3 = CreateCTE("CTE3", query3);

            var query4 = new QuerySpecification(); // CTE4 depends on CTE1 and CTE3
            var colRef4a = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "CTE1" }, new Identifier { Value = "ColA" } } } };
            var colRef4c = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "CTE3" }, new Identifier { Value = "ColC" } } } };
            query4.SelectElements.Add(new SelectScalarExpression { Expression = colRef4a });
            query4.FromClause = new FromClause { TableReferences = {
                new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "CTE1" } } } },
                new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "CTE3" } } } }
            } };
            var cteDef4 = CreateCTE("CTE4", query4);


            var cteList = new List<CommonTableExpression> { cteDef1, cteDef2, cteDef3, cteDef4 };
            var localCteNames = new HashSet<string>(cteList.Select(c => c.ExpressionName.Value), StringComparer.OrdinalIgnoreCase);

            // Act - Use reflection to call the private helper method (or make it internal/public for testing)
            var methodInfo = typeof(CteScopeProcessor).GetMethod("BuildLocalDependencyGraph", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(methodInfo, "BuildLocalDependencyGraph method not found.");

            var dependencyGraph = (Dictionary<string, HashSet<string>>)methodInfo.Invoke(_processor, new object[] { cteList, localCteNames })!;

            // Assert
            Assert.IsNotNull(dependencyGraph);
            Assert.AreEqual(4, dependencyGraph.Count);

            Assert.IsTrue(dependencyGraph.ContainsKey("CTE1"));
            Assert.AreEqual(0, dependencyGraph["CTE1"].Count); // Depends only on TableA

            Assert.IsTrue(dependencyGraph.ContainsKey("CTE2"));
            Assert.AreEqual(1, dependencyGraph["CTE2"].Count);
            Assert.IsTrue(dependencyGraph["CTE2"].Contains("CTE1"));

            Assert.IsTrue(dependencyGraph.ContainsKey("CTE3"));
            Assert.AreEqual(0, dependencyGraph["CTE3"].Count); // Depends only on TableC

            Assert.IsTrue(dependencyGraph.ContainsKey("CTE4"));
            Assert.AreEqual(2, dependencyGraph["CTE4"].Count);
            Assert.IsTrue(dependencyGraph["CTE4"].Contains("CTE1"));
            Assert.IsTrue(dependencyGraph["CTE4"].Contains("CTE3"));
        }

        [TestMethod]
        public void TopologicalSort_SimpleDependencies_ReturnsCorrectOrder()
        {
            // Arrange
            var dependencyGraph = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
            {
                { "CTE1", new HashSet<string>(StringComparer.OrdinalIgnoreCase) },          // Depends on nothing local
                { "CTE2", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "CTE1" } }, // Depends on CTE1
                { "CTE3", new HashSet<string>(StringComparer.OrdinalIgnoreCase) },          // Depends on nothing local
                { "CTE4", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "CTE1", "CTE3" } } // Depends on CTE1, CTE3
            };
            var allNodes = new List<string> { "CTE1", "CTE2", "CTE3", "CTE4" };

            // Act - Use reflection to call the private helper method
            var methodInfo = typeof(CteScopeProcessor).GetMethod("TopologicalSort", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(methodInfo, "TopologicalSort method not found.");

            var result = (List<string>)methodInfo.Invoke(_processor, new object[] { allNodes, dependencyGraph })!;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(4, result.Count);

            // Check relative order based on dependencies
            Assert.IsTrue(result.IndexOf("CTE1") < result.IndexOf("CTE2")); // CTE1 must come before CTE2
            Assert.IsTrue(result.IndexOf("CTE1") < result.IndexOf("CTE4")); // CTE1 must come before CTE4
            Assert.IsTrue(result.IndexOf("CTE3") < result.IndexOf("CTE4")); // CTE3 must come before CTE4

            // Check that all expected nodes are present
            CollectionAssert.AreEquivalent(allNodes, result);
        }

        [TestMethod]
        public void TopologicalSort_MoreComplexDependencies_ReturnsCorrectOrder()
        {
            // Arrange: A->B, A->C, B->D, C->D, D->E
            var dependencyGraph = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
            {
                { "A", new HashSet<string>(StringComparer.OrdinalIgnoreCase) },
                { "B", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "A" } },
                { "C", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "A" } },
                { "D", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "B", "C" } },
                { "E", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "D" } }
            };
            var allNodes = new List<string> { "A", "B", "C", "D", "E" };

            // Act
             var methodInfo = typeof(CteScopeProcessor).GetMethod("TopologicalSort", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
             Assert.IsNotNull(methodInfo, "TopologicalSort method not found.");
             var result = (List<string>)methodInfo.Invoke(_processor, new object[] { allNodes, dependencyGraph })!;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(5, result.Count);
            Assert.IsTrue(result.IndexOf("A") < result.IndexOf("B"));
            Assert.IsTrue(result.IndexOf("A") < result.IndexOf("C"));
            Assert.IsTrue(result.IndexOf("B") < result.IndexOf("D"));
            Assert.IsTrue(result.IndexOf("C") < result.IndexOf("D"));
            Assert.IsTrue(result.IndexOf("D") < result.IndexOf("E"));
            CollectionAssert.AreEquivalent(allNodes, result);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException), "Cyclic dependency detected among CTEs")]
        public void TopologicalSort_CyclicGraph_ThrowsException()
        {
            // Arrange: A->B, B->C, C->A
            var dependencyGraph = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
            {
                { "A", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "C" } },
                { "B", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "A" } },
                { "C", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "B" } }
            };
            var allNodes = new List<string> { "A", "B", "C" };

            // Act - Use reflection to call the private helper method
            var methodInfo = typeof(CteScopeProcessor).GetMethod("TopologicalSort", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsNotNull(methodInfo, "TopologicalSort method not found.");

            try
            {
                 methodInfo.Invoke(_processor, new object[] { allNodes, dependencyGraph });
            }
            catch (System.Reflection.TargetInvocationException ex)
            {
                 // Unwrap the inner exception thrown by the invoked method
                 throw ex.InnerException ?? ex;
            }
        }
    }
}
