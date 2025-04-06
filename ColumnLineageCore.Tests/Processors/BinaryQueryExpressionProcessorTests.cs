using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors; // Added
using ColumnLineageCore.Processors;
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
    public class BinaryQueryExpressionProcessorTests
    {
        private BinaryQueryExpressionProcessor _processor = null!;
        private MockProcessingContext _mockContext = null!;
        private MockProcessorFactory _mockFactory = null!;
        private StubLineageGraph _stubGraph = null!;
        private MockQuerySpecificationProcessor _mockQuerySpecProcessor = null!; // Changed type
        private StubBinaryQueryProcessor _stubBinaryProcessor = null!; // For nested binary queries

        [TestInitialize]
        public void TestInitialize()
        {
            _processor = new BinaryQueryExpressionProcessor();
            _mockFactory = new MockProcessorFactory();
            _stubGraph = new StubLineageGraph();
            _mockContext = new MockProcessingContext(_stubGraph, _mockFactory); // Inject both

            // Register stubs
            _mockQuerySpecProcessor = new MockQuerySpecificationProcessor(); // Changed type
            _stubBinaryProcessor = new StubBinaryQueryProcessor(); // Initialize
            _mockFactory.RegisteredProcessors[typeof(QuerySpecification)] = _mockQuerySpecProcessor; // Use mock
            _mockFactory.RegisteredProcessors[typeof(BinaryQueryExpression)] = _stubBinaryProcessor; // Register self for nesting
             // Register QueryParenthesisExpression if needed, or handle via helper
        }

        // Helper to create simple QuerySpecification returning specific output
        private QuerySpecification CreateQuerySpecWithOutput(List<OutputColumn> output)
        {
            // The actual structure doesn't matter much as the stub controls the output
            var querySpec = new QuerySpecification();
            // Add dummy select elements matching the count for clarity if needed
            for(int i=0; i < output.Count; i++)
            {
                 querySpec.SelectElements.Add(new SelectScalarExpression { Expression = new IntegerLiteral { Value = i.ToString() } });
            }
            return querySpec;
        }

        [TestMethod]
        public void ProcessQuery_Union_CallsProcessorsAndCombinesLineage()
        {
            // Arrange
            var output1 = new List<OutputColumn> { new OutputColumn("ColA", new ColumnNode("Src1", "T1")) };
            var output2 = new List<OutputColumn> { new OutputColumn("ColA", new ColumnNode("Src2", "T2")) }; // Same output name
            var query1 = CreateQuerySpecWithOutput(output1);
            var query2 = CreateQuerySpecWithOutput(output2);

            // Use the mock processor registered in TestInitialize
            var outputs = new Queue<List<OutputColumn>>();
            outputs.Enqueue(output1);
            outputs.Enqueue(output2);
            _mockQuerySpecProcessor.ProcessQueryAction = (qs, ctx) => outputs.Dequeue(); // Set custom action on the mock

            var binaryQuery = new BinaryQueryExpression
            {
                FirstQueryExpression = query1,
                SecondQueryExpression = query2,
                BinaryQueryExpressionType = BinaryQueryExpressionType.Union
            };

            // Act
            var combinedOutput = _processor.ProcessQuery(binaryQuery, _mockContext);

            // Assert
            // Verify processors were called for sub-queries
            Assert.AreEqual(2, _mockQuerySpecProcessor.CallCount, "ProcessQuery on QuerySpecification mock should be called twice.");
            // Check the flag captured by the mock *during* the call
            Assert.IsTrue(_mockQuerySpecProcessor.CapturedContexts.All(ctx => ctx != null)); // Ensure context was captured
            Assert.AreEqual(2, _mockQuerySpecProcessor.CapturedContexts.Count); // Should have captured context twice
            Assert.IsTrue(_mockQuerySpecProcessor.CapturedIsSubqueryFlag, "Context should be marked as subquery when processor was called."); // Check the captured flag value

            // Verify combined output structure
            Assert.AreEqual(1, combinedOutput.Count);
            Assert.AreEqual("ColA", combinedOutput[0].OutputName);
            Assert.AreEqual("T1.Src1", combinedOutput[0].SourceNode?.Id, "Combined output should represent source from the first query.");

            // Verify graph elements (final output node and edges from both sources)
            Assert.AreEqual(3, _stubGraph.AddedNodes.Count); // T1.Src1, T2.Src2, ColA (output)
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "T1.Src1"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "T2.Src2"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "ColA"));

            Assert.AreEqual(2, _stubGraph.AddedEdges.Count); // T1.Src1 -> ColA, T2.Src2 -> ColA
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == "T1.Src1" && e.TargetNodeId == "ColA"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == "T2.Src2" && e.TargetNodeId == "ColA"));
        }

         [TestMethod]
        public void ProcessQuery_UnionInSubqueryContext_AddsNodesButNoEdges()
        {
             // Arrange
            var output1 = new List<OutputColumn> { new OutputColumn("ColA", new ColumnNode("Src1", "T1")) };
            var output2 = new List<OutputColumn> { new OutputColumn("ColA", new ColumnNode("Src2", "T2")) };
            var query1 = CreateQuerySpecWithOutput(output1);
            var query2 = CreateQuerySpecWithOutput(output2);

            // Use the mock processor registered in TestInitialize
            var outputs = new Queue<List<OutputColumn>>();
            outputs.Enqueue(output1);
            outputs.Enqueue(output2);
            _mockQuerySpecProcessor.ProcessQueryAction = (qs, ctx) => outputs.Dequeue(); // Set custom action on the mock

            var binaryQuery = new BinaryQueryExpression
            {
                FirstQueryExpression = query1,
                SecondQueryExpression = query2,
                BinaryQueryExpressionType = BinaryQueryExpressionType.Union
            };

             _mockContext.IsSubquery = true; // Set the outer context to subquery

            // Act
            var combinedOutput = _processor.ProcessQuery(binaryQuery, _mockContext);

             // Assert
             Assert.AreEqual(1, combinedOutput.Count); // Structure is still returned

             // Verify graph elements (only source nodes should be added/ensured)
             Assert.AreEqual(2, _stubGraph.AddedNodes.Count); // T1.Src1, T2.Src2
             Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "T1.Src1"));
             Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "T2.Src2"));
             Assert.AreEqual(0, _stubGraph.AddedEdges.Count); // No edges added
        }

         [TestMethod]
        public void ProcessQuery_UnionInCteDefinitionContext_PopulatesCteInfo()
        {
             // Arrange
            var output1 = new List<OutputColumn> { new OutputColumn("ColA", new ColumnNode("Src1", "T1")) };
            var output2 = new List<OutputColumn> { new OutputColumn("ColA", new ColumnNode("Src2", "T2")) };
            var query1 = CreateQuerySpecWithOutput(output1);
            var query2 = CreateQuerySpecWithOutput(output2);

            // Use the mock processor registered in TestInitialize
            var outputs = new Queue<List<OutputColumn>>();
            outputs.Enqueue(output1);
            outputs.Enqueue(output2);
            _mockQuerySpecProcessor.ProcessQueryAction = (qs, ctx) => outputs.Dequeue(); // Set custom action on the mock

            var binaryQuery = new BinaryQueryExpression { /* ... setup ... */ FirstQueryExpression = query1, SecondQueryExpression = query2, BinaryQueryExpressionType = BinaryQueryExpressionType.Union };

             var cteInfoToPopulate = new CteInfo("DefiningCTE", binaryQuery);
             _mockContext.IsProcessingCteDefinition = true;
             _mockContext.CteInfoToPopulate = cteInfoToPopulate;

            // Act
            var combinedOutput = _processor.ProcessQuery(binaryQuery, _mockContext);

             // Assert
             Assert.AreEqual(1, combinedOutput.Count);

             // Verify graph elements (sources + intermediate CTE node)
             Assert.AreEqual(3, _stubGraph.AddedNodes.Count); // T1.Src1, T2.Src2, DefiningCTE.ColA
             Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "T1.Src1"));
             Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "T2.Src2"));
             Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "DefiningCTE.ColA"));
             Assert.AreEqual(0, _stubGraph.AddedEdges.Count); // No edges added directly by binary processor in CTE context

             // Verify CteInfo population (maps to first source)
             Assert.IsTrue(cteInfoToPopulate.OutputColumnSources.ContainsKey("ColA"));
             Assert.AreEqual("T1.Src1", cteInfoToPopulate.OutputColumnSources["ColA"].Id);
         }

    }

     // --- Enhanced Stub for QuerySpecificationProcessor needed for these tests ---
     public class MockQuerySpecificationProcessor : IQueryExpressionProcessor<QuerySpecification>
     {
         public List<OutputColumn> ExpectedOutput { get; set; } = new List<OutputColumn>();
         public List<QuerySpecification> ProcessedFragments { get; } = new List<QuerySpecification>();
         public List<IProcessingContext> CapturedContexts { get; } = new List<IProcessingContext>();
         public bool CapturedIsSubqueryFlag { get; private set; } // Added missing property
         public Func<QuerySpecification, IProcessingContext, List<OutputColumn>>? ProcessQueryAction { get; set; } // Action for custom mock behavior
         public int CallCount => ProcessedFragments.Count;


         public List<OutputColumn> ProcessQuery(QuerySpecification queryExpression, IProcessingContext context)
         {
             ProcessedFragments.Add(queryExpression);
             CapturedContexts.Add(context);
             CapturedIsSubqueryFlag = context.IsSubquery; // Capture flag value

             if (ProcessQueryAction != null)
             {
                 return ProcessQueryAction(queryExpression, context);
             }

             // Default behavior if no action set
             foreach (var col in ExpectedOutput) { if (col.SourceNode != null) context.Graph.AddNode(col.SourceNode); }
             return ExpectedOutput;
         }
          public void Process(QuerySpecification fragment, IProcessingContext context) { ProcessQuery(fragment, context); }
     }
}
