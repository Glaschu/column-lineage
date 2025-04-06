using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors; // Added for processor interfaces
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
    // --- Stubs/Mocks for Dependencies ---

    // Stub QuerySpecification Processor
    public class StubQuerySpecificationProcessor : IQueryExpressionProcessor<QuerySpecification>
    {
        public List<OutputColumn> ExpectedOutput { get; set; } = new List<OutputColumn>();
        public QuerySpecification? LastProcessedFragment { get; private set; }
        public IProcessingContext? CapturedContext { get; private set; } // Renamed for clarity
        public bool CapturedIsSubqueryFlag { get; private set; } // Added to capture the flag value

        public List<OutputColumn> ProcessQuery(QuerySpecification queryExpression, IProcessingContext context)
        {
            LastProcessedFragment = queryExpression;
            CapturedContext = context; // Capture the context reference
            CapturedIsSubqueryFlag = context.IsSubquery; // Capture the flag's value *at call time*
            // Simulate adding nodes/edges based on ExpectedOutput
            foreach (var col in ExpectedOutput)
            {
                if (col.SourceNode != null) context.Graph.AddNode(col.SourceNode);
                // Normally adds edges too, but simplified for stub
            }
            return ExpectedOutput;
        }

        // Explicit implementation of base interface method if needed, or rely on ProcessQuery
         public void Process(QuerySpecification fragment, IProcessingContext context)
         {
              ProcessQuery(fragment, context); // Delegate to ProcessQuery
         }
    }

     // Stub BinaryQueryExpression Processor (similar structure)
     public class StubBinaryQueryProcessor : IQueryExpressionProcessor<BinaryQueryExpression>
     {
         public List<OutputColumn> ExpectedOutput { get; set; } = new List<OutputColumn>();
         public BinaryQueryExpression? LastProcessedFragment { get; private set; }
         public IProcessingContext? CapturedContext { get; private set; } // Renamed for clarity
         public bool CapturedIsSubqueryFlag { get; private set; } // Added to capture the flag value

         public List<OutputColumn> ProcessQuery(BinaryQueryExpression queryExpression, IProcessingContext context)
         {
             LastProcessedFragment = queryExpression;
             CapturedContext = context; // Capture the context reference
             CapturedIsSubqueryFlag = context.IsSubquery; // Capture the flag's value *at call time*
             // Simulate adding nodes/edges
             foreach (var col in ExpectedOutput) { if (col.SourceNode != null) context.Graph.AddNode(col.SourceNode); }
             return ExpectedOutput;
         }
          public void Process(BinaryQueryExpression fragment, IProcessingContext context) { ProcessQuery(fragment, context); }
     }


    // --- Test Class ---
    [TestClass]
    public class QueryDerivedTableProcessorTests
    {
        private QueryDerivedTableProcessor _processor = null!;
        private MockProcessingContext _mockContext = null!;
        private MockProcessorFactory _mockFactory = null!;
        private StubQuerySpecificationProcessor _stubQuerySpecProcessor = null!;
        private StubBinaryQueryProcessor _stubBinaryQueryProcessor = null!; // Added

        [TestInitialize]
        public void TestInitialize()
        {
            _processor = new QueryDerivedTableProcessor();
            _mockFactory = new MockProcessorFactory();
            _mockContext = new MockProcessingContext(factory: _mockFactory);

            // Register stub processors
            _stubQuerySpecProcessor = new StubQuerySpecificationProcessor();
            _stubBinaryQueryProcessor = new StubBinaryQueryProcessor(); // Added
            _mockFactory.RegisteredProcessors[typeof(QuerySpecification)] = _stubQuerySpecProcessor;
            _mockFactory.RegisteredProcessors[typeof(BinaryQueryExpression)] = _stubBinaryQueryProcessor; // Added
             // Note: QueryParenthesisExpression is handled recursively within the processor itself for now
        }

        // Helper to create QueryDerivedTable with QuerySpecification
        private QueryDerivedTable CreateDerivedTableSpec(string alias, SelectElement selectElement)
        {
            var querySpec = new QuerySpecification();
            querySpec.SelectElements.Add(selectElement);
            // Add dummy FROM if needed by processor logic later
            // querySpec.FromClause = new FromClause();
            // querySpec.FromClause.TableReferences.Add(new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "DummyTable" } } } });

            return new QueryDerivedTable
            {
                Alias = new Identifier { Value = alias },
                QueryExpression = querySpec
            };
        }

         // Helper to create QueryDerivedTable with BinaryQueryExpression
        private QueryDerivedTable CreateDerivedTableBinary(string alias)
        {
             // Create dummy binary expression (e.g., SELECT 1 UNION SELECT 2)
             var literal1 = new IntegerLiteral { Value = "1" };
             var select1 = new QuerySpecification { SelectElements = { new SelectScalarExpression { Expression = literal1 } } };
             var literal2 = new IntegerLiteral { Value = "2" };
             var select2 = new QuerySpecification { SelectElements = { new SelectScalarExpression { Expression = literal2 } } };

             var binaryQuery = new BinaryQueryExpression
             {
                 FirstQueryExpression = select1,
                 SecondQueryExpression = select2,
                 BinaryQueryExpressionType = BinaryQueryExpressionType.Union
             };

             return new QueryDerivedTable
             {
                 Alias = new Identifier { Value = alias },
                 QueryExpression = binaryQuery
             };
        }

        // Helper to create SelectScalarExpression
        private SelectScalarExpression CreateSelectScalar(string columnName, string? alias = null)
        {
             var colRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = columnName } } } };
             var selectScalar = new SelectScalarExpression { Expression = colRef };
             if (alias != null) { selectScalar.ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = alias } }; }
             return selectScalar;
        }


        [TestMethod]
        public void Process_QuerySpecification_CallsCorrectProcessorAndAddsToSourceMap()
        {
            // Arrange
            var selectElement = CreateSelectScalar("SourceCol", "DerivedCol");
            var derivedTable = CreateDerivedTableSpec("DT", selectElement);
            var expectedOutput = new List<OutputColumn> { new OutputColumn("DerivedCol", new ColumnNode("SourceCol", "BaseTable")) };
            _stubQuerySpecProcessor.ExpectedOutput = expectedOutput;

            // Act
            _processor.Process(derivedTable, _mockContext);

            // Assert
            // Verify processor was called
            Assert.IsNotNull(_stubQuerySpecProcessor.LastProcessedFragment);
            Assert.AreSame(derivedTable.QueryExpression, _stubQuerySpecProcessor.LastProcessedFragment);
            Assert.IsNotNull(_stubQuerySpecProcessor.CapturedContext); // Check captured context
            Assert.IsTrue(_stubQuerySpecProcessor.CapturedIsSubqueryFlag, "Context should be marked as subquery when inner processor is called."); // Check captured flag

            // Verify source map update
            Assert.AreEqual(1, _mockContext.CurrentSourceMap.Count);
            Assert.IsTrue(_mockContext.CurrentSourceMap.ContainsKey("DT"));
            var sourceInfo = _mockContext.CurrentSourceMap["DT"];
            Assert.AreEqual("DT", sourceInfo.Name);
            Assert.AreEqual(SourceType.Subquery, sourceInfo.Type);
            Assert.IsNotNull(sourceInfo.SubqueryOutputColumns);
            Assert.AreEqual(1, sourceInfo.SubqueryOutputColumns.Count);
            Assert.AreEqual("DerivedCol", sourceInfo.SubqueryOutputColumns[0].OutputName);
            Assert.AreEqual("BaseTable.SourceCol", sourceInfo.SubqueryOutputColumns[0].SourceNode?.Id);
        }

         [TestMethod]
        public void Process_BinaryQueryExpression_CallsCorrectProcessorAndAddsToSourceMap() // Added Test
        {
            // Arrange
            var derivedTable = CreateDerivedTableBinary("DT_Union");
            var expectedOutput = new List<OutputColumn> { new OutputColumn("UnionCol", null) }; // Example output
            _stubBinaryQueryProcessor.ExpectedOutput = expectedOutput;

            // Act
            _processor.Process(derivedTable, _mockContext);

            // Assert
            // Verify processor was called
            Assert.IsNotNull(_stubBinaryQueryProcessor.LastProcessedFragment);
            Assert.AreSame(derivedTable.QueryExpression, _stubBinaryQueryProcessor.LastProcessedFragment);
             Assert.IsNotNull(_stubBinaryQueryProcessor.CapturedContext); // Check captured context
            Assert.IsTrue(_stubBinaryQueryProcessor.CapturedIsSubqueryFlag, "Context should be marked as subquery when inner processor is called."); // Check captured flag

            // Verify source map update
            Assert.AreEqual(1, _mockContext.CurrentSourceMap.Count);
            Assert.IsTrue(_mockContext.CurrentSourceMap.ContainsKey("DT_Union"));
            var sourceInfo = _mockContext.CurrentSourceMap["DT_Union"];
            Assert.AreEqual("DT_Union", sourceInfo.Name);
            Assert.AreEqual(SourceType.Subquery, sourceInfo.Type);
            Assert.IsNotNull(sourceInfo.SubqueryOutputColumns);
            Assert.AreEqual(1, sourceInfo.SubqueryOutputColumns.Count);
             Assert.AreEqual("UnionCol", sourceInfo.SubqueryOutputColumns[0].OutputName); // Check output name
        }


        [TestMethod]
        public void Process_NoAlias_DoesNotAddToSourceMap()
        {
            // Arrange
             var selectElement = CreateSelectScalar("SourceCol", "DerivedCol");
             var querySpec = new QuerySpecification();
             querySpec.SelectElements.Add(selectElement);
             var derivedTable = new QueryDerivedTable { QueryExpression = querySpec, Alias = null }; // No Alias

            // Act
            _processor.Process(derivedTable, _mockContext);

            // Assert
            Assert.AreEqual(0, _mockContext.CurrentSourceMap.Count, "Source map should be empty if derived table has no alias.");
            Assert.IsNull(_stubQuerySpecProcessor.LastProcessedFragment, "Inner processor should not be called if alias is missing.");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Process_NullDerivedTable_ThrowsArgumentNullException()
        {
            // Act
            _processor.Process(null!, _mockContext);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Process_NullContext_ThrowsArgumentNullException()
        {
            // Arrange
             var selectElement = CreateSelectScalar("SourceCol", "DerivedCol");
             var derivedTable = CreateDerivedTableSpec("DT", selectElement);

            // Act
            _processor.Process(derivedTable, null!);
        }
    }
}
