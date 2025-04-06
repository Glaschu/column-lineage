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
    // --- Stubs for Processors used by QuerySpecificationProcessor ---

    public class StubSelectScalarProcessor : ISelectElementProcessor<SelectScalarExpression>
    {
        public List<OutputColumn> OutputToReturn { get; set; } = new List<OutputColumn>();
        public SelectScalarExpression? LastProcessedFragment { get; private set; }
        public List<OutputColumn> ProcessElement(SelectScalarExpression selectElement, IProcessingContext context)
        {
            LastProcessedFragment = selectElement;
            return OutputToReturn;
        }
         public void Process(SelectScalarExpression fragment, IProcessingContext context) { ProcessElement(fragment, context); }
    }

    public class StubSelectStarProcessor : ISelectElementProcessor<SelectStarExpression>
    {
        public List<OutputColumn> OutputToReturn { get; set; } = new List<OutputColumn>();
        public SelectStarExpression? LastProcessedFragment { get; private set; }
        public List<OutputColumn> ProcessElement(SelectStarExpression selectElement, IProcessingContext context)
        {
            LastProcessedFragment = selectElement;
            return OutputToReturn;
        }
         public void Process(SelectStarExpression fragment, IProcessingContext context) { ProcessElement(fragment, context); }
    }

    // Re-use StubNamedTableProcessor from Join tests or redefine if needed
    // public class StubNamedTableProcessor : ITableReferenceProcessor<NamedTableReference> { ... }


    // --- Test Class ---
    [TestClass]
    public class QuerySpecificationProcessorTests
    {
        private QuerySpecificationProcessor _processor = null!;
        private MockProcessingContext _mockContext = null!;
        private MockProcessorFactory _mockFactory = null!;
        private StubNamedTableProcessor _stubNamedTableProcessor = null!;
        private StubSelectScalarProcessor _stubSelectScalarProcessor = null!;
        private StubSelectStarProcessor _stubSelectStarProcessor = null!;

        [TestInitialize]
        public void TestInitialize()
        {
            _processor = new QuerySpecificationProcessor();
            _mockFactory = new MockProcessorFactory();
            _mockContext = new MockProcessingContext(factory: _mockFactory);

            // Register stubs
            _stubNamedTableProcessor = new StubNamedTableProcessor();
            _stubSelectScalarProcessor = new StubSelectScalarProcessor();
            _stubSelectStarProcessor = new StubSelectStarProcessor();

            _mockFactory.RegisteredProcessors[typeof(NamedTableReference)] = _stubNamedTableProcessor;
            _mockFactory.RegisteredProcessors[typeof(SelectScalarExpression)] = _stubSelectScalarProcessor;
            _mockFactory.RegisteredProcessors[typeof(SelectStarExpression)] = _stubSelectStarProcessor;
            // Register other needed stubs (Join, DerivedTable, etc.) if testing those paths
        }

        // Helper to create QuerySpecification
        private QuerySpecification CreateQuerySpec(IList<SelectElement> selectElements, IList<TableReference>? fromElements = null)
        {
            var querySpec = new QuerySpecification();
            foreach(var se in selectElements) querySpec.SelectElements.Add(se);
            if (fromElements != null)
            {
                querySpec.FromClause = new FromClause();
                foreach(var fe in fromElements) querySpec.FromClause.TableReferences.Add(fe);
            }
            return querySpec;
        }

         // Helper to create NamedTableReference
        private NamedTableReference CreateNamedRef(string name)
        {
             var schemaObject = new SchemaObjectName();
            schemaObject.Identifiers.Add(new Identifier { Value = name });
            return new NamedTableReference { SchemaObject = schemaObject };
        }

         // Helper to create SelectScalarExpression
        private SelectScalarExpression CreateSelectScalar(string columnName, string? sourceIdentifier = null)
        {
            var multiPartId = new MultiPartIdentifier();
            if (sourceIdentifier != null) multiPartId.Identifiers.Add(new Identifier { Value = sourceIdentifier });
            multiPartId.Identifiers.Add(new Identifier { Value = columnName });
            var colRef = new ColumnReferenceExpression { MultiPartIdentifier = multiPartId };
            return new SelectScalarExpression { Expression = colRef };
        }

         // Helper to create SelectStarExpression
        private SelectStarExpression CreateSelectStar(string? qualifier = null)
        {
            var starExp = new SelectStarExpression();
            if (qualifier != null)
            {
                starExp.Qualifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = qualifier } } };
            }
            return starExp;
        }


        [TestMethod]
        public void ProcessQuery_SimpleSelectFromTable_DelegatesCorrectly()
        {
            // Arrange
            var tableRef = CreateNamedRef("TableA");
            var selectElement = CreateSelectScalar("Col1", "TableA");
            var querySpec = CreateQuerySpec(new List<SelectElement> { selectElement }, new List<TableReference> { tableRef });

            var expectedOutput = new List<OutputColumn> { new OutputColumn("Col1", new ColumnNode("Col1", "TableA")) };
            _stubSelectScalarProcessor.OutputToReturn = expectedOutput;

            // Act
            var resultOutput = _processor.ProcessQuery(querySpec, _mockContext);

            // Assert
            // Verify FROM clause processing
            Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is NamedTableReference));
            Assert.AreSame(tableRef, _stubNamedTableProcessor.LastProcessedFragment);
            // Removed check for _mockContext.CurrentSourceMap.Count as the processor creates a local scope map

            // Verify SELECT element processing
             Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is SelectScalarExpression));
            Assert.AreSame(selectElement, _stubSelectScalarProcessor.LastProcessedFragment);

            // Verify overall output
            Assert.AreEqual(1, resultOutput.Count);
            Assert.AreEqual(expectedOutput[0], resultOutput[0]);
        }

        [TestMethod]
        public void ProcessQuery_SelectStar_DelegatesCorrectly()
        {
             // Arrange
            var tableRef = CreateNamedRef("TableA"); // Assume this provides columns for '*'
            var starElement = CreateSelectStar();
            var querySpec = CreateQuerySpec(new List<SelectElement> { starElement }, new List<TableReference> { tableRef });

            var expectedOutput = new List<OutputColumn> {
                new OutputColumn("Col1", new ColumnNode("Col1", "TableA")),
                new OutputColumn("Col2", new ColumnNode("Col2", "TableA"))
            };
            _stubSelectStarProcessor.OutputToReturn = expectedOutput; // Stub returns expanded columns

            // Act
            var resultOutput = _processor.ProcessQuery(querySpec, _mockContext);

             // Assert
             // Verify FROM
             Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is NamedTableReference));
             Assert.AreSame(tableRef, _stubNamedTableProcessor.LastProcessedFragment);

             // Verify SELECT
             Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is SelectStarExpression));
             Assert.AreSame(starElement, _stubSelectStarProcessor.LastProcessedFragment);

             // Verify output
             Assert.AreEqual(2, resultOutput.Count);
             CollectionAssert.AreEquivalent(expectedOutput, resultOutput);
        }

         [TestMethod]
        public void ProcessQuery_MultipleSelectElements_DelegatesAll()
        {
             // Arrange
            var tableRef = CreateNamedRef("TableA");
            var selectScalar = CreateSelectScalar("Col1", "TableA");
            var starElement = CreateSelectStar();
            var querySpec = CreateQuerySpec(new List<SelectElement> { selectScalar, starElement }, new List<TableReference> { tableRef });

             _stubSelectScalarProcessor.OutputToReturn = new List<OutputColumn> { new OutputColumn("Col1", null) };
             _stubSelectStarProcessor.OutputToReturn = new List<OutputColumn> { new OutputColumn("ColA", null), new OutputColumn("ColB", null) };


            // Act
            var resultOutput = _processor.ProcessQuery(querySpec, _mockContext);

             // Assert
             Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is NamedTableReference));
             Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is SelectScalarExpression));
             Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is SelectStarExpression));
             Assert.AreSame(selectScalar, _stubSelectScalarProcessor.LastProcessedFragment);
             Assert.AreSame(starElement, _stubSelectStarProcessor.LastProcessedFragment);

             Assert.AreEqual(3, resultOutput.Count); // 1 from scalar + 2 from star
        }

         [TestMethod]
        public void ProcessQuery_NoFromClause_ProcessesSelectOnly()
        {
             // Arrange
             var selectElement = CreateSelectScalar("Col1"); // SELECT Col1 (no FROM) - Invalid SQL but test processor logic
             var querySpec = CreateQuerySpec(new List<SelectElement> { selectElement });
             _stubSelectScalarProcessor.OutputToReturn = new List<OutputColumn> { new OutputColumn("Col1", null) };


            // Act
            var resultOutput = _processor.ProcessQuery(querySpec, _mockContext);

             // Assert
             Assert.AreEqual(0, _mockFactory.GetProcessorCalls.Count(f => f is TableReference)); // No table ref processor called
             Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is SelectScalarExpression));
             Assert.AreSame(selectElement, _stubSelectScalarProcessor.LastProcessedFragment);
             Assert.AreEqual(1, resultOutput.Count);
        }

        [TestMethod]
        public void ProcessQuery_AmbiguousUnqualifiedColumn_ReturnsEmptyForAmbiguous()
        {
            // Arrange
            var tableRefA = CreateNamedRef("TableA"); // Provides Col1
            var tableRefB = CreateNamedRef("TableB"); // Also provides Col1
            var selectElement = CreateSelectScalar("Col1"); // Unqualified, ambiguous
            var querySpec = CreateQuerySpec(new List<SelectElement> { selectElement }, new List<TableReference> { tableRefA, tableRefB });

            // Configure the SelectScalar processor stub to return an empty list,
            // simulating the behavior when ambiguity resolution fails within it.
            _stubSelectScalarProcessor.OutputToReturn = new List<OutputColumn>();

            // Act
            var resultOutput = _processor.ProcessQuery(querySpec, _mockContext);

            // Assert
            // Verify FROM clause processing happened for both tables
            Assert.AreEqual(2, _mockFactory.GetProcessorCalls.Count(f => f is NamedTableReference));

            // Verify SELECT element processing was attempted
            Assert.AreEqual(1, _mockFactory.GetProcessorCalls.Count(f => f is SelectScalarExpression));
            Assert.AreSame(selectElement, _stubSelectScalarProcessor.LastProcessedFragment);

            // Verify overall output is empty because the ambiguous column wasn't resolved
            Assert.AreEqual(0, resultOutput.Count, "Output should be empty when unqualified column is ambiguous.");
        }

        // TODO: Add tests for WHERE, GROUP BY, HAVING, ORDER BY clauses when implemented.
    }
}
