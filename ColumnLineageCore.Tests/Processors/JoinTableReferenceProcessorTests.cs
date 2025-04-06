using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Processors;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom; // Ensure this is present

namespace ColumnLineageCore.Tests.Processors
{
    // --- Mocks/Stubs for Dependencies ---

    // Mock Processor Factory to track calls
    public class MockProcessorFactory : IProcessorFactory
    {
        public Dictionary<Type, object> RegisteredProcessors { get; } = new Dictionary<Type, object>();
        public List<TSqlFragment> GetProcessorCalls { get; } = new List<TSqlFragment>();

        public ISqlFragmentProcessor<TFragment> GetProcessor<TFragment>(TFragment? fragment = null) where TFragment : TSqlFragment
        {
            if (fragment != null)
            {
                GetProcessorCalls.Add(fragment); // Track the fragment instance passed
            }

            if (RegisteredProcessors.TryGetValue(typeof(TFragment), out var processorObj))
            {
                if (processorObj is ISqlFragmentProcessor<TFragment> specificProcessor)
                {
                    return specificProcessor;
                }
                throw new InvalidCastException($"Processor for {typeof(TFragment).Name} is wrong type.");
            }
            throw new NotSupportedException($"No processor registered for {typeof(TFragment).Name}");
        }
    }

    // Stub Processors for children
    public class StubNamedTableProcessor : Interfaces.Processors.ITableReferenceProcessor<NamedTableReference>
    {
        public NamedTableReference? LastProcessedFragment { get; private set; }
        public void Process(NamedTableReference fragment, IProcessingContext context) { LastProcessedFragment = fragment; }
    }
     public class StubJoinTableProcessor : Interfaces.Processors.ITableReferenceProcessor<JoinTableReference>
    {
        public JoinTableReference? LastProcessedFragment { get; private set; }
        public void Process(JoinTableReference fragment, IProcessingContext context) { LastProcessedFragment = fragment; }
    }
    // Add Stub for QueryDerivedTableProcessor if needed for tests


    // --- Test Class ---
    [TestClass]
    public class JoinTableReferenceProcessorTests
    {
        private JoinTableReferenceProcessor _processor = null!;
        private MockProcessingContext _mockContext = null!;
        private MockProcessorFactory _mockFactory = null!;
        private StubNamedTableProcessor _stubNamedProcessor = null!;
         private StubJoinTableProcessor _stubJoinProcessor = null!; // For nested joins

        [TestInitialize]
        public void TestInitialize()
        {
            _processor = new JoinTableReferenceProcessor();
            _mockFactory = new MockProcessorFactory();
            _mockContext = new MockProcessingContext(factory: _mockFactory); // Inject mock factory

            // Register stub processors in the mock factory
            _stubNamedProcessor = new StubNamedTableProcessor();
            _stubJoinProcessor = new StubJoinTableProcessor();
            _mockFactory.RegisteredProcessors[typeof(NamedTableReference)] = _stubNamedProcessor;
             _mockFactory.RegisteredProcessors[typeof(JoinTableReference)] = _stubJoinProcessor; // Register self for nested joins
            // Register QueryDerivedTable stub if testing that path
        }

        // Helper to create simple NamedTableReference
        private NamedTableReference CreateNamedRef(string name)
        {
             var schemaObject = new SchemaObjectName();
            schemaObject.Identifiers.Add(new Identifier { Value = name });
            return new NamedTableReference { SchemaObject = schemaObject };
        }

         // Helper to create simple QualifiedJoin
        private QualifiedJoin CreateQualifiedJoin(TableReference first, TableReference second)
        {
            return new QualifiedJoin
            {
                FirstTableReference = first,
                SecondTableReference = second,
                QualifiedJoinType = QualifiedJoinType.Inner, // Default type for test
                // Use 1=1 as a simple dummy search condition
                SearchCondition = new BooleanComparisonExpression
                {
                    FirstExpression = new IntegerLiteral { Value = "1" },
                    SecondExpression = new IntegerLiteral { Value = "1" },
                    ComparisonType = BooleanComparisonType.Equals
                }
            };
        }


        [TestMethod]
        public void Process_QualifiedJoin_DelegatesBothChildrenToFactory()
        {
            // Arrange
            var tableA = CreateNamedRef("TableA");
            var tableB = CreateNamedRef("TableB");
            var joinRef = CreateQualifiedJoin(tableA, tableB);

            // Act
            _processor.Process(joinRef, _mockContext);

            // Assert
            Assert.AreEqual(2, _mockFactory.GetProcessorCalls.Count, "Factory GetProcessor should be called twice.");
            // Check that the correct fragments were passed to the factory
            CollectionAssert.Contains(_mockFactory.GetProcessorCalls, tableA);
            CollectionAssert.Contains(_mockFactory.GetProcessorCalls, tableB);
            // Check that the stub processors were actually called via Process
            Assert.AreSame(tableB, _stubNamedProcessor.LastProcessedFragment, "StubNamedProcessor should have processed TableB last."); // Expect the second table processed
             Assert.IsTrue(_stubNamedProcessor.LastProcessedFragment == tableA || _stubNamedProcessor.LastProcessedFragment == tableB, "StubNamedProcessor should have processed one of the tables."); // This remains a valid check

        }

         [TestMethod]
        public void Process_NestedJoin_DelegatesRecursively()
        {
            // Arrange
            var tableA = CreateNamedRef("TableA");
            var tableB = CreateNamedRef("TableB");
            var tableC = CreateNamedRef("TableC");
            var innerJoin = CreateQualifiedJoin(tableB, tableC);
            var outerJoin = CreateQualifiedJoin(tableA, innerJoin); // A JOIN (B JOIN C)

            // Act
            _processor.Process(outerJoin, _mockContext);

            // Assert
            // Check that the factory was called for the immediate children of outerJoin
            Assert.AreEqual(2, _mockFactory.GetProcessorCalls.Count, "Factory GetProcessor should be called twice (for A and innerJoin).");
            CollectionAssert.Contains(_mockFactory.GetProcessorCalls, tableA, "Factory should have been called for tableA.");
            CollectionAssert.Contains(_mockFactory.GetProcessorCalls, innerJoin, "Factory should have been called for innerJoin.");

            // Check that the correct stub processors were invoked for the immediate children
            Assert.IsNotNull(_stubNamedProcessor.LastProcessedFragment, "StubNamedProcessor should have been called for tableA.");
            Assert.AreSame(tableA, _stubNamedProcessor.LastProcessedFragment);
            Assert.IsNotNull(_stubJoinProcessor.LastProcessedFragment, "StubJoinProcessor should have been called for innerJoin.");
            Assert.AreSame(innerJoin, _stubJoinProcessor.LastProcessedFragment);
        }


        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Process_NullJoinRef_ThrowsArgumentNullException()
        {
            // Act
            _processor.Process(null!, _mockContext);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Process_NullContext_ThrowsArgumentNullException()
        {
            // Arrange
             var tableA = CreateNamedRef("TableA");
            var tableB = CreateNamedRef("TableB");
            var joinRef = CreateQualifiedJoin(tableA, tableB);

            // Act
            _processor.Process(joinRef, null!);
        }
    }
}
