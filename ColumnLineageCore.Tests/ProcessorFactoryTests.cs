using ColumnLineageCore;
using ColumnLineageCore.Interfaces;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic; // Required for List

namespace ColumnLineageCore.Tests
{
    // --- Dummy/Mock Classes for Testing ---

    // Dummy Fragment Types
    public class DummyFragmentA : TSqlFragment { public override void Accept(TSqlFragmentVisitor visitor) { } public override void AcceptChildren(TSqlFragmentVisitor visitor) { } }
    public class DummyFragmentB : TSqlFragment { public override void Accept(TSqlFragmentVisitor visitor) { } public override void AcceptChildren(TSqlFragmentVisitor visitor) { } }

    // Dummy Processor Implementations
    public class DummyProcessorA : ISqlFragmentProcessor<DummyFragmentA>
    {
        public bool ProcessCalled { get; private set; } = false;
        public void Process(DummyFragmentA fragment, IProcessingContext context) { ProcessCalled = true; }
    }

    public class DummyProcessorB : ISqlFragmentProcessor<DummyFragmentB>
    {
         public void Process(DummyFragmentB fragment, IProcessingContext context) { /* No action needed for test */ }
    }

    // Dummy Processor with dependencies (for factory function test)
    public class DummyProcessorWithDep : ISqlFragmentProcessor<DummyFragmentA>
    {
        public string Dependency { get; }
        public DummyProcessorWithDep(string dependency) { Dependency = dependency; }
        public void Process(DummyFragmentA fragment, IProcessingContext context) { }
    }


    // --- Test Class ---

    [TestClass]
    public class ProcessorFactoryTests
    {
        private ProcessorFactory _factory = null!;

        [TestInitialize]
        public void TestInitialize()
        {
            _factory = new ProcessorFactory();
        }

        [TestMethod]
        public void RegisterProcessor_Type_GetProcessor_ReturnsCorrectInstance()
        {
            // Arrange
            _factory.RegisterProcessor<DummyFragmentA, DummyProcessorA>();
            var fragment = new DummyFragmentA();

            // Act
            var processor = _factory.GetProcessor(fragment); // Use instance overload

            // Assert
            Assert.IsNotNull(processor);
            Assert.IsInstanceOfType(processor, typeof(DummyProcessorA));
        }

         [TestMethod]
        public void RegisterProcessor_Type_GetProcessorGeneric_ReturnsCorrectInstance()
        {
            // Arrange
            _factory.RegisterProcessor<DummyFragmentA, DummyProcessorA>();

            // Act
            var processor = _factory.GetProcessor<DummyFragmentA>(); // Use generic overload

            // Assert
            Assert.IsNotNull(processor);
            Assert.IsInstanceOfType(processor, typeof(DummyProcessorA));
        }


        [TestMethod]
        public void RegisterProcessorInstance_GetProcessor_ReturnsSameInstance()
        {
            // Arrange
            var instance = new DummyProcessorA();
            _factory.RegisterProcessorInstance<DummyFragmentA>(instance);
            var fragment = new DummyFragmentA();

            // Act
            var processor = _factory.GetProcessor(fragment);

            // Assert
            Assert.IsNotNull(processor);
            Assert.AreSame(instance, processor, "Should return the exact registered instance.");
        }

        [TestMethod]
        public void RegisterProcessorFactory_GetProcessor_ReturnsInstanceFromFactoryFunc()
        {
            // Arrange
            string expectedDependency = "TestDep";
            _factory.RegisterProcessorFactory<DummyFragmentA>(() => new DummyProcessorWithDep(expectedDependency));
            var fragment = new DummyFragmentA();

            // Act
            var processor = _factory.GetProcessor(fragment);

            // Assert
            Assert.IsNotNull(processor);
            Assert.IsInstanceOfType(processor, typeof(DummyProcessorWithDep));
            Assert.AreEqual(expectedDependency, ((DummyProcessorWithDep)processor).Dependency);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void GetProcessor_UnregisteredType_ThrowsNotSupportedException()
        {
            // Arrange
            var fragment = new DummyFragmentB(); // Type B is not registered

            // Act
            _factory.GetProcessor(fragment);
        }

         [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void GetProcessorGeneric_UnregisteredType_ThrowsNotSupportedException()
        {
            // Arrange - Nothing registered

            // Act
            _factory.GetProcessor<DummyFragmentB>(); // Type B is not registered
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void RegisterProcessorInstance_NullInstance_ThrowsArgumentNullException()
        {
            // Act
            _factory.RegisterProcessorInstance<DummyFragmentA>(null!);
        }

         [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void RegisterProcessorFactory_NullFunc_ThrowsArgumentNullException()
        {
            // Act
            _factory.RegisterProcessorFactory<DummyFragmentA>(null!);
        }

        // Test case for incompatible type returned by factory (harder to trigger cleanly without DI)
        // [TestMethod]
        // [ExpectedException(typeof(InvalidOperationException))]
        // public void GetProcessor_FactoryReturnsIncompatibleType_ThrowsInvalidOperationException()
        // {
        //     // Arrange
        //     // Register a factory that incorrectly returns a processor for a different fragment type
        //     _factory.RegisterProcessorFactory<DummyFragmentA>(() => (ISqlFragmentProcessor<DummyFragmentA>)(object)new DummyProcessorB()); // Force incorrect cast
        //     var fragment = new DummyFragmentA();
        //
        //     // Act
        //     _factory.GetProcessor(fragment);
        // }
    }
}
