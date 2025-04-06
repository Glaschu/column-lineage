using ColumnLineageCore;
using ColumnLineageCore.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace ColumnLineageCore.Tests
{
    [TestClass]
    public class LineageGraphTests
    {
        private LineageGraph _graph = null!; // Non-null initialized in TestInitialize
        private ColumnNode _nodeA = null!;
        private ColumnNode _nodeB = null!;
        private ColumnNode _nodeC = null!;

        [TestInitialize]
        public void TestInitialize()
        {
            _graph = new LineageGraph();
            _nodeA = new ColumnNode("ColA", "Table1"); // Table1.ColA
            _nodeB = new ColumnNode("ColB", "Table1"); // Table1.ColB
            _nodeC = new ColumnNode("OutputCol");      // OutputCol
        }

        [TestMethod]
        public void AddNode_NewNode_AddsNodeAndReturnsIt()
        {
            // Act
            var addedNode = _graph.AddNode(_nodeA);

            // Assert
            Assert.AreEqual(_nodeA, addedNode, "Should return the added node.");
            Assert.AreEqual(1, _graph.Nodes.Count(), "Graph should contain 1 node.");
            Assert.IsTrue(_graph.Nodes.Contains(_nodeA), "Graph should contain the added node.");
        }

        [TestMethod]
        public void AddNode_DuplicateNode_ReturnsExistingNodeAndDoesNotAdd()
        {
            // Arrange
            _graph.AddNode(_nodeA); // Add first time
            var duplicateNodeA = new ColumnNode("ColA", "Table1"); // Same ID

            // Act
            var returnedNode = _graph.AddNode(duplicateNodeA);

            // Assert
            Assert.AreEqual(_nodeA, returnedNode, "Should return the existing node.");
            Assert.AreNotSame(duplicateNodeA, returnedNode, "Should return the original instance, not the duplicate.");
            Assert.AreEqual(1, _graph.Nodes.Count(), "Graph should still contain only 1 node.");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AddNode_NullNode_ThrowsArgumentNullException()
        {
            // Act
            _graph.AddNode(null!); // Pass null explicitly
        }

        [TestMethod]
        public void AddEdge_NewEdge_AddsNodesAndEdgeAndReturnsEdge()
        {
            // Act
            var addedEdge = _graph.AddEdge(_nodeA, _nodeC);

            // Assert
            Assert.IsNotNull(addedEdge, "Returned edge should not be null.");
            Assert.AreEqual(_nodeA.Id, addedEdge.SourceNodeId, "Edge source ID should match.");
            Assert.AreEqual(_nodeC.Id, addedEdge.TargetNodeId, "Edge target ID should match.");

            Assert.AreEqual(2, _graph.Nodes.Count(), "Graph should contain 2 nodes (source and target).");
            Assert.IsTrue(_graph.Nodes.Contains(_nodeA), "Graph should contain source node.");
            Assert.IsTrue(_graph.Nodes.Contains(_nodeC), "Graph should contain target node.");

            Assert.AreEqual(1, _graph.Edges.Count(), "Graph should contain 1 edge.");
            Assert.IsTrue(_graph.Edges.Any(e => e.SourceNodeId == _nodeA.Id && e.TargetNodeId == _nodeC.Id), "Graph should contain the added edge.");
        }

        [TestMethod]
        public void AddEdge_DuplicateEdge_ReturnsExistingEdgeAndDoesNotAdd()
        {
            // Arrange
            var firstEdge = _graph.AddEdge(_nodeA, _nodeC);
            var duplicateNodeA = new ColumnNode("ColA", "Table1"); // Different instance, same ID
            var duplicateNodeC = new ColumnNode("OutputCol");      // Different instance, same ID

            // Act
            var returnedEdge = _graph.AddEdge(duplicateNodeA, duplicateNodeC);

            // Assert
            Assert.AreEqual(firstEdge, returnedEdge, "Should return the existing edge.");
            Assert.AreSame(firstEdge, returnedEdge, "Should return the original edge instance."); // Check instance equality
            Assert.AreEqual(2, _graph.Nodes.Count(), "Graph should still contain only 2 nodes.");
            Assert.AreEqual(1, _graph.Edges.Count(), "Graph should still contain only 1 edge.");
        }

         [TestMethod]
        public void AddEdge_NodesAlreadyExist_AddsOnlyEdge()
        {
            // Arrange
            _graph.AddNode(_nodeA);
            _graph.AddNode(_nodeC);
            int initialNodeCount = _graph.Nodes.Count();

            // Act
            var addedEdge = _graph.AddEdge(_nodeA, _nodeC);

            // Assert
            Assert.AreEqual(initialNodeCount, _graph.Nodes.Count(), "Node count should not change.");
            Assert.AreEqual(1, _graph.Edges.Count(), "Graph should contain 1 edge.");
            Assert.AreEqual(_nodeA.Id, addedEdge.SourceNodeId);
            Assert.AreEqual(_nodeC.Id, addedEdge.TargetNodeId);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AddEdge_NullSource_ThrowsArgumentNullException()
        {
            // Act
            _graph.AddEdge(null!, _nodeC);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AddEdge_NullTarget_ThrowsArgumentNullException()
        {
            // Act
            _graph.AddEdge(_nodeA, null!);
        }

        [TestMethod]
        public void Nodes_Property_ReturnsAllAddedNodes()
        {
            // Arrange
            _graph.AddNode(_nodeA);
            _graph.AddNode(_nodeB);
            _graph.AddNode(_nodeC);

            // Act
            var nodes = _graph.Nodes.ToList();

            // Assert
            Assert.AreEqual(3, nodes.Count);
            CollectionAssert.Contains(nodes, _nodeA);
            CollectionAssert.Contains(nodes, _nodeB);
            CollectionAssert.Contains(nodes, _nodeC);
        }

        [TestMethod]
        public void Edges_Property_ReturnsAllAddedEdges()
        {
             // Arrange
            var edge1 = _graph.AddEdge(_nodeA, _nodeC);
            var edge2 = _graph.AddEdge(_nodeB, _nodeC);

            // Act
            var edges = _graph.Edges.ToList();

            // Assert
            Assert.AreEqual(2, edges.Count);
            CollectionAssert.Contains(edges, edge1);
            CollectionAssert.Contains(edges, edge2);
        }
    }
}
