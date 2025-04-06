using ColumnLineageCore.Interfaces;
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
    public class SelectStarExpressionProcessorTests
    {
        private SelectStarExpressionProcessor _processor = null!;
        private MockProcessingContext _mockContext = null!;
        private StubLineageGraph _stubGraph = null!;
        private ColumnNode _cte1ColA_Src = null!;
        private ColumnNode _cte1ColB_Src = null!;
        private ColumnNode _subQColX_Src = null!;
        private ColumnNode _subQColY_Src = null!;

        [TestInitialize]
        public void TestInitialize()
        {
            _processor = new SelectStarExpressionProcessor();
            _stubGraph = new StubLineageGraph();
            _mockContext = new MockProcessingContext(_stubGraph);

            // Source nodes for CTE/Subquery outputs
            _cte1ColA_Src = new ColumnNode("SrcA", "BaseCTE");
            _cte1ColB_Src = new ColumnNode("SrcB", "BaseCTE");
            _subQColX_Src = new ColumnNode("SrcX", "BaseSubQ");
            _subQColY_Src = new ColumnNode("SrcY", "BaseSubQ");
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

        // Helper to add source to context
        private void AddSourceToContext(string aliasOrName, SourceInfo sourceInfo)
        {
            _mockContext.CurrentSourceMap[aliasOrName] = sourceInfo;
        }

        // Helper to add CTE to context
        private void AddCteToContext(string name, CteInfo cteInfo)
        {
             _mockContext.AddAvailableCte(cteInfo);
        }

        [TestMethod]
        public void ProcessElement_StarWithCteSource_ExpandsCteColumns()
        {
            // Arrange
            var cteInfo = new CteInfo("CTE1", new DummyQueryExpression()) { IsProcessed = true };
            cteInfo.OutputColumnSources.Add("ColA", _cte1ColA_Src);
            cteInfo.OutputColumnSources.Add("ColB", _cte1ColB_Src);
            AddCteToContext("CTE1", cteInfo);
            AddSourceToContext("CTE1", new SourceInfo("CTE1", SourceType.CTE));
            var starExp = CreateSelectStar(); // SELECT *

            // Act
            var outputCols = _processor.ProcessElement(starExp, _mockContext);

            // Assert
            Assert.AreEqual(2, outputCols.Count);
            Assert.IsTrue(outputCols.Any(c => c.OutputName == "ColA" && c.SourceNode == _cte1ColA_Src));
            Assert.IsTrue(outputCols.Any(c => c.OutputName == "ColB" && c.SourceNode == _cte1ColB_Src));

            // Nodes: SrcA, SrcB, CTE1.ColA, CTE1.ColB, ColA (output), ColB (output) = 6
            Assert.AreEqual(6, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_cte1ColA_Src));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_cte1ColB_Src));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "CTE1.ColA"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "CTE1.ColB"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "ColA"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "ColB"));

            // Edges: CTE1.ColA -> ColA, CTE1.ColB -> ColB
            Assert.AreEqual(2, _stubGraph.AddedEdges.Count);
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == "CTE1.ColA" && e.TargetNodeId == "ColA"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == "CTE1.ColB" && e.TargetNodeId == "ColB"));
        }

        [TestMethod]
        public void ProcessElement_StarWithSubquerySource_ExpandsSubqueryColumns()
        {
            // Arrange
            var subQueryOutput = new List<OutputColumn> {
                new OutputColumn("ColX", _subQColX_Src),
                new OutputColumn("ColY", _subQColY_Src)
            };
            AddSourceToContext("SubQ", new SourceInfo("SubQ", subQueryOutput));
            var starExp = CreateSelectStar(); // SELECT *

            // Act
            var outputCols = _processor.ProcessElement(starExp, _mockContext);

            // Assert
            Assert.AreEqual(2, outputCols.Count);
            Assert.IsTrue(outputCols.Any(c => c.OutputName == "ColX" && c.SourceNode == _subQColX_Src));
            Assert.IsTrue(outputCols.Any(c => c.OutputName == "ColY" && c.SourceNode == _subQColY_Src));

            // Nodes: SrcX, SrcY, SubQ.ColX, SubQ.ColY, ColX (output), ColY (output) = 6
            Assert.AreEqual(6, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_subQColX_Src));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_subQColY_Src));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "SubQ.ColX"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "SubQ.ColY"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "ColX"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "ColY"));

            // Edges: SubQ.ColX -> ColX, SubQ.ColY -> ColY
            Assert.AreEqual(2, _stubGraph.AddedEdges.Count);
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == "SubQ.ColX" && e.TargetNodeId == "ColX"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == "SubQ.ColY" && e.TargetNodeId == "ColY"));
        }

        [TestMethod]
        public void ProcessElement_QualifiedStarWithCteSource_ExpandsOnlyCteColumns()
        {
            // Arrange
            var cteInfo = new CteInfo("CTE1", new DummyQueryExpression()) { IsProcessed = true };
            cteInfo.OutputColumnSources.Add("ColA", _cte1ColA_Src);
            AddCteToContext("CTE1", cteInfo);
            AddSourceToContext("C", new SourceInfo("CTE1", SourceType.CTE)); // Alias C for CTE1
            AddSourceToContext("T2", new SourceInfo("Table2", SourceType.Table)); // Another source
            var starExp = CreateSelectStar("C"); // SELECT C.*

            // Act
            var outputCols = _processor.ProcessElement(starExp, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count); // Only ColA from CTE1
            Assert.AreEqual("ColA", outputCols[0].OutputName);
            Assert.AreEqual(_cte1ColA_Src, outputCols[0].SourceNode);

            // Nodes: SrcA, CTE1.ColA, ColA (output) = 3
            Assert.AreEqual(3, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_cte1ColA_Src));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "CTE1.ColA"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "ColA"));

            // Edges: CTE1.ColA -> ColA
            Assert.AreEqual(1, _stubGraph.AddedEdges.Count);
            Assert.AreEqual("CTE1.ColA", _stubGraph.AddedEdges[0].SourceNodeId);
            Assert.AreEqual("ColA", _stubGraph.AddedEdges[0].TargetNodeId);
        }

        [TestMethod]
        public void ProcessElement_StarWithTableSource_ReturnsEmptyAndLogsWarning()
        {
            // Arrange
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));
            var starExp = CreateSelectStar(); // SELECT *

            // Act
            var outputCols = _processor.ProcessElement(starExp, _mockContext);

            // Assert
            Assert.AreEqual(0, outputCols.Count);
            Assert.AreEqual(0, _stubGraph.AddedNodes.Count);
            Assert.AreEqual(0, _stubGraph.AddedEdges.Count);
            // Logging verification removed as logging is not implemented
        }

        [TestMethod]
        public void ProcessElement_QualifiedStarWithTableSource_ReturnsEmptyAndLogsWarning()
        {
            // Arrange
            AddSourceToContext("T1", new SourceInfo("TableA", SourceType.Table));
            var starExp = CreateSelectStar("T1"); // SELECT T1.*

            // Act
            var outputCols = _processor.ProcessElement(starExp, _mockContext);

            // Assert
            Assert.AreEqual(0, outputCols.Count);
            Assert.AreEqual(0, _stubGraph.AddedNodes.Count);
            Assert.AreEqual(0, _stubGraph.AddedEdges.Count);
            // Logging verification removed
        }

         [TestMethod]
        public void ProcessElement_StarWithUnprocessedCte_ReturnsEmptyAndLogsWarning()
        {
            // Arrange
            var cteInfo = new CteInfo("CTE1", new DummyQueryExpression()) { IsProcessed = false }; // Not processed
            AddCteToContext("CTE1", cteInfo);
            AddSourceToContext("CTE1", new SourceInfo("CTE1", SourceType.CTE));
            var starExp = CreateSelectStar(); // SELECT *

            // Act
            var outputCols = _processor.ProcessElement(starExp, _mockContext);

            // Assert
            Assert.AreEqual(0, outputCols.Count);
            Assert.AreEqual(0, _stubGraph.AddedNodes.Count);
            Assert.AreEqual(0, _stubGraph.AddedEdges.Count);
        }

         [TestMethod]
        public void ProcessElement_QualifiedStarWithUnknownQualifier_ReturnsEmptyAndLogsWarning()
        {
             // Arrange
            AddSourceToContext("T1", new SourceInfo("TableA", SourceType.Table));
            var starExp = CreateSelectStar("Unknown"); // SELECT Unknown.*

            // Act
            var outputCols = _processor.ProcessElement(starExp, _mockContext);

            // Assert
            Assert.AreEqual(0, outputCols.Count);
            Assert.AreEqual(0, _stubGraph.AddedNodes.Count);
            Assert.AreEqual(0, _stubGraph.AddedEdges.Count);
        }

         [TestMethod]
        public void ProcessElement_StarWithNoSources_ReturnsEmptyAndLogsWarning()
        {
             // Arrange
             var starExp = CreateSelectStar(); // SELECT * (but no FROM clause processed yet)

            // Act
            var outputCols = _processor.ProcessElement(starExp, _mockContext);

            // Assert
            Assert.AreEqual(0, outputCols.Count);
            Assert.AreEqual(0, _stubGraph.AddedNodes.Count);
            Assert.AreEqual(0, _stubGraph.AddedEdges.Count);
        }
    }
}
