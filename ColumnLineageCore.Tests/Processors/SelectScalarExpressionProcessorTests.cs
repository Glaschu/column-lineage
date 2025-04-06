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
    public class SelectScalarExpressionProcessorTests
    {
        private SelectScalarExpressionProcessor _processor = null!;
        private MockProcessingContext _mockContext = null!;
        private StubLineageGraph _stubGraph = null!;
        private ColumnNode _tableACol1 = null!;
        private ColumnNode _tableBCol1 = null!;
        private ColumnNode _cte1ColA = null!; // Intermediate node
        private ColumnNode _cte1UltimateSource = null!; // Ultimate source for CTE1.ColA
        private ColumnNode _subQColX = null!; // Intermediate node
        private ColumnNode _subQUltimateSource = null!; // Ultimate source for SubQ.ColX

        [TestInitialize]
        public void TestInitialize()
        {
            _processor = new SelectScalarExpressionProcessor();
            _stubGraph = new StubLineageGraph();
            _mockContext = new MockProcessingContext(_stubGraph);

            // Predefined nodes for sources
            _tableACol1 = new ColumnNode("Col1", "TableA");
            _tableBCol1 = new ColumnNode("Col1", "TableB"); // Same column name, different table
            _cte1UltimateSource = new ColumnNode("SourceCol", "BaseTableForCTE");
            _cte1ColA = new ColumnNode("ColA", "CTE1"); // Intermediate node for CTE1.ColA
            _subQUltimateSource = new ColumnNode("SourceCol", "BaseTableForSubQ");
            _subQColX = new ColumnNode("ColX", "SubQ"); // Intermediate node for SubQ.ColX
        }

        // Helper to create SelectScalarExpression with ColumnReference
        private SelectScalarExpression CreateSelectColRef(string columnName, string? sourceIdentifier = null, string? alias = null)
        {
            var multiPartId = new MultiPartIdentifier();
            if (sourceIdentifier != null) multiPartId.Identifiers.Add(new Identifier { Value = sourceIdentifier });
            multiPartId.Identifiers.Add(new Identifier { Value = columnName });

            var colRef = new ColumnReferenceExpression { MultiPartIdentifier = multiPartId }; // Assign initialized MultiPartIdentifier
            var selectScalar = new SelectScalarExpression { Expression = colRef };
            if (alias != null) { selectScalar.ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = alias } }; }
            return selectScalar;
        }

        // Helper to create SelectScalarExpression with Literal
        private SelectScalarExpression CreateSelectLiteral(string value, string alias)
        {
            var literal = new StringLiteral { Value = value };
            return new SelectScalarExpression
            {
                Expression = literal,
                ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = alias } }
            };
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
        public void ProcessElement_SimpleTableColumn_AddsNodeAndEdge()
        {
            // Arrange
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));
            var selectScalar = CreateSelectColRef("Col1", "TableA"); // SELECT TableA.Col1

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("Col1", outputCols[0].OutputName);
            Assert.AreEqual(_tableACol1, outputCols[0].SourceNode); // Check ultimate source

            Assert.AreEqual(2, _stubGraph.AddedNodes.Count); // TableA.Col1, Col1 (output)
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_tableACol1));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "Col1")); // Output node

            Assert.AreEqual(1, _stubGraph.AddedEdges.Count); // TableA.Col1 -> Col1
            Assert.AreEqual(_tableACol1.Id, _stubGraph.AddedEdges[0].SourceNodeId);
            Assert.AreEqual("Col1", _stubGraph.AddedEdges[0].TargetNodeId);
        }

        [TestMethod]
        public void ProcessElement_TableColumnWithAlias_AddsNodeAndEdgeWithAliasAsOutput()
        {
            // Arrange
            AddSourceToContext("T1", new SourceInfo("TableA", SourceType.Table));
            var selectScalar = CreateSelectColRef("Col1", "T1", "AliasCol1"); // SELECT T1.Col1 AS AliasCol1

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("AliasCol1", outputCols[0].OutputName);
            Assert.AreEqual(_tableACol1, outputCols[0].SourceNode);

            Assert.AreEqual(2, _stubGraph.AddedNodes.Count); // TableA.Col1, AliasCol1 (output)
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_tableACol1));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "AliasCol1"));

            Assert.AreEqual(1, _stubGraph.AddedEdges.Count); // TableA.Col1 -> AliasCol1
            Assert.AreEqual(_tableACol1.Id, _stubGraph.AddedEdges[0].SourceNodeId);
            Assert.AreEqual("AliasCol1", _stubGraph.AddedEdges[0].TargetNodeId);
        }

         [TestMethod]
        public void ProcessElement_UnqualifiedColumnSingleSource_ResolvesCorrectly()
        {
            // Arrange
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table)); // Only one source
            var selectScalar = CreateSelectColRef("Col1"); // SELECT Col1

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("Col1", outputCols[0].OutputName);
            Assert.AreEqual(_tableACol1, outputCols[0].SourceNode);

            Assert.AreEqual(1, _stubGraph.AddedEdges.Count);
            Assert.AreEqual(_tableACol1.Id, _stubGraph.AddedEdges[0].SourceNodeId);
            Assert.AreEqual("Col1", _stubGraph.AddedEdges[0].TargetNodeId);
        }

        [TestMethod]
        public void ProcessElement_UnqualifiedColumnAmbiguousSource_ReturnsEmptyAndLogs()
        {
             // Arrange
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));
            AddSourceToContext("TableB", new SourceInfo("TableB", SourceType.Table)); // Two sources
            var selectScalar = CreateSelectColRef("Col1"); // SELECT Col1 (ambiguous)

            // Act
             var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

             // Assert (Updated: Ambiguity should now result in no output/edges)
             Assert.AreEqual(0, outputCols.Count, "Output list should be empty due to ambiguity.");
             Assert.AreEqual(0, _stubGraph.AddedNodes.Count, "No nodes should be added due to ambiguity.");
             Assert.AreEqual(0, _stubGraph.AddedEdges.Count, "No edges should be added due to ambiguity.");
        }


        [TestMethod]
        public void ProcessElement_CteColumn_AddsEdgeFromCteIntermediateNode()
        {
            // Arrange
            var cteInfo = new CteInfo("CTE1", new DummyQueryExpression()) { IsProcessed = true };
            cteInfo.OutputColumnSources.Add("ColA", _cte1UltimateSource); // CTE1.ColA comes from BaseTableForCTE.SourceCol
            AddCteToContext("CTE1", cteInfo);
            AddSourceToContext("CTE1", new SourceInfo("CTE1", SourceType.CTE));
            var selectScalar = CreateSelectColRef("ColA", "CTE1", "OutputA"); // SELECT CTE1.ColA AS OutputA

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("OutputA", outputCols[0].OutputName);
            Assert.AreEqual(_cte1UltimateSource, outputCols[0].SourceNode); // Ultimate source

            // Nodes: BaseTableForCTE.SourceCol, CTE1.ColA (intermediate), OutputA (final)
            Assert.AreEqual(3, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_cte1UltimateSource));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_cte1ColA)); // Check intermediate node
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "OutputA"));

            // Edge: CTE1.ColA -> OutputA
            Assert.AreEqual(1, _stubGraph.AddedEdges.Count);
            Assert.AreEqual(_cte1ColA.Id, _stubGraph.AddedEdges[0].SourceNodeId); // Edge from intermediate
            Assert.AreEqual("OutputA", _stubGraph.AddedEdges[0].TargetNodeId);
        }

        [TestMethod]
        public void ProcessElement_SubqueryColumn_AddsEdgeFromSubqueryIntermediateNode()
        {
            // Arrange
            var subQueryOutput = new List<OutputColumn> { new OutputColumn("ColX", _subQUltimateSource) };
            AddSourceToContext("SubQ", new SourceInfo("SubQ", subQueryOutput)); // SubQ alias maps to subquery output
            var selectScalar = CreateSelectColRef("ColX", "SubQ", "OutputX"); // SELECT SubQ.ColX AS OutputX

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("OutputX", outputCols[0].OutputName);
            Assert.AreEqual(_subQUltimateSource, outputCols[0].SourceNode); // Ultimate source

            // Nodes: BaseTableForSubQ.SourceCol, SubQ.ColX (intermediate), OutputX (final)
            Assert.AreEqual(3, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_subQUltimateSource));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_subQColX)); // Check intermediate node
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "OutputX"));

            // Edges:
            // 1. BaseTableForSubQ.SourceCol -> SubQ.ColX (added on demand during resolution)
            // 2. SubQ.ColX -> OutputX (added by AddGraphElements)
            Assert.AreEqual(2, _stubGraph.AddedEdges.Count);
            var edge1 = _stubGraph.AddedEdges.FirstOrDefault(e => e.SourceNodeId == _subQUltimateSource.Id && e.TargetNodeId == _subQColX.Id);
            var edge2 = _stubGraph.AddedEdges.FirstOrDefault(e => e.SourceNodeId == _subQColX.Id && e.TargetNodeId == "OutputX");
            Assert.IsNotNull(edge1, "Edge ultimate -> intermediate missing.");
            Assert.IsNotNull(edge2, "Edge intermediate -> final missing.");
        }


        [TestMethod]
        public void ProcessElement_Literal_AddsOutputNodeOnly()
        {
            // Arrange
            var selectScalar = CreateSelectLiteral("abc", "LiteralOutput"); // SELECT 'abc' AS LiteralOutput

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("LiteralOutput", outputCols[0].OutputName);
            Assert.IsNull(outputCols[0].SourceNode); // No source node

            Assert.AreEqual(1, _stubGraph.AddedNodes.Count); // Only LiteralOutput (output)
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "LiteralOutput"));
            Assert.AreEqual(0, _stubGraph.AddedEdges.Count); // No edges
        }

        [TestMethod]
        public void ProcessElement_ColumnWithoutAlias_UsesColumnNameAsOutput()
        {
             // Arrange
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));
            var selectScalar = CreateSelectColRef("Col1", "TableA"); // SELECT TableA.Col1

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("Col1", outputCols[0].OutputName); // Output name is Col1
            Assert.AreEqual(_tableACol1, outputCols[0].SourceNode);
            Assert.AreEqual("Col1", _stubGraph.AddedEdges[0].TargetNodeId); // Edge target is Col1
        }

         [TestMethod]
        public void ProcessElement_InCteDefinitionContext_AddsEdgeToCteIntermediateNode()
        {
            // Arrange
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));
            var selectScalar = CreateSelectColRef("Col1", "TableA", "CteOutCol"); // SELECT TableA.Col1 AS CteOutCol
            var cteInfoToPopulate = new CteInfo("DefiningCTE", new DummyQueryExpression());
            _mockContext.IsProcessingCteDefinition = true;
            _mockContext.CteInfoToPopulate = cteInfoToPopulate;

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("CteOutCol", outputCols[0].OutputName);
            Assert.AreEqual(_tableACol1, outputCols[0].SourceNode); // Ultimate source

            // Nodes: TableA.Col1, DefiningCTE.CteOutCol (intermediate)
            Assert.AreEqual(2, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_tableACol1));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "DefiningCTE.CteOutCol"));

            // Edge: TableA.Col1 -> DefiningCTE.CteOutCol
            Assert.AreEqual(1, _stubGraph.AddedEdges.Count);
            Assert.AreEqual(_tableACol1.Id, _stubGraph.AddedEdges[0].SourceNodeId);
            Assert.AreEqual("DefiningCTE.CteOutCol", _stubGraph.AddedEdges[0].TargetNodeId);

            // Verify CteInfo population
            Assert.IsTrue(cteInfoToPopulate.OutputColumnSources.ContainsKey("CteOutCol"));
            Assert.AreEqual(_tableACol1, cteInfoToPopulate.OutputColumnSources["CteOutCol"]);
        }

         [TestMethod]
        public void ProcessElement_InSubqueryContext_AddsNodesButNoEdges()
        {
             // Arrange
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));
            var selectScalar = CreateSelectColRef("Col1", "TableA", "SubQOutput"); // SELECT TableA.Col1 AS SubQOutput
            _mockContext.IsSubquery = true;

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

             // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("SubQOutput", outputCols[0].OutputName);
            Assert.AreEqual(_tableACol1, outputCols[0].SourceNode);

            // Nodes: TableA.Col1 (source) should be added/ensured
            Assert.AreEqual(1, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(_tableACol1));

             Assert.AreEqual(0, _stubGraph.AddedEdges.Count); // No edges added in subquery context
        }

        [TestMethod]
        public void ProcessElement_WindowFunction_AddsEdgesFromPartitionAndOrderSources()
        {
            // Arrange: SELECT rn = ROW_NUMBER() OVER(PARTITION BY PCol ORDER BY OCol) FROM TableA
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));

            var partitionColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "PCol" } } } };
            var orderColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "OCol" } } } };

            // Window functions are represented by FunctionCall with an OverClause
            var windowFunc = new FunctionCall
            {
                FunctionName = new Identifier { Value = "ROW_NUMBER" },
                OverClause = new OverClause() // Initialize OverClause
                // Parameters are usually empty for ROW_NUMBER
            };
            // Add partitions and order by to the OverClause
            windowFunc.OverClause.Partitions.Add(partitionColRef);
            windowFunc.OverClause.OrderByClause = new OrderByClause { OrderByElements = { new ExpressionWithSortOrder { Expression = orderColRef } } };

            var selectScalar = new SelectScalarExpression
            {
                Expression = windowFunc,
                ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "rn" } }
            };

            var pcolSourceNode = new ColumnNode("PCol", "TableA");
            var ocolSourceNode = new ColumnNode("OCol", "TableA");

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("rn", outputCols[0].OutputName);
            Assert.IsNull(outputCols[0].SourceNode, "Window function output should have null ultimate source.");

            // Nodes: TableA.PCol, TableA.OCol, rn (output) = 3
            Assert.AreEqual(3, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(pcolSourceNode));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(ocolSourceNode));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "rn"));

            // Edges: TableA.PCol -> rn, TableA.OCol -> rn
            Assert.AreEqual(2, _stubGraph.AddedEdges.Count);
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == pcolSourceNode.Id && e.TargetNodeId == "rn"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == ocolSourceNode.Id && e.TargetNodeId == "rn"));
        }

        [TestMethod]
        public void ProcessElement_WindowFunctionWithParamsAndAgg_AddsEdgesFromParamAndAggSources()
        {
            // Arrange: SELECT LagCol = LAG(ColA) OVER(ORDER BY Id), SumCol = SUM(ColB) OVER(PARTITION BY Grp) FROM TableA
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));

            var lagParamColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "ColA" } } } };
            var lagOrderColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "Id" } } } };
            var sumParamColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "ColB" } } } };
            var sumPartitionColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "Grp" } } } };

            var lagFunc = new FunctionCall
            {
                FunctionName = new Identifier { Value = "LAG" },
                Parameters = { lagParamColRef }, // ColA is a parameter
                OverClause = new OverClause { OrderByClause = new OrderByClause { OrderByElements = { new ExpressionWithSortOrder { Expression = lagOrderColRef } } } }
            };
            var sumFunc = new FunctionCall
            {
                FunctionName = new Identifier { Value = "SUM" },
                Parameters = { sumParamColRef }, // ColB is a parameter
                OverClause = new OverClause { Partitions = { sumPartitionColRef } }
            };

            var selectLag = new SelectScalarExpression { Expression = lagFunc, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "LagCol" } } };
            var selectSum = new SelectScalarExpression { Expression = sumFunc, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "SumCol" } } };

            var colASourceNode = new ColumnNode("ColA", "TableA");
            var idSourceNode = new ColumnNode("Id", "TableA");
            var colBSourceNode = new ColumnNode("ColB", "TableA");
            var grpSourceNode = new ColumnNode("Grp", "TableA");

            // Act
            var outputLag = _processor.ProcessElement(selectLag, _mockContext);
            var outputSum = _processor.ProcessElement(selectSum, _mockContext); // Process separately for clarity

            // Assert LagCol
            Assert.AreEqual(1, outputLag.Count);
            Assert.AreEqual("LagCol", outputLag[0].OutputName);
            Assert.IsNull(outputLag[0].SourceNode);

            // Assert SumCol
            Assert.AreEqual(1, outputSum.Count);
            Assert.AreEqual("SumCol", outputSum[0].OutputName);
            Assert.IsNull(outputSum[0].SourceNode);

            // Nodes: TableA.ColA, TableA.Id, TableA.ColB, TableA.Grp, LagCol, SumCol = 6
            Assert.AreEqual(6, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(colASourceNode));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(idSourceNode));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(colBSourceNode));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(grpSourceNode));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "LagCol"));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "SumCol"));

            // Edges:
            // Lag: TableA.ColA -> LagCol (Param), TableA.Id -> LagCol (Order)
            // Sum: TableA.ColB -> SumCol (Param), TableA.Grp -> SumCol (Partition)
            Assert.AreEqual(4, _stubGraph.AddedEdges.Count);
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == colASourceNode.Id && e.TargetNodeId == "LagCol"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == idSourceNode.Id && e.TargetNodeId == "LagCol"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == colBSourceNode.Id && e.TargetNodeId == "SumCol"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == grpSourceNode.Id && e.TargetNodeId == "SumCol"));
        }


        [TestMethod]
        public void ProcessElement_BinaryExpression_AddsEdgesFromBothSources()
        {
            // Arrange: SELECT Combined = T1.ColA + T2.ColB FROM TableA T1, TableB T2
            AddSourceToContext("T1", new SourceInfo("TableA", SourceType.Table));
            AddSourceToContext("T2", new SourceInfo("TableB", SourceType.Table));

            var colRefA = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "T1" }, new Identifier { Value = "ColA" } } } };
            var colRefB = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "T2" }, new Identifier { Value = "ColB" } } } };

            var binaryExpr = new BinaryExpression
            {
                FirstExpression = colRefA,
                SecondExpression = colRefB,
                BinaryExpressionType = BinaryExpressionType.Add
            };

            var selectScalar = new SelectScalarExpression
            {
                Expression = binaryExpr,
                ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "Combined" } }
            };

            var sourceNodeA = new ColumnNode("ColA", "TableA");
            var sourceNodeB = new ColumnNode("ColB", "TableB");

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("Combined", outputCols[0].OutputName);
            Assert.IsNull(outputCols[0].SourceNode, "Binary expression output should have null ultimate source.");

            // Nodes: TableA.ColA, TableB.ColB, Combined (output) = 3
            Assert.AreEqual(3, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(sourceNodeA));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(sourceNodeB));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "Combined"));

            // Edges: TableA.ColA -> Combined, TableB.ColB -> Combined
            Assert.AreEqual(2, _stubGraph.AddedEdges.Count);
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == sourceNodeA.Id && e.TargetNodeId == "Combined"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == sourceNodeA.Id && e.TargetNodeId == "Combined"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == sourceNodeA.Id && e.TargetNodeId == "Combined"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == sourceNodeB.Id && e.TargetNodeId == "Combined"));
        }

        [TestMethod]
        public void ProcessElement_NestedFunctionCall_AddsEdgeFromInnerSource()
        {
            // Arrange: SELECT Result = ISNULL(SUM(ColA), 0) FROM TableA
            AddSourceToContext("TableA", new SourceInfo("TableA", SourceType.Table));

            var innerColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "ColA" } } } };
            var sumFunc = new FunctionCall { FunctionName = new Identifier { Value = "SUM" }, Parameters = { innerColRef } };
            var zeroLiteral = new IntegerLiteral { Value = "0" };
            var isnullFunc = new FunctionCall { FunctionName = new Identifier { Value = "ISNULL" }, Parameters = { sumFunc, zeroLiteral } };

            var selectScalar = new SelectScalarExpression
            {
                Expression = isnullFunc,
                ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "Result" } }
            };

            var sourceNodeA = new ColumnNode("ColA", "TableA");

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("Result", outputCols[0].OutputName);
            Assert.IsNull(outputCols[0].SourceNode, "Nested function output should have null ultimate source.");

            // Nodes: TableA.ColA, Result (output) = 2
            Assert.AreEqual(2, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(sourceNodeA));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "Result"));

            // Edge: TableA.ColA -> Result
            Assert.AreEqual(1, _stubGraph.AddedEdges.Count);
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == sourceNodeA.Id && e.TargetNodeId == "Result"));
        }

        [TestMethod]
        public void ProcessElement_CaseExpression_AddsEdgesFromWhenAndThenElseSources()
        {
            // Arrange: SELECT Result = CASE WHEN T1.ColA > 0 THEN T1.ColB ELSE T2.ColC END FROM TableA T1, TableB T2
            AddSourceToContext("T1", new SourceInfo("TableA", SourceType.Table));
            AddSourceToContext("T2", new SourceInfo("TableB", SourceType.Table));

            var whenColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "T1" }, new Identifier { Value = "ColA" } } } };
            var thenColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "T1" }, new Identifier { Value = "ColB" } } } };
            var elseColRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "T2" }, new Identifier { Value = "ColC" } } } };

            // WHEN condition needs to be a BooleanExpression
            var comparisonExpr = new BooleanComparisonExpression
            {
                FirstExpression = whenColRef,
                SecondExpression = new IntegerLiteral { Value = "0" },
                ComparisonType = BooleanComparisonType.GreaterThan
            };

            var caseExpr = new SearchedCaseExpression
            {
                WhenClauses = { new SearchedWhenClause { WhenExpression = comparisonExpr, ThenExpression = thenColRef } }, // Use BooleanComparisonExpression
                ElseExpression = elseColRef
            };

            var selectScalar = new SelectScalarExpression
            {
                Expression = caseExpr,
                ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "Result" } }
            };

            var sourceNodeA = new ColumnNode("ColA", "TableA");
            var sourceNodeB = new ColumnNode("ColB", "TableA");
            var sourceNodeC = new ColumnNode("ColC", "TableB");

            // Act
            var outputCols = _processor.ProcessElement(selectScalar, _mockContext);

            // Assert
            Assert.AreEqual(1, outputCols.Count);
            Assert.AreEqual("Result", outputCols[0].OutputName);
            Assert.IsNull(outputCols[0].SourceNode, "CASE expression output should have null ultimate source.");

            // Nodes: TableA.ColA, TableA.ColB, TableB.ColC, Result (output) = 4
            Assert.AreEqual(4, _stubGraph.AddedNodes.Count);
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(sourceNodeA));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(sourceNodeB));
            Assert.IsTrue(_stubGraph.AddedNodes.Contains(sourceNodeC));
            Assert.IsTrue(_stubGraph.AddedNodes.Any(n => n.Id == "Result"));

            // Edges: TableA.ColA -> Result, TableA.ColB -> Result, TableB.ColC -> Result
            Assert.AreEqual(3, _stubGraph.AddedEdges.Count);
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == sourceNodeA.Id && e.TargetNodeId == "Result"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == sourceNodeB.Id && e.TargetNodeId == "Result"));
            Assert.IsTrue(_stubGraph.AddedEdges.Any(e => e.SourceNodeId == sourceNodeC.Id && e.TargetNodeId == "Result"));
        }

    }
}
