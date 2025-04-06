using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors; // Added
using ColumnLineageCore.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Moq;
using System.Collections.Generic;
using Xunit;

namespace ColumnLineageCore.Tests
{
    /// <summary>
    /// Contains tests primarily focused on the LineageAnalyzer's orchestration,
    /// ensuring it calls the correct processors and handles basic script structures.
    /// More detailed processor logic is tested in specific processor test files.
    /// </summary>
    public class SqlParserTests // Renaming might be appropriate later (e.g., LineageAnalyzerTests)
    {
        private readonly Mock<IAstProvider> _mockAstProvider;
        private readonly Mock<IProcessorFactory> _mockProcessorFactory;
        private readonly Mock<ICteScopeProcessor> _mockCteProcessor;
        private readonly Mock<IViewDefinitionProvider> _mockViewProvider; // Added mock
        private readonly LineageAnalyzer _analyzer;

        public SqlParserTests()
        {
            _mockAstProvider = new Mock<IAstProvider>();
            _mockProcessorFactory = new Mock<IProcessorFactory>();
            _mockCteProcessor = new Mock<ICteScopeProcessor>();
            _mockViewProvider = new Mock<IViewDefinitionProvider>(); // Instantiate mock

            // Setup default behaviors if needed
            // Example: Setup a generic processor mock
            var mockGenericProcessor = new Mock<ISqlFragmentProcessor<TSqlFragment>>();
            _mockProcessorFactory.Setup(f => f.GetProcessor(It.IsAny<TSqlFragment>()))
                                 .Returns(mockGenericProcessor.Object);

            // Instantiate Analyzer with all required mocks
            _analyzer = new LineageAnalyzer(
                _mockAstProvider.Object,
                _mockProcessorFactory.Object,
                _mockCteProcessor.Object,
                _mockViewProvider.Object // Pass mock
            );
        }

        private TSqlFragment ParseTestSql(string sql, out IList<ParseError> errors)
        {
            var parser = new TSql160Parser(true, SqlEngineType.All);
            var fragment = parser.Parse(new System.IO.StringReader(sql), out errors);
            return fragment;
        }

        [Fact]
        public void Analyze_SimpleSelect_CallsProcessor()
        {
            // Arrange
            var sql = "SELECT ColA FROM Table1;";
            var script = ParseTestSql(sql, out var errors) as TSqlScript;
            Xunit.Assert.NotNull(script);
            Xunit.Assert.Empty(errors);

            _mockAstProvider.Setup(p => p.Parse(sql, out errors)).Returns(script);

            // Mock the specific processor for QuerySpecification
            var mockQuerySpecProcessor = new Mock<IQueryExpressionProcessor<QuerySpecification>>();
            _mockProcessorFactory.Setup(f => f.GetProcessor(It.IsAny<QuerySpecification>()))
                                 .Returns(mockQuerySpecProcessor.Object);

            // Act
            _analyzer.Analyze(sql);

            // Assert
            // Verify that the processor for QuerySpecification was retrieved and its method called
            _mockProcessorFactory.Verify(f => f.GetProcessor(It.IsAny<QuerySpecification>()), Times.Once);
            mockQuerySpecProcessor.Verify(p => p.ProcessQuery(It.IsAny<QuerySpecification>(), It.IsAny<IProcessingContext>()), Times.Once);
        }

        // Add more tests for different SQL structures (INSERT, UPDATE, CTEs, etc.)
        // focusing on verifying that the LineageAnalyzer correctly invokes the
        // appropriate processors via the factory.
    }
}
