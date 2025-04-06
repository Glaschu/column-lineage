using ColumnLineageCore;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting; // Assuming MSTest, adjust if using xUnit or NUnit
using System.Linq;

namespace ColumnLineageCore.Tests
{
    [TestClass]
    public class AstProviderTests
    {
        [TestMethod]
        public void Parse_ValidSql_ReturnsFragmentWithoutErrors()
        {
            // Arrange
            var provider = new AstProvider();
            string sql = "SELECT ColumnA, ColumnB FROM Table1;";

            // Act
            var fragment = provider.Parse(sql, out var errors);

            // Assert
            Assert.IsNotNull(fragment, "Fragment should not be null for valid SQL.");
            Assert.IsInstanceOfType(fragment, typeof(TSqlScript), "Fragment should be a TSqlScript.");
            Assert.IsFalse(errors.Any(), "There should be no errors for valid SQL.");
        }

        [TestMethod]
        public void Parse_InvalidSql_ReturnsFragmentWithErrors()
        {
            // Arrange
            var provider = new AstProvider();
            // Intentionally invalid SQL
            string sql = "SELECT ColumnA FROMM Table1 WHERE";

            // Act
            var fragment = provider.Parse(sql, out var errors);

            // Assert
            Assert.IsNotNull(fragment, "Parser should still return a fragment even with errors.");
            Assert.IsTrue(errors.Any(), "There should be errors for invalid SQL.");
            // Optional: Assert specific error messages or count if needed
            Assert.IsTrue(errors.Any(e => e.Message.Contains("Incorrect syntax near")), "Expected a syntax error.");
        }

        [TestMethod]
        public void Parse_EmptySql_ReturnsFragmentWithoutErrors()
        {
            // Arrange
            var provider = new AstProvider();
            string sql = ""; // Empty script

            // Act
            var fragment = provider.Parse(sql, out var errors);

            // Assert
            Assert.IsNotNull(fragment, "Fragment should not be null for empty SQL.");
            Assert.IsInstanceOfType(fragment, typeof(TSqlScript), "Fragment should be a TSqlScript.");
            Assert.IsFalse(errors.Any(), "There should be no errors for empty SQL.");
            Assert.AreEqual(0, ((TSqlScript)fragment).Batches.Count, "Empty script should have zero batches.");
        }

        // Add more tests for specific SQL Server versions or features if needed
        // e.g., testing different SqlEngineType options if made configurable
    }
}
