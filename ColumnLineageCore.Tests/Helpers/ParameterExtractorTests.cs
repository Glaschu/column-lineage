using Microsoft.VisualStudio.TestTools.UnitTesting;
using ColumnLineageCore.Helpers;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Linq;

namespace ColumnLineageCore.Tests.Helpers
{
    [TestClass]
    public class ParameterExtractorTests
    {
        [TestMethod]
        public void ExtractProcedureParameters_SimpleProc_ExtractsParameters()
        {
            // Arrange
            string sql = @"
CREATE PROCEDURE MyProc
    @Input1 INT,
    @Input2 VARCHAR(100),
    @Output1 DECIMAL(10,2) OUTPUT
AS
BEGIN
    SELECT @Input1, @Input2, @Output1;
END";

            // Act
            var parameters = ParameterExtractor.ExtractProcedureParameters(sql);

            // Assert
            Assert.IsNotNull(parameters);
            Assert.AreEqual(3, parameters.Count);

            Assert.AreEqual("@Input1", parameters[0].VariableName.Value);
            Assert.AreEqual(ParameterModifier.None, parameters[0].Modifier); // Use Modifier property
            Assert.AreEqual("INT", (parameters[0].DataType as SqlDataTypeReference)?.Name?.BaseIdentifier?.Value);

            Assert.AreEqual("@Input2", parameters[1].VariableName.Value);
            Assert.AreEqual(ParameterModifier.None, parameters[1].Modifier); // Use Modifier property
            Assert.AreEqual("VARCHAR", (parameters[1].DataType as SqlDataTypeReference)?.Name?.BaseIdentifier?.Value);

            Assert.AreEqual("@Output1", parameters[2].VariableName.Value);
            Assert.AreEqual(ParameterModifier.Output, parameters[2].Modifier); // Use Modifier property and enum value
            Assert.AreEqual("DECIMAL", (parameters[2].DataType as SqlDataTypeReference)?.Name?.BaseIdentifier?.Value);
        }

        [TestMethod]
        public void ExtractProcedureParameters_NoParameters_ReturnsEmptyList()
        {
            // Arrange
            string sql = @"
CREATE PROCEDURE NoParamsProc
AS
BEGIN
    SELECT 1;
END";

            // Act
            var parameters = ParameterExtractor.ExtractProcedureParameters(sql);

            // Assert
            Assert.IsNotNull(parameters);
            Assert.AreEqual(0, parameters.Count);
        }

        [TestMethod]
        public void ExtractProcedureParameters_InvalidSql_ReturnsNull()
        {
            // Arrange
            string sql = @"CREATE PROC"; // Invalid syntax

            // Act
            var parameters = ParameterExtractor.ExtractProcedureParameters(sql);

            // Assert
            Assert.IsNull(parameters);
        }

        [TestMethod]
        public void ExtractProcedureParameters_NotCreateProcedure_ReturnsNull()
        {
            // Arrange
            string sql = @"SELECT * FROM TableA;"; // Not a CREATE PROCEDURE statement

            // Act
            var parameters = ParameterExtractor.ExtractProcedureParameters(sql);

            // Assert
            Assert.IsNull(parameters);
        }
    }
}
