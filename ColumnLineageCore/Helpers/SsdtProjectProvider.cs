using ColumnLineageCore.Interfaces;
using System.IO;
using System.Xml.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using System.Linq;
using System;

namespace ColumnLineageCore.Helpers
{
    /// <summary>
    /// Provides SQL object discovery for SQL Server Database Projects (SSDT).
    /// </summary>
    public class SsdtProjectProvider : IProjectProvider
    {
        public IEnumerable<SqlObjectDefinition> DiscoverSqlObjects(string projectPath)
        {
            var objects = new List<SqlObjectDefinition>();
            
            // Handle both .sqlproj file and directory paths
            string projectDirectory;
            string? sqlProjFile = null;

            if (File.Exists(projectPath) && Path.GetExtension(projectPath).Equals(".sqlproj", StringComparison.OrdinalIgnoreCase))
            {
                sqlProjFile = projectPath;
                projectDirectory = Path.GetDirectoryName(projectPath) ?? throw new ArgumentException("Invalid project path");
            }
            else if (Directory.Exists(projectPath))
            {
                projectDirectory = projectPath;
                var sqlProjFiles = Directory.GetFiles(projectDirectory, "*.sqlproj", SearchOption.TopDirectoryOnly);
                if (sqlProjFiles.Length > 0)
                {
                    sqlProjFile = sqlProjFiles[0]; // Take the first .sqlproj file found
                }
            }
            else
            {
                throw new ArgumentException($"Invalid project path: {projectPath}");
            }

            // If we have a .sqlproj file, parse it to get explicit file references
            List<string> explicitFiles = new();
            if (sqlProjFile != null && File.Exists(sqlProjFile))
            {
                explicitFiles = ParseSqlProjFile(sqlProjFile, projectDirectory);
            }

            // Discover SQL files - use explicit files if available, otherwise scan directory
            var sqlFiles = explicitFiles.Any() 
                ? explicitFiles.Where(File.Exists)
                : Directory.GetFiles(projectDirectory, "*.sql", SearchOption.AllDirectories);

            foreach (var filePath in sqlFiles)
            {
                try
                {
                    var sqlContent = File.ReadAllText(filePath);
                    var objectDef = AnalyzeSqlFile(filePath, sqlContent, projectDirectory);
                    if (objectDef != null)
                    {
                        objects.Add(objectDef);
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[SsdtProjectProvider] Error processing file {filePath}: {ex.Message}");
                }
            }

            return objects;
        }

        private List<string> ParseSqlProjFile(string sqlProjPath, string projectDirectory)
        {
            var files = new List<string>();
            
            try
            {
                var doc = XDocument.Load(sqlProjPath);
                var buildElements = doc.Descendants()
                    .Where(e => e.Name.LocalName == "Build" && e.Attribute("Include") != null);

                foreach (var element in buildElements)
                {
                    var includeValue = element.Attribute("Include")?.Value;
                    if (!string.IsNullOrEmpty(includeValue) && includeValue.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
                    {
                        // Normalize path separators for cross-platform compatibility
                        var normalizedInclude = includeValue.Replace('\\', Path.DirectorySeparatorChar);
                        
                        // Convert relative path to absolute path
                        var fullPath = Path.IsPathRooted(normalizedInclude) 
                            ? normalizedInclude 
                            : Path.Combine(projectDirectory, normalizedInclude);
                        
                        var resolvedPath = Path.GetFullPath(fullPath);
                        files.Add(resolvedPath);
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SsdtProjectProvider] Error parsing .sqlproj file {sqlProjPath}: {ex.Message}");
            }

            return files;
        }

        private SqlObjectDefinition? AnalyzeSqlFile(string filePath, string sqlContent, string projectDirectory)
        {
            // Get relative path for better object naming
            var relativePath = Path.GetRelativePath(projectDirectory, filePath);
            var pathParts = relativePath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            
            // Determine object type based on directory structure and SQL content
            var objectType = DetermineObjectType(pathParts, sqlContent);
            if (objectType == null) return null;

            // Extract schema and object name using enhanced parsing
            var (schema, objectName) = ExtractSchemaAndName(pathParts, sqlContent);
            
            return new SqlObjectDefinition
            {
                Name = objectName,
                Schema = schema,
                Type = objectType.Value,
                FilePath = filePath,
                SqlContent = sqlContent,
                Dependencies = ExtractAllDependencies(sqlContent), // Enhanced dependency extraction
                Parameters = ExtractParameters(sqlContent, objectType.Value),
                Columns = ExtractColumns(sqlContent, objectType.Value),
                ColumnDefinitions = ExtractDetailedColumns(sqlContent, objectType.Value),
                DatasetNamespace = "sqlserver://localhost", // Could be configurable
                Metadata = ExtractMetadata(filePath, sqlContent)
            };
        }

        private SqlObjectType? DetermineObjectType(string[] pathParts, string sqlContent)
        {
            // Enhanced directory structure detection - check all path parts
            var pathString = string.Join("/", pathParts).ToLowerInvariant();
            
            // Common SSDT patterns (case-insensitive)
            if (pathString.Contains("table") || pathString.Contains("/tables/"))
                return SqlObjectType.Table;
            if (pathString.Contains("view") || pathString.Contains("/views/"))
                return SqlObjectType.View;
            if (pathString.Contains("procedure") || pathString.Contains("/procedures/") || 
                pathString.Contains("stored procedure") || pathString.Contains("/stored procedures/"))
                return SqlObjectType.StoredProcedure;
            if (pathString.Contains("function") || pathString.Contains("/functions/"))
                return SqlObjectType.Function;
            if (pathString.Contains("security") && sqlContent.TrimStart().StartsWith("CREATE SCHEMA", StringComparison.OrdinalIgnoreCase))
                return SqlObjectType.Schema;

            // Enhanced SQL content analysis with better regex patterns
            var trimmedContent = sqlContent.TrimStart();
            var createMatch = System.Text.RegularExpressions.Regex.Match(
                trimmedContent, 
                @"^\s*CREATE\s+(TABLE|VIEW|PROCEDURE|PROC|FUNCTION|SCHEMA)\s+",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Multiline);

            if (createMatch.Success)
            {
                var objectTypeStr = createMatch.Groups[1].Value.ToUpperInvariant();
                return objectTypeStr switch
                {
                    "TABLE" => SqlObjectType.Table,
                    "VIEW" => SqlObjectType.View,
                    "PROCEDURE" or "PROC" => SqlObjectType.StoredProcedure,
                    "FUNCTION" => SqlObjectType.Function,
                    "SCHEMA" => SqlObjectType.Schema,
                    _ => null
                };
            }

            return null; // Unknown or unsupported object type
        }

        private (string schema, string objectName) ExtractSchemaAndName(string[] pathParts, string sqlContent)
        {
            // Try to extract from directory structure first
            string schema = "dbo"; // default
            string objectName = Path.GetFileNameWithoutExtension(pathParts.Last());

            // Look for schema in path structure
            for (int i = 0; i < pathParts.Length - 1; i++)
            {
                if (IsSchemaDirectory(pathParts[i]))
                {
                    schema = pathParts[i];
                    break;
                }
            }

            // Try to extract from SQL content if possible
            try
            {
                var lines = sqlContent.Split('\n');
                foreach (var line in lines.Take(10)) // Check first 10 lines
                {
                    var trimmedLine = line.Trim();
                    if (trimmedLine.StartsWith("CREATE ", StringComparison.OrdinalIgnoreCase))
                    {
                        // Try to extract schema.objectname pattern
                        var match = System.Text.RegularExpressions.Regex.Match(
                            trimmedLine, 
                            @"CREATE\s+(?:TABLE|VIEW|PROCEDURE|PROC|FUNCTION|SCHEMA)\s+(?:\[([^\]]+)\]\.)?(?:\[([^\]]+)\]|(\w+))",
                            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                        if (match.Success)
                        {
                            if (!string.IsNullOrEmpty(match.Groups[1].Value))
                            {
                                schema = match.Groups[1].Value;
                            }
                            if (!string.IsNullOrEmpty(match.Groups[2].Value))
                            {
                                objectName = match.Groups[2].Value;
                            }
                            else if (!string.IsNullOrEmpty(match.Groups[3].Value))
                            {
                                objectName = match.Groups[3].Value;
                            }
                        }
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SsdtProjectProvider] Error extracting schema/name from SQL: {ex.Message}");
            }

            return (schema, objectName);
        }

        private bool IsSchemaDirectory(string directoryName)
        {
            // Common schema names in SSDT projects
            var commonSchemas = new[] { "dbo", "Sales", "Production", "HumanResources", "Person", "Purchasing" };
            return commonSchemas.Contains(directoryName, StringComparer.OrdinalIgnoreCase);
        }

        private List<string> ExtractAllDependencies(string sqlContent)
        {
            var dependencies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            // Use SQL DOM parser for more accurate dependency extraction
            try
            {
                var parser = new TSql160Parser(true);
                IList<ParseError> errors;
                var fragment = parser.Parse(new StringReader(sqlContent), out errors);
                
                if (fragment != null && errors.Count == 0)
                {
                    var visitor = new DependencyVisitor();
                    fragment.Accept(visitor);
                    foreach (var dep in visitor.Dependencies)
                    {
                        dependencies.Add(dep);
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SsdtProjectProvider] Error parsing SQL for dependencies: {ex.Message}");
            }

            // Fallback to regex-based extraction for additional patterns
            ExtractDependenciesWithRegex(sqlContent, dependencies);

            return dependencies.ToList();
        }

        private void ExtractDependenciesWithRegex(string sqlContent, HashSet<string> dependencies)
        {
            // Enhanced regex patterns for different dependency types
            var patterns = new[]
            {
                // Standard table/view references
                @"(?:FROM|JOIN)\s+(?:\[?([^\]\s,\)]+)\]?\.)?(?:\[?([^\]\s,\)\(]+)\]?)",
                
                // Function calls - EXEC, EXECUTE
                @"(?:EXEC|EXECUTE)\s+(?:\[?([^\]\s,\)]+)\]?\.)?(?:\[?([^\]\s,\)\(]+)\]?)",
                
                // INSERT INTO, UPDATE, DELETE FROM
                @"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(?:\[?([^\]\s,\)]+)\]?\.)?(?:\[?([^\]\s,\)\(]+)\]?)",
                
                // Function calls in expressions
                @"(?:\[?([^\]\s,\)]+)\]?\.)?(?:\[?([^\]\s,\)\(]+)\]?)\s*\(",
                
                // WITH clause CTEs
                @"WITH\s+(?:\[?([^\]\s,\)]+)\]?\s+AS\s*\()",
                
                // Dynamic SQL references (basic detection)
                @"'(?:SELECT|INSERT|UPDATE|DELETE).*?(?:FROM|INTO|JOIN)\s+(?:\[?([^\]']+)\]?)'"
            };

            foreach (var pattern in patterns)
            {
                var matches = System.Text.RegularExpressions.Regex.Matches(
                    sqlContent, 
                    pattern, 
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase | 
                    System.Text.RegularExpressions.RegexOptions.Multiline);

                foreach (System.Text.RegularExpressions.Match match in matches)
                {
                    for (int i = 1; i < match.Groups.Count; i++)
                    {
                        var value = match.Groups[i].Value.Trim();
                        if (!string.IsNullOrEmpty(value) && !IsKeyword(value) && !IsSystemObject(value))
                        {
                            // Build full name if we have schema.object pattern
                            if (i == 2 && !string.IsNullOrEmpty(match.Groups[1].Value))
                            {
                                dependencies.Add($"{match.Groups[1].Value.Trim()}.{value}");
                            }
                            else if (i == 1)
                            {
                                dependencies.Add(value);
                            }
                        }
                    }
                }
            }
        }

        private List<string> ExtractParameters(string sqlContent, SqlObjectType objectType)
        {
            var parameters = new List<string>();
            
            if (objectType == SqlObjectType.StoredProcedure || objectType == SqlObjectType.Function)
            {
                // Extract parameter definitions
                var paramPattern = @"@(\w+)\s+(\w+(?:\([^)]+\))?(?:\s+(?:OUTPUT|OUT))?)\s*(?:,|\)|AS|BEGIN)";
                var matches = System.Text.RegularExpressions.Regex.Matches(
                    sqlContent, 
                    paramPattern, 
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                foreach (System.Text.RegularExpressions.Match match in matches)
                {
                    parameters.Add($"@{match.Groups[1].Value}");
                }
            }

            return parameters;
        }

        private List<string> ExtractColumns(string sqlContent, SqlObjectType objectType)
        {
            var columns = new List<string>();
            
            if (objectType == SqlObjectType.Table)
            {
                // Extract column definitions from CREATE TABLE using SQL DOM
                try
                {
                    var parser = new TSql160Parser(true);
                    IList<ParseError> errors;
                    var fragment = parser.Parse(new StringReader(sqlContent), out errors);
                    
                    if (fragment != null && errors.Count == 0)
                    {
                        var visitor = new ColumnExtractionVisitor();
                        fragment.Accept(visitor);
                        return visitor.Columns;
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[SsdtProjectProvider] Error parsing columns with SQL DOM: {ex.Message}");
                }

                // Fallback to regex-based extraction
                ExtractColumnsWithRegex(sqlContent, columns);
            }
            else if (objectType == SqlObjectType.View)
            {
                // For views, extract columns from SELECT statement
                ExtractViewColumns(sqlContent, columns);
            }

            return columns;
        }

        private void ExtractColumnsWithRegex(string sqlContent, List<string> columns)
        {
            // Enhanced regex for CREATE TABLE column extraction
            var createTableMatch = System.Text.RegularExpressions.Regex.Match(
                sqlContent, 
                @"CREATE\s+TABLE[^(]+\(\s*(.+?)\s*\)",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);

            if (createTableMatch.Success)
            {
                var columnDefinitions = createTableMatch.Groups[1].Value;
                
                // Split by commas, but respect parentheses and brackets
                var columnParts = SplitRespectingParentheses(columnDefinitions, ',');
                
                foreach (var part in columnParts)
                {
                    var trimmedPart = part.Trim();
                    if (string.IsNullOrEmpty(trimmedPart) || IsConstraintDefinition(trimmedPart))
                        continue;

                    // Extract column name (first identifier)
                    var columnMatch = System.Text.RegularExpressions.Regex.Match(
                        trimmedPart, 
                        @"^\s*\[?(\w+)\]?",
                        System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                    if (columnMatch.Success)
                    {
                        var columnName = columnMatch.Groups[1].Value;
                        if (!IsConstraintKeyword(columnName))
                        {
                            columns.Add(columnName);
                        }
                    }
                }
            }
        }

        private void ExtractViewColumns(string sqlContent, List<string> columns)
        {
            // Extract column aliases and direct references from VIEW SELECT
            var selectMatch = System.Text.RegularExpressions.Regex.Match(
                sqlContent,
                @"CREATE\s+VIEW[^(]*(?:\([^)]*\))?\s+AS\s+SELECT\s+(.+?)\s+FROM",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);

            if (selectMatch.Success)
            {
                var selectList = selectMatch.Groups[1].Value;
                var columnParts = SplitRespectingParentheses(selectList, ',');

                foreach (var part in columnParts)
                {
                    var trimmedPart = part.Trim();
                    
                    // Look for AS alias or extract last identifier
                    var aliasMatch = System.Text.RegularExpressions.Regex.Match(
                        trimmedPart,
                        @"(?:.*\s+AS\s+\[?(\w+)\]?)|(?:.*\.)?(\[?(\w+)\]?)$",
                        System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                    if (aliasMatch.Success)
                    {
                        var columnName = aliasMatch.Groups[1].Value;
                        if (string.IsNullOrEmpty(columnName))
                        {
                            columnName = aliasMatch.Groups[3].Value.Trim('[', ']');
                        }

                        if (!string.IsNullOrEmpty(columnName) && !IsKeyword(columnName))
                        {
                            columns.Add(columnName);
                        }
                    }
                }
            }
        }

        private List<string> SplitRespectingParentheses(string input, char separator)
        {
            var result = new List<string>();
            var current = new System.Text.StringBuilder();
            int parenthesesLevel = 0;
            bool inBrackets = false;

            foreach (char c in input)
            {
                if (c == '[' && !inBrackets)
                    inBrackets = true;
                else if (c == ']' && inBrackets)
                    inBrackets = false;
                else if (c == '(' && !inBrackets)
                    parenthesesLevel++;
                else if (c == ')' && !inBrackets)
                    parenthesesLevel--;
                else if (c == separator && parenthesesLevel == 0 && !inBrackets)
                {
                    result.Add(current.ToString());
                    current.Clear();
                    continue;
                }

                current.Append(c);
            }

            if (current.Length > 0)
                result.Add(current.ToString());

            return result;
        }

        private bool IsConstraintDefinition(string definition)
        {
            var constraintKeywords = new[] { "CONSTRAINT", "PRIMARY KEY", "FOREIGN KEY", "CHECK", "UNIQUE", "INDEX" };
            var trimmed = definition.TrimStart().ToUpperInvariant();
            return constraintKeywords.Any(kw => trimmed.StartsWith(kw));
        }

        private List<SqlColumnDefinition> ExtractDetailedColumns(string sqlContent, SqlObjectType objectType)
        {
            var columnDefinitions = new List<SqlColumnDefinition>();

            try
            {
                var parser = new TSql160Parser(true);
                IList<ParseError> errors;
                var fragment = parser.Parse(new StringReader(sqlContent), out errors);
                
                if (fragment != null && errors.Count == 0)
                {
                    var visitor = new ColumnExtractionVisitor();
                    fragment.Accept(visitor);
                    return visitor.ColumnDefinitions;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[SsdtProjectProvider] Error extracting detailed columns: {ex.Message}");
            }

            // Fallback to basic column extraction
            var columns = ExtractColumns(sqlContent, objectType);
            return columns.Select(c => new SqlColumnDefinition 
            { 
                Name = c, 
                DataType = "unknown",
                Description = string.Empty 
            }).ToList();
        }

        private Dictionary<string, object> ExtractMetadata(string filePath, string sqlContent)
        {
            var metadata = new Dictionary<string, object>();
            
            metadata["filePath"] = filePath;
            metadata["fileSize"] = sqlContent.Length;
            metadata["lastModified"] = File.GetLastWriteTime(filePath);
            
            // Extract comments and extended properties
            var comments = ExtractComments(sqlContent);
            if (comments.Any())
            {
                metadata["comments"] = comments;
            }

            return metadata;
        }

        private List<string> ExtractComments(string sqlContent)
        {
            var comments = new List<string>();
            
            // Extract single-line comments
            var singleLineComments = System.Text.RegularExpressions.Regex.Matches(
                sqlContent, 
                @"--\s*(.+)$", 
                System.Text.RegularExpressions.RegexOptions.Multiline);
            
            foreach (System.Text.RegularExpressions.Match match in singleLineComments)
            {
                comments.Add(match.Groups[1].Value.Trim());
            }

            // Extract multi-line comments
            var multiLineComments = System.Text.RegularExpressions.Regex.Matches(
                sqlContent, 
                @"/\*(.*?)\*/", 
                System.Text.RegularExpressions.RegexOptions.Singleline);
            
            foreach (System.Text.RegularExpressions.Match match in multiLineComments)
            {
                comments.Add(match.Groups[1].Value.Trim());
            }

            return comments.Where(c => !string.IsNullOrWhiteSpace(c)).ToList();
        }

        private bool IsSystemObject(string objectName)
        {
            var systemObjects = new[] { "sys", "INFORMATION_SCHEMA", "master", "tempdb", "model", "msdb" };
            return systemObjects.Any(so => objectName.StartsWith(so, StringComparison.OrdinalIgnoreCase));
        }

        private bool IsConstraintKeyword(string word)
        {
            var keywords = new[] { "CONSTRAINT", "PRIMARY", "FOREIGN", "KEY", "CHECK", "DEFAULT", "UNIQUE", "INDEX" };
            return keywords.Contains(word, StringComparer.OrdinalIgnoreCase);
        }

        private bool IsKeyword(string word)
        {
            var keywords = new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "WHERE", "ORDER", "GROUP", "HAVING", "UNION" };
            return keywords.Contains(word, StringComparer.OrdinalIgnoreCase);
        }
    }

    /// <summary>
    /// SQL DOM visitor to extract object dependencies from parsed SQL.
    /// </summary>
    internal class DependencyVisitor : TSqlFragmentVisitor
    {
        public HashSet<string> Dependencies { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public override void Visit(NamedTableReference node)
        {
            if (node.SchemaObject != null)
            {
                var schemaName = node.SchemaObject.SchemaIdentifier?.Value;
                var objectName = node.SchemaObject.BaseIdentifier?.Value;

                if (!string.IsNullOrEmpty(objectName))
                {
                    if (!string.IsNullOrEmpty(schemaName))
                    {
                        Dependencies.Add($"{schemaName}.{objectName}");
                    }
                    else
                    {
                        Dependencies.Add(objectName);
                    }
                }
            }
            base.Visit(node);
        }

        public override void Visit(ExecuteStatement node)
        {
            // Simplified approach - just note that we found an execute statement
            // TODO: Enhance procedure name extraction when SQL DOM API is better understood
            try
            {
                // For now, just indicate a generic dependency
                Dependencies.Add("EXECUTE_STATEMENT");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error extracting procedure dependency: {ex.Message}");
            }
            base.Visit(node);
        }

        public override void Visit(FunctionCall node)
        {
            if (node.FunctionName != null)
            {
                var functionName = node.FunctionName.Value;

                if (!string.IsNullOrEmpty(functionName))
                {
                    Dependencies.Add(functionName);
                }
            }
            base.Visit(node);
        }

        public override void Visit(CommonTableExpression node)
        {
            if (node.ExpressionName?.Value != null)
            {
                Dependencies.Add(node.ExpressionName.Value);
            }
            base.Visit(node);
        }
    }

    /// <summary>
    /// SQL DOM visitor to extract column definitions from CREATE TABLE statements.
    /// </summary>
    internal class ColumnExtractionVisitor : TSqlFragmentVisitor
    {
        public List<string> Columns { get; } = new List<string>();
        public List<SqlColumnDefinition> ColumnDefinitions { get; } = new List<SqlColumnDefinition>();

        public override void Visit(CreateTableStatement node)
        {
            if (node.Definition?.ColumnDefinitions != null)
            {
                foreach (var columnDef in node.Definition.ColumnDefinitions)
                {
                    var columnName = columnDef.ColumnIdentifier?.Value;
                    if (!string.IsNullOrEmpty(columnName))
                    {
                        Columns.Add(columnName);
                        
                        // Extract data type information
                        var dataType = ExtractDataType(columnDef.DataType);
                        ColumnDefinitions.Add(new SqlColumnDefinition
                        {
                            Name = columnName,
                            DataType = dataType,
                            Description = ExtractColumnDescription(columnDef)
                        });
                    }
                }
            }
            base.Visit(node);
        }

        private string ExtractDataType(DataTypeReference? dataType)
        {
            if (dataType == null) return "unknown";

            if (dataType is SqlDataTypeReference sqlDataType)
            {
                var typeName = sqlDataType.SqlDataTypeOption.ToString();
                
                // Add parameters if present
                if (sqlDataType.Parameters?.Count > 0)
                {
                    var parameters = string.Join(", ", sqlDataType.Parameters.Select(p => p.Value));
                    return $"{typeName}({parameters})";
                }
                
                return typeName;
            }
            else if (dataType is UserDataTypeReference userDataType)
            {
                var schemaName = userDataType.Name?.SchemaIdentifier?.Value;
                var typeName = userDataType.Name?.BaseIdentifier?.Value;
                
                if (!string.IsNullOrEmpty(schemaName))
                    return $"{schemaName}.{typeName}";
                else
                    return typeName ?? "unknown";
            }

            return dataType.GetType().Name;
        }

        private string ExtractColumnDescription(Microsoft.SqlServer.TransactSql.ScriptDom.ColumnDefinition columnDef)
        {
            // This could be enhanced to extract from extended properties or comments
            return string.Empty;
        }
    }
}
