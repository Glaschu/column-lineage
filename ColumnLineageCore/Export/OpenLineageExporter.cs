using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Model;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ColumnLineageCore.Export
{
    /// <summary>
    /// Exports lineage data to OpenLineage format.
    /// </summary>
    public class OpenLineageExporter
    {
        public OpenLineageDocument ExportProject(ProjectLineageResult projectResult, string projectName, string projectNamespace = "default")
        {
            var document = new OpenLineageDocument
            {
                EventType = "COMPLETE",
                EventTime = DateTime.UtcNow,
                Run = new OpenLineageRun
                {
                    RunId = Guid.NewGuid().ToString(),
                    Facets = new Dictionary<string, object>
                    {
                        ["nominalTime"] = new { nominalStartTime = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") }
                    }
                },
                Job = new OpenLineageJob
                {
                    Namespace = projectNamespace,
                    Name = projectName,
                    Facets = new Dictionary<string, object>
                    {
                        ["documentation"] = new { description = $"SSDT Project: {projectName}" },
                        ["sourceCode"] = new { language = "sql", source = "SSDT Project Analysis" }
                    }
                },
                Inputs = CreateInputDatasets(projectResult),
                Outputs = CreateOutputDatasets(projectResult)
            };

            return document;
        }

        private List<OpenLineageDataset> CreateInputDatasets(ProjectLineageResult projectResult)
        {
            var inputs = new List<OpenLineageDataset>();

            // Add all tables as input datasets
            var tables = projectResult.DiscoveredObjects
                .Where(o => o.Type == SqlObjectType.Table)
                .ToList();

            foreach (var table in tables)
            {
                inputs.Add(CreateDatasetFromObject(table, "input", projectResult));
            }

            return inputs;
        }

        private List<OpenLineageDataset> CreateOutputDatasets(ProjectLineageResult projectResult)
        {
            var outputs = new List<OpenLineageDataset>();

            // Add views and procedures as output datasets
            var outputObjects = projectResult.DiscoveredObjects
                .Where(o => o.Type == SqlObjectType.View || o.Type == SqlObjectType.StoredProcedure)
                .ToList();

            foreach (var obj in outputObjects)
            {
                outputs.Add(CreateDatasetFromObject(obj, "output", projectResult));
            }

            return outputs;
        }

        private OpenLineageDataset CreateDatasetFromObject(SqlObjectDefinition sqlObject, string facetType, ProjectLineageResult projectResult)
        {
            var dataset = new OpenLineageDataset
            {
                Namespace = sqlObject.DatasetNamespace.IsNullOrEmpty() ? "sqlserver://localhost" : sqlObject.DatasetNamespace,
                Name = sqlObject.DatasetName,
                Facets = new Dictionary<string, object>()
            };

            // Add schema facet with standard OpenLineage format
            var schemaFields = new List<object>();

            // If we have explicit column definitions, use them
            if (sqlObject.ColumnDefinitions.Any())
            {
                schemaFields.AddRange(sqlObject.ColumnDefinitions.Select(c => new
                {
                    name = c.Name,
                    type = c.DataType,
                    description = c.Description
                }));
            }
            else
            {
                // For datasets without explicit column definitions, infer from lineage
                var relevantColumns = GetColumnsForDataset(sqlObject, projectResult);
                schemaFields.AddRange(relevantColumns.Select(col => new
                {
                    name = col.name,
                    type = col.type,
                    description = col.description
                }));
            }

            if (schemaFields.Any())
            {
                dataset.Facets["schema"] = new
                {
                    _producer = "https://github.com/agredyaev/column-lineage",
                    _schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                    fields = schemaFields.ToArray()
                };
            }

            // Add dataSource facet
            dataset.Facets["dataSource"] = new
            {
                _producer = "https://github.com/agredyaev/column-lineage",
                _schemaURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/DataSourceDatasetFacet",
                name = dataset.Namespace,
                uri = sqlObject.Name
            };

            // Add column lineage facet for output datasets (views, procedures)
            if (facetType == "output" && (sqlObject.Type == SqlObjectType.View || sqlObject.Type == SqlObjectType.StoredProcedure))
            {
                var columnLineageFacet = CreateColumnLineageFacet(sqlObject, projectResult);
                if (columnLineageFacet != null)
                {
                    dataset.Facets["columnLineage"] = columnLineageFacet;
                }
            }

            // Add documentation facet
            dataset.Facets["documentation"] = new
            {
                description = $"{sqlObject.Type}: {sqlObject.FullName}"
            };

            return dataset;
        }

        private List<(string name, string type, string description)> GetColumnsForDataset(SqlObjectDefinition sqlObject, ProjectLineageResult projectResult)
        {
            var columns = new List<(string name, string type, string description)>();

            if (sqlObject.Type == SqlObjectType.Table)
            {
                // For input tables, get columns that are referenced as sources in the lineage
                var sourceColumns = projectResult.CombinedLineage.Nodes
                    .Where(n => !string.IsNullOrEmpty(n.SourceName) && n.SourceName == sqlObject.Name)
                    .Select(n => n.Name)
                    .Distinct()
                    .ToList();

                foreach (var columnName in sourceColumns)
                {
                    columns.Add((columnName, GetDefaultDataType(columnName), $"Column from {sqlObject.Name}"));
                }
            }
            else if (sqlObject.Type == SqlObjectType.View || sqlObject.Type == SqlObjectType.StoredProcedure)
            {
                // For output datasets (views/procedures), get the output columns
                var outputColumns = projectResult.CombinedLineage.Nodes
                    .Where(n => string.IsNullOrEmpty(n.SourceName)) // Output columns have no source
                    .Select(n => n.Name)
                    .Distinct()
                    .ToList();

                foreach (var columnName in outputColumns)
                {
                    columns.Add((columnName, GetDefaultDataType(columnName), $"Output column from {sqlObject.Name}"));
                }
            }

            return columns;
        }

        private string GetDefaultDataType(string columnName)
        {
            // Provide reasonable default data types based on column name patterns
            var lowerName = columnName.ToLower();
            
            if (lowerName.Contains("id"))
                return "INTEGER";
            else if (lowerName.Contains("date") || lowerName.Contains("time"))
                return "TIMESTAMP";
            else if (lowerName.Contains("price") || lowerName.Contains("amount"))
                return "DECIMAL(10,2)";
            else if (lowerName.Contains("name") || lowerName.Contains("category"))
                return "VARCHAR(255)";
            else
                return "VARCHAR(255)"; // Default fallback
        }

        private object? CreateColumnLineageFacet(SqlObjectDefinition outputObject, ProjectLineageResult projectResult)
        {
            var lineageGraph = projectResult.CombinedLineage;
            
            // Find the processed object that corresponds to this output object
            var processedObject = projectResult.ProcessedObjects
                .FirstOrDefault(p => p.SqlObject.FullName == outputObject.FullName);
            
            if (processedObject == null || !processedObject.LineageResult.Edges.Any())
            {
                return null;
            }

            var fields = new Dictionary<string, object>();
            var datasetTransformations = new List<object>();

            // Group edges by target column (output fields)
            var outputColumns = processedObject.LineageResult.Edges
                .GroupBy(e => e.TargetNodeId)
                .ToList();

            foreach (var outputColumnGroup in outputColumns)
            {
                var outputColumnName = outputColumnGroup.Key;
                var inputFields = new List<object>();

                foreach (var edge in outputColumnGroup)
                {
                    var sourceNode = processedObject.LineageResult.Nodes
                        .FirstOrDefault(n => n.Id == edge.SourceNodeId);
                    
                    if (sourceNode != null && !string.IsNullOrEmpty(sourceNode.SourceName))
                    {
                        // Determine transformation type based on the relationship
                        var transformation = DetermineTransformation(sourceNode, outputColumnName, edge);
                        
                        inputFields.Add(new
                        {
                            @namespace = outputObject.DatasetNamespace.IsNullOrEmpty() ? "sqlserver://localhost" : outputObject.DatasetNamespace,
                            name = sourceNode.SourceName,
                            field = sourceNode.Name,
                            transformations = new[] { new
                            {
                                type = transformation.Type,
                                subtype = transformation.Subtype,
                                description = transformation.Description,
                                masking = transformation.Masking
                            }}
                        });

                        // Add to dataset-level transformations for all transformations
                        datasetTransformations.Add(new
                        {
                            @namespace = outputObject.DatasetNamespace.IsNullOrEmpty() ? "sqlserver://localhost" : outputObject.DatasetNamespace,
                            name = sourceNode.SourceName,
                            field = sourceNode.Name,
                            transformations = new[] { new
                            {
                                type = transformation.Type,
                                subtype = transformation.Subtype,
                                description = transformation.Description,
                                masking = transformation.Masking
                            }}
                        });
                    }
                }

                if (inputFields.Any())
                {
                    fields[outputColumnName] = new { inputFields = inputFields.ToArray() };
                }
            }

            var columnLineageFacet = new
            {
                _producer = "https://github.com/agredyaev/column-lineage",
                _schemaURL = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json",
                fields = fields,
                dataset = datasetTransformations.ToArray()
            };

            return columnLineageFacet;
        }

        private ColumnTransformation DetermineTransformation(ColumnNode sourceNode, string outputColumnName, LineageEdge edge)
        {
            // Default transformation
            var transformationType = "DIRECT";
            var transformationSubtype = "IDENTITY";
            var description = "";
            var masking = false;

            // Determine transformation type based on column names and context
            if (sourceNode.Name == outputColumnName)
            {
                // Direct column mapping
                transformationType = "DIRECT";
                transformationSubtype = "IDENTITY";
                description = "Direct column mapping";
            }
            else if (outputColumnName.Contains("Category") && sourceNode.Name.Contains("Category"))
            {
                // Likely a join or transformation
                transformationType = "DIRECT";
                transformationSubtype = "TRANSFORMATION";
                description = "Column transformation or join";
            }
            else if (sourceNode.Name == "Price" && outputColumnName == "PriceCategory")
            {
                // CASE statement or calculation
                transformationType = "DIRECT";
                transformationSubtype = "TRANSFORMATION";
                description = "CASE expression transformation";
            }
            else
            {
                // General transformation
                transformationType = "DIRECT";
                transformationSubtype = "TRANSFORMATION";
                description = "Column transformation";
            }

            return new ColumnTransformation
            {
                Type = transformationType,
                Subtype = transformationSubtype,
                Description = description,
                Masking = masking
            };
        }

        public string ExportToJson(OpenLineageDocument document, bool indented = true)
        {
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = indented,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };

            return JsonSerializer.Serialize(document, options);
        }
    }

    // OpenLineage data model classes
    public class OpenLineageDocument
    {
        public string EventType { get; set; } = string.Empty;
        public DateTime EventTime { get; set; }
        public OpenLineageRun Run { get; set; } = new();
        public OpenLineageJob Job { get; set; } = new();
        public List<OpenLineageDataset> Inputs { get; set; } = new();
        public List<OpenLineageDataset> Outputs { get; set; } = new();
    }

    public class OpenLineageRun
    {
        public string RunId { get; set; } = string.Empty;
        public Dictionary<string, object> Facets { get; set; } = new();
    }

    public class OpenLineageJob
    {
        public string Namespace { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public Dictionary<string, object> Facets { get; set; } = new();
    }

    public class OpenLineageDataset
    {
        public string Namespace { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public Dictionary<string, object> Facets { get; set; } = new();
    }

    public class ColumnTransformation
    {
        public string Type { get; set; } = string.Empty;
        public string Subtype { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public bool Masking { get; set; } = false;
    }
}

// Extension method for string null/empty checks
public static class StringExtensions
{
    public static bool IsNullOrEmpty(this string? value)
    {
        return string.IsNullOrEmpty(value);
    }
}
