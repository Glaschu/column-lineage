MERGE Customer AS target USING CustomerStaging AS source ON target.CustomerID = source.CustomerID WHEN MATCHED THEN UPDATE SET PersonID = source.PersonID;
