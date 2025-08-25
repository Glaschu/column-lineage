-- Test Cases for MERGE Operations
MERGE Sales.Customer AS target
USING (
    SELECT 
        CustomerID,
        PersonID,
        StoreID,
        TerritoryID,
        AccountNumber,
        ModifiedDate
    FROM Sales.CustomerStaging
    WHERE ProcessedDate IS NULL
) AS source ON target.CustomerID = source.CustomerID
WHEN MATCHED THEN
    UPDATE SET 
        PersonID = source.PersonID,
        StoreID = source.StoreID,
        TerritoryID = source.TerritoryID,
        AccountNumber = source.AccountNumber,
        ModifiedDate = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (PersonID, StoreID, TerritoryID, AccountNumber, ModifiedDate)
    VALUES (source.PersonID, source.StoreID, source.TerritoryID, source.AccountNumber, GETDATE())
WHEN NOT MATCHED BY SOURCE AND target.ModifiedDate < DATEADD(YEAR, -1, GETDATE()) THEN
    DELETE;

-- Complex MERGE with CTEs
WITH CustomerUpdates AS (
    SELECT 
        c.CustomerID,
        p.FirstName,
        p.LastName,
        p.EmailPromotion,
        a.City,
        a.StateProvinceID
    FROM Staging.CustomerData c
    INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    INNER JOIN Person.BusinessEntityAddress bea ON p.BusinessEntityID = bea.BusinessEntityID
    INNER JOIN Person.Address a ON bea.AddressID = a.AddressID
    WHERE c.ProcessFlag = 1
),
TerritoryMapping AS (
    SELECT 
        sp.StateProvinceID,
        st.TerritoryID,
        st.Name AS TerritoryName
    FROM Person.StateProvince sp
    INNER JOIN Sales.SalesTerritory st ON sp.TerritoryID = st.TerritoryID
)
MERGE Sales.Customer AS target
USING (
    SELECT 
        cu.CustomerID,
        cu.FirstName + ' ' + cu.LastName AS FullName,
        cu.EmailPromotion,
        tm.TerritoryID,
        tm.TerritoryName
    FROM CustomerUpdates cu
    INNER JOIN TerritoryMapping tm ON cu.StateProvinceID = tm.StateProvinceID
) AS source ON target.CustomerID = source.CustomerID
WHEN MATCHED THEN
    UPDATE SET 
        TerritoryID = source.TerritoryID,
        ModifiedDate = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TerritoryID, ModifiedDate)
    VALUES (source.TerritoryID, GETDATE());
