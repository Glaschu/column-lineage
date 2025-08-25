CREATE PROCEDURE TestProcWithFunction
AS
BEGIN
    -- Multiple CTEs within procedure
    WITH CustomerBase AS (
        SELECT 
            c.CustomerID,
            p.FirstName,
            p.LastName,
            c.TerritoryID
        FROM Sales.Customer c
        INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    ),
    OrderSummary AS (
        SELECT 
            soh.CustomerID,
            COUNT(*) AS OrderCount,
            SUM(soh.TotalDue) AS TotalDue
        FROM Sales.SalesOrderHeader soh
        GROUP BY soh.CustomerID
    ),
    FinalResults AS (
        SELECT 
            cb.CustomerID,
            cb.FirstName,
            cb.LastName,
            cb.TerritoryID,
            os.OrderCount,
            os.TotalDue,
            ROW_NUMBER() OVER (ORDER BY os.TotalDue DESC) AS CustomerRank
        FROM CustomerBase cb
        LEFT JOIN OrderSummary os ON cb.CustomerID = os.CustomerID
    )
    SELECT 
        CustomerID,
        FirstName,
        LastName,
        TerritoryID,
        OrderCount,
        TotalDue,
        CustomerRank
    FROM FinalResults
    WHERE CustomerRank <= 100;
END
