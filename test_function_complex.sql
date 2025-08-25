-- Test only the first function to avoid parse errors
CREATE FUNCTION dbo.GetCustomerAnalysis
(
    @Year INT
)
RETURNS TABLE
AS
RETURN
(
    WITH CustomerBase AS (
        SELECT 
            c.CustomerID,
            p.FirstName,
            p.LastName,
            c.TerritoryID
        FROM Sales.Customer c
        INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    ),
    OrderMetrics AS (
        SELECT 
            soh.CustomerID,
            COUNT(*) AS OrderCount,
            SUM(soh.TotalDue) AS TotalDue,
            MIN(soh.OrderDate) AS FirstOrderDate,
            MAX(soh.OrderDate) AS LastOrderDate
        FROM Sales.SalesOrderHeader soh
        WHERE YEAR(soh.OrderDate) = @Year
        GROUP BY soh.CustomerID
    ),
    FinalResults AS (
        SELECT 
            cb.CustomerID,
            cb.FirstName,
            cb.LastName,
            cb.TerritoryID,
            ISNULL(om.OrderCount, 0) AS OrderCount,
            ISNULL(om.TotalDue, 0) AS TotalDue,
            om.FirstOrderDate,
            om.LastOrderDate,
            CASE 
                WHEN om.TotalDue > 10000 THEN 'High Value'
                WHEN om.TotalDue > 5000 THEN 'Medium Value'
                ELSE 'Low Value'
            END AS CustomerSegment
        FROM CustomerBase cb
        LEFT JOIN OrderMetrics om ON cb.CustomerID = om.CustomerID
    )
    SELECT 
        CustomerID,
        FirstName,
        LastName,
        TerritoryID,
        OrderCount,
        TotalDue,
        FirstOrderDate,
        LastOrderDate,
        CustomerSegment
    FROM FinalResults
);
