-- Simple table-valued function with multiple CTEs
CREATE FUNCTION dbo.GetCustomerOrderSummary
(
    @StartDate DATE,
    @EndDate DATE
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
    OrderTotals AS (
        SELECT 
            soh.CustomerID,
            COUNT(*) AS OrderCount,
            SUM(soh.TotalDue) AS TotalDue,
            AVG(soh.SubTotal) AS AverageOrderValue
        FROM Sales.SalesOrderHeader soh
        WHERE soh.OrderDate BETWEEN @StartDate AND @EndDate
        GROUP BY soh.CustomerID
    )
    SELECT 
        cb.CustomerID,
        cb.FirstName,
        cb.LastName,
        ISNULL(ot.OrderCount, 0) AS OrderCount,
        ISNULL(ot.TotalDue, 0) AS TotalDue,
        ISNULL(ot.AverageOrderValue, 0) AS AverageOrderValue
    FROM CustomerBase cb
    LEFT JOIN OrderTotals ot ON cb.CustomerID = ot.CustomerID
);
