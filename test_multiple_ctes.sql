-- Test Cases for Multiple CTEs
WITH CTE1 AS (
    SELECT 
        p.ProductID,
        p.Name AS ProductName,
        p.ListPrice
    FROM Production.Product p
    WHERE p.ListPrice > 100
),
CTE2 AS (
    SELECT 
        c.CustomerID,
        c.PersonID,
        p.FirstName,
        p.LastName
    FROM Sales.Customer c
    INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
),
CTE3 AS (
    SELECT 
        soh.SalesOrderID,
        soh.CustomerID,
        sod.ProductID,
        sod.OrderQty,
        sod.UnitPrice
    FROM Sales.SalesOrderHeader soh
    INNER JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
)
SELECT 
    c2.FirstName,
    c2.LastName,
    c1.ProductName,
    c3.OrderQty,
    c3.UnitPrice,
    (c3.OrderQty * c3.UnitPrice) AS LineTotal
FROM CTE1 c1
INNER JOIN CTE3 c3 ON c1.ProductID = c3.ProductID
INNER JOIN CTE2 c2 ON c3.CustomerID = c2.CustomerID
WHERE c3.OrderQty > 5;

-- Test Recursive CTE
WITH EmployeeHierarchy AS (
    -- Anchor: Find top-level managers
    SELECT 
        BusinessEntityID,
        FirstName,
        LastName,
        ManagerID,
        0 AS Level
    FROM HumanResources.Employee e
    INNER JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
    WHERE ManagerID IS NULL
    
    UNION ALL
    
    -- Recursive: Find all subordinates
    SELECT 
        e.BusinessEntityID,
        p.FirstName,
        p.LastName,
        e.ManagerID,
        eh.Level + 1
    FROM HumanResources.Employee e
    INNER JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
    INNER JOIN EmployeeHierarchy eh ON e.ManagerID = eh.BusinessEntityID
)
SELECT 
    BusinessEntityID,
    FirstName,
    LastName,
    Level,
    REPLICATE('  ', Level) + FirstName + ' ' + LastName AS HierarchyDisplay
FROM EmployeeHierarchy
WHERE Level <= 3
ORDER BY Level, LastName;
