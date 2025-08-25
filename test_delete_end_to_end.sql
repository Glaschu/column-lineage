CREATE PROCEDURE test_delete_end_to_end AS
BEGIN
    -- Create a temp table for tracking
    DECLARE @ProductsToDelete TABLE (
        ProductID int,
        Reason varchar(50)
    );
    
    -- Populate products to delete based on sales data
    INSERT INTO @ProductsToDelete (ProductID, Reason)
    SELECT 
        p.ProductID,
        CASE 
            WHEN AVG(sod.UnitPrice) < 10 THEN 'Low Price'
            WHEN COUNT(sod.SalesOrderDetailID) < 5 THEN 'Low Sales'
            ELSE 'Other'
        END as Reason
    FROM Production.Product p
    LEFT JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
    WHERE p.DiscontinuedDate IS NOT NULL
    GROUP BY p.ProductID, p.Name;
    
    -- Select what we would delete (simulating DELETE logic)
    SELECT 
        ptd.ProductID,
        ptd.Reason,
        p.Name as ProductName,
        pc.Name as CategoryName
    FROM @ProductsToDelete ptd
    INNER JOIN Production.Product p ON ptd.ProductID = p.ProductID
    INNER JOIN Production.ProductCategory pc ON p.ProductSubcategoryID = pc.ProductCategoryID
    WHERE ptd.Reason IN ('Low Price', 'Low Sales');
END
