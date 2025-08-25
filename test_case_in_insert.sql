CREATE PROCEDURE test_case_in_insert AS
BEGIN
    DECLARE @TestTable TABLE (
        ProductID int,
        PriceLevel varchar(20)
    );
    
    INSERT INTO @TestTable (ProductID, PriceLevel)
    SELECT 
        p.ProductID,
        CASE 
            WHEN p.ListPrice > 1000 THEN 'Expensive'
            WHEN p.ListPrice > 100 THEN 'Moderate'
            ELSE 'Cheap'
        END
    FROM Production.Product p;
    
    SELECT 
        t.ProductID,
        t.PriceLevel
    FROM @TestTable t;
END
