CREATE PROCEDURE test_case_select_only AS
BEGIN
    SELECT 
        p.ProductID,
        CASE 
            WHEN p.ListPrice > 1000 THEN 'Expensive'
            WHEN p.ListPrice > 100 THEN 'Moderate'
            ELSE 'Cheap'
        END as PriceLevel
    FROM Production.Product p;
END
