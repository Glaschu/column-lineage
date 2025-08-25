CREATE PROCEDURE test_case_cast AS
BEGIN
    SELECT 
        p.ProductID,
        CASE 
            WHEN p.ListPrice > 1000 THEN 'Expensive'
            WHEN p.ListPrice > 100 THEN 'Moderate'
            ELSE 'Cheap'
        END as PriceCategory,
        CAST(p.ListPrice as varchar(50)) as PriceText,
        CONVERT(varchar(10), p.ModifiedDate, 101) as ModifiedDateText,
        UPPER(p.Name) as ProductNameUpper
    FROM Production.Product p
    WHERE p.ProductCategoryID IS NOT NULL;
END
