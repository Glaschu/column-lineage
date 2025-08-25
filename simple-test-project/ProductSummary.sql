CREATE VIEW [dbo].[ProductSummary]
AS
SELECT 
    ProductID,
    ProductName,
    Price,
    CASE 
        WHEN Price > 100 THEN 'Expensive'
        WHEN Price > 50 THEN 'Moderate'
        ELSE 'Cheap'
    END AS PriceCategory
FROM [dbo].[Products]
WHERE CreateDate >= DATEADD(MONTH, -1, GETDATE())
