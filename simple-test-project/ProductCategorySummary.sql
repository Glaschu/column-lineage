CREATE VIEW [dbo].[ProductCategorySummary]
AS
SELECT 
    p.ProductID,
    p.ProductName,
    p.Price,
    c.CategoryName,
    c.Description as CategoryDescription
FROM [dbo].[Products] p
LEFT JOIN [dbo].[Categories] c ON p.CategoryID = c.CategoryID
WHERE p.CreateDate >= DATEADD(MONTH, -3, GETDATE())
