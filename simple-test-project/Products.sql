CREATE TABLE [dbo].[Products]
(
    [ProductID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [ProductName] NVARCHAR(100) NOT NULL,
    [CategoryID] INT NULL,
    [Price] DECIMAL(18,2) NOT NULL,
    [CreateDate] DATETIME2 DEFAULT GETDATE()
)
