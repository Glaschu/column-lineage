CREATE TABLE [dbo].[Customers]
(
    [CustomerID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [CustomerName] NVARCHAR(100) NOT NULL,
    [Email] NVARCHAR(255) NULL,
    [CreateDate] DATETIME2 DEFAULT GETDATE(),
    [IsActive] BIT DEFAULT 1
)
