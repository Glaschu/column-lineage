CREATE TABLE [dbo].[Orders]
(
    [OrderID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [CustomerID] INT NOT NULL,
    [OrderDate] DATETIME2 DEFAULT GETDATE(),
    [TotalAmount] DECIMAL(18,2) NOT NULL,
    [Status] NVARCHAR(50) DEFAULT 'Pending',
    FOREIGN KEY ([CustomerID]) REFERENCES [dbo].[Customers]([CustomerID])
)
