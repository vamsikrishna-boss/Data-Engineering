SELECT * FROM [dbo].[Bank Satatement Data]

SELECT * FROM [dbo].[Bank Satatement Data] WHERE Date = '2023-01-16'

SELECT distinct [Customer Name] from [dbo].[Bank Satatement Data]
SELECT *,YEAR(Date) AS Year_ID, 
MONTH(Date) AS Month_ID, 
Day(Date) AS Day_ID,
CONCAT(Year(Date),'-',Datename(month,date)) AS [YYYY-MM]
FROM [dbo].[Bank Satatement Data]

ALTER TABLE [dbo].[Bank Satatement Data]
ALTER COLUMN Date Datetime;


SELECT [Customer Name],SUM(Deposits) AS D,
SUM(Withdraws) AS W,SUM(Balance) AS B from [dbo].[Bank Satatement Data]
Group by [Customer Name]