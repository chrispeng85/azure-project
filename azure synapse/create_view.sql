USE gold_db
GO

--creates a stored procedure that creates a view for all files

CREATE OR ALTER PROC CreateSQLServerlessView_gold @ViewName nvchar(100)

AS 
BEGIN 

DECLARE @statement VARCHAR(MAX)

    SET @statement = N'CREATE OR ALTER VIEW ' + @ViewName + ' AS
        SELECT *
        FROM 
            OPENROWSET(
                BULK ''https://storageaccountoct31.blob.core.windows.net/gold/SalesLT' + @Viewname + '/'',
                FORMAT = ''DELTA''
            ) as [result]
            '
EXEC (@statement)

END