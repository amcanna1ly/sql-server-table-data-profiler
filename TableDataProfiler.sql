/*=======================================================================================
    Name:        TableDataProfiler.sql
    Author:      Alex McAnnally
    Last Edited: 2025-11-23

    Purpose:
        Generic table profiling utility for SQL Server. For a given table, it reports:
            - Total row count
            - Null count and percent null
            - Distinct count
            - Min and max values (as nvarchar(4000))
            - Min and max string length
            - Sample value

    Notes:
        - Run in the context of the target database.
        - Set @SchemaName and @TableName before execution.
        - Intended for data discovery, ETL design, and data quality checks.
        - For very large tables, consider sampling or adding filters.
=======================================================================================*/

--========================================================================
-- 1. Target table
--========================================================================
DECLARE @SchemaName sysname = 'dbo';
DECLARE @TableName  sysname = 'RNDC14_NDC_MSTR';

--========================================================================
-- 2. Validate table exists
--========================================================================
IF OBJECT_ID(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) IS NULL
BEGIN
    RAISERROR('Table %s.%s does not exist in this database.', 16, 1, @SchemaName, @TableName);
    RETURN;
END

DECLARE @FullTableName nvarchar(400) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

--========================================================================
-- 3. Get total row count for the table
--========================================================================
DECLARE @TotalRows bigint;

DECLARE @RowCountSQL nvarchar(max) = N'SELECT @TotalRowsOut = COUNT(*) FROM ' + @FullTableName + ';';
EXEC sp_executesql @RowCountSQL, N'@TotalRowsOut bigint OUTPUT', @TotalRowsOut = @TotalRows OUTPUT;

IF @TotalRows IS NULL
    SET @TotalRows = 0;

--========================================================================
-- 4. Temp tables for columns and profile output
--========================================================================
IF OBJECT_ID('tempdb..#ColumnList') IS NOT NULL DROP TABLE #ColumnList;
IF OBJECT_ID('tempdb..#ColumnProfile') IS NOT NULL DROP TABLE #ColumnProfile;

CREATE TABLE #ColumnList
(
    ColumnId   int IDENTITY(1, 1) PRIMARY KEY,
    ColumnName sysname,
    DataType   nvarchar(128),
    MaxLength  int
);

INSERT INTO #ColumnList (ColumnName, DataType, MaxLength)
SELECT 
      c.name AS ColumnName
    , t.name AS DataType
    , c.max_length AS MaxLength
FROM sys.columns AS c
INNER JOIN sys.types AS t
    ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(@FullTableName)
ORDER BY c.column_id;

CREATE TABLE #ColumnProfile
(
    ColumnId        int,
    ColumnName      sysname,
    DataType        nvarchar(128),
    MaxLength       int,
    TotalRows       bigint,
    NullCount       bigint,
    PercentNull     decimal(5, 2),
    DistinctCount   bigint,
    MinValue        nvarchar(4000),
    MaxValue        nvarchar(4000),
    MinLength       int,
    MaxValueLength  int,
    SampleValue     nvarchar(4000)
);

--========================================================================
-- 5. Loop through columns and build profile metrics
--========================================================================
DECLARE 
      @ColumnId    int
    , @ColumnName  sysname
    , @DataType    nvarchar(128)
    , @ColMaxLen   int
    , @SQL         nvarchar(max);

DECLARE ColumnCursor CURSOR FAST_FORWARD FOR
    SELECT ColumnId, ColumnName, DataType, MaxLength
    FROM #ColumnList
    ORDER BY ColumnId;

OPEN ColumnCursor;

FETCH NEXT FROM ColumnCursor INTO @ColumnId, @ColumnName, @DataType, @ColMaxLen;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = N'
        INSERT INTO #ColumnProfile
        (
              ColumnId
            , ColumnName
            , DataType
            , MaxLength
            , TotalRows
            , NullCount
            , PercentNull
            , DistinctCount
            , MinValue
            , MaxValue
            , MinLength
            , MaxValueLength
            , SampleValue
        )
        SELECT
              ' + CAST(@ColumnId AS nvarchar(10)) + ' AS ColumnId
            , ' + QUOTENAME(@ColumnName, '''') + ' AS ColumnName
            , ' + QUOTENAME(@DataType, '''') + ' AS DataType
            , ' + CAST(@ColMaxLen AS nvarchar(10)) + ' AS MaxLength
            , ' + CAST(@TotalRows AS nvarchar(20)) + ' AS TotalRows
            , SUM(CASE WHEN ' + QUOTENAME(@ColumnName) + ' IS NULL THEN 1 ELSE 0 END) AS NullCount
            , CASE WHEN ' + CAST(@TotalRows AS nvarchar(20)) + ' = 0 
                   THEN 0.00
                   ELSE CAST(SUM(CASE WHEN ' + QUOTENAME(@ColumnName) + ' IS NULL THEN 1 ELSE 0 END) * 100.0 
                             / NULLIF(' + CAST(@TotalRows AS nvarchar(20)) + ', 0) AS decimal(5,2))
              END AS PercentNull
            , COUNT(DISTINCT ' + QUOTENAME(@ColumnName) + ') AS DistinctCount
            , MIN(CONVERT(nvarchar(4000), ' + QUOTENAME(@ColumnName) + ')) AS MinValue
            , MAX(CONVERT(nvarchar(4000), ' + QUOTENAME(@ColumnName) + ')) AS MaxValue
            , MIN(LEN(CONVERT(nvarchar(4000), ' + QUOTENAME(@ColumnName) + '))) AS MinLength
            , MAX(LEN(CONVERT(nvarchar(4000), ' + QUOTENAME(@ColumnName) + '))) AS MaxValueLength
            , MAX(CONVERT(nvarchar(4000), ' + QUOTENAME(@ColumnName) + ')) AS SampleValue
        FROM ' + @FullTableName + ';';

    EXEC sp_executesql @SQL;

    FETCH NEXT FROM ColumnCursor INTO @ColumnId, @ColumnName, @DataType, @ColMaxLen;
END

CLOSE ColumnCursor;
DEALLOCATE ColumnCursor;

--========================================================================
-- 6. Final result
--========================================================================
SELECT
      ColumnId
    , ColumnName
    , DataType
    , MaxLength
    , TotalRows
    , NullCount
    , PercentNull
    , DistinctCount
    , MinValue
    , MaxValue
    , MinLength
    , MaxValueLength
    , SampleValue
FROM #ColumnProfile
ORDER BY ColumnId;

-- End of script
