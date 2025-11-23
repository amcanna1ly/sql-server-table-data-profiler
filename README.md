# sql-server-table-data-profiler
A SQL Server table profiling utility that analyzes column level metadata, null percentages, distinct counts, value ranges, and length statistics. Ideal for data discovery, ETL design, and data quality assessment across any SQL Server table.

# SQL Server Table Data Profiler

`TableDataProfiler.sql` is a reusable SQL Server utility script that profiles any table in a database and returns detailed column-level data quality and distribution metrics. It helps analysts, SQL developers, and ETL engineers quickly understand the structure and characteristics of unfamiliar datasets.

## Features

For each column in the target table, the profiler returns:

- **Column name, data type, and max length**
- **Total row count** for the table
- **Null count and percent null**
- **Distinct value count**
- **Minimum and maximum values** (converted to `sql_variant`)
- **Minimum and maximum string length**
- **Sample value** for quick inspection

These metrics are essential for:
- Data discovery
- ETL design and validation
- Identifying data quality issues
- Choosing correct column types
- Detecting categorical vs continuous variables
- Evaluating sparsity and missingness

## How It Works

1. The script verifies that the target table exists.
2. It counts total rows in the table.
3. It retrieves column metadata from `sys.columns` and `sys.types`.
4. For each column, it dynamically computes:
   - Null statistics  
   - Distinct counts  
   - Value ranges  
   - Length distributions  
5. Results are stored in a temporary table and output as one consolidated result set.

This design allows it to profile **any** table without requiring knowledge of the underlying schema.

## Usage

1. Open `TableDataProfiler.sql` in SSMS or Azure Data Studio.
2. At the top of the script, set the table you want to profile:

   ```sql
   DECLARE @SchemaName sysname = 'dbo';
   DECLARE @TableName  sysname = 'YourTableNameHere';

