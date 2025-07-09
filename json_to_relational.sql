CREATE OR ALTER PROCEDURE dbo.json_to_relational
        @blob                       nvarchar(max),      -- The JSON blob
        @schema_name                sysname=N'dbo',     -- The database schema in which to create tables
        @base_table_name            sysname=NULL,       -- If the JSON is not an object with an array, put the data in this base table.
        @proc_name                  sysname,            -- Name of the stored procedure, may be qualified or unqualified
        @column_class_delimiter     nvarchar(10)=N'.',  -- How to delimit attributes within attributes, within attributes, etc.
        @drop_and_recreate          bit=1,              -- Drop/Create the table?
        @print                      bit=0               -- print the SQL statements for debug purposes?
AS

SET NOCOUNT, XACT_ABORT, ARITHABORT ON;


DECLARE @sql nvarchar(max), @msg nvarchar(max), @wrap_object bit=0, @convert_to_array bit=0, @print_sql nvarchar(max);

--- Trim
WHILE (LEFT(@blob, 1) IN (N' ', NCHAR(9), NCHAR(10), CHAR(13))) SET @blob=SUBSTRING(@blob, 2, LEN(@blob));
WHILE (RIGHT(@blob, 1) IN (N' ', NCHAR(9), NCHAR(10), CHAR(13))) SET @blob=LEFt(@blob, LEN(@blob)-1);

IF (SCHEMA_ID(@schema_name)) IS NULL
    THROW 50001, N'The schema name does not exist in this database.', 1;

--- If you've specified a @base_table_name, you can pass an array to the JSON blob, rather than a regular JSON object.
IF (@base_table_name IS NOT NULL
      --AND EXISTS (SELECT NULL FROM OPENJSON(@blob) WHERE [type] NOT IN (4, 5))
        AND NOT EXISTS (SELECT NULL FROM OPENJSON(@blob) WHERE [type]=4 AND [key]=@base_table_name))
    SELECT @convert_to_array=(CASE WHEN LEFT(@blob, 1)=N'[' AND RIGHT(@blob, 1)=N']' THEN 1 ELSE 0 END),
           @wrap_object=1,
           @blob=N'{'+QUOTENAME(@base_table_name, '"')+N': '+(CASE WHEN LEFT(@blob, 1)=N'[' AND RIGHT(@blob, 1)=N']' THEN @blob ELSE N'['+@blob+N']' END)+N'}';

--- On the other hand, if you haven't passed @base_table_name, the top-level JSON blob _must_ be an an array object:
IF (NOT EXISTS (SELECT NULL FROM OPENJSON(@blob) WHERE [type]=4))
    THROW 50001, N'The top-level attribute/object of the JSON blob needs to be a JSON object containing an array object. Alternatively, try specifying the @base_table_name.', 1;


--- Clean up @proc_name. If no schema is explicitly declared, @schema_name is used. 
SET @proc_name=QUOTENAME(ISNULL(PARSENAME(@proc_name, 2), @schema_name))+N'.'+QUOTENAME(PARSENAME(@proc_name, 1));






-------------------------------------------------------------------------------
--- Parse the blob
-------------------------------------------------------------------------------

WITH cte AS (
    --- Get the root (array) object from the blob...
    SELECT CAST(CONVERT(binary(2), ROW_NUMBER() OVER (ORDER BY TRY_CAST(obj.[key] AS smallint), obj.[key])) AS varbinary(64)) AS id,
           CAST(NULL AS varbinary(64)) AS parent_id,
           CAST(obj.[key] AS sysname) AS [key],
           CAST((CASE WHEN obj.[type]=4 THEN obj.[key] ELSE @base_table_name END) AS sysname) COLLATE database_default AS [table],
           CAST(NULL AS sysname) COLLATE database_default AS parent_table,
           CAST((CASE WHEN obj.[type]!=4 THEN obj.[key] END) AS sysname) COLLATE database_default AS [column],
           CAST(NULL AS smallint) AS [row],
           obj.[value] AS [value],
           obj.[type] AS [type],
           CAST(N'$'+ISNULL(N'.'+@base_table_name, N'') AS nvarchar(max)) COLLATE database_default AS json_path
    FROM OPENJSON(@blob) AS obj
    WHERE [type]=4

    UNION ALL

    --- ... and recurse all of its child elements (arrays, objects, strings, numbers, booleans)
    SELECT CAST(parent.id+CONVERT(binary(2), ROW_NUMBER() OVER (PARTITION BY parent.id ORDER BY TRY_CAST(obj.[key] AS smallint), obj.[key])) AS varbinary(64)) AS id,
           parent.id AS parent_id,
           CAST(obj.[key] AS sysname) AS [key],
           CAST((CASE WHEN obj.[type]=4 THEN obj.[key] ELSE parent.[table] END) AS sysname) COLLATE database_default AS [table],
           parent.[table] AS parent_table,
           CAST((CASE WHEN obj.[type]!=4 AND TRY_CAST(obj.[key] AS smallint) IS NULL
                      THEN ISNULL((CASE WHEN parent.[type]!=4 THEN parent.[column] END)+@column_class_delimiter, N'')+obj.[key]
                      --- Arrays with values, rather than objects, get a column name of "value"
                      WHEN parent.[type]=4 AND obj.[type]<4
                      THEN N'value'
                      END) AS sysname) COLLATE database_default AS [column],
           CAST((CASE WHEN parent.[type]=4 THEN obj.[key] ELSE parent.[row] END) AS smallint) AS [row],
           obj.[value] AS [value],
           obj.[type] AS [type],
           CAST(ISNULL(parent.json_path, N'')+(CASE WHEN TRY_CAST(obj.[key] AS smallint) IS NULL THEN N'.'+
                (CASE WHEN obj.[key] LIKE N'%[^0-9a-zA-Z_]%'
                      THEN N'"'+REPLACE(REPLACE(obj.[key], N'\', N'\\'), N'"', N'\"')+N'"'
                      ELSE obj.[key] END) ELSE N'' END) AS nvarchar(max)) AS json_path
    FROM cte AS parent
    CROSS APPLY OPENJSON(parent.[value]) AS obj
    WHERE parent.[type] IN (4, 5))

--- ... and finally, put the complete parsed JSON structure in a temp table, #json_data:
SELECT parent_id,
       id,
       [key] COLLATE database_default AS [key],
       [table],
       [column],
       [row],
       MAX(NULLIF(parent_table, [table])) OVER (PARTITION BY [table]) AS parent_table,
       CAST(NULL AS int) AS parent_row,
       [type],
       (CASE WHEN [type] NOT IN (4, 5) THEN [value] END) COLLATE database_default AS [value],
       (CASE WHEN LEFT(json_path, 1)=N'.' THEN SUBSTRING(json_path, 2, LEN(json_path)) ELSE json_path END) AS json_path
INTO #json_data
FROM cte
OPTION (MAXRECURSION 128);

CREATE UNIQUE CLUSTERED INDEX id ON #json_data (id);








--- Index (table, row, column) 
CREATE INDEX table_column
    ON #json_data
        ([table], [row], [column])
    INCLUDE (parent_row, [value])
    WHERE ([table] IS NOT NULL AND [column] IS NOT NULL AND [row] IS NOT NULL);










-------------------------------------------------------------------------------
--- If two tables share the same name but have multiple parent tables, we
--- need to disambiguate them by qualifying their names.
-------------------------------------------------------------------------------

--- Find tables that have multiple "parent" tables, i.e. multiple json paths:
SELECT MIN(id) AS id, MIN(parent_id) AS parent_id,
       [table], [table] AS new_table,
       json_path, json_path AS parent_json_path
INTO #table_disambiguation
FROM (
    SELECT id, parent_id, [table], json_path,
           (CASE WHEN MIN(json_path) OVER (PARTITION BY [table])=MAX(json_path) OVER (PARTITION BY [table])
                 THEN 0 ELSE 1 END) AS _has_multiple_parents
    FROM #json_data
    WHERE [table] IS NOT NULL
      AND [column] IS NULL
      AND parent_table IS NOT NULL
    ) AS sub
WHERE _has_multiple_parents=1
GROUP BY [table], json_path;

--- Iteratively traverse up in the JSON blob until we have unique names for
--- each table..
WHILE (@@ROWCOUNT!=0)
    UPDATE td
    SET td.parent_id=p.parent_id,
        td.parent_json_path=p.json_path,
        td.new_table=(CASE WHEN p.[type] IN (4, 5) AND TRY_CAST(p.[key] AS smallint) IS NULL THEN p.[key]+@column_class_delimiter ELSE N'' END)+td.new_table
    FROM (
        SELECT *, COUNT(*) OVER (PARTITION BY new_table) AS _duplicates
        FROM #table_disambiguation
        ) AS td
    INNER JOIN #json_data AS p ON p.id=td.parent_id
    WHERE td._duplicates>1;

--- .. and then apply those new table names:
UPDATE d
SET d.[table]=td.new_table
FROM #table_disambiguation AS td
INNER JOIN #json_data AS d ON d.[table]=td.[table] AND LEFT(d.json_path, LEN(td.parent_json_path))=td.parent_json_path;







--- Compute unique row numbers for each value
UPDATE sub
SET sub.[row]=sub._row
FROM (
    SELECT v.[row], ROW_NUMBER() OVER (PARTITION BY v.[table] ORDER BY v.id) AS _row
    FROM #json_data AS v
    INNER JOIN #json_data AS arr ON v.parent_id=arr.id
    WHERE v.[type] NOT IN (4, 5) AND arr.[type]=4
    ) AS sub;





--- Make the index (table, row, column) unique:
CREATE UNIQUE INDEX table_column
    ON #json_data
        ([table], [row], [column])
    INCLUDE (parent_row, [value])
    WHERE ([table] IS NOT NULL AND [column] IS NOT NULL AND [row] IS NOT NULL)
    WITH (DROP_EXISTING=ON);






-------------------------------------------------------------------------------
--- Parse the field values to determine an optimum SQL Server datatype
-------------------------------------------------------------------------------

WITH patterns AS (
    SELECT *
    FROM (
        --- Wildcard patterns
        VALUES (N'date', N'[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]'),
               (N'datetime2(3)', N'[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9][T ][0-2][0-9]:[0-5][0-9]:[0-5][0-9]%'),
               (N'datetimeoffset(3)', N'[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9][T ][0-2][0-9]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z'),
               (N'time(3)', N'[0-2][0-9]:[0-5][0-9]:[0-5][0-9]%'),
               (N'uniqueidentifier', REPLICATE(N'[0-9a-f]', 8)+N'-'+REPLICATE(N'[0-9a-f]', 4)+N'-'+REPLICATE(N'[0-9a-f]', 4)+N'-'+REPLICATE(N'[0-9a-f]', 4)+N'-'+REPLICATE(N'[0-9a-f]', 12)),
               (N'uniqueidentifier', REPLICATE(N'[0-9a-f]', 32))
    ) AS x(sql_type, pattern))

SELECT c.[table], c.[column],
       CAST(MAX((CASE WHEN p.[type]=4 THEN 1 ELSE 0 END)) AS bit) AS is_array,
       (CASE
        WHEN MIN(pat.sql_type)=MAX(pat.sql_type) THEN MIN(pat.sql_type)

        WHEN MIN(c.[type])=1 AND MAX(c.[type])=1 AND MIN(LEN(c.[value]))=MAX(LEN(c.[value])) AND MAX(LEN(c.[value]))<=32 THEN N'nchar('+CAST(MAX(LEN(c.[value])) AS nvarchar(10))+N')'

        WHEN MIN(c.[type])=1 AND MAX(c.[type])=1 AND MIN(LEN(c.[value]))<MAX(LEN(c.[value])) AND MAX(LEN(c.[value]))>=1000 THEN N'nvarchar(max)'
        WHEN MIN(c.[type])=1 AND MAX(c.[type])=1 AND MIN(LEN(c.[value]))<MAX(LEN(c.[value])) AND MAX(LEN(c.[value]))<=25 THEN N'nvarchar('+CAST(MAX(LEN(c.[value])) AS nvarchar(10))+N')'
        WHEN MIN(c.[type])=1 AND MAX(c.[type])=1 AND MIN(LEN(c.[value]))<=MAX(LEN(c.[value])) THEN N'nvarchar('+CAST(POWER(10, CEILING(LOG10(MAX(LEN(c.[value]))))) AS nvarchar(10))+N')'

        WHEN MIN(c.[type])=2 AND MAX(c.[type])=2 AND MAX(num.prec)>0 THEN N'numeric('+CAST(MAX(num.scale)+2 AS nvarchar(10))+N', '+CAST(MAX(num.prec) AS nvarchar(10))+N')'
        WHEN MIN(c.[type])=1 AND MAX(c.[type])=1 AND COUNT(DISTINCT c.[value])=2 AND MIN(c.[value])=N'N' AND MAX(c.[value])=N'Y' THEN N'bit'
        WHEN MIN(c.[type])=1 AND MAX(c.[type])=1 AND COUNT(DISTINCT c.[value])=2 AND MIN(LOWER(c.[value]))=N'false' AND MAX(LOWER(c.[value]))=N'true' THEN N'bit'
        WHEN MIN(c.[type])=2 AND MAX(c.[type])=2 AND MAX(num.prec)=0 AND MIN(c.[value])=N'0' AND MAX(c.[value])=N'1' THEN N'bit'
        WHEN MIN(c.[type])=2 AND MAX(c.[type])=2 AND MAX(num.prec)=0 AND MAX(num.scale)<=2 THEN N'tinyint'
        WHEN MIN(c.[type])=2 AND MAX(c.[type])=2 AND MAX(num.prec)=0 AND MAX(num.scale)<=4 THEN N'smallint'
        WHEN MIN(c.[type])=2 AND MAX(c.[type])=2 AND MAX(num.prec)=0 AND MAX(num.scale)<=8 THEN N'int'
        WHEN MIN(c.[type])=2 AND MAX(c.[type])=2 AND MAX(num.prec)=0 THEN N'bigint'
        WHEN MIN(c.[type])=3 AND MAX(c.[type])=3 THEN N'bit'
        END) COLLATE database_default AS sql_type,
       CAST(0 AS bit) AS is_identity,
       CAST((CASE WHEN COUNT(c.[value])=MAX(rc.row_count) THEN 0 WHEN MAX(rc.row_count) IS NULL THEN NULL ELSE 1 END) AS bit) AS is_nullable,
       CAST((CASE WHEN COUNT(c.[value])=COUNT(DISTINCT c.[value]) THEN 1 ELSE 0 END) AS bit) AS is_unique,
       CAST(0 AS bit) AS is_primary_key,
       CAST(0 AS bit) AS is_foreign_key,
       CAST(NULL AS sysname) COLLATE database_default AS [references],
       MIN(MIN(LEN(c.id))) OVER (PARTITION BY c.[table]) AS dependency_order,
       MIN(b.base_json_path) AS base_json_path,
       (CASE WHEN MIN(b.base_json_path)=MAX(c.json_path) THEN N'$'
             ELSE N'$.'+SUBSTRING(MAX(c.json_path), LEN(MIN(b.base_json_path))+2, LEN(MAX(c.json_path))) END) AS relative_json_path
INTO #json_types
FROM #json_data AS c
LEFT JOIN #json_data AS p ON c.parent_id=p.id
INNER JOIN (
    SELECT [table], MIN(json_path) AS base_json_path
    FROM #json_data
    WHERE [type] IN (4, 5)
    GROUP BY [table]
    ) AS b ON c.[table]=b.[table]
OUTER APPLY (
    SELECT TOP (1) sql_type
    FROM patterns
    WHERE c.[type]=1 AND c.[value] LIKE pattern
    ORDER BY LEN(pattern) DESC
    ) AS pat
OUTER APPLY (
    SELECT SUM(LEN(ss.[value])) AS scale,
           MAX((CASE WHEN ss.ordinal=2 THEN LEN(ss.[value]) ELSE 0 END)) AS prec
    FROM STRING_SPLIT(c.[value], N'.', 1) AS ss
    WHERE c.[type]=2
    GROUP BY ()
    ) AS num
LEFT JOIN (
    SELECT [table], COUNT(DISTINCT [row]) AS row_count
    FROM #json_data
    WHERE [type]=5
      AND [table] IS NOT NULL
      AND [column] IS NULL
    GROUP BY [table]
    ) AS rc ON c.[table]=rc.[table]
WHERE c.[table] IS NOT NULL
  AND c.[column] IS NOT NULL
GROUP BY c.[table], c.[column]
HAVING COUNT(c.[value])>0;

CREATE UNIQUE CLUSTERED INDEX IX ON #json_types ([table], [column]);








-------------------------------------------------------------------------------
--- Identify the most suitable single-column primary key, if there is one.
-------------------------------------------------------------------------------

WITH type_ranking AS (
    SELECT *
    FROM (
        VALUES (N'bigint', 1),
               (N'numeric(_[0-9], 0)', 2),
               (N'numeric([6-9], 0)', 3),
               (N'int', 4),
               (N'date', 5),
               (N'datetime%', 6),
               (N'nchar([4-8])', 7),
               (N'numeric([0-5], 0)', 8),
               (N'nchar([2-3])', 9),
               (N'nchar(__)', 10),
               (N'nvarchar(__)', 11),
               (N'nvarchar(_)', 12),
               (N'smallint', 13),
               (N'time%', 14),
               (N'tinyint', 15),
               (N'%(max)', 100)
    ) AS x(pattern, prio)),

name_ranking AS (
    SELECT *
    FROM (
        VALUES (N'id', 1),
               (N'%id', 2),
               (N'%text%', 100),
               (N'desc%', 100)
    ) AS x(pattern, prio))

UPDATE sub
SET sub.is_primary_key=1
FROM (
    SELECT j.*, ROW_NUMBER() OVER (PARTITION BY j.[table] ORDER BY ISNULL(t.prio, 20)*ISNULL(n.prio, 10)) AS _prio
    FROM #json_types AS j
    OUTER APPLY (
        SELECT TOP (1) prio
        FROM type_ranking
        WHERE j.sql_type LIKE pattern
        ORDER BY prio
        ) AS t
    OUTER APPLY (
        SELECT TOP (1) prio
        FROM name_ranking
        WHERE j.[column] LIKE pattern
        ORDER BY prio
        ) AS n
    WHERE j.is_unique=1
      AND j.is_nullable=0
) AS sub
WHERE _prio=1;



--- Array members are now primary keys.
UPDATE #json_types
SET is_primary_key=1
WHERE is_array=1
  AND [column]=N'value';





-------------------------------------------------------------------------------
--- Add synthetic primary key (identity) columns, where there is no
--- suitable primary key column.
-------------------------------------------------------------------------------

INSERT INTO #json_types ([table], [column], is_array, sql_type, is_nullable, is_unique, is_primary_key, is_foreign_key, is_identity, dependency_order)
SELECT [table],
       (CASE WHEN COUNT((CASE WHEN [column]=N'id' THEN 1 END))=0 THEN N'id'
             WHEN COUNT((CASE WHEN [column]=N'ident' THEN 1 END))=0 THEN N'ident'
             WHEN COUNT((CASE WHEN [column]=N'pk' THEN 1 END))=0 THEN N'pk'
             WHEN COUNT((CASE WHEN [column]=N'rowno' THEN 1 END))=0 THEN N'rowno'
             ELSE N'##identity'
             END) AS [column],
       MAX(CAST(is_array AS tinyint)) AS is_array,
       N'int' AS sql_type,
       0 AS is_nullable,
       1 AS is_unique,
       1 AS is_primary_key,
       0 AS is_foreign_key,
       1 AS is_identity,
       MAX(dependency_order) AS dependency_order
FROM #json_types
GROUP BY [table]
HAVING MAX(CAST(is_primary_key AS tinyint))=0;





--- Add foreign key columns
INSERT INTO #json_types ([table], [column], is_array, sql_type, is_nullable, is_unique, is_primary_key, is_foreign_key, is_identity, [references], dependency_order)
SELECT rel.[table],
       pk.[table]+@column_class_delimiter+pk.[column] AS [column],
       t.is_array, pk.sql_type, pk.is_nullable, 0 AS is_unique, 0 AS is_primary_key, 1 AS is_foreign_key,
       0 AS is_identity, rel.parent_table AS [references], t.dependency_order
FROM (
    SELECT DISTINCT [table], parent_table
    FROM #json_data
    WHERE parent_table IS NOT NULL
    ) AS rel
INNER JOIN (
    SELECT [table], dependency_order, MAX(CAST(is_array AS tinyint)) AS is_array
    FROM #json_types
    WHERE dependency_order IS NOT NULL
    GROUP BY [table], dependency_order
    ) AS t ON rel.[table]=t.[table]
INNER JOIN #json_types AS pk ON rel.parent_table=pk.[table] AND pk.is_primary_key=1
LEFT JOIN #json_types AS fk ON rel.[table]=fk.[table] AND fk.[column]=pk.[column];




--- Make the dependency_order column dense
UPDATE sub
SET sub.dependency_order=sub._order
FROM (
    SELECT *, DENSE_RANK() OVER (ORDER BY dependency_order, [table]) AS _order
    FROM #json_types
) AS sub;



--- Synthetic columns in #json_types, like primary/foreign keys, don't have a base_json_path,
--- so we'll apply it from another column in the same table:
UPDATE a
SET a.base_json_path=b.base_json_path
FROM #json_types AS a
INNER JOIN #json_types AS b ON a.[table]=b.[table]
WHERE a.base_json_path IS NULL
  AND b.base_json_path IS NOT NULL;






-------------------------------------------------------------------------------
--- Drop tables that already exist if @drop_and_recreate=1
-------------------------------------------------------------------------------

IF (@drop_and_recreate=1) BEGIN;
    SELECT @sql=STRING_AGG(CAST([sql] AS nvarchar(max)), N'
') WITHIN GROUP (ORDER BY dependency_order DESC)
    FROM (
        SELECT dependency_order, N'DROP TABLE IF EXISTS '+QUOTENAME(@schema_name)+N'.'+QUOTENAME([table])+N';' AS [sql]
        FROM #json_types
        GROUP BY [table], dependency_order
    ) AS sub

    IF (@print=1) PRINT @sql;
    EXECUTE sys.sp_executesql @sql;
END;




-------------------------------------------------------------------------------
--- Add new columns to existing tables
-------------------------------------------------------------------------------

SELECT @sql=STRING_AGG(CAST(N'ALTER TABLE '+QUOTENAME(@schema_name)+N'.'+QUOTENAME(t.[table])+N' ADD '+
    QUOTENAME(t.[column])+N' '+t.sql_type+
    (CASE WHEN t.is_identity=1 THEN N' IDENTITY(1, 1)' ELSE N'' END)+N' '+
    (CASE WHEN t.is_nullable=1 OR p.[rowcount]>0 THEN N'NULL' ELSE N'NOT NULL' END)+N';' AS nvarchar(max)), N'
')
FROM #json_types AS t
INNER JOIN sys.tables AS tbl ON tbl.[schema_id]=SCHEMA_ID(@schema_name) AND tbl.[name] COLLATE database_default=t.[table]
LEFT JOIN sys.columns AS col ON tbl.[object_id]=col.[object_id] AND col.[name] COLLATE database_default=t.[column]
INNER JOIN (
    SELECT [object_id], SUM([rows]) AS [rowcount]
    FROM sys.partitions
    GROUP BY [object_id]
    ) AS p ON tbl.[object_id]=p.[object_id]
WHERE col.column_id IS NULL;

IF (@print=1) PRINT @sql;
EXECUTE sys.sp_executesql @sql;





-------------------------------------------------------------------------------
--- Construct and run the CREATE TABLE statements
-------------------------------------------------------------------------------

SELECT @sql=N'BEGIN TRANSACTION;

'+STRING_AGG(CAST(create_table_sql AS nvarchar(max)), N'
') WITHIN GROUP (ORDER BY dependency_order)+N'

COMMIT TRANSACTION;'
FROM (
    SELECT t.dependency_order, N'
    CREATE TABLE '+QUOTENAME(@schema_name)+N'.'+QUOTENAME(t.[table])+N' (
        '+STRING_AGG(CAST(
            QUOTENAME(t.[column])+N' '+
            t.sql_type+
            (CASE WHEN t.is_identity=1 THEN N' IDENTITY(1, 1)' ELSE N'' END)+N' '+
            (CASE WHEN t.is_nullable=1 THEN N'NULL' ELSE N'NOT NULL' END) AS nvarchar(max)), N',
        ') WITHIN GROUP (ORDER BY (CASE WHEN t.is_primary_key=1 THEN 1 ELSE 2 END), (CASE WHEN t.is_foreign_key=1 THEN 1 ELSE 2 END), t.[column])+ISNULL(N',
        CONSTRAINT '+QUOTENAME(N'PK_'+t.[table])+N' PRIMARY KEY CLUSTERED ('+ISNULL((CASE WHEN MAX(CAST(t.is_array AS tinyint))=1 THEN QUOTENAME(MAX((CASE WHEN t.is_foreign_key=1 THEN t.[column] END)))+N', ' END), N'')+QUOTENAME(MAX((CASE WHEN t.is_primary_key=1 THEN t.[column] END)))+N')', N'')+ISNULL(N',
        CONSTRAINT '+QUOTENAME(N'FK_'+t.[table]+N'_'+MIN(t.[references]))+N' FOREIGN KEY ('+QUOTENAME(MAX((CASE WHEN t.is_foreign_key=1 THEN t.[column] END)))+
        N') REFERENCES '+QUOTENAME(@schema_name)+N'.'+QUOTENAME(MIN(t.[references]))+N' ('+(SELECT QUOTENAME([column]) FROM #json_types WHERE [table]=MIN(t.[references]) AND is_primary_key=1)+N') ON DELETE CASCADE', N'')+N'
    );' AS create_table_sql
    FROM #json_types AS t
    WHERE t.[table] NOT IN (SELECT [name] COLLATE database_default FROM sys.tables WHERE [schema_id]=SCHEMA_ID(@schema_name))
    GROUP BY t.[table], t.dependency_order
) AS sub;

IF (@print=1) PRINT @sql;
EXECUTE sys.sp_executesql @sql;






-------------------------------------------------------------------------------
--- Construct the stored procedure
-------------------------------------------------------------------------------

WITH step1 AS (
    --- All "foreign key" tables, i.e. tables that are dependent on another table
    SELECT fk.[table],
           N'#table'+CAST(pk.dependency_order AS nvarchar(10)) AS [references],
           pk.[table] AS parent,
           pk.[column] AS referenced_column,
           fk.[column] AS referencing_column,
           fk.is_array,
           N'JSON_VALUE('+QUOTENAME(pk.[table])+N'.[value], '+QUOTENAME(pk.relative_json_path, N'''')+N')' AS json_value_sql,
           N'OPENJSON('+QUOTENAME(pk.[table])+N'.[value]'+N', '+QUOTENAME(N'$.'+SUBSTRING(fk.base_json_path, LEN(pk.base_json_path)+2, LEN(fk.base_json_path)), N'''')+N')' AS openjson_sql,
           CAST(N'CAST('+QUOTENAME(fk.[table])+N'.[key] AS smallint)' AS nvarchar(max)) AS rank_order_sql
    FROM #json_types AS fk
    INNER JOIN #json_types AS pk ON fk.[references]=pk.[table]
    WHERE fk.is_foreign_key=1
      AND pk.is_primary_key=1

    UNION ALL

    --- ... and the primary key table(s), which do not have a foreign key to
    --- another table.
    SELECT pk.[table],
           NULL AS [references],
           NULL AS parent,
           NULL AS referenced_column,
           NULL AS referencing_column,
           pk.is_array,
           NULL AS json_value_sql,
           CAST(N'OPENJSON(@blob'+N', '+QUOTENAME(pk.base_json_path, N'''')+N')' AS nvarchar(max)) AS openjson_sql,
           CAST(N'CAST('+QUOTENAME(pk.[table])+N'.[key] AS smallint)' AS nvarchar(max)) AS rank_order_sql
    FROM #json_types AS pk
    WHERE pk.[table] NOT IN (SELECT [table] FROM #json_types WHERE is_foreign_key=1)
      AND pk.is_primary_key=1),

step2 AS (
    --- Add information about each table's primary key, referenced table (and
    --- foreign key column), as well as the T-SQL for the UPDATE and INSERT
    --- clauses of the MERGE statement.
    SELECT cols.dependency_order,
           step1.[table],
           MAX((CASE WHEN cols.is_primary_key=1 THEN cols.[column] END)) AS pk_column,
           step1.[references],
           step1.parent,
           step1.referenced_column,
           step1.referencing_column,
           step1.is_array,
           step1.json_value_sql,
           step1.openjson_sql,
           N'WITH ('+STRING_AGG((CASE WHEN step1.is_array=0 THEN N'
        '+QUOTENAME(cols.[column])+N' '+cols.sql_type+N' '+QUOTENAME(cols.relative_json_path, N'''') END), N',')+N'
    )' AS with_sql,
           STRING_AGG(N'tbl.'+QUOTENAME(cols.[column])+N'=src.'+QUOTENAME(cols.[column]), N', ') WITHIN GROUP (ORDER BY cols.[column]) AS update_sql,
           STRING_AGG(QUOTENAME(cols.[column]), N', ') WITHIN GROUP (ORDER BY cols.[column]) AS insert_header_sql,
           STRING_AGG(N'src.'+QUOTENAME(cols.[column]), N', ') WITHIN GROUP (ORDER BY cols.[column]) AS insert_values_sql,
           step1.rank_order_sql,
           MAX(ref_cols.[column]) AS parent_identity_column
    FROM step1
    INNER JOIN #json_types AS cols ON step1.[table]=cols.[table]
    LEFT JOIN #json_types AS ref_cols ON cols.[references]=ref_cols.[table] AND ref_cols.is_identity=1
    WHERE cols.relative_json_path IS NOT NULL
       OR cols.is_identity=1
    GROUP BY cols.dependency_order, step1.[table], step1.[references], step1.parent, step1.referenced_column,
             step1.referencing_column, step1.is_array, step1.json_value_sql, step1.openjson_sql, step1.rank_order_sql),

step3 AS (
    --- Construct the FROM clause by traversing up the dependency chaing.
    --- Anchor...
    SELECT dependency_order, [table], pk_column, parent, [references], referenced_column,
           referencing_column, is_array, parent_identity_column, with_sql, rank_order_sql,
           update_sql, insert_header_sql, insert_values_sql,
           CAST(openjson_sql+N' AS '+QUOTENAME([table]) AS nvarchar(max)) AS [from]
    FROM step2

    UNION ALL

    --- ... and recursion.
    SELECT s3.dependency_order, s3.[table], s3.pk_column, s2.parent, s3.[references], s3.referenced_column,
           s3.referencing_column, s3.is_array, s3.parent_identity_column, s3.with_sql, s2.rank_order_sql+N', '+s3.rank_order_sql,
           s3.update_sql, s3.insert_header_sql, s3.insert_values_sql,
           CAST(s2.openjson_sql+N' AS '+QUOTENAME(s2.[table])+N'
        CROSS APPLY '+s3.[from] AS nvarchar(max)) AS [from]
    FROM step3 AS s3
    INNER JOIN step2 AS s2 ON s3.parent=s2.[table])


--- Putting it all together into one big stored procedure.
SELECT @sql=N'CREATE OR ALTER PROCEDURE '+@proc_name+N'
    @blob       nvarchar(max)
AS

SET NOCOUNT ON;

'+(CASE WHEN @wrap_object=1 THEN N'
SET @blob=N''{'+REPLACE(QUOTENAME(@base_table_name, '"'), N'''', N'''''')+N': ''+'+
    (CASE WHEN @convert_to_array=1 THEN N'@blob' ELSE N'N''[''+@blob+N'']''' END)+N'+N''}'';'
ELSE N'' END)+N'

'+STRING_AGG(N'
-------------------------------------------------------------------------------
--- '+[table]+N'
-------------------------------------------------------------------------------

PRINT '+QUOTENAME([table], N'''')+N';
'
--- Create a blank temp table called #table(n) where the n is the dependency order.
--- This table contains three columns:
--- * #parent_seq, which is the parent row number in the JSON blob,
--- * #seq, the current "level"'s row number, and
--- * the primary key from the current table.
--- This table will be used in an OUTPUT clause in the following MERGE statement,
--- so that we have a mapping table between the recently inserted row(s) and their
--- unique ids, which will help us join subsequent table rows to rows in this table.
+N'
SELECT CAST(NULL AS bigint) AS [#parent_seq], CAST(NULL AS bigint) AS [#seq], '+QUOTENAME(pk_column)+N'
INTO #table'+CAST(dependency_order AS nvarchar(10))+N'
FROM '+QUOTENAME(@schema_name)+N'.'+QUOTENAME([table])+N'
WHERE 1=0;'
--- "src" is the constructed OPENJSON data, joined to the parent #table(n) object
--- which creates the complete source data of the MERGE statement for this table
+N'
WITH src AS (
    SELECT x.[#parent_seq], x.[#seq], '+
        (CASE WHEN is_array=1 THEN N'x.[value]' ELSE N'j.*' END)+
        ISNULL(N', ref.'+QUOTENAME(referenced_column)+N' AS '+QUOTENAME(referencing_column), N'')+N'
    FROM (
        SELECT DENSE_RANK() OVER (ORDER BY '+rank_order_sql+N') AS [#seq],
               '+ISNULL(N'DENSE_RANK() OVER (ORDER BY '+LEFT(rank_order_sql, NULLIF(CHARINDEX(N', CAST('+QUOTENAME([table]), rank_order_sql), 0)-1)+N')', N'NULL')+N' AS [#parent_seq],
               '+QUOTENAME([table])+N'.[value]
        FROM '+[from]+N'
    ) AS x'+(CASE WHEN is_array=0 THEN N'
    CROSS APPLY OPENJSON(x.[value]) '+ISNULL(with_sql, N'')+N' AS j' ELSE N'' END)+ISNULL(N'
    INNER JOIN '+[references]+N' AS ref ON x.[#parent_seq]=ref.[#seq]', N'')+N'
),'
--- "tbl" is the target table in the MERGE statement. We're filtering this table,
--- so it only contains rows that relate to parent objects in this JSON blob, which
--- allows us to delete unmatched rows, without clearing out all of the previously
--- loaded rows from the table.
+N'
tbl AS (
    SELECT *
    FROM '+QUOTENAME(@schema_name)+N'.'+QUOTENAME([table])+ISNULL(N'
    WHERE '+QUOTENAME(referencing_column)+N' IN (SELECT '+QUOTENAME(referenced_column)+N' FROM '+[references]+N')', N'')+N'
)

MERGE INTO tbl
USING src ON '+
    --- Only join on the referencing_column if there is a parent table:
    ISNULL(N'src.'+QUOTENAME(referencing_column)+N'=tbl.'+QUOTENAME(referencing_column)+N' AND ', N'')+
    --- .. and on the primary key of this level:
    N'src.'+QUOTENAME(pk_column)+N'=tbl.'+QUOTENAME(pk_column)+N'

WHEN NOT MATCHED BY TARGET THEN
    INSERT ('+insert_header_sql+ISNULL(N', '+QUOTENAME(referencing_column), N'')+N')
    VALUES ('+insert_values_sql+ISNULL(N', src.'+QUOTENAME(referencing_column), N'')+N')

WHEN MATCHED THEN
    UPDATE SET '+update_sql+(CASE WHEN [references] IS NOT NULL THEN N'

WHEN NOT MATCHED BY SOURCE THEN
    DELETE' ELSE N'' END)+N'
'
--- The OUTPUT clause saves the inserted primary key, along with the generated #parent_seq and #seq
--- for future reference, so we can join rows in dependent tables to this table:
+N'
OUTPUT src.[#parent_seq], src.[#seq], ISNULL(src.'+QUOTENAME(pk_column)+N', deleted.'+QUOTENAME(pk_column)+N')
INTO #table'+CAST(dependency_order AS nvarchar(10))+N' ([#parent_seq], [#seq], '+QUOTENAME(pk_column)+N');

PRINT ''...''+STR(@@ROWCOUNT, 10, 0)+'' row(s)'';', N'

') WITHIN GROUP (ORDER BY dependency_order)
FROM step3
WHERE parent IS NULL;





-------------------------------------------------------------------------------
--- Print the T-SQL for the stored procedure, then execute it:
-------------------------------------------------------------------------------

IF (@print=1) BEGIN;
    SET @print_sql=REPLACE(REPLACE(@sql, NCHAR(13)+NCHAR(10), NCHAR(10)), NCHAR(13), NCHAR(10));
    WHILE (@print_sql IS NOT NULL) BEGIN;
        PRINT LEFT(@print_sql, CHARINDEX(NCHAR(10), @print_sql+NCHAR(10))-1);
        SET @print_sql=NULLIF(SUBSTRING(@print_sql, CHARINDEX(NCHAR(10), @print_sql+NCHAR(10))+1, LEN(@print_sql)), N'');
    END;
END;

EXECUTE sys.sp_executesql @sql;


GO
