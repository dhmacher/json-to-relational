# json-to-relational

A T-SQL utility procedure to load a JSON object into one or more relational tables. The procedure

* creates one or more relational tables modelled after the JSON object supplied
* adds primary key and foreign key constraints to those tables
* builds a stored procedure that loads a JSON object into those tables

## Disclaimer

This code is provided as-is.

If you did not pay me to write or implement this code, I will not accept any liability or unpaid support work.
It is solely up to you as the developer/dba to review and test the script.

Remember that because the script drops and creates database objects based on the contents of an input JSON object,
it is by definition vulnerable to injection in the JSON object. Do not automate the meta procedure, as a malicious
JSON object may unintentionally drop or alter tables in your database.

For optimum isolation, assign a unique schema for your JSON imports using the `@schema_name` argument.

## Arguments

* `@blob`                       nvarchar(max): the JSON blob
* `@schema_name`                sysname: the database schema in which to create tables (defaults to "dbo")
* `@base_table_name`            sysname: if the JSON is not an object with an array, put the data in this base table.
* `@proc_name`                  sysname: the name of the stored procedure, may be qualified or unqualified
* `@column_class_delimiter`     nvarchar(10): how to delimit attributes within attributes, within attributes, etc.
* `@drop_and_recreate`          bit: drop & create tables if they already exist.
* `@print`                      bit: print the T-SQL code for debug purposes.

Required arguments are `@blob` and `@proc_name`.

The JSON blob must be a valid JSON object containing at least one array. You can "fix" the JSON blob in-flight by
specifying the @base_table_name argument manually.

The `@column_class_delimiter` is used to qualify column names derived from the JSON object. For instance, when object `a` contains an object `b`, which contains an attribute `c`, this will generate a column called `a_b_c` if the `@column_class_delimiter` is `_` (underscore). The default delimiter is `.`.

## The workflow

* Run the `dbo.json_to_relational` procedure once with a relatively large, representative JSON object. This is a one-time step, and creates tables and a stored procedure (`@proc_name`).
* Run the new stored procedure to load JSON objects into the table structure. This step can be repeated.

## How it works

### Parsing

The procedure parses the JSON object using recursive OPENJSON calls on all arrays and objects. Every array object
forms a new table name. Every attribute name becomes a column name.

Where arrays contain only values and no objects, the default column name "value" is used.

### Table name disambiguation

If two distinct tables share names across the JSON blob, the table names are qualified until
they are unique. Consider the following example:

```
{"test": [
    {"name": "Somebody",
     "id": 100,
     "a": {"tags": [1, 2, 3, 4]},
     "b": {"tags": [5, 6, 7, 8]},
     "c": {"tags": [9, 10, 11, 12]},
     "d": {
        "d1": "Hello",
        "d2": "World"
     },
     "e": [{"ex": "Test 1"},
           {"ex": "Test 2", "f": [{"fx": 1}, {"fx": 2}, {"fx": 3}]},
           {"ex": "Test 3"}]
    },
    {"name": "Somebody else",
     "id": 101,
     "a": {"tags": [101, 102, 103, 104]},
     "b": {"tags": [105, 106, 107, 108]},
     "c": {"tags": [109, 110, 111, 112]},
     "d": {
        "d1": "Hello",
        "d2": "World 2"
     }
    }
]}
```

Note how attributes `a`, `b`, and `c`, all have a `tags` array. Rather than creating one `tags` table, and
constructing a connecting relational table between `test2` and `tags`, the procedure will disambiguate the
three tags tables, so that they are created as `a.tags`, `b.tags` and `c.tags` (assuming `column_class_delimiter` is `.`).

### Mapping datatypes

Looking at the JSON object, the procedure makes an educated guess of the optimal SQL Server datatype for
each column of each table. This is a two-step process:

* Matching wildcard patterns on text strings (ISO date strings, timestamps, unique identifiers)
* Determining the minimum & maximum length of strings, the scale and precision of numeric values, etc.

### Determining a primary key

The procedure will attempt to identify a candidate primary key by looking at values that are non-nullable
(i.e. available on every row) and unique. If multiple candidates are available, the "most suitable" one is
chosen by looking at the precision/size of the datatype as well as the name of the column.

Note that a small dataset may yield primary keys that are not unique when the size of the JSON blob
increases.

If a primary key cannot be selected, an IDENTITY column is created as the table's primary key.

## How duplicate data is handled

In tables with a (non-synthetic, i.e. not an IDENTITY) primary key, rows can be updated and/or deleted.

* Rows in top-level tables can be inserted or updated.
* Rows in sub-level tables can be inserted, updated or deleted.

When provisioning the tables, all foreign key constraints are created as ON DELETE CASCADE. 

## Deploying

With all the information collected and normalized, the procedure will

* Drop existing tables (if `@drop_and_recreate` is 1)
* Add new columns to any existing tables
* Creating tables that do not yet exist
* Creating the stored procedure `@proc_name`
