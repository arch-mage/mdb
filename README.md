# mdb

> [mdbtools][mdbtools] port for go

This provides small API for reading Microsoft Jet Database. Currently, this
only works for JET3 since reading it is my only purpose.

## examples

Example copying to sqlite3.

```go
package main

import (
    "database/sql"
    "fmt"
    "io"
    "os"
    "strings"

    "github.com/arch-mage/mdb"

    _ "github.com/mattn/go-sqlite3"
)

func main() {
    if err := copyTables("db.mdb", "db.sqlite3"); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func copyTables(mdbfile, sqlitefile string) error {
    //
    // open sqlite3
    //
    db, err := sql.Open("sqlite3", sqlitefile)
    if err != nil {
        return err
    }
    defer db.Close()

    //
    // open mdb
    //
    file, err := os.Open(mdbfile)
    if err != nil {
        return err
    }
    defer file.Close()

    //
    // list tables
    //
    tables, err := mdb.Tables(file)
    if err != nil {
        return err
    }

    //
    // make transaction
    //
    tx, err := db.Begin()
    if err != nil {
        return err
    }

    for _, table := range tables {
        if table.Sys { // skip system table
            continue
        }
        if err := createTable(tx, file, table); err != nil {
            return err
        }
        if err := copyRows(tx, file, table); err != nil {
            return err
        }
    }

    return tx.Commit()
}

func createTable(tx *sql.Tx, file io.ReadSeeker, table mdb.Table) error {
    //
    // prepare type conversion
    //
    var typeConversion = map[string]string{
        "Bool":       "BOOl",
        "Byte":       "BYTE",
        "Int":        "INTEGER",
        "LongInt":    "INTEGER",
        "Money":      "NUMERIC",
        "Float":      "REAL",
        "Double":     "REAL",
        "DateTime":   "DATETIME",
        "Binary":     "BLOB",
        "Text":       "TEXT",
        "LongBinary": "BLOB",
        "LongText":   "TEXT",
        "GUID":       "TEXT",
        "Numeric":    "NUMERIC",
    }

    //
    // collect columns
    //
    columns := make([]string, len(table.Columns))
    for i, column := range table.Columns {
        columns[i] = fmt.Sprintf("%s %s", column.Name, typeConversion[column.Type])
        columns[i] = column.Name
    }

    //
    // construct query
    //
    query := fmt.Sprintf(
        "CREATE TABLE %s (%s)",
        table.Name,
        strings.Join(columns, ", "),
    )

    //
    // execute
    //
    _, err := tx.Exec(query)
    return err
}

func copyRows(tx *sql.Tx, file io.ReadSeeker, table mdb.Table) error {

    //
    // collect columns
    //
    columns := make([]string, len(table.Columns))
    bindings := make([]string, len(table.Columns))
    for i, column := range table.Columns {
        columns[i] = column.Name
        bindings[i] = "?"
    }

    //
    // construct query
    //
    query := fmt.Sprintf(
        "INSERT INTO %s (%s) VALUES (%s)",
        table.Name,
        strings.Join(columns, ", "),
        strings.Join(bindings, ", "),
    )

    //
    // prepare it
    //
    stmt, err := tx.Prepare(query)
    if err != nil {
        return err
    }
    defer stmt.Close()

    //
    // iterate rows
    //
    rows, err := mdb.Rows(file, table.Name)
    if err != nil {
        return err
    }
    for {
        fields, err := rows.Next()
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }

        //
        // insert
        //
        if _, err = stmt.Exec(fields...); err != nil {
            return err
        }
    }
    return nil
}
```

[mdbtools]: https://github.com/brianb/mdbtools
