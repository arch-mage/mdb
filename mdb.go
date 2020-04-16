// Package mdb provides utilities to inspect mdb database. Currently
// only MDB Jet 3 database is implemented.
//
// The provided API is not compatible with database/sql package. But,
// to implements one using this package is trivial.
package mdb

import (
	"bytes"
	"errors"
	"io"
	"strings"
)

// ErrNotMDBJet3 indicates that the file is not a valid MDB Jet3.
var ErrNotMDBJet3 = errors.New("not MDB Jet 3 database.")

// IsValidMDBJet3 returns nil if given file is a valid MDB Jet3
// database file.
func IsValidMDBJet3(file io.ReadSeeker) (err error) {
	const header = "\x00\x01\x00\x00Standard Jet DB\x00"

	var buff [20]byte

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return
	}

	if _, err = io.ReadFull(file, buff[:]); err != nil {
		return
	}

	if bytes.Equal([]byte(header), buff[:]) {
		return nil
	}

	return ErrNotMDBJet3
}

// Table is a table in mdb file.
type Table struct {
	Name    string
	Sys     bool
	Columns []Column
}

// Column is a field in mdb table.
type Column struct {
	Num  int
	Name string
	Type string
}

func (column Column) String() string {
	return column.Name
}

// String returns pretty printed table structure.
func (table Table) String() string {
	var buff strings.Builder
	buff.WriteString(table.Name)
	if len(table.Columns) == 0 {
		return buff.String()
	}
	buff.WriteByte('(')
	buff.WriteString(table.Columns[0].Name)
	for _, col := range table.Columns[1:] {
		buff.WriteString(", ")
		buff.WriteString(col.Name)
	}
	buff.WriteByte(')')
	return buff.String()
}

// Tables returns list of Table in an mdb file.
func Tables(file io.ReadSeeker) (tables []Table, err error) {
	entries, err := readCatalog(file)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.kind != objectTypeTable {
			continue
		}
		var table Table
		table.Name = entry.name
		table.Sys = entry.flags&0x80000002 != 0

		tdefPage := entry.id & 0x00ffffff
		def, err := readTdef(file, table.Name, tdefPage)
		if err != nil {
			return nil, err
		}
		table.Columns = make([]Column, def.numCols)
		for i, col := range def.columns {
			table.Columns[i].Num = col.num
			table.Columns[i].Name = col.name
			table.Columns[i].Type = col.kind.String()
		}

		tables = append(tables, table)
	}
	return
}

// Rows create an iterator over rows for given table in mdb database.
func Rows(file io.ReadSeeker, table string) (*Iterator, error) {
	def, err := findTdef(file, table)
	if err != nil {
		return nil, err
	}
	iter := &iterator{file: file, def: def}
	return &Iterator{iter: iter}, nil
}

// Iterator provides method Next to fetch row data in table.
type Iterator struct {
	iter *iterator
}

// Next fetches a single row in associated table. An io.EOF error
// indicates that there is no more data.
//
// This method is not thread safe.
func (rows *Iterator) Next() ([]interface{}, error) {
	return rows.iter.next()
}
