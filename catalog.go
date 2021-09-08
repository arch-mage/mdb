package mdb

import (
	"fmt"
	"io"
	"time"
)

type entry struct {
	id       int
	parentID int
	name     string
	kind     objectType
	flags    int
	created  time.Time
	updated  time.Time
}

func readCatalog(file io.ReadSeeker) (entries []entry, err error) {
	def, err := readTdef(file, "MSysObjects", 2)
	if err != nil {
		return nil, err
	}
	iter := &iterator{file: file, def: def}
	for {
		fields, err := iter.next()
		if err == io.EOF {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}

		var ent entry
		var ok bool
		for i, col := range def.columns {
			switch col.name {
			case "Name":
				if ent.name, ok = fields[i].(string); !ok {
					return nil, fmt.Errorf("invalid mdb")
				}
			case "Id":
				if ent.id, ok = fields[i].(int); !ok {
					return nil, fmt.Errorf("invalid mdb")
				}
			case "ParentId":
				if ent.parentID, ok = fields[i].(int); !ok {
					return nil, fmt.Errorf("invalid mdb")
				}
			case "Type":
				if kind, ok := fields[i].(int); ok {
					ent.kind = objectType(kind & 0x7f)
				} else {
					return nil, fmt.Errorf("invalid mdb")
				}
			case "Flags":
				if ent.flags, ok = fields[i].(int); !ok {
					return nil, fmt.Errorf("invalid mdb")
				}
			case "DateCreate":
				if ent.created, ok = fields[i].(time.Time); !ok {
					return nil, fmt.Errorf("invalid mdb")
				}
			case "DateUpdate":
				if ent.updated, ok = fields[i].(time.Time); !ok {
					return nil, fmt.Errorf("invalid mdb")
				}
			}
		}
		entries = append(entries, ent)
	}
}
