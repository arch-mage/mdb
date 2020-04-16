package mdb

import "fmt"

const pageSize = 2048

type dataType byte

const (
	typeBool       dataType = 0x01 // Boolean         ( 1 bit )
	typeByte       dataType = 0x02 // Byte            ( 8 bits)
	typeInt        dataType = 0x03 // Integer         (16 bits)
	typeLongInt    dataType = 0x04 // Long Integer    (32 bits)
	typeMoney      dataType = 0x05 // Currency        (64 bits)
	typeFloat      dataType = 0x06 // Single          (32 bits)
	typeDouble     dataType = 0x07 // Double          (64 bits)
	typeDateTime   dataType = 0x08 // Date/Time       (64 bits)
	typeBinary     dataType = 0x09 // Binary        (255 bytes)
	typeText       dataType = 0x0A // Text          (255 bytes)
	typeLongBinary dataType = 0x0B // OLE = Long binary
	typeLongText   dataType = 0x0C // Memo = Long text
	typeUNKNOWN_0D dataType = 0x0D
	typeUNKNOWN_0E dataType = 0x0E
	typeGUID       dataType = 0x0F // GUID
	typeNumeric    dataType = 0x10 // Scaled decimal  (17 bytes)
)

var dataTypeNames = map[dataType]string{
	typeBool:       "Bool",
	typeByte:       "Byte",
	typeInt:        "Int",
	typeLongInt:    "LongInt",
	typeMoney:      "Money",
	typeFloat:      "Float",
	typeDouble:     "Double",
	typeDateTime:   "DateTime",
	typeBinary:     "Binary",
	typeText:       "Text",
	typeLongBinary: "LongBinary",
	typeLongText:   "LongText",
	typeUNKNOWN_0D: "UNKNOWN_0D",
	typeUNKNOWN_0E: "UNKNOWN_0E",
	typeGUID:       "GUID",
	typeNumeric:    "Numeric",
}

func (b dataType) String() string {
	if name, ok := dataTypeNames[b]; ok {
		return name
	}
	return fmt.Sprintf("0x%02x", byte(b))
}

type objectType int

const (
	objectTypeForm objectType = iota
	objectTypeTable
	objectTypeMacro
	objectTypeSystemTable
	objectTypeReport
	objectTypeQuery
	objectTypeLinkedTable
	objectTypeModule
	objectTypeRelationship
	objectTypeUnknown09
	objectTypeUnknown0A
	objectTypeDatabaseProperty
)

var objectTypeNames = map[objectType]string{
	objectTypeForm:             "Form",
	objectTypeTable:            "Table",
	objectTypeMacro:            "Macro",
	objectTypeSystemTable:      "SystemTable",
	objectTypeReport:           "Report",
	objectTypeQuery:            "Query",
	objectTypeLinkedTable:      "LinkedTable",
	objectTypeModule:           "Module",
	objectTypeRelationship:     "Relationship",
	objectTypeDatabaseProperty: "DatabaseProperty",
}

func (n objectType) String() string {
	if name, ok := objectTypeNames[n]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(0x%02x)", int(n))
}
