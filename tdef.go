package mdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

type tdef struct {
	name        string
	numRows     int
	kind        byte
	maxCols     int
	numVarCols  int
	numCols     int
	numIdx      int
	numRealIdx  int
	usedPageMap []byte
	freePageMap []byte
	columns     []column
}

type column struct {
	name       string
	kind       dataType
	num        int
	size       int
	varlenOff  int
	fixedOff   int
	isFixed    bool
	isAutoLong bool
	isAutoUUID bool
}

func readTdef(file io.ReadSeeker, name string, page int) (def *tdef, err error) {
	buff, err := readPage(file, page)
	if err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(buff[0:4]) != 0x43560102 {
		return nil, fmt.Errorf("invalid mdb signature")
	}
	if binary.LittleEndian.Uint32(buff[4:8]) != 0 {
		return nil, fmt.Errorf("multi page table definiton is not implemented")
	}
	buff = buff[8:]

	def = new(tdef)
	def.name = name
	def.numRows = int(binary.LittleEndian.Uint32(buff[4:8]))
	def.kind = buff[12]
	def.maxCols = int(binary.LittleEndian.Uint16(buff[13:15]))
	def.numVarCols = int(binary.LittleEndian.Uint16(buff[15:17]))
	def.numCols = int(binary.LittleEndian.Uint16(buff[17:19]))
	def.numIdx = int(binary.LittleEndian.Uint16(buff[19:23]))
	def.numRealIdx = int(binary.LittleEndian.Uint16(buff[23:27]))

	usedPagePtr := int(binary.LittleEndian.Uint32(buff[27:31]))
	freePagePtr := int(binary.LittleEndian.Uint32(buff[31:35]))

	if def.usedPageMap, err = findRowWithPtr(file, usedPagePtr); err != nil {
		return nil, err
	}
	if def.freePageMap, err = findRowWithPtr(file, freePagePtr); err != nil {
		return nil, err
	}
	buff = buff[35:]
	buff = buff[def.numRealIdx*8:]

	def.columns = make([]column, def.numCols)
	for i := 0; i < def.numCols; i++ {
		def.columns[i].kind = dataType(buff[0])
		def.columns[i].num = int(binary.LittleEndian.Uint16(buff[1:3]))
		def.columns[i].size = int(binary.LittleEndian.Uint16(buff[16:18]))

		def.columns[i].varlenOff = int(binary.LittleEndian.Uint16(buff[3:5]))
		def.columns[i].fixedOff = int(binary.LittleEndian.Uint16(buff[14:16]))

		flags := binary.LittleEndian.Uint16(buff[13:])
		def.columns[i].isFixed = flags&0x01 != 0
		def.columns[i].isAutoLong = flags&0x04 != 0
		def.columns[i].isAutoUUID = flags&0x40 != 0
		buff = buff[18:]
	}

	for i := 0; i < def.numCols; i++ {
		def.columns[i].name = string(buff[1 : int(buff[0])+1])
		buff = buff[int(buff[0])+1:]
	}

	sort.Slice(def.columns, func(a, b int) bool {
		return def.columns[a].num < def.columns[b].num
	})
	return def, nil
}

func findTdef(file io.ReadSeeker, name string) (def *tdef, err error) {
	entries, err := readCatalog(file)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.kind != objectTypeTable || entry.name != name {
			continue
		}
		return readTdef(file, entry.name, entry.id&0x00ffffff)
	}
	return nil, fmt.Errorf("table %q does not exists", name)
}
