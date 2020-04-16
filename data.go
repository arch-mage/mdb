package mdb

import (
	"encoding/binary"
	"fmt"
	"io"
)

type iterator struct {
	err  error
	row  int
	file io.ReadSeeker
	page int
	def  *tdef
	buff []byte
}

func (it *iterator) next() (fields []interface{}, err error) {
	if it.err != nil {
		return nil, it.err
	}
	if it.def.numRows == 0 {
		it.err = io.EOF
		return nil, it.err
	}
	if it.page == 0 {
		if err = it.loadPage(); err != nil {
			return nil, err
		}
	}
	rows := int(binary.LittleEndian.Uint16(it.buff[8:]))
	for {
		if it.row >= rows {
			if err := it.loadPage(); err != nil {
				return nil, err
			}
			it.row = 0
			rows = int(binary.LittleEndian.Uint16(it.buff[8:]))
		}

		buff := findRow(it.buff, it.row)
		it.row++
		if buff == nil {
			continue
		}
		if fields, it.err = decodeRow(it.file, it.def, buff); it.err != nil {
			return nil, it.err
		}
		return fields, nil
	}
}

func (it *iterator) loadPage() (err error) {
	it.buff, it.page, it.err = nextDataPage(it.file, it.def.usedPageMap, it.page)
	if it.err != nil {
		return it.err
	}
	if it.page == 0 {
		it.err = io.EOF
		return it.err
	}
	return nil
}

func nextDataPage(file io.ReadSeeker, bitmap []byte, prev int) (buff []byte, next int, err error) {
	if next, err = nextDataPageNum(file, bitmap, prev); err != nil {
		return
	}
	if buff, err = readPage(file, next); err != nil {
		return
	}
	return
}

func nextDataPageNum(file io.ReadSeeker, bitmap []byte, prev int) (int, error) {
	if bitmap[0] == 0 {
		pageNum := int(binary.LittleEndian.Uint32(bitmap[1:5]))
		bitidx := 0
		if prev >= pageNum {
			bitidx = prev - pageNum + 1
		}
		size := (len(bitmap) - 5) * 8
		bmap := bitmap[5:]
		for i := bitidx; i < size; i++ {
			if bmap[i/8]&(1<<(i%8)) != 0 {
				return pageNum + i, nil
			}
		}
		return 0, nil
	}
	if bitmap[0] == 1 {
		bitlen := (pageSize - 4) * 8
		maxmap := (len(bitmap) - 1) / 4
		bitidx := (prev + 1) / bitlen
		offset := (prev + 1) % bitlen
		for idx := bitidx; idx < maxmap; idx++ {
			page := int(binary.LittleEndian.Uint32(bitmap[idx*4+1:]))
			if page == 0 {
				continue
			}
			buff, err := readPage(file, page)
			if err != nil {
				return 0, err
			}
			if buff[0] != 5 {
				return 0, fmt.Errorf("invalid page usage map")
			}
			bmap := buff[4:]
			for i := offset; i < bitlen; i++ {
				if bmap[i/8]&(1<<(i%8)) != 0 {
					return idx*bitlen + i, nil
				}
			}
			offset = 0
		}
		return 0, nil
	}
	return 0, fmt.Errorf("unknown page map type %x", bitmap[0])
}

func decodeRow(file io.ReadSeeker, def *tdef, buff []byte) (fields []interface{}, err error) {
	if def.numCols != int(buff[0]) {
		return nil, fmt.Errorf("num cols mismatch")
	}

	bitmaskSize := (def.numCols + 7) / 8
	varlenOffsets := []int{}

	var numVarCols int
	if def.numVarCols > 0 {
		numVarCols = int(buff[len(buff)-bitmaskSize-1])
		numJumps := (len(buff) - 1) / 256
		colPtr := len(buff) - bitmaskSize - numJumps - 2
		if (colPtr-numVarCols)/256 < numJumps {
			numJumps--
		}

		jumpsUsed := 0
		for i := 0; i <= numVarCols; i++ {
			if jumpsUsed < numJumps && i == int(buff[len(buff)-bitmaskSize-jumpsUsed-2]) {
				jumpsUsed++
			}
			varlenOffsets = append(varlenOffsets, int(buff[colPtr-i])+jumpsUsed*256)
		}
	}

	nullMask := buff[(len(buff) - bitmaskSize):]
	numFixedColsFound := 0
	numFixedCols := def.numCols - numVarCols
	fields = make([]interface{}, def.numCols)
	for i, col := range def.columns {
		byteNum := col.num / 8
		bitNum := col.num % 8
		isNull := !(nullMask[byteNum]&(1<<bitNum) != 0)
		if col.kind == typeBool { // bool
			if isNull {
				fields[i] = true
			} else {
				fields[i] = false
			}
		} else if isNull {
			// nil
		} else if col.isFixed && numFixedColsFound < numFixedCols {
			start := col.fixedOff + 1
			end := start + col.size
			if fields[i], err = decodeValue(file, col.kind, buff[start:end]); err != nil {
				return nil, err
			}
			numFixedColsFound++
		} else if !col.isFixed && col.varlenOff < numVarCols {
			start := varlenOffsets[col.varlenOff]
			end := varlenOffsets[col.varlenOff+1]
			if fields[i], err = decodeValue(file, col.kind, buff[start:end]); err != nil {
				return nil, err
			}
		} else {
			// nil
		}
	}

	return fields, nil
}

// readPage read a page from mdb file. the page is alwaysh 2048 bytes
// length.
func readPage(file io.ReadSeeker, num int) (buff []byte, err error) {
	if _, err = file.Seek(pageSize*int64(num), io.SeekStart); err != nil {
		return nil, err
	}
	buff = make([]byte, pageSize)
	if _, err = io.ReadFull(file, buff); err != nil {
		return nil, err
	}
	return buff, nil
}

// findRowWithPtr search for row data in file for given
// pointer. Pointer is a 32bit (4 bytes) number. Three most
// significant bit's is page number and the single remaining byte is
// row number.
func findRowWithPtr(file io.ReadSeeker, ptr int) (buff []byte, err error) {
	// 00000000 00000000 00000000 00000000
	// -------------------------- --------
	//            page              row
	return findRowInPage(file, ptr>>8, ptr&0xff)
}

// findRowInPage search for row data in file for given page and row.
func findRowInPage(file io.ReadSeeker, page, row int) (buff []byte, err error) {
	if buff, err = readPage(file, page); err != nil {
		return
	}
	buff = findRow(buff, row)
	return
}

// findRow get row data in buff for given row.
func findRow(buff []byte, row int) []byte {
	if start, _, end, deleted := findRowOffset(buff, row); !deleted {
		return buff[start:end]
	}
	return nil
}

// findRowOffset get offset information for given row in buff. buff
// should be a single mdb page.
func findRowOffset(buff []byte, row int) (start, size, end int, deleted bool) {
	start = int(binary.LittleEndian.Uint16(buff[10+row*2:]))
	end = pageSize
	if row != 0 {
		end = int(binary.LittleEndian.Uint16(buff[8+row*2:]) & 0x1fff)
	}
	deleted = start&0x4000 != 0
	start = start & 0x1fff
	size = end - start
	return start, size, end, deleted
}
