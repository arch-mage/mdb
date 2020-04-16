package mdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

func decodeValue(file io.ReadSeeker, kind dataType, buff []byte) (val interface{}, err error) {
	switch kind {
	case typeBool:
		return buff[0] != 0, nil
	case typeByte:
		return buff[0], nil
	case typeInt:
		return int(binary.LittleEndian.Uint16(buff[:2])), nil
	case typeLongInt:
		return int(binary.LittleEndian.Uint32(buff[:4])), nil
	case typeMoney:
		return nil, fmt.Errorf("data type %s is not implemented", kind)
	case typeFloat:
		return nil, fmt.Errorf("data type %s is not implemented", kind)
	case typeDouble:
		return nil, fmt.Errorf("data type %s is not implemented", kind)
	case typeDateTime:
		return decodeDateTime(buff), nil
	case typeBinary:
		return buff, nil
	case typeText:
		return string(buff), nil
	case typeLongBinary:
		return decodeLong(file, buff)
	case typeLongText:
		buff, err = decodeLong(file, buff)
		return string(buff), err
	case typeUNKNOWN_0D:
		return nil, fmt.Errorf("data type %s is not implemented", kind)
	case typeUNKNOWN_0E:
		return nil, fmt.Errorf("data type %s is not implemented", kind)
	case typeGUID:
		return nil, fmt.Errorf("data type %s is not implemented", kind)
	case typeNumeric:
		return nil, fmt.Errorf("data type %s is not implemented", kind)
	default:
		return nil, fmt.Errorf("invalid data type: %s", kind)
	}
}

func decodeLong(file io.ReadSeeker, buff []byte) ([]byte, error) {
	head := int(binary.LittleEndian.Uint32(buff[:4]))
	size := head & 0x00ffffff
	switch buff[3] {
	case 0x80: // inline
		return buff[12 : 12+size], nil
	case 0x40: // single page
		return findRowWithPtr(file, int(binary.LittleEndian.Uint32(buff[4:8])))
	case 0x00:
		// below
	default:
		return nil, fmt.Errorf("invalid long data header 0x%02x", buff[3])
	}

	var out bytes.Buffer

	ptr := int(binary.LittleEndian.Uint32(buff[4:8]))
	for ptr != 0 {
		data, err := findRowWithPtr(file, ptr)
		if err != nil {
			return nil, err
		}
		if out.Len()+len(data)-4 > head {
			break
		}
		if size == 0 {
			break
		}
		out.Write(data[4:])
		ptr = int(binary.LittleEndian.Uint32(data[:4]))
	}

	if out.Len() != head {
		return nil, fmt.Errorf("incorrect data length")
	}
	return out.Bytes(), nil
}

func decodeDateTime(buff []byte) time.Time {
	d := math.Float64frombits(binary.LittleEndian.Uint64(buff))
	return time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC).Add(time.Duration(d)*time.Hour*24 + time.Second*time.Duration(math.Abs(d-float64(int(d)))*86400+0.5))
}
