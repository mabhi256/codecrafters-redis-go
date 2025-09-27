package main

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
)

//go:embed empty.rdb
var emptyRDB []byte

func (server *RedisServer) propagateEmptyRDB(conn net.Conn) {
	if server.role == "master" {
		rdb := fmt.Sprintf("$%d\r\n%s", len(emptyRDB), emptyRDB)
		_, err := conn.Write([]byte(rdb))
		if err != nil {
			fmt.Println("Error sending response:", err.Error())
			os.Exit(1)
		}
	}
}

type RdbParser struct {
	reader *bufio.Reader
	pos    int
}

func NewRdbParser(data []byte) *RdbParser {
	return &RdbParser{
		reader: bufio.NewReader(bytes.NewReader(data)),
		pos:    0,
	}
}

func (rp *RdbParser) readByte() (byte, error) {
	b, err := rp.reader.ReadByte()
	if err == nil {
		rp.pos++
	}

	return b, err
}

func (rp *RdbParser) readNBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(rp.reader, buf)
	if err == nil {
		rp.pos += n
	}

	return buf, err
}

// Length is big-endian
func (rp *RdbParser) readLength() (uint64, bool, error) {
	b, err := rp.readByte()
	if err != nil {
		return 0, false, err
	}

	// 0xC0 = 0b1100_0000
	switch (b & 0xC0) >> 6 { // Check 2 most significant bits
	case 0:
		// next 6 bits represent the length
		return uint64(b & 0x3F), false, nil // 0b0011_1111

	case 1:
		// Read one additional byte. The combined 14 bits represent the length
		b_next, err := rp.readByte()
		if err != nil {
			return 0, false, err
		}
		return uint64(b&0x3F)<<8 | uint64(b_next), false, nil

	case 2:
		// Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
		next, err := rp.readNBytes(4)
		if err != nil {
			return 0, false, err
		}
		// uint64(next[0])<<24 | uint64(next[1])<<16 | uint64(next[2])<<8 | uint64(next[3])
		return uint64(binary.BigEndian.Uint32(next)), false, nil

	case 3:
		// Special encoding: The remaining 6 bits indicate the type of int in the following bytes
		return uint64(b & 0x3F), true, nil

	default:
		return 0, false, fmt.Errorf("invalid length encoding")
	}
}

func (rp *RdbParser) readString() (string, error) {
	n, isSpecial, err := rp.readLength()
	if err != nil {
		return "", err
	}

	if isSpecial {
		var value int64 // int value is little-endian
		switch n {
		case 0:
			bits, err := rp.readByte()
			if err != nil {
				return "", err
			}
			value = int64(int8(bits))

		case 1:
			bytes, err := rp.readNBytes(2)
			if err != nil {
				return "", err
			}
			// value = int64(int16(bytes[0]) | int16(bytes[1])<<8)
			value = int64(binary.LittleEndian.Uint16(bytes))

		case 2:
			bytes, err := rp.readNBytes(4)
			if err != nil {
				return "", err
			}
			// value = int64(bytes[0]) | int64(bytes[1])<<8 | int64(bytes[2])<<16 | int64(bytes[3])<<24
			value = int64(binary.LittleEndian.Uint32(bytes))
		}

		return fmt.Sprintf("%d", value), nil
	}

	bytes, err := rp.readNBytes(int(n))
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (rp *RdbParser) Parse() (map[string]string, map[string]int64, error) {
	cache := make(map[string]string)
	expiryMap := make(map[string]int64)
	auxiliary := make(map[string]string)

	// magic word "REDIS"
	magic, err := rp.readNBytes(5)
	if err != nil {
		return nil, nil, err
	}
	if string(magic) != "REDIS" {
		return nil, nil, fmt.Errorf("invalid redis rdb file")
	}

	// RDB version 4 bytes
	_, err = rp.readNBytes(4)
	if err != nil {
		return nil, nil, err
	}
	// fmt.Printf("RDB version: %s\n", string(version))

	expiry := int64(-1)

parseLoop:
	for {
		b, err := rp.readByte() // b is either opcode or value type
		if err != nil {
			return nil, nil, err
		}

		switch b {
		case 0xFA: // AUX, Auxiliary fields
			key, err := rp.readString()
			if err != nil {
				return nil, nil, err
			}
			value, err := rp.readString()
			if err != nil {
				return nil, nil, err
			}
			auxiliary[key] = value

		case 0xFE: // SELECTDB, database selector (id)
			_, _, err := rp.readLength()
			if err != nil {
				return nil, nil, err
			}
			// fmt.Printf("Database#%d\n\n", numDb)

		case 0xFB: // RESIZEDB, Hash table sizes for the main keyspace and expires
			_, _, err := rp.readLength()
			if err != nil {
				return nil, nil, err
			}
			// fmt.Printf("Hash table size: %d\n", hashTableSize)

			_, _, err = rp.readLength()
			if err != nil {
				return nil, nil, err
			}
			// fmt.Printf("Expire table size: %d\n\n", expireTableSize)

		case 0xFD: // EXPIRETIME, Expire time in seconds
			expBytes, err := rp.readNBytes(4)
			if err != nil {
				return nil, nil, err
			}
			expiry = int64(binary.LittleEndian.Uint32(expBytes) * 1000) // convert to ms

		case 0xFC: // EXPIRETIME, Expire time in ms
			expBytes, err := rp.readNBytes(8)
			if err != nil {
				return nil, nil, err
			}
			expiry = int64(binary.LittleEndian.Uint64(expBytes))

		case 0xFF: // EOF, check 8-byte checksum
			// we will check CRC later
			break parseLoop

		default:
			// b is value type
			key, err := rp.readString()
			if err != nil {
				return nil, nil, err
			}

			switch b {
			case 0:
				// String encoding
				value, err := rp.readString()
				if err != nil {
					return nil, nil, err
				}

				cache[key] = value
				if expiry > 0 {
					expiryMap[key] = expiry
					expiry = -1
				}

			default:
				return nil, nil, fmt.Errorf("unsupported value type: %d", b)
			}
		}
	}

	// fmt.Println("Auxiliary fields:")
	// for key, value := range auxiliary {
	// 	fmt.Println(key, "->", value)
	// }
	// fmt.Println()

	return cache, expiryMap, nil
}

func verifyCRC64(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("invalid rdb file, too short")
	}

	storedCRCbytes := data[len(data)-8:]
	storedCRC := binary.LittleEndian.Uint64(storedCRCbytes)

	computedCRC := Digest(data[:len(data)-8])

	if storedCRC != computedCRC {
		return fmt.Errorf("invalid rdb file, CRC mismatch: stored=0x%x, computed=0x%x", storedCRC, computedCRC)
	}

	return nil
}

func (server *RedisServer) ParseRdb() (map[string]string, map[string]int64, error) {
	data, err := os.ReadFile(filepath.Join(server.dir, server.dbFilename))
	if err != nil {
		return nil, nil, err
	}

	if err := verifyCRC64(data); err != nil {
		return nil, nil, err
	}

	// No need to have a streaming parser by reading os.Open(filepath),
	// since all our data will load in the ram anyway
	parser := NewRdbParser(data)
	cache, expiry, err := parser.Parse()
	if err != nil {
		return nil, nil, err
	}

	return cache, expiry, nil
}
