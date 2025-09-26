package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

func (server *RedisServer) handshake() (net.Conn, error) {
	conn, err := net.Dial("tcp", server.master)
	if err != nil {
		return nil, err
	}

	ping := encodeStringArray([]string{"PING"})
	_, err = conn.Write([]byte(ping))
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	receiveLine(reader) // receive PONG

	replconf := encodeStringArray([]string{"REPLCONF", "listening-port", server.port})
	_, err = conn.Write([]byte(replconf))
	if err != nil {
		return nil, err
	}
	receiveLine(reader) // receive OK

	replconf2 := encodeStringArray([]string{"REPLCONF", "capa", "psync2"})
	_, err = conn.Write([]byte(replconf2))
	if err != nil {
		return nil, err
	}
	receiveLine(reader) // receive OK

	psync := encodeStringArray([]string{"PSYNC", server.replID, fmt.Sprintf("%d", server.replOffset)})
	_, err = conn.Write([]byte(psync))
	if err != nil {
		return nil, err
	}
	syncStr, err := receiveLine(reader) // receive +FULLRESYNC <REPL_ID> 0\r\n
	if err != nil {
		return nil, err
	}
	syncStr = strings.TrimRight(syncStr, "\r\n")
	syncParts := strings.SplitN(syncStr, " ", 3)
	if strings.HasSuffix(syncParts[0], "FULLRESYNC") {
		server.replID = syncParts[1]
		server.replOffset, err = strconv.Atoi(syncParts[2])
		if err != nil {
			return nil, err
		}
	}

	// receive RDB
	rdbHeader, err := receiveLine(reader) // Read $<length>\r\n
	if err != nil {
		return nil, err
	}
	rdbHeader = strings.TrimRight(rdbHeader, "\r\n")
	rdbLengthStr := rdbHeader[1:]
	rdbLength, err := strconv.Atoi(rdbLengthStr)
	if err != nil {
		return nil, err
	}

	rdbData := make([]byte, rdbLength)
	_, err = io.ReadFull(reader, rdbData)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
