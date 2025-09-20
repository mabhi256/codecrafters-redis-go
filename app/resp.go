package main

import (
	"net"
)

func receive(conn net.Conn) (string, error) {
	msg := ""

	CR := false
	LF := false
	for !CR || !LF {
		bytes := make([]byte, 1)
		_, err := conn.Read(bytes)
		if err != nil {
			return "", err
		}

		recv := string(bytes)
		switch recv {
		case "\r":
			CR = true
		case "\n":
			LF = true
		default:
			msg += recv
		}
	}

	return msg, nil
}
