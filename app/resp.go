package main

import (
	"fmt"
	"net"
	"os"
)

func receive(conn net.Conn) string {
	msg := ""

	CR := false
	LF := false
	for !CR || !LF {
		bytes := make([]byte, 1)
		_, err := conn.Read(bytes)
		if err != nil {
			fmt.Println("Error reading bytes from connection: ", err.Error())
			os.Exit(1)
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

	return msg
}
