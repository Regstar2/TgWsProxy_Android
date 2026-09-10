//go:build (linux || android) && !amd64 && !arm64

package main

import "net"

func tcpInfoFields(net.Conn) string {
	return "tcp_info=unavailable"
}
