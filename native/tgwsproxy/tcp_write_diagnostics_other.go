//go:build !linux && !android

package main

import "net"

func tcpSendQueueBytes(net.Conn) int {
	return -1
}

func tcpSendBufferBytes(net.Conn) int {
	return -1
}
