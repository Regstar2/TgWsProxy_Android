//go:build !linux && !android

package main

import "net"

func tcpSendQueueBytes(net.Conn) int {
	return -1
}

func tcpNotSentBytes(net.Conn) int {
	return -1
}

func tcpSendBufferBytes(net.Conn) int {
	return -1
}

func tcpTransportState(net.Conn) string {
	return "tcp_info=unavailable"
}
