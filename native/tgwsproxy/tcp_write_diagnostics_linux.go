//go:build linux || android

package main

import (
	"net"
	"syscall"
	"unsafe"
)

const (
	linuxTIOCOUTQ    = 0x5411
	linuxSIOCOUTQNSD = 0x894B
)

func tcpSendQueueBytes(conn net.Conn) int {
	return tcpIoctlInt(conn, linuxTIOCOUTQ)
}

// tcpNotSentBytes returns only bytes which have not yet been sent by TCP.
// Unlike TIOCOUTQ/SIOCOUTQ, SIOCOUTQNSD excludes bytes already sent but not
// acknowledged. That distinction matters for #29: using the total queue as a
// drain signal can stall on healthy in-flight data.
func tcpNotSentBytes(conn net.Conn) int {
	return tcpIoctlInt(conn, linuxSIOCOUTQNSD)
}

func tcpIoctlInt(conn net.Conn, request uintptr) int {
	tcpConn := underlyingTCPConn(conn)
	if tcpConn == nil {
		return -1
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return -1
	}
	value := int32(-1)
	controlErr := rawConn.Control(func(fd uintptr) {
		_, _, errno := syscall.Syscall(
			syscall.SYS_IOCTL,
			fd,
			request,
			uintptr(unsafe.Pointer(&value)),
		)
		if errno != 0 {
			value = -1
		}
	})
	if controlErr != nil || value < 0 {
		return -1
	}
	return int(value)
}

func tcpSendBufferBytes(conn net.Conn) int {
	tcpConn := underlyingTCPConn(conn)
	if tcpConn == nil {
		return -1
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return -1
	}
	value := -1
	controlErr := rawConn.Control(func(fd uintptr) {
		bufferBytes, socketErr := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF)
		if socketErr == nil {
			value = bufferBytes
		}
	})
	if controlErr != nil {
		return -1
	}
	return value
}
