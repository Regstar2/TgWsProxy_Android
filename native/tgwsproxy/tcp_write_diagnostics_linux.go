//go:build linux || android

package main

import (
	"net"
	"syscall"
	"unsafe"
)

const linuxTIOCOUTQ = 0x5411

func tcpSendQueueBytes(conn net.Conn) int {
	tcpConn := underlyingTCPConn(conn)
	if tcpConn == nil {
		return -1
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return -1
	}
	pending := int32(-1)
	controlErr := rawConn.Control(func(fd uintptr) {
		_, _, errno := syscall.Syscall(
			syscall.SYS_IOCTL,
			fd,
			uintptr(linuxTIOCOUTQ),
			uintptr(unsafe.Pointer(&pending)),
		)
		if errno != 0 {
			pending = -1
		}
	})
	if controlErr != nil || pending < 0 {
		return -1
	}
	return int(pending)
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
