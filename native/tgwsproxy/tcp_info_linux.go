//go:build (linux || android) && (amd64 || arm64)

package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

// Linux extends tcp_info by appending fields. Read only the returned length,
// so old Android kernels report absent counters as -1 instead of invented zeroes.
// Both supported architectures here are little-endian.
func tcpInfoFields(conn net.Conn) string {
	tcpConn := underlyingTCPConn(conn)
	if tcpConn == nil {
		return "tcp_info=unavailable"
	}
	raw, err := tcpConn.SyscallConn()
	if err != nil {
		return "tcp_info=unavailable"
	}
	var value [256]byte
	length := uint32(len(value))
	var socketErr syscall.Errno
	controlErr := raw.Control(func(fd uintptr) {
		_, _, socketErr = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd,
			syscall.IPPROTO_TCP, syscall.TCP_INFO,
			uintptr(unsafe.Pointer(&value[0])), uintptr(unsafe.Pointer(&length)), 0)
	})
	if controlErr != nil || socketErr != 0 {
		return fmt.Sprintf("tcp_info=unavailable tcp_info_errno=%d", socketErr)
	}
	if length > uint32(len(value)) {
		length = uint32(len(value))
	}
	return formatTCPInfo(value[:length])
}

func formatTCPInfo(value []byte) string {
	u32 := func(offset int) int64 {
		if len(value) < offset+4 {
			return -1
		}
		return int64(binary.LittleEndian.Uint32(value[offset:]))
	}
	u64 := func(offset int) int64 {
		if len(value) < offset+8 {
			return -1
		}
		return int64(binary.LittleEndian.Uint64(value[offset:]))
	}
	return fmt.Sprintf("tcp_info_len=%d unacked=%d retrans=%d total_retrans=%d rto_us=%d rtt_us=%d cwnd=%d snd_mss=%d bytes_acked=%d bytes_sent=%d notsent=%d snd_wnd=%d",
		len(value), u32(24), u32(36), u32(100), u32(8), u32(68), u32(80), u32(16),
		u64(120), u64(200), u32(144), u32(228))
}
