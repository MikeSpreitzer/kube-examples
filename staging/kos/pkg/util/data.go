package util

import (
	"encoding/binary"
	"net"
)

func MapStringStringGet(m map[string]string, i string) (mi string) {
	if m != nil {
		mi = m[i]
	}
	return
}

// makeUint32From4Bytes makes a uint32 from a byte array
func MakeUint32FromIPv4(ip []byte) uint32 {
	var iv uint32
	if len(ip) > 4 {
		iv = binary.BigEndian.Uint32(ip[len(ip)-4:])
	} else {
		iv = binary.BigEndian.Uint32(ip)
	}
	return iv
}

func MakeIPv4FromUint32(n uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, n)
	return ip
}

