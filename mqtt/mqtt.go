package mqtt

import (
	"fmt"
	"net"
	"io"
	"log"
)

const(
    CONNECT = uint8(iota + 1)
    CONNACK
    PUBLISH
    PUBACK
    PUBREC
    PUBREL
    PUBCOMP
    SUBSCRIBE
    SUBACK
    UNSUBSCRIBE
    UNSUBACK
    PINGREQ
    PINGRESP
    DISCONNECT
)

const(
    ACCEPTED = uint8(iota)
    UNACCEPTABLE_PROTOCOL_VERSION
    IDENTIFIER_REJECTED
    SERVER_UNAVAILABLE
    BAD_USERNAME_OR_PASSWORD
    NOT_AUTHORIZED
)

type Mqtt struct{
    FixedHeader *FixedHeader
    ProtocolName, TopicName, ClientId, WillTopic, WillMessage, Username, Password string
    ProtocolVersion uint8
    ConnectFlags *ConnectFlags
    KeepAliveTimer, MessageId uint16
    Data []byte
    Topics []string
    Topics_qos []uint8
    ReturnCode uint8
}

type ConnectInfo struct {
    Protocol string // Must be 'MQIsdp' for now
    Version uint8
    UsernameFlag bool
    PasswordFlag bool
    WillRetain bool
    WillQos uint8
    WillFlag bool
    CleanSession bool
    Keepalive uint16
}

type FixedHeader struct {
    MessageType uint8
    DupFlag bool
	Retain bool
    QosLevel uint8
    Length uint32
}

type ConnectFlags struct{
    UsernameFlag, PasswordFlag, WillRetain, WillFlag, CleanSession bool
    WillQos uint8
}

func (mqtt *Mqtt)Show() {
	mqtt.FixedHeader.Show()
	mqtt.ConnectFlags.Show()

    fmt.Println("ProtocolName:", mqtt.ProtocolName)
	fmt.Println("Version:", mqtt.ProtocolVersion)
	fmt.Println("TopicName:", mqtt.TopicName)
	fmt.Println("ClientId:", mqtt.ClientId)
	fmt.Println("WillTopic:", mqtt.WillTopic)
	fmt.Println("WillMessage:", mqtt.WillMessage)
	fmt.Println("Username:", mqtt.Username)
	fmt.Println("Password:", mqtt.Password)
	fmt.Println("KeepAliveTimer:", mqtt.KeepAliveTimer)
	fmt.Println("MessageId:", mqtt.MessageId)

	fmt.Println("Data:", mqtt.Data)
	fmt.Println("Topics:", len(mqtt.Topics))
	for i := 0; i < len(mqtt.Topics); i++ {
		fmt.Printf("(%s) (qos=%d)\n", mqtt.Topics[i], mqtt.Topics_qos[i])
	}
	fmt.Println("ReturnCode:", mqtt.ReturnCode)
}

func (header *FixedHeader)Show() {
	fmt.Println("header detail:")
	fmt.Println("message type: ", MessageTypeStr(header.MessageType))
	fmt.Println("DupFlag: ", header.DupFlag)
	fmt.Println("Retain: ", header.Retain)
	fmt.Println("QOS: ", header.QosLevel)
	fmt.Println("length: ", header.Length)
	fmt.Println("\n=====================\n")
}

func (flags *ConnectFlags)Show() {
	fmt.Println("connect flags detail:")
    fmt.Println("UsernameFlag:", flags.UsernameFlag)
	fmt.Println("PasswordFlag:", flags.PasswordFlag)
	fmt.Println("WillRetain:", flags.WillRetain)
	fmt.Println("WillFlag:", flags.WillFlag)
	fmt.Println("WillQos:", flags.WillQos)
	fmt.Println("CleanSession:", flags.CleanSession)
	fmt.Println("\n=====================\n")
}

func ParseFixedHeader(conn *net.Conn) *FixedHeader {
	var buf = make([]byte, 2)
	n, _ := io.ReadFull(*conn, buf)
	if n != len(buf) {
		log.Println("read header failed")
		return nil
	}

    byte1 := buf[0]
    header := new(FixedHeader)
    header.MessageType = uint8(byte1 & 0xF0 >> 4)
    header.DupFlag = byte1 & 0x08 > 0
    header.QosLevel = uint8(byte1 & 0x06 >> 1)
    header.Retain = byte1 & 0x01 > 0

	byte2 := buf[1]
    header.Length = decodeVarLength(byte2, conn)
    return header
}

func ReadCompleteCommand(conn *net.Conn) (*FixedHeader, []byte) {
	fixed_header := ParseFixedHeader(conn)
	if fixed_header == nil {
		log.Panic("failed to read fixed header")
	}
	length := fixed_header.Length
	buf := make([]byte, length)
	n, _ := io.ReadFull(*conn, buf)
	if uint32(n) != length {
		log.Panicf("failed to read %d bytes specified in fixed header, only %d read", length, n)
	}
	log.Printf("Complete command(%s) read into buffer\n", MessageTypeStr(fixed_header.MessageType))

	return fixed_header, buf
}

// CONNECT parse
func parseConnectInfo(buf []byte) *ConnectInfo {
	var info = new(ConnectInfo)
	info.Protocol, buf = parseUTF8(buf)
	info.Version, buf = parseUint8(buf)
	flagByte := buf[0]
	log.Println("parsing connect flag:", flagByte)
	info.UsernameFlag = (flagByte & 0x80) > 0
	info.PasswordFlag = (flagByte & 0x40) > 0
	info.WillRetain = (flagByte & 0x20) > 0
	info.WillQos = uint8(flagByte & 0x18 >> 3)
	info.WillFlag = (flagByte & 0x04) > 0
	info.CleanSession = (flagByte & 0x02) > 0
	buf = buf[1:]
	info.Keepalive, _ = parseUint16(buf)
    return info
}

func (con_info *ConnectInfo)Show() {
	fmt.Println("connect info detail:")
	fmt.Println("Protocol:", con_info.Protocol)
	fmt.Println("Version:", con_info.Version)
	fmt.Println("Username Flag:", con_info.UsernameFlag)
	fmt.Println("Password Flag:", con_info.PasswordFlag)
	fmt.Println("WillRetain:", con_info.WillRetain)
	fmt.Println("WillQos:", con_info.WillQos)
	fmt.Println("WillFlag:", con_info.WillFlag)
	fmt.Println("CleanSession:", con_info.CleanSession)

	fmt.Println("Keepalive:", con_info.Keepalive)
}

// Fixed header parse



func decodeVarLength(cur byte, conn *net.Conn) uint32 {
	length := uint32(0)
	multi := uint32(1)

	for {
		length += multi * uint32(cur & 0x7f)
		if cur & 0x80 == 0 {
			break
		}
		buf := make([]byte, 1)
		n, _ := io.ReadFull(*conn, buf)
		if n != 1 {
			log.Panic("failed to read variable length in MQTT header")
		}
		cur = buf[0]
		multi *= 128
	}
	
	return length
}

func parseUint16(buf []byte) (uint16, []byte) {
	return uint16(buf[0] << 8) + uint16(buf[1]), buf[2:]
}

func parseUint8(buf []byte) (uint8, []byte) {
	return uint8(buf[0]), buf[1:]
}

func parseUTF8(buf []byte) (string, []byte) {
	length, buf := parseUint16(buf)
	str := buf [: length]
	return string(str), buf[length:]
}

func MessageTypeStr(mt uint8) string {
	var strArray = []string {
		"reserved",
		"CONNECT",
		"CONNACK",
		"PUBLISH",
		"PUBACK",
		"PUBREC",
		"PUBREL",
		"PUBCOMP",
		"SUBSCRIBE",
		"SUBACK",
		"UNSUBSCRIBE",
		"UNSUBACK",
		"PINGREQ",
		"PINGRESP",
		"DISCONNEC"}
	return strArray[mt]
}
