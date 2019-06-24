package message

import "time"

type Message interface {
	//SetTimestamp(t time.Time)
	//SetLevel(level int)
	//SetBytes(bytes []byte)

	Timestamp() time.Time
	Level() int
	Bytes() []byte
	String() string
}


