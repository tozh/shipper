package message

import "time"

type Message interface {
	//SetTimestamp(t time.Time)
	Timestamp() time.Time
	//SetLevel(level int)
	Level() int
	//SetData(data []byte)
	Data() []byte
}


