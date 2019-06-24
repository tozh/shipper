package tail

import "time"

const DefaultLevelForLine = 3

type Line struct {
	ts    time.Time
	bytes []byte
	level int
}

func (l *Line) Timestamp() time.Time {
	return l.ts
}

func (l *Line) Level() int {
	return l.level
}

func (l *Line) Bytes() []byte {
	return l.bytes
}

func (l *Line) String() string {
	return string(l.bytes)
}

func NewLine(data []byte, level int) *Line {
	return &Line{
		ts:    time.Now(),
		bytes: data,
		level: level,
	}
}
