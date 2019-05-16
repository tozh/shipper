package tail

import "time"

const DefaultLevelForLine = 3

type Line struct {
	ts    time.Time
	data  []byte
	level int
}

func (l *Line) Timestamp() time.Time {
	return l.ts
}

func (l *Line) Level() int {
	return l.level
}

func (l *Line) Data() []byte {
	return l.data
}

func NewLine(data []byte, level int) *Line {
	return &Line{
		ts:    time.Now(),
		data:  data,
		level: level,
	}
}
