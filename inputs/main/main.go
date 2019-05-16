package main

import (
	"github.com/labstack/gommon/log"
	"github.com/zhaotong0312/shipper/inputs/tail"
)

func main() {
	w := tail.NewWatcher()
	err := w.Add("/Users/tzhao/ship-test/foo2.txt")
	if err != nil {
		log.Panic(err)
	}
	w.Run()
}
