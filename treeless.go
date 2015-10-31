package main

import (
	"flag"
	"fmt"
	"treeless/com"
	"treeless/server"
)

func main() {
	defaultConf := flag.Bool("defaultConf", false, "Write a new dafault DB configuration in default.conf")
	flag.Parse()
	if *defaultConf {
		fmt.Println(string(tlcom.DefaultAccessConf.Marshal()))
	} else {
		tlserver.Start()
		select {}
	}
}
