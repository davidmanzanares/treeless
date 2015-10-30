package main

import (
	"flag"
	"treeless/com"
	"treeless/server"
)

func main() {
	defaultConf := flag.Bool("defaultConf", false, "Write a new dafault DB configuration in default.conf")
	flag.Parse()
	if *defaultConf {
		tlcom.DefaultAccessConf.WriteToFile("default.conf")
	} else {
		tlserver.Start()
		select {}
	}
}
