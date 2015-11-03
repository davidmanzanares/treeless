package main

import (
	"flag"
	"fmt"
	"treeless/com"
	"treeless/server"
)

func main() {
	defaultConf := flag.Bool("defaultConf", false, "Write a new dafault DB configuration in default.conf")
	newDBName := flag.String("createDB", "", "Create a new DB")
	asocDB := flag.String("asocDB", "", "Associate to an existing DB")
	monitor := flag.String("monitor", "", "Monitor an existing DB")
	flag.Parse()
	if *defaultConf {
		fmt.Println(string(tlcom.DefaultAccessConf.Marshal()))
	} else if *monitor != "" {
		tlcom.CreateAccess(&tlcom.AccessConf{8, []string{"192.168.55.10:9876", "192.168.55.11:9876"}})
	} else if *asocDB != "" {
		tlserver.Start(*newDBName != "")
		select {}
	} else {
		fmt.Println("No operations requested.")
	}
}
