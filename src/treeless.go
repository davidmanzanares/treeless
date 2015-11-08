package main

import (
	"flag"
	"fmt"
	"treeless/src/com"
)

func main() {
	create := flag.Bool("create", false, "Create a new DB server group")
	redundancy := flag.Int("redundancy", 2, "Redundancy of the new DB server group")
	assoc := flag.String("assoc", "", "Associate to an existing DB server group")
	port := flag.String("port", "9876", "Use this port as the localhost server port")
	monitor := flag.String("monitor", "", "Monitor an existing DB")
	flag.Parse()
	if *monitor != "" {
		_, err := tlcom.ConnectAsClient(*monitor)
		if err != nil {
			fmt.Println("Access couldn't be established")
			fmt.Println(err)
		} else {
			select {}
		}
	} else if *assoc != "" {
		tlcom.Start(false, *assoc, *port, -1)
		select {}
	} else if *create {
		tlcom.Start(true, "", *port, *redundancy)
		select {}
	} else {
		fmt.Println("No operations requested.")
		flag.Usage()
	}
}
