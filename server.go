package main

import "treeless/server"

func main() {
	tlserver.Start()
	select {}
}
