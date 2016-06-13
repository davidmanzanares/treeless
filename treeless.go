package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"
	"treeless/com"
	"treeless/dist/heartbeat"
	"treeless/dist/servergroup"
	"treeless/server"
)
import (
	_ "net/http/pprof"
	"runtime/debug"
)

const DefaultDBSize = 1024 * 1024 * 128
const DefaultPort = 9876
const DefaultRedundancy = 2
const DefaultNumChunk = 8

func main() {
	//Recover: log and quit
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			log.Println("STACK TRACE:\n", string(debug.Stack()))
			panic(r)
		}
	}()
	log.Println("Treeless args:", os.Args)

	rand.Seed(time.Now().UnixNano())

	//Main parameters
	create := flag.Bool("create", false, "Create a new DB server group")
	assoc := flag.String("assoc", "", "Associate a new node to an existing DB server group")
	monitor := flag.String("monitor", "", "Monitor an existing DB")
	//Additional parameters
	port := flag.Int("port", DefaultPort, "Port to use by the new DB server node")
	open := flag.Bool("open", false, "Open an existing DB folder instead of creating a new one, use with -dbpath")
	redundancy := flag.Int("redundancy", DefaultRedundancy, "Redundancy of the new DB server group")
	chunks := flag.Int("chunks", DefaultNumChunk, "Number of chunks of the new DB server group")
	procs := flag.Int("procs", runtime.NumCPU(), "GOMAXPROCS")
	size := flag.Int64("size", DefaultDBSize, "DB chunk size in bytes")
	dbpath := flag.String("dbpath", "", "Filesystem path to store DB info, don't set it to use only RAM")
	cpuprofile := flag.String("cpuprofile", "", "Write cpu profile info to file")
	webprofile := flag.Bool("webprofile", false, "Set webprofile on")
	localIP := flag.String("localip", com.GetLocalIP(),
		"Set the local IP, Treeless will use a non loopback IP if the flag is missing")
	logToFile := flag.String("logtofile", "", "Set an output file for logging")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix(*localIP + ":" + fmt.Sprint(*port) + " ")

	runtime.GOMAXPROCS(*procs)

	if *logToFile != "" {
		f, err := os.OpenFile(*logToFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("Error when opening log file")
			return
		}
		defer f.Close()
		log.SetOutput(f)
	}

	var f *os.File
	if *cpuprofile != "" {
		go func() {
			f, err := os.Create(*cpuprofile)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("CPU profile started")
			pprof.StartCPUProfile(f)
		}()
	}
	if *webprofile {
		go func() {
			fmt.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	var s *server.DBServer
	if *monitor != "" {
		sg, err := servergroup.Assoc(*monitor, "")
		if err != nil {
			fmt.Println(err)
			return
		}
		//Start heartbeat listener
		hb := heartbeat.Start(sg)
		go func() {
			for {
				fmt.Println("\033[H\033[2J" + sg.String())
				time.Sleep(time.Millisecond * 100)
			}
		}()
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		hb.Stop()
		return
	} else if *create {
		s = server.Create(*localIP, *port, *dbpath, uint64(*size), *open, *chunks, *redundancy)
	} else if *assoc != "" {
		s = server.Assoc(*localIP, *port, *dbpath, uint64(*size), *open, *assoc)
	} else {
		flag.Usage()
		fmt.Println("No operations passed. Use one of these: -create, -assoc -monitor.")
		os.Exit(1)
	}
	//Wait for an interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Println("Interrupt signal recieved")
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
		f.Close()
		fmt.Println("Profiling output generated")
		fmt.Println("View the pprof graph with:")
		fmt.Println("go tool pprof --png treeless cpu.prof > a.png")
	}
	s.Stop()
	log.Println("Server stopped")
}
