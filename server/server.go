package server

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/pprof"
	"sync/atomic"
	"time"
	"treeless/com"
	"treeless/core"
)
import _ "net/http/pprof"

type DBServer struct {
	coreDB         *tlcore.DB
	m              *tlcore.Map
	udpCon         net.Conn
	tcpListener    *net.TCPListener
	tcpConnections []*net.TCPConn
	closed         int32
}

const bufferSize = 2048

func (db *DBServer) isClosed() bool {
	return atomic.LoadInt32(&db.closed) != 0
}

func (db *DBServer) Close() {
	atomic.StoreInt32(&db.closed, 1)
	db.udpCon.Close()
	db.tcpListener.Close()
	for i := 0; i < len(db.tcpConnections); i++ {
		db.tcpConnections[i].Close()
	}
	db.coreDB.Close()
}

func listenToConnetion(conn *net.TCPConn, id int, db *DBServer) {
	db.tcpConnections = append(db.tcpConnections, conn)

	//log.Println("New connection accepted. Connection ID:", id)
	//tcpWriter will buffer TCP writes to send more message in less TCP packets
	//this technique allows bigger throughtputs, but latency in increased a little
	writeCh := make(chan com.Message, 1024)
	go com.TCPWriter(conn, writeCh)

	processMessage := func(message com.Message) {
		switch message.Type {
		case tlcore.OpGet:
			var response com.Message
			rval, err := db.m.Get(message.Key)
			//fmt.Println("Get operation", key, rval, err)
			response.ID = message.ID
			if err != nil {
				response.Type = com.OpGetResponseError
				response.Value = []byte(err.Error())
			} else {
				response.Type = com.OpGetResponse
				response.Value = rval
			}
			writeCh <- response
		case tlcore.OpPut:
			err := db.m.Put(message.Key, message.Value)
			//fmt.Println("Put operation", message.Key, message.Value, err)
			if err != nil {
				panic(err)
			}
		}
	}

	com.TCPReader(conn, processMessage)

	close(writeCh)

	conn.Close()
	//log.Println("Connection closed. Connection ID:", id)
}

func listenToNewConnections(db *DBServer) {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{Port: 9876})
	if err != nil {
		panic(err)
	}
	db.tcpListener = ln
	for i := 0; ; i++ {
		conn, err := ln.AcceptTCP()
		if err != nil {
			if db.isClosed() {
				return
			}
			panic(err)
		}
		go listenToConnetion(conn, i, db)
	}
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func Init() *DBServer {
	//go http.ListenAndServe("localhost:8080", nil)
	//Recover
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			panic(r)
		}
	}()

	//Default values
	dbPath := "tmpDB"
	//Parse args
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			time.Sleep(time.Second * 1)
			fmt.Println("start")
			pprof.StartCPUProfile(f)
			time.Sleep(time.Second * 10)
			pprof.StopCPUProfile()
			f.Close()
			fmt.Println("fflushed")
		}()
	}
	//Launch core
	var db DBServer
	db.coreDB = tlcore.Create(dbPath)
	var err error
	db.m, err = db.coreDB.AllocMap("map1")
	if err != nil {
		panic(err)
	}
	//Init server
	db.udpCon = com.ReplyToPings()
	go listenToNewConnections(&db)
	return &db
}
