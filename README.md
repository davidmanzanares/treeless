# Treeless
Distributed NoSQL key-value DB written in Go.

## Features
* High performance (~1MOp/s per node)
* High availability (AP system with eventual consistency)
  * Last Writer Wins policy
  * Read-repair
  * Asynchronous repair
* Automatic rebalance
* Simple API (Get, Set, Del and CAS operations)
* Storage
    * Index always stored in RAM
    * Key-value pairs stored in RAM, SSD or HDD (memory mapped files are used)

## API Example
    c, err := client.Connect(addr)
	c.Set([]byte("hola"), []byte("mundo"))
	value, _, _ := c.Get([]byte("hola"))
	c.Close()

## CLI Example
Creating a new database:

    ./treeless -create -port 10000 -dbpath DB0 -localip 127.0.0.1  -redundancy 2

Adding a new node to an existing database:

    ./treeless -assoc 127.0.0.1:10000 -port 10001 -dbpath DB1 -localip 127.0.0.1

## Status
All tests are passed, but you may still find serious bugs. Use with care.

