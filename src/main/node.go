package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)


//Types for modeling a node

type Key string

type NodeAddress string

type Node struct {
	Address     NodeAddress
	FingerTable []NodeAddress
	Predecessor NodeAddress
	Successors  []NodeAddress

	Bucket map[Key]string
}

//Functions for creating nodes
func CreateNode(ip string, port int) {
	node := InitializeChordNode(ip, port)
	rpc.Register(node)

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		log.Fatal("Error starting RPC server:", err)
	}
	defer listener.Close()

	fmt.Printf("Chord node started at %s:%d\n", ip, port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Error accepting connection:", err)
		}
		go rpc.ServeConn(conn)
	}
}

func InitializeChordNode(ip string, port int) *Node {
	node := &Node{
		Address:     NodeAddress(fmt.Sprintf("%s:%d", ip, port)),
		FingerTable: make([]NodeAddress, 0), 
		Predecessor: "",                     
		Successors:  make([]NodeAddress, 0), 
		Bucket:      make(map[Key]string),   
	}
	return node
}

//Node rpc functions
func (node *Node) Ping(request string, reply *string) error {
	fmt.Println("RAN PING FUNCTION")
	*reply = "Pong"
	return nil
}

func (node *Node) Get(request Key, reply *string) error {
	value, exists := node.Bucket[request]
	if !exists {
		return fmt.Errorf("Key not found: %s", request)
	}

	*reply = value
	return nil
}

func (node *Node) Put(kvPair map[string]string, reply *bool) error {
	for key, value := range kvPair {
		node.Bucket[Key(key)] = value
	}

	*reply = true
	return nil
}

func (node *Node) Delete(request Key, reply *bool) error {
	delete(node.Bucket, request)

	*reply = true
	return nil
}

//Node handlers for key values
func PingChordNode(address string) {
	fmt.Println("ADRRESS IN PING HANDLER", address)
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("Error connecting to Chord node:", err)
	}

	var reply string
	err = client.Call("Node.Ping", "Ping request", &reply)
	if err != nil {
		log.Fatal("Error calling Ping method:", err)
	}

	fmt.Println("Ping response from", address, ":", reply)
}

// Function to perform the get operation on the specified Chord node
func GetKeyValue(address string, key Key) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("Error connecting to Chord node:", err)
	}

	var reply string
	err = client.Call("Node.Get", key, &reply)
	if err != nil {
		log.Fatal("Error calling Get method:", err)
	}

	fmt.Printf("Get response from %s for key %s: %s\n", address, key, reply)
}

// Function to perform the put operation on the specified Chord node
func PutKeyValue(address string, key Key, value string) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("Error connecting to Chord node:", err)
	}

	kvPair := map[string]string{string(key): value}
	var reply bool
	err = client.Call("Node.Put", kvPair, &reply)
	if err != nil {
		log.Fatal("Error calling Put method:", err)
	}

	fmt.Printf("Put response from %s for key %s: %t\n", address, key, reply)
}

// Function to perform the delete operation on the specified Chord node
func DeleteKeyValue(address string, key Key) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("Error connecting to Chord node:", err)
	}

	var reply bool
	err = client.Call("Node.Delete", key, &reply)
	if err != nil {
		log.Fatal("Error calling Delete method:", err)
	}

	fmt.Printf("Delete response from %s for key %s: %t\n", address, key, reply)
}