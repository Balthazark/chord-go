package main

import (
	"crypto/sha1"
	"fmt"
	"log"
	"math"
	"math/big"
	"net"
	"net/rpc"
)

//Types for modeling a node

type Key string

type NodeAddress string

type Node struct {
	Id          *big.Int
	Address     NodeAddress
	FingerTable []NodeAddress
	Predecessor NodeAddress
	Successors  []NodeAddress

	Bucket map[Key]string
}

// Functions for creating nodes
func CreateNode(ip string, port, r int) {
	node := InitializeChordNode(ip, port, r)
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

func InitializeChordNode(ip string, port, r int) *Node {
	node := &Node{
		Id:          hashString(fmt.Sprintf("%s:%d", ip, port)),
		Address:     NodeAddress(fmt.Sprintf("%s:%d", ip, port)),
		FingerTable: make([]NodeAddress, 0),
		Predecessor: "",
		Successors:  make([]NodeAddress, 0),
		Bucket:      make(map[Key]string),
	}

	node.Successors = append(node.Successors, node.Address)
	return node
}

func (node *Node) Self(request string, reply *Node) error {
	*reply = *node
	return nil
}

func getNode(address string) *Node {

	if address == "" {
		return nil
	}

	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("Error connecting to Chord node", err)
	}
	var reply Node
	err = client.Call("Node.Self", "", &reply)
	if err != nil {
		log.Fatal("Error calling Join method")
	}

	return &reply
}

// Node rpc functions
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

func (node *Node) Dump(request *struct{}, reply *struct{}) error {
	fmt.Println("Id: ", node.Id)
	fmt.Println("Adress: ", node.Address)
	fmt.Println("Pred: ", node.Predecessor)
	fmt.Println("SUcc: ", node.Successors)
	fmt.Println("BUCKET: ", node.Bucket)
	return nil
}

func (node *Node) Join(successorAddress string, reply *string) error {
	node.Successors = append(node.Successors, NodeAddress(successorAddress))
	*reply = "Successfully joined"
	return nil
}

func (node *Node) GetAll(id *big.Int, reply *map[string]string) error {

	temp := make(map[string]string, 0)

	for key, value := range node.Bucket {
		if between(id, hashString(string(key)), node.Id, true) {
			temp[string(key)] = value
			delete(node.Bucket, key)
		}
	}

	*reply = temp
	return nil
}

func handleGetAll(node *Node, successorAddress string) map[string]string {
	client, err := rpc.DialHTTP("tcp", successorAddress)
	if err != nil {
		log.Fatal("Error connecting to successor node")
	}
	var reply map[string]string
	err = client.Call("Node.GetAll", node.Id, &reply)
	if err != nil {
		log.Fatal("Error calling get all method: ", err)
	}
	return reply
}

func (node *Node) AddSuccessor(successorAddress string) {
	client, err := rpc.DialHTTP("tcp", successorAddress)
	if err != nil {
		log.Fatal("Error connecting to Chord node", err)
	}
	var reply string
	err = client.Call("Node.Join", node.Address, &reply)
	if err != nil {
		log.Fatal("Error calling Join method")
	}

	fmt.Println(reply)
}

func handleAddPredecessor(node string, predecessor string) {
	client, err := rpc.DialHTTP("tcp", node)
	if err != nil {
		log.Fatal("Error connecting to Chord node", err)
	}
	var reply string
	err = client.Call("Node.AddPredecessor", predecessor, &reply)
	if err != nil {
		log.Fatal("Error calling Join method")
	}

	fmt.Println(reply)
}

func (node *Node) AddPredecessor(predecessorAddress string, reply *string) error {
	node.Predecessor = NodeAddress(predecessorAddress)
	*reply = "Added pred"
	return nil
}

func (node *Node) DumpNode() {
	client, err := rpc.DialHTTP("tcp", string(node.Address))
	if err != nil {
		log.Fatal("Error connecting to Chord node", err)
	}

	var reply struct{}
	var args struct{}

	err = client.Call("Node.Dump", &args, &reply)
	if err != nil {
		log.Fatal("Error calling dump method")
	}
}

// Node handlers for key values
func PingChordNode(address string) {
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
func GetKeyValue(start *Node, key Key) {
	keyHash := hashString(string(key))
	address := find(keyHash, start)

	client, err := rpc.DialHTTP("tcp", string(address))
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
func PutKeyValue(start *Node, key Key, value string) {
	keyHash := hashString(string(key))
	fmt.Println("KEYHASH BEFORE PUT", keyHash)
	address := find(keyHash, start)

	client, err := rpc.DialHTTP("tcp", string(address))
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
func DeleteKeyValue(start *Node, key Key) {
	keyHash := hashString(string(key))
	address := find(keyHash, start)

	client, err := rpc.DialHTTP("tcp", string(address))
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

// Helpers
func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func between(start, elt, end *big.Int, inclusive bool) bool {
	fmt.Print(end.Cmp(start))
	if end.Cmp(start) == 0 {
		return true
	} else if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}

func find(id *big.Int, start *Node) NodeAddress {
	found, nextNode := false, start
	maxSteps := 32
	i := 0

	for !found && i < maxSteps {
		found, nextNode = nextNode.find_successor(id)
		i++
	}
	if found {
		return nextNode.Address
	} else {
		log.Fatal("find failed")
		return ""
	}
}

func (node *Node) find_successor(id *big.Int) (bool, *Node) {
	successor := getNode(string(node.Successors[0]))
	fmt.Println("ID: ", id)

	if between(node.Id, id, successor.Id, true) {
		fmt.Println(successor.Address)
		fmt.Println(node.Address)
		return true, successor
	} else {
		return false, successor
	}
}

func (node *Node) stabilize() {
	// Retrieve the predecessor of the successor
	successor := getNode(string(node.Successors[0]))
	x := getNode(string(successor.Predecessor))

	println(x)

	// Check if x is a valid predecessor
	if x != nil && between(node.Id, x.Id, successor.Id, false) {
		node.Successors[0] = x.Address // Update successor if x is a valid predecessor
	}

	// Notify the successor about the current node (n)
	successor.notify(node)
}

func (node *Node) notify(predecessorCandidate *Node) {
	predecessor := getNode(string(node.Predecessor))
	// Check if the received predecessorCandidate is a valid predecessor
	if predecessor == nil || between(predecessor.Id, predecessorCandidate.Id, node.Id, false) {
		// Update the predecessor of the current node
		handleAddPredecessor(string(node.Address), string(predecessorCandidate.Address))
	}
}

func (node *Node) fix_fingers(){
	for i := range node.FingerTable{

		entry := node.Id.Add( node.Id, big.NewInt(int64(math.Pow(2,float64(i)))))
		id := entry.Mod(entry,big.NewInt(int64(math.Pow(2,32))))

		inBetween, nextNode := node.find_successor(id)

		for !inBetween {
			nextNode.find_successor(id)
		}
		
		node.FingerTable[i] = nextNode.Address
	}
}

	
