package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"slices"
	"strconv"
)

type FlagType struct {
	regex    string
	dataType DataType
}

type DataType int

const (
	STRING = iota
	INT
)

type Key string

type NodeAddress string

type Node struct {
	Address     NodeAddress
	FingerTable []NodeAddress
	Predecessor NodeAddress
	Successors  []NodeAddress

	Bucket map[Key]string
}

var flagMap = map[string]FlagType{
	"-a":    {regex: `^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$`, dataType: STRING},
	"-p":    {regex: `^([0-9]|[1-9][0-9]{1,4}|[1-5][0-9]{4}|6[0-5][0-5][0-3][0-5])$`, dataType: INT},
	"--ja":  {regex: `^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$`, dataType: STRING},
	"--jp":  {regex: `^([0-9]|[1-9][0-9]{1,4}|[1-5][0-9]{4}|6[0-5][0-5][0-3][0-5])$`, dataType: INT},
	"--ts":  {regex: `^(60000|[1-9]\d{0,4})$`, dataType: INT},
	"--tff": {regex: `^(60000|[1-9]\d{0,4})$`, dataType: INT},
	"--tcp": {regex: `^(60000|[1-9]\d{0,4})$`, dataType: INT},
	"-r":    {regex: `^([1-9]|[1-2]\d|3[0-2])$`, dataType: INT},
	"-i":    {regex: `^[0-9a-fA-F]{1,40}$`, dataType: STRING},
}

var requiredFlags = []string{"-a", "-p", "--ts", "--tff", "--tcp", "-r"}

func validateArgs(args []string) (bool, map[string]string) {

	createNewChord := true
	flags := make(map[string]string)

	for _, flag := range requiredFlags {
		if !slices.Contains(args, flag) {
			log.Fatal("Missing required flag", flag)
		}
	}

	if slices.Contains(args, "--ja") != slices.Contains(args, "--jp") {
		log.Fatal("Both --ja and --jp has to be passed")

	}

	if slices.Contains(args, "--ja") {
		createNewChord = false
	}

	for i := 1; i < len(args); i += 2 {
		flag := flagMap[args[i]]
		if flag.regex == "" {
			log.Fatal("Invalid flag: ", args[i])
		}
		matched, err := regexp.MatchString(flag.regex, args[i+1])
		if err != nil {
			log.Fatal(err)
		}

		if !matched {
			log.Fatal("Value for flag: ", args[i], " not valid: ", args[i+1])
		}

		flags[args[i]] = args[i+1]

	}

	return createNewChord, flags
}

func createNode(ip string, port int) {
	node := initializeChordNode(ip, port)
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

func initializeChordNode(ip string, port int) *Node {
	node := &Node{
		Address:     NodeAddress(fmt.Sprintf("%s:%d", ip, port)),
		FingerTable: make([]NodeAddress, 0), 
		Predecessor: "",                     
		Successors:  make([]NodeAddress, 0), 
		Bucket:      make(map[Key]string),   
	}
	return node
}

func (node *Node) Ping(request string, reply *string) error {
	fmt.Println("RAN PING FUNCTION")
	*reply = "Pong"
	return nil
}

func handleInput(port int) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		switch input {
		case "help":
			fmt.Println("Available commands: help, quit, port, ...")
		case "quit":
			os.Exit(0)
		case "port":
			fmt.Println("Current port:", port)
		case "ping":
			fmt.Print("Enter address to ping (format: IP:Port): ")
			scanner.Scan()
			address := scanner.Text()
			pingChordNode(address)
		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

func parsePort(portArg string) int {
	port, err := strconv.Atoi(portArg)
	if err != nil {
		log.Fatal("Invalid port number:", err)
	}
	return port
}

func pingChordNode(address string) {
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

func main() {

	args := os.Args

	fmt.Println("ARGS", args)

	isNewRing, argsMap := validateArgs(args)

	port := parsePort(argsMap["-p"])

	if isNewRing {
		fmt.Print("New chord ring started")
		createNode(argsMap["-a"], port)
		return 
	}

	node := initializeChordNode(argsMap["-a"], port)

	rpc.Register(node)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", parsePort(args[4])))
	if err != nil {
		log.Fatal("Error starting RPC server:", err)
	}
	defer listener.Close()

	fmt.Printf("Chord node started at %s\n", node.Address)
	go http.Serve(listener, nil)
	go handleInput(port)

	select {}
}
