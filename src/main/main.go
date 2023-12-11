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
	"strings"
	"time"
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

func handleInput(port int, node *Node) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		switch input {
		case "help":
			fmt.Println("Available commands: help, quit, port, ...")
		case "quit":
			handleNodeShutdown(node)
			os.Exit(0)
		case "port":
			fmt.Println("Current port:", port)
		case "ping":
			fmt.Print("Enter address to ping (format: IP:Port): ")
			scanner.Scan()
			address := scanner.Text()
			PingChordNode(address)
		case "get":
			fmt.Print("Usage: get <key>")
			scanner.Scan()
			input := scanner.Text()
			getArgs := strings.Fields(input)
			if len(getArgs) < 1 {
				fmt.Println("Invalid command. Usage: get <key>")
				continue
			}
			key := Key(getArgs[0])
			GetKeyValue(node,  key)
		case "put":
			fmt.Println("Enter key value address: <key> <value>")
			scanner.Scan()
			input := scanner.Text()
			putArgs := strings.Fields(input)
			if len(putArgs) < 2 {
				fmt.Println("Invalid command. Usage: put <key> <value>")
				continue
			}
			key := Key(putArgs[0])
			value := putArgs[1]
			PutKeyValue(node, key, value)
		case "delete":
			fmt.Println("Usage: get <key>")
			scanner.Scan()
			input := scanner.Text()
			deleteArgs := strings.Fields(input)
			if len(deleteArgs) < 1 {
				fmt.Println("Invalid command. Usage: delete <key>")
				continue
			}
			key := Key(deleteArgs[0])
			DeleteKeyValue(node, key)
		case "dump":
			node.DumpNode()
		case "join":
			fmt.Println("Enter: <successorAddress>")
			scanner.Scan()
			input := scanner.Text()
			joinArgs := strings.Fields(input)
			if len(joinArgs) < 1 {
				fmt.Println("Invalid command. Usage: join <address>")
				continue
			}
			node.AddSuccessor(joinArgs[0])

		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

func handleStabilize(node *Node, timeOut,r int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Stabilize")
			node.stabilize(r)
		}
	}
}

func handleFingers(node *Node, timeOut int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Fingers")
			node.fix_fingers()
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

func main() {

	args := os.Args

	fmt.Println("ARGS", args)

	isNewRing, argsMap := validateArgs(args)

	port := parsePort(argsMap["-p"])
	r := parsePort(argsMap["-r"])	
	ts := parsePort(argsMap["--ts"])
	tff := parsePort(argsMap["--tff"])

	node := InitializeChordNode(argsMap["-a"], port);

	if !isNewRing {
		joinNode := getNode(fmt.Sprintf("%s:%s",argsMap["--ja"],argsMap["--jp"]))
		successor := find(node.Id,joinNode)
		node.Successors[0] = successor
	}


	rpc.Register(node)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", parsePort(args[4])))
	if err != nil {
		log.Fatal("Error starting RPC server:", err)
	}
	defer listener.Close()

	fmt.Printf("Chord node started at %s\n", node.Address)
	go http.Serve(listener, nil)
	go handleInput(port,node)

	node.stabilize(r)
	go handleStabilize(node, ts,r)
	go handleFingers(node,tff)

	select {}
}
