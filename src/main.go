package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"slices"
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

func validateArgs(args []string) {

	for _, flag := range requiredFlags {
		if !slices.Contains(args, flag) {
			log.Fatal("Missing required flag", flag)
		}
	}

	if slices.Contains(args, "--ja") != slices.Contains(args, "--jp") {
		log.Fatal("Both --ja and --jp has to be passed")

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

	}
}

func main() {

	args := os.Args

	fmt.Println("ARGS", args)

	validateArgs(args)

	// listener, err := net.Listen("tcp", ":"+port)
	// if err != nil {
	// 	fmt.Println("Error starting server on port:", port, "\n", "Error:", err)
	// }

	// fmt.Println("Server started on port:", port)

	// defer listener.Close()

}
