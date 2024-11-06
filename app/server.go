package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	resp "github.com/codecrafters-io/redis-starter-go/parser"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {

	// res, err := resp.Marshal([]int{23, 43, 45, 54})
	// res, err := resp.Marshal([]float64{34.54, 21.454})
	// if err != nil {
	// 	fmt.Println("error: ", err.Error())
	// 	return
	// }
	// fmt.Printf("%q\n", res)

	var skibidi = &[]any{}
	err := resp.Unmarshal([]byte("*2\r\n$4\r\necho\r\n$3\r\nhey\r\n"), skibidi)
	if err != nil {
		fmt.Println("error unmarshal: ", err)
		return
	}
	fmt.Println("yeay succeed, res: ", skibidi)

	return
	// get cli args
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {

		if len(args[i]) > 3 {
			// if args starts with --
			if args[i][0] == 45 && args[i][1] == 45 {
				err := WriteConfig(args[i][2:], args[i+1])
				if err != nil {
					panic(err)
				}
				i++
				continue
			}
		} else {
			fmt.Printf("invalid args: %s\n", args[i])
			return
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	id := 0
	for {
		fmt.Println("waiting for connection...")
		conn, err := l.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		} else {
			fmt.Println("Accepting new connection: ", conn.RemoteAddr())
		}
		id++
		go handleConnection(conn, id)
	}
}

func handleConnection(conn net.Conn, goId int) {

	buffer := make([]byte, 1024)
	defer conn.Close()

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println(wrapGoroutineLogs("error reading data from connection: ", goId, err.Error()))
			return
		}
		data := string(buffer[:n])
		fmt.Println(wrapGoroutineLogs(fmt.Sprintf("received: %q\n", data), goId))
		response, closeConn, err := processConn(data)
		if err != nil {
			fmt.Println(wrapGoroutineLogs("error creating response according to request", goId, err.Error()))
			return
		}
		fmt.Println(wrapGoroutineLogs(fmt.Sprintf("response: %q, closeAfter: %t", response, closeConn), goId))

		_, err = conn.Write([]byte(response))
		if closeConn {
			return
		}

		if err != nil {
			fmt.Println(wrapGoroutineLogs("Error writing response ", goId, err.Error()))
			return
		}
		// if no error, we gonna read the same conn in the next loop
		fmt.Println(wrapGoroutineLogs("continue to next read", goId))

	}
}

func wrapGoroutineLogs(text string, goId int, a ...any) string {
	if len(a) == 0 {
		return fmt.Sprintf("[goroutine %d] -- %s", goId, text)
	}
	return fmt.Sprintf("[goroutine %d] -- %s%v", goId, text, a[0])
}

func processConn(data string) (stringResponse string, close bool, err error) {

	dataContents, err := simpleRESPParser(data)

	if err != nil {
		return fmt.Sprintf("-ERR failed to parse %s", err.Error()), false, nil
	}

	switch data {
	case "*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n":
		return "", true, nil
	case "*1\r\n$4\r\nping\r\n":
		return "+PONG\r\n", false, nil
	case "*1\r\n$4\r\nPING\r\n":
		return "+PONG\r\n", false, nil
	default:
		returnedString := ""

		for _, content := range dataContents {
			// check for command
			if val, ok := knownCommand[strings.ToLower(content.dataContent)]; ok {
				returnedString, err = val(dataContents[1:])
				if err != nil {
					return fmt.Sprintf("-ERR executing command: %s\r\n", err), false, nil
				}
				return returnedString, false, nil
			}
		}
		return fmt.Sprintf("-ERR unknown command %s\r\n", dataContents[0].dataContent), false, nil
	}
}
