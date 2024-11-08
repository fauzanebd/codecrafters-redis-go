package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	resp "github.com/codecrafters-io/redis-starter-go/resp"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {

	// get cli args
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {

		if len(args[i]) > 3 {
			// if args starts with --
			if args[i][0] == '-' && args[i][1] == '-' {
				err := resp.WriteConfig(args[i][2:], args[i+1])
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

		fmt.Println(wrapGoroutineLogs(fmt.Sprintf("received: %q\n", string(buffer[:n])), goId))
		response, closeConn, err := processConn(buffer[:n])
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

func processConn(data []byte) (stringResponse string, close bool, err error) {

	contentsPtr := &[]string{}
	err = resp.Unmarshal(data, contentsPtr)
	contents := *contentsPtr

	if err != nil {
		return fmt.Sprintf("-ERR failed to parse %s", err.Error()), false, nil
	}

	returnedString := ""
	for _, content := range contents {
		if val, ok := resp.KnownCommand[strings.ToLower(content)]; ok {
			returnedString, err = val(contents[1:])
			if err != nil {
				return fmt.Sprintf("-ERR executing command: %s\r\n", err), false, nil
			}
			return returnedString, false, nil
		}
	}
	return fmt.Sprintf("-ERR unknown command %s\r\n", contents[0]), false, nil

}
