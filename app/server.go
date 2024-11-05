package main

import (
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
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
		fmt.Printf("Imma let goroutine %d handle this %s connection\n", id, conn.RemoteAddr())
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
		response, closeConn, err := processData(data)
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

func processData(data string) (stringResponse string, close bool, err error) {

	switch data {
	case "*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n":
		return "", true, nil
	case "*1\r\n$4\r\nping\r\n":
		return "+PONG\r\n", false, nil
	case "*1\r\n$4\r\nPING\r\n":
		return "+PONG\r\n", false, nil
	default:
		return "+\r\n", false, nil
	}
}
