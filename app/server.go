package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type RESPdata struct {
	dataContent string
	dataType    string
}

type commandFn func([]RESPdata) string

var knownCommand map[string]commandFn = map[string]commandFn{
	"hello": func(args []RESPdata) string {
		return "Hello, world!"
	},
	"echo": func(args []RESPdata) string {
		var sb strings.Builder

		if len(args) > 0 {
			if len(args) > 1 {
				sb.WriteString(fmt.Sprintf("*%d", len(args)))
				sb.WriteString("\r\n")
			}
			for _, v := range args {
				if strings.Contains(v.dataType, "$") {
					sb.WriteString("+")
				} else {
					sb.WriteString(v.dataType)
					sb.WriteString("\r\n")
				}
				sb.WriteString(v.dataContent)
				sb.WriteString("\r\n")
			}
			return sb.String()
		}
		return ""
	},
}

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
		handleConnection(conn, id)
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
				returnedString = val(dataContents[1:])
				break
			}
		}
		return returnedString, false, nil
	}
}

func simpleRESPParser(respString string) ([]RESPdata, error) {
	if len(respString) == 0 {
		return nil, fmt.Errorf("got nothing")
	}
	respSplitted := strings.Split(respString, "\r\n")
	arrLength, err := strconv.Atoi(string(respSplitted[0][1]))
	if err != nil {
		return nil, fmt.Errorf("error converting array length from RESP string")
	}
	data := []RESPdata{}
	for i := 0; i < arrLength; i++ {
		dataType := respSplitted[1+(2*i)]
		dataContent := respSplitted[2+(2*i)]
		data = append(data, RESPdata{
			dataContent: dataContent,
			dataType:    dataType,
		})
	}
	return data, nil
}
