package main

import (
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"
)

type RESPdata struct {
	dataContent string
	dataType    string
}

func (data RESPdata) toRESPString() string {

	var sb strings.Builder
	if strings.Contains(data.dataType, "$") {
		sb.WriteString("+")
	} else {
		sb.WriteString(data.dataType)
		sb.WriteString("\r\n")
	}
	sb.WriteString(data.dataContent)
	sb.WriteString("\r\n")
	return sb.String()
}

type commandFn func(any) (string, error)

var knownCommand map[string]commandFn = map[string]commandFn{
	"hello": func(args any) (string, error) {
		return "Hello, world!", nil
	},
	"echo": func(args any) (string, error) {

		values, ok := args.([]RESPdata)
		if !ok {
			return "", fmt.Errorf("invalid parameter for echo")
		}
		var sb strings.Builder

		if len(values) > 0 {
			if len(values) > 1 {
				sb.WriteString(fmt.Sprintf("*%d", len(values)))
				sb.WriteString("\r\n")
			}
			for _, v := range values {
				sb.WriteString(v.toRESPString())
			}
			return sb.String(), nil
		}
		return "", nil
	},
	"set": func(args any) (string, error) {
		values, ok := args.([]RESPdata)
		if !ok {
			return "", fmt.Errorf("invalid parameter for set")
		}
		if len(values) > 1 {
			storageMap[values[0].dataContent] = values[1]
			if len(values) == 4 {
				if strings.ToLower(values[2].dataContent) == "px" {
					expiryMS, err := strconv.Atoi(values[3].dataContent)
					if err != nil {
						return "", fmt.Errorf("wrong parameter for expiry")
					}
					go func() {
						time.Sleep(time.Duration(expiryMS) * time.Millisecond)
						delete(storageMap, values[0].dataContent)
					}()
				}
			} else if len(values) == 3 {
				return "", fmt.Errorf("cant separate value with space")
			} else if len(values) > 4 {
				return "", fmt.Errorf("unhandled parameters")
			}
			return okRESPString, nil
		} else if len(values) == 1 {
			storageMap[values[0].dataContent] = RESPdata{}
			return okRESPString, nil
		} else {
			return "", fmt.Errorf("no key given")
		}
	},
	"get": func(args any) (string, error) {
		values, ok := args.([]RESPdata)
		if !ok {
			return "", fmt.Errorf("invalid parameter for get")
		}
		if len(values) > 1 {
			return "", fmt.Errorf("insert one key")
		} else if len(values) == 1 {
			if val, ok := storageMap[values[0].dataContent]; ok {
				return val.toRESPString(), nil
			}
			return nullRESPBulkString, nil
		} else {
			return "", fmt.Errorf("no key given")
		}
	},
	"config": func(args any) (string, error) {
		values, ok := args.([]RESPdata)
		if !ok {
			return "", fmt.Errorf("unknown redis protocol: %s", args)
		}
		if len(values) > 0 {
			if com, ok := configKnownCommand[strings.ToLower(values[0].dataContent)]; ok {
				res, err := com(values[1:])
				return res, err
			}
			return "", fmt.Errorf("unknown config command: %s", values[0].dataContent)
		} else {
			return "", fmt.Errorf("empty config command")
		}
	},
}

var configKnownCommand map[string]commandFn = map[string]commandFn{
	"get": func(args any) (string, error) {
		values, ok := args.([]RESPdata)
		if !ok {
			return "", fmt.Errorf("unknown redis protocol: %s", args)
		}
		var sb strings.Builder
		availableConfig, err := ReadConfig()
		if err != nil {
			if _, ok := err.(*fs.PathError); !ok {
				return "", fmt.Errorf("error reading config file: %s", err)
			} else {
				return nullRESPBulkString, nil
			}
		}
		if len(values) > 1 {
			configValues := []string{}
			for _, val := range values {
				if v, ok := availableConfig[val.dataContent]; ok {
					configValues = append(configValues, RESPdata{dataContent: v, dataType: "+"}.toRESPString())
				}
			}
			sb.WriteString(fmt.Sprintf("*%d\r\n", len(values)))
			sb.WriteString(strings.Join(configValues, ""))
			return sb.String(), nil
		} else if len(values) == 1 {
			for _, val := range values {
				if v, ok := availableConfig[val.dataContent]; ok {
					return RESPdata{dataContent: v, dataType: fmt.Sprintf("$%d", len(v))}.toRESPString(), nil
				}
			}
		}
		return nullRESPBulkString, nil
	},
}

const okRESPString = "+OK\r\n"
const nullRESPBulkString = "$-1\r\n"

var storageMap map[string]RESPdata = map[string]RESPdata{}

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
