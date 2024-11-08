package resp

import (
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"
)

type commandFn func(any) (string, error)

var KnownCommand map[string]commandFn = map[string]commandFn{
	"ping": func(args any) (string, error) {
		return "+PONG\r\n", nil
	},
	"hello": func(args any) (string, error) {
		return "Hello, world!", nil
	},
	"echo": func(args any) (string, error) {
		values, ok := args.([]string)
		if !ok {
			return "", fmt.Errorf("invalid parameter for echo")
		}
		if len(values) == 1 {
			res, err := Marshal(values[0])
			if err != nil {
				return "", fmt.Errorf("failed to marshal data: %s", err.Error())
			}
			return res, nil
		}
		res, err := Marshal(values)
		if err != nil {
			return "", fmt.Errorf("failed to marshal data: %s", err.Error())
		}
		return res, nil
	},
	"set": func(args any) (string, error) {
		values, ok := args.([]string)
		if !ok {
			return "", fmt.Errorf("invalid parameter for set")
		}
		if len(values) > 1 {
			StorageMap[values[0]] = values[1]
			if len(values) == 4 {
				if strings.ToLower(values[2]) == "px" {
					expiryMS, err := strconv.Atoi(values[3])
					if err != nil {
						return "", fmt.Errorf("wrong parameter for expiry")
					}
					go func() {
						time.Sleep(time.Duration(expiryMS) * time.Millisecond)
						delete(StorageMap, values[0])
					}()
				}
			} else if len(values) == 3 {
				return "", fmt.Errorf("cant separate value with space")
			} else if len(values) > 4 {
				return "", fmt.Errorf("unhandled parameters")
			}
			return OKRESPString, nil
		} else if len(values) == 1 {
			StorageMap[values[0]] = ""
			return OKRESPString, nil
		} else {
			return "", fmt.Errorf("no key given")
		}
	},
	"get": func(args any) (string, error) {
		values, ok := args.([]string)
		if !ok {
			return "", fmt.Errorf("invalid parameter for get")
		}
		if len(values) > 1 {
			return "", fmt.Errorf("insert one key")
		} else if len(values) == 1 {
			if val, ok := StorageMap[values[0]]; ok {
				res, err := Marshal(val)
				if err != nil {
					return "", fmt.Errorf("error marshal resp string: %s", err.Error())
				}
				return res, nil
			}
			return NullRESPBulkString, nil
		} else {
			return "", fmt.Errorf("no key given")
		}
	},
	"config": func(args any) (string, error) {
		values, ok := args.([]string)
		if !ok {
			return "", fmt.Errorf("unknown redis protocol: %s", args)
		}
		if len(values) > 0 {
			if com, ok := configKnownCommand[strings.ToLower(values[0])]; ok {
				res, err := com(values[1:])
				return res, err
			}
			return "", fmt.Errorf("unknown config command: %s", values[0])
		} else {
			return "", fmt.Errorf("empty config command")
		}
	},
}

var configKnownCommand map[string]commandFn = map[string]commandFn{
	"get": func(args any) (string, error) {
		keys, ok := args.([]string)
		if !ok {
			return "", fmt.Errorf("unknown redis protocol: %s", args)
		}

		availableConfig, err := ReadConfig()
		if err != nil {
			if _, ok := err.(*fs.PathError); !ok {
				return "", fmt.Errorf("error reading config file: %s", err)
			} else {
				return NullRESPBulkString, nil
			}
		}
		if len(keys) > 1 {
			configValues := []string{}
			for _, key := range keys {
				if val, ok := availableConfig[key]; ok {
					configValues = append(configValues, key)
					configValues = append(configValues, val)
				}
			}
			res, err := Marshal(configValues)
			if err != nil {
				return "", fmt.Errorf("error marshal resp string: %s", err.Error())
			}
			return res, nil
		} else if len(keys) == 1 {
			configValues := []string{}
			if val, ok := availableConfig[keys[0]]; ok {
				configValues = append(configValues, keys[0])
				configValues = append(configValues, val)
			}
			res, err := Marshal(configValues)
			if err != nil {
				return "", fmt.Errorf("error marshal resp string: %s", err.Error())
			}
			return res, nil
		}
		return NullRESPBulkString, nil
	},
}

const OKRESPString = "+OK\r\n"
const NullRESPBulkString = "$-1\r\n"

var StorageMap map[string]string = make(map[string]string)
