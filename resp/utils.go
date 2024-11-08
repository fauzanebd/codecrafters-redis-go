package resp

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

type CustomError struct {
	errorMsg string
}

func (err *CustomError) Error() string {
	return err.errorMsg
}

func NewError(errMsg string) *CustomError {
	return &CustomError{errorMsg: errMsg}
}

func ReadConfig() (map[string]string, error) {
	config := make(map[string]string)
	file, err := os.Open("./redis.conf")
	if err != nil {
		return config, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		if scanner.Bytes()[0] == 35 {
			continue
		}
		splitted := strings.Split(scanner.Text(), " ")
		if len(splitted) < 2 {
			continue
		}
		config[splitted[0]] = strings.Join(splitted[1:], " ")

	}
	return config, nil
}

func WriteConfig(key string, value string) error {

	existingConfig, err := ReadConfig()
	if err != nil {
		if _, ok := err.(*fs.PathError); !ok {
			return fmt.Errorf("error writing config file: %s", err)
		}
	}
	if val, ok := existingConfig[key]; ok {
		if val == value {
			// no need to write anything
			return nil
		}
	}
	existingConfig[key] = value

	configData := []byte{}
	written := 0
	for k, v := range existingConfig {
		if written == 0 {
			configData = append(configData, []byte(fmt.Sprintf("%s %s\n", k, v))...)
			continue
		}
		configData = append(configData, []byte(fmt.Sprintf("\n%s %s", k, v))...)
	}

	err = os.WriteFile("./redis.conf", configData, 0777)
	if err != nil {
		return fmt.Errorf("error writing config file: %s", err)
	}
	return nil
}
