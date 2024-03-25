package badger

import (
	"fmt"
	"strconv"
	"strings"
)

func valForPath(key string, s interface{}) (v interface{}, err error) {
	keys := strings.Split(key, ".")

	var value interface{} = s
	for _, key := range keys {
		if value, err = getPath(key, value); err != nil {
			break
		}
	}
	if err == nil {
		return value, nil
	}
	return nil, err
}
func getPath(key string, s interface{}) (v interface{}, err error) {
	var (
		i  int64
		ok bool
	)
	switch s.(type) {
	case map[string]interface{}:
		if v, ok = s.(map[string]interface{})[key]; !ok {
			err = fmt.Errorf("Key not present. [Key:%s]", key)
		}
	case []interface{}:
		if i, err = strconv.ParseInt(key, 10, 64); err == nil {
			array := s.([]interface{})
			if int(i) < len(array) {
				v = array[i]
			} else {
				err = fmt.Errorf("Index out of bounds. [Index:%d] [Array:%v]", i, array)
			}
		}
	}
	//fmt.Println("Value:", v, " Key:", key, "Error:", err)
	return v, err
}
