package tlcom

import (
	"encoding/json"
	"io/ioutil"
)

//AccessConf represents a DB access configuration
type AccessConf struct {
	Chunks  int
	Servers []string
}

var DefaultAccessConf = AccessConf{256, []string{"127.0.0.1:9876"}}

func (ac *AccessConf) WriteToFile(path string) {
	b, err := json.MarshalIndent(ac, "", "\t")
	if err != nil {
		panic(err)
	}
	ioutil.WriteFile(path, b, 0777)
}
