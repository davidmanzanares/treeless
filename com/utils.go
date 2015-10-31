package tlcom

import "encoding/json"

//AccessConf represents a DB access configuration
type AccessConf struct {
	Chunks  int
	Servers []string
}

var DefaultAccessConf = &AccessConf{8, []string{"127.0.0.1:9876"}}

func (ac *AccessConf) Marshal() []byte {
	b, err := json.MarshalIndent(ac, "", "\t")
	if err != nil {
		panic(err)
	}
	return b
}
