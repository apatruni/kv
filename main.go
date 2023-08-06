package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"

	"github.com/labstack/echo/v4"
	"gopkg.in/yaml.v2"
)

type conf struct {
	Peers map[string]string `yaml:"peers"`
	Rest  map[string]string `yaml:"rest"`
}

var RecvConnections []net.Conn
var DialConnections []net.Conn

func (c *conf) unMarshalConfig() *conf {
	yamlFile, err := ioutil.ReadFile("./config.yml")
	if err != nil {
		fmt.Println("Error reading the config file: ", err)
		os.Exit(1)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		fmt.Println("Error during unmarshall: ", err)
		os.Exit(1)
	}
	return c
}

var Map map[string]string

// Entrypoint
func main() {
	args := os.Args
	pid := args[1]

	var c conf
	c.unMarshalConfig()
	fmt.Println(c.Peers)
	go setupServer(pid, c.Peers[pid])
	go connectToPeers(pid, c.Peers)
	go heartBeats(pid)
	Map = make(map[string]string)
	Map["test"] = "some value"
	e := echo.New()
	//e.POST("/put", putFn)
	e.GET("/get/:key", getFn)
	e.Logger.Fatal(e.Start(c.Rest[pid]))
}

func getFn(context echo.Context) error {
	found, ok := Map[context.Param("key")]
	if !ok {
		context.Response().WriteHeader(404)
		context.Response().Write([]byte("Not found"))
		return nil
	}
	context.Response().WriteHeader(200)
	context.Response().Write([]byte(found))
	return nil
}

func setupServer(pid string, address string) {
	fmt.Println("Starting to set up server for: ", pid)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Error listening on address: ", address, err)
		os.Exit(1)
	}
	defer listener.Close()
	for {
		connection, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
		}
		RecvConnections = append(RecvConnections, connection)
	}
}

func connectToPeers(pid string, peers map[string]string) {
	for peerPid, peerAddr := range peers {
		if pid != peerPid {
			connection, err := net.Dial("tcp", peerAddr)
			for err != nil {
				time.Sleep(2 * time.Second)
				connection, err = net.Dial("tcp", peerAddr)
			}
			DialConnections = append(DialConnections, connection)
		}
	}
	if len(DialConnections) != 2 {
		fmt.Println("Did not establish correct number of connections")
		os.Exit(1)
	}
	fmt.Println("Finished connecting to peers for pid: ", pid)
}

func heartBeats(pid string) {
	time.Sleep(3 * time.Second)
	helloMsg := make([]byte, len([]byte("hello")))
	for {
		for i, connection := range DialConnections {
			_, err := connection.Write(helloMsg)
			if err != nil {
				DialConnections[i] = nil
			}
		}
		time.Sleep(3 * time.Second)
	}
}
