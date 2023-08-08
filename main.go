package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"gopkg.in/yaml.v2"
)

type conf struct {
	Peers map[string][]string `yaml:"peers"`
	Rest  map[string]string   `yaml:"rest"`
}

type KV struct {
	Key   string
	Value string
}

var RecvConnections []net.Conn
var DialConnections []net.Conn

type PeerConnection struct {
	RecvConnection    net.Conn
	DialConnection    net.Conn
	RecvConnectionRaw string
	DialConnectionRaw string
}

var Peers map[string]PeerConnection

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
var ProposalMap map[string]string

// Entrypoint
func main() {

	pid := os.Args[1]
	var c conf = conf{}
	c.unMarshalConfig()

	Peers = createPeers(c)
	go setupServer(pid, c.Peers[pid][0])
	go connectToPeers(pid, c.Peers)
	go heartBeats(pid)
	Map = make(map[string]string)
	Map["test"] = "some value"
	e := echo.New()
	e.POST("/put", putFn)
	e.GET("/get/:key", getFn)
	go Listen()
	e.Logger.Fatal(e.Start(c.Rest[pid]))
}

func createPeers(c conf) map[string]PeerConnection {
	Peers := make(map[string]PeerConnection, len(c.Peers))
	for pid, peerDetails := range c.Peers {
		Peers[pid] = PeerConnection{
			RecvConnectionRaw: peerDetails[0],
			DialConnectionRaw: peerDetails[1],
		}
	}
	return Peers
}

func getFn(context echo.Context) error {
	found, ok := Map[context.Param("key")]
	if !ok {
		context.Response().WriteHeader(404)
		context.Response().Write([]byte("Not found"))
		return nil
	}
	context.Response().WriteHeader(200)
	context.Response().Header().Set("Content-Length", string(len(found)))
	context.Response().Write([]byte(found))
	return nil
}

func putFn(context echo.Context) error {
	//propose := []byte("Proposal")
	var kvBody *KV = new(KV)
	context.Bind(kvBody)
	Map[kvBody.Key] = kvBody.Value
	//cnt := 0
	// for _, peer := range DialConnections {
	// 	_, _ = peer.Write(propose)
	// 	_, _ = peer.Read(propose)
	// 	cnt++
	// }
	// if cnt == 2 {
	lenKey := strconv.Itoa(len(kvBody.Key))
	lenVal := strconv.Itoa(len(kvBody.Value))

	for _, peer := range DialConnections {
		_, _ = peer.Write([]byte("Write|" + lenKey + "|" + kvBody.Key + "|" + lenVal + "|" + kvBody.Value))
		fmt.Println("Write|" + lenKey + "|" + kvBody.Key + "|" + lenVal + "|" + kvBody.Value)
	}
	// }
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
		fmt.Println("here")
	}
}

func connectToPeers(pid string, peers map[string][]string) {
	for peerPid, peerAddr := range peers {
		if pid != peerPid {
			connection, err := net.Dial("tcp", peerAddr[1])
			for err != nil {
				time.Sleep(2 * time.Second)
				connection, err = net.Dial("tcp", peerAddr[1])
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

func Listen() {
	for {
		msg := make([]byte, 100)
		for _, peer := range RecvConnections {
			n, err := peer.Read(msg)

			str := string(msg[:n])

			if err == nil {
				vals := strings.Split(string(str), "|")
				if len(vals) < 3 {
					time.Sleep(3 * time.Second)
					continue
				}
				lenKey, _ := strconv.Atoi(vals[1])
				lenVal, _ := strconv.Atoi(vals[3])
				Map[vals[2][:lenKey]] = vals[4][:lenVal]

			}

		}
	}
}
