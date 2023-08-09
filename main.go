package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo"
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

var DialConnections []net.Conn

type PeerConnection struct {
	HeartbeatConnection         net.Conn
	DialConnection              net.Conn
	IncomingHeartbeatConnection net.Conn
	RecvConnection              net.Conn
	HeartbeatConnectionStr      string
	DialConnectionStr           string
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
	Peers = createPeers(c, pid)

	go setupServer(pid)
	time.Sleep(2 * time.Second)
	connectToPeers(pid)
	time.Sleep(2 * time.Second)
	fmt.Println(pid, Peers)
	go Listen()
	time.Sleep(2 * time.Second)
	// go heartBeats(pid)
	Map = make(map[string]string)
	e := echo.New()
	e.POST("/put", putFn)
	e.GET("/get/:key", getFn)

	e.Logger.Fatal(e.Start(c.Rest[pid]))
}

func createPeers(c conf, currentPid string) map[string]PeerConnection {
	Peers := make(map[string]PeerConnection, len(c.Peers))
	for pid, peerDetails := range c.Peers {
		Peers[pid] = PeerConnection{
			HeartbeatConnectionStr: peerDetails[0],
			DialConnectionStr:      peerDetails[1],
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
	var kvBody *KV = new(KV)
	context.Bind(kvBody)
	Map[kvBody.Key] = kvBody.Value

	lenKey := strconv.Itoa(len(kvBody.Key))
	lenVal := strconv.Itoa(len(kvBody.Value))
	for pid, peerConnection := range Peers {
		if pid != os.Args[1] {
			_, _ = peerConnection.DialConnection.Write([]byte("Write|" + lenKey + "|" + kvBody.Key + "|" + lenVal + "|" + kvBody.Value))
		}
	}
	return nil
}

func setupServer(currentPid string) {
	currentPeer := Peers[currentPid]
	heartbeatListener, err := net.Listen("tcp", currentPeer.HeartbeatConnectionStr)
	if err != nil {
		fmt.Println("Error setting up heartbeat listener: ", heartbeatListener, err)
		os.Exit(1)
	}
	defer heartbeatListener.Close()
	proposalListener, err := net.Listen("tcp", currentPeer.DialConnectionStr)
	if err != nil {
		fmt.Println("Error setting up proposal listener: ", proposalListener, err)
		os.Exit(1)
	}
	defer proposalListener.Close()

	go setupConnectionListener(heartbeatListener, "heartbeat")
	go setupConnectionListener(proposalListener, "data")
	for {

	}
}

func setupConnectionListener(listener net.Listener, typ string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
		}
		msg := make([]byte, 40)
		n, _ := conn.Read(msg)
		str := string(msg[:n])
		vals := strings.Split(string(str), "|")
		lenPid, _ := strconv.Atoi(vals[0])
		peerPid := vals[1]
		if typ == "heartbeat" {
			if peerConn, ok := Peers[peerPid[:lenPid]]; ok {
				peerConn.IncomingHeartbeatConnection = conn
				Peers[peerPid[:lenPid]] = peerConn
			}
		} else {
			if peerConn, ok := Peers[peerPid[:lenPid]]; ok {
				peerConn.RecvConnection = conn
				Peers[peerPid[:lenPid]] = peerConn
			}
		}
	}
}

func connectToPeers(currentPid string) {
	for peerPid, peerConnection := range Peers {
		if currentPid != peerPid {
			dataSndConn, err := net.Dial("tcp", peerConnection.DialConnectionStr)
			if err != nil {
				fmt.Println("Error connecting to ", peerPid, " for data send, exiting", err)
				os.Exit(1)
			}
			peerConnection.DialConnection = dataSndConn
			currentPidStrLen := strconv.Itoa(len(currentPid))
			dataSndConn.Write([]byte(currentPidStrLen + "|" + currentPid))
			heartbeatConn, err := net.Dial("tcp", peerConnection.HeartbeatConnectionStr)
			if err != nil {
				fmt.Println("Error connecting to ", peerPid, " for heartbeat, exiting")
				os.Exit(1)
			}
			peerConnection.HeartbeatConnection = heartbeatConn
			heartbeatConn.Write([]byte(currentPidStrLen + "|" + currentPid))
			Peers[peerPid] = peerConnection
		}
	}
}

// func heartBeats(currentPid string) {
// 	time.Sleep(3 * time.Second)
// 	helloMsg := make([]byte, len([]byte("hello")))
// 	for {
// 		for pid, connection := range Peers {
// 			if currentPid != pid {
// 				_, err := connection.HeartbeatConnection.Write(helloMsg)
// 				if err != nil {
// 					fmt.Println("Error during heartbeat, removing ")
// 				}
// 			}
// 		}
// 		time.Sleep(3 * time.Second)
// 	}
// }

func Listen() {
	for {
		msg := make([]byte, 100)

		for pid, peerConnection := range Peers {

			if pid != os.Args[1] {
				n, err := peerConnection.RecvConnection.Read(msg)
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
}
