package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo"
	"gopkg.in/yaml.v2"
)

type conf struct {
	Peers map[string][]string `yaml:"peers"`
	Rest  map[string]string   `yaml:"rest"`
}

type KV struct {
	Key   string `json:"key" form:"key"`
	Value string `json:"value" form:"value"`
}

type PeerConnection struct {
	HeartbeatConnection         net.Conn
	DialConnection              net.Conn
	IncomingHeartbeatConnection net.Conn
	RecvConnection              net.Conn
	HeartbeatConnectionStr      string
	DialConnectionStr           string
	mutex                       sync.Mutex // protects writes on this peer's conns
}

var (
	Peers       map[string]*PeerConnection
	Order       map[int]string
	MyPriority  int
	MapKV       map[string]string
	ProposalMap map[string]string

	peersMutex sync.Mutex
	kvMutex    sync.RWMutex
)

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

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: main <pid> <priority>")
		os.Exit(1)
	}

	Order = make(map[int]string)
	Order[1] = "alpha"
	Order[2] = "beta"
	Order[0] = "gamma"

	pid := os.Args[1]
	p, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Invalid priority:", os.Args[2])
		os.Exit(1)
	}
	MyPriority = p

	// initialize maps
	MapKV = make(map[string]string)
	ProposalMap = make(map[string]string)
	Peers = make(map[string]*PeerConnection)

	var c conf = conf{}
	c.unMarshalConfig()

	// build Peers (strings)
	for peerID, details := range c.Peers {
		if len(details) < 2 {
			fmt.Println("peer", peerID, "needs two endpoints in config (heartbeat, data)")
			os.Exit(1)
		}
		Peers[peerID] = &PeerConnection{
			HeartbeatConnectionStr: details[0],
			DialConnectionStr:      details[1],
		}
	}

	// start listeners for this node
	go setupServer(pid)

	// give listeners a moment to start
	time.Sleep(500 * time.Millisecond)

	// connect to other peers
	connectToPeers(pid)

	// start background heartbeat loops for peers
	for id := range Peers {
		if id == pid {
			continue
		}
		go peerHeartbeatLoop(id)
	}

	// Start per-peer read loops
	for id := range Peers {
		if id == pid {
			continue
		}
		go peerReadLoop(id, Peers[id])
	}

	// small delay then trigger leader election attempt
	time.Sleep(1 * time.Second)
	go leaderElection(pid)

	// start REST server (Echo)
	e := echo.New()
	e.POST("/kv/:key", putHandler)
	e.GET("/kv/:key", getHandler)
	e.DELETE("/kv/:key", deleteHandler)

	restAddr := c.Rest[pid]
	fmt.Println("REST server listening on", restAddr)
	e.Logger.Fatal(e.Start(restAddr))
}

func leaderElection(myPid string) {
	// simple circular token passing
	// send our priority to next node once
	nextPid := Order[(MyPriority+1)%3]
	if nextPeer, ok := Peers[nextPid]; ok {
		msg := fmt.Sprintf("LeaderElection|%d", MyPriority)
		writeToPeer(nextPid, nextPeer, []byte(msg))
		fmt.Println(myPid, "sent leader election token with priority", MyPriority, "to", nextPid)
	}
}

func writeToPeer(pid string, pc *PeerConnection, data []byte) error {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	// prefer DialConnection for data channel writes
	if pc.DialConnection != nil {
		_, err := pc.DialConnection.Write(data)
		return err
	}
	// fallback - maybe peer accepted but our dial didn't work; try RecvConnection if it is usable
	if pc.RecvConnection != nil {
		_, err := pc.RecvConnection.Write(data)
		return err
	}
	return fmt.Errorf("no connection available to peer %s", pid)
}

func setupServer(currentPid string) {
	peer := Peers[currentPid]
	if peer == nil {
		fmt.Println("no peer config for", currentPid)
		os.Exit(1)
	}

	hListener, err := net.Listen("tcp", peer.HeartbeatConnectionStr)
	if err != nil {
		fmt.Println("Error setting up heartbeat listener:", err)
		os.Exit(1)
	}
	dListener, err := net.Listen("tcp", peer.DialConnectionStr)
	if err != nil {
		fmt.Println("Error setting up data listener:", err)
		os.Exit(1)
	}

	fmt.Println(currentPid, "listening heartbeat on", peer.HeartbeatConnectionStr, "data on", peer.DialConnectionStr)

	// Accept heartbeats
	go func() {
		for {
			conn, err := hListener.Accept()
			if err != nil {
				fmt.Println("heartbeat accept err:", err)
				continue
			}
			// handle handshake in goroutine
			go handshakeAccept(conn, "heartbeat")
		}
	}()

	// Accept data connections
	go func() {
		for {
			conn, err := dListener.Accept()
			if err != nil {
				fmt.Println("data accept err:", err)
				continue
			}
			go handshakeAccept(conn, "data")
		}
	}()
}

func handshakeAccept(conn net.Conn, typ string) {
	// read initial handshake: "<len>|<pid>"
	buf := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		// If handshake didn't come in time, still keep connection but log
		fmt.Println("handshake read err:", err)
		// close connection - we expect handshake immediately
		conn.Close()
		return
	}
	str := string(buf[:n])
	parts := strings.SplitN(str, "|", 2)
	if len(parts) != 2 {
		fmt.Println("invalid handshake:", str)
		conn.Close()
		return
	}
	lenPid, err := strconv.Atoi(parts[0])
	if err != nil || len(parts[1]) < lenPid {
		fmt.Println("bad handshake length or pid:", str)
		conn.Close()
		return
	}
	peerPid := parts[1][:lenPid]

	peersMutex.Lock()
	pc, ok := Peers[peerPid]
	peersMutex.Unlock()
	if !ok {
		fmt.Println("handshake from unknown peer", peerPid)
		conn.Close()
		return
	}

	if typ == "heartbeat" {
		pc.mutex.Lock()
		pc.IncomingHeartbeatConnection = conn
		pc.mutex.Unlock()
		fmt.Println("accepted heartbeat from", peerPid)
	} else {
		pc.mutex.Lock()
		pc.RecvConnection = conn
		pc.mutex.Unlock()
		fmt.Println("accepted data connection from", peerPid)
	}
}

func connectToPeers(currentPid string) {
	for peerPid, pc := range Peers {
		if peerPid == currentPid {
			continue
		}

		// connect data (dial to peer's data port)
		dataConn, err := net.Dial("tcp", pc.DialConnectionStr)
		if err != nil {
			fmt.Println("Error connecting to", peerPid, "data:", err)
		} else {
			lenStr := strconv.Itoa(len(currentPid))
			_, _ = dataConn.Write([]byte(lenStr + "|" + currentPid))
			pc.mutex.Lock()
			pc.DialConnection = dataConn
			pc.mutex.Unlock()
			fmt.Println(currentPid, "connected data ->", peerPid)
		}

		// connect heartbeat
		hConn, err := net.Dial("tcp", pc.HeartbeatConnectionStr)
		if err != nil {
			fmt.Println("Error connecting to", peerPid, "heartbeat:", err)
		} else {
			lenStr := strconv.Itoa(len(currentPid))
			_, _ = hConn.Write([]byte(lenStr + "|" + currentPid))
			pc.mutex.Lock()
			pc.HeartbeatConnection = hConn
			pc.mutex.Unlock()
			fmt.Println(currentPid, "connected heartbeat ->", peerPid)
		}
	}
}

func peerHeartbeatLoop(pid string) {
	pc := Peers[pid]
	if pc == nil {
		return
	}
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		pc.mutex.Lock()
		hc := pc.HeartbeatConnection
		pc.mutex.Unlock()
		if hc != nil {
			_, err := hc.Write([]byte("HB|ping"))
			if err != nil {
				fmt.Println("heartbeat write err to", pid, err)
			}
		}
	}
}

func peerReadLoop(pid string, pc *PeerConnection) {
	for {
		// wait for RecvConnection to be set by handshakeAccept
		pc.mutex.Lock()
		conn := pc.RecvConnection
		pc.mutex.Unlock()

		if conn == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// read messages in a loop
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			// connection may have been closed; reset and wait for reconnect
			fmt.Println("read error from", pid, ":", err)
			pc.mutex.Lock()
			pc.RecvConnection = nil
			pc.mutex.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if n == 0 {
			continue
		}
		handleMessage(pid, string(buf[:n]))
	}
}

func handleMessage(from string, raw string) {
	fmt.Println("received from", from, "->", raw)
	parts := strings.Split(raw, "|")
	switch parts[0] {
	case "Write":
		if len(parts) >= 5 {
			lenKey, _ := strconv.Atoi(parts[1])
			key := parts[2]
			if len(key) > lenKey {
				key = key[:lenKey]
			}
			lenVal, _ := strconv.Atoi(parts[3])
			val := parts[4]
			if len(val) > lenVal {
				val = val[:lenVal]
			}
			kvMutex.Lock()
			MapKV[key] = val
			kvMutex.Unlock()
			fmt.Println("applied Write", key, val)
		}
	case "Delete":
		if len(parts) >= 3 {
			lenKey, _ := strconv.Atoi(parts[1])
			key := parts[2]
			if len(key) > lenKey {
				key = key[:lenKey]
			}
			kvMutex.Lock()
			delete(MapKV, key)
			kvMutex.Unlock()
			fmt.Println("applied Delete", key)
		}
	case "LeaderElection":
		// received a priority token — basic circular algorithm:
		if len(parts) < 2 {
			return
		}
		receivedPriority, err := strconv.Atoi(parts[1])
		if err != nil {
			return
		}
		// if token returns to originator with same value it means that originator is leader
		if receivedPriority == MyPriority {
			fmt.Println("I am the leader (priority)", MyPriority)
			return
		}
		// if received priority is higher than mine, forward it
		if receivedPriority > MyPriority {
			// forward to next
			next := Order[(MyPriority+1)%3]
			if pc, ok := Peers[next]; ok {
				msg := fmt.Sprintf("LeaderElection|%d", receivedPriority)
				_ = writeToPeer(next, pc, []byte(msg))
				fmt.Println("forwarded higher leader token", receivedPriority, "to", next)
			}
		} else {
			// if it's lower, we drop it (or we could inject our own higher priority)
			fmt.Println("dropped lower leader token", receivedPriority)
		}
	default:
		// unknown message
		fmt.Println("unknown message:", raw)
	}
}
