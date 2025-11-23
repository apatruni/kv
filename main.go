package main

import (
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo"
	"gopkg.in/yaml.v2"
)

type conf struct {
	Peers    map[string][]string `yaml:"peers"`
	Rest     map[string]string   `yaml:"rest"`
	Priority map[string]int      `yaml:"priority"`
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
	mutex                       sync.Mutex
}

var (
	Peers      map[string]*PeerConnection
	NextNode   map[string]string
	MyPriority int
	MapKV      map[string]string

	peersMutex sync.Mutex
	kvMutex    sync.RWMutex
)

func (c *conf) unMarshalConfig() *conf {
	yamlFile, err := os.ReadFile("./config.yml")
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

////////////////////////////////////////////////////////////////////////////////
// RING BUILDING (Scales to N nodes)
////////////////////////////////////////////////////////////////////////////////

func buildRing(c *conf) {
	var list []struct {
		pid      string
		priority int
	}

	// convert priority map → sortable slice
	for pid, pr := range c.Priority {
		list = append(list, struct {
			pid      string
			priority int
		}{pid, pr})
	}

	// sort by priority ascending (lower number = higher priority)
	sort.Slice(list, func(i, j int) bool {
		return list[i].priority < list[j].priority
	})

	N := len(list)
	NextNode = make(map[string]string, N)

	// build ring successor map
	for i := 0; i < N; i++ {
		current := list[i].pid
		next := list[(i+1)%N].pid
		NextNode[current] = next
	}

	fmt.Println("=== RING ORDER ===")
	for _, e := range list {
		fmt.Println(" ", e.pid, "->", NextNode[e.pid])
	}
	fmt.Println("==================")
}

////////////////////////////////////////////////////////////////////////////////
// MAIN
////////////////////////////////////////////////////////////////////////////////

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: main <pid>")
		os.Exit(1)
	}

	pid := os.Args[1]

	var c conf
	c.unMarshalConfig()

	// load priority from config
	priorityValue, ok := c.Priority[pid]
	if !ok {
		fmt.Println("No priority entry in config for pid:", pid)
		os.Exit(1)
	}
	MyPriority = priorityValue

	// build dynamic ring
	buildRing(&c)

	// init maps
	MapKV = make(map[string]string)
	Peers = make(map[string]*PeerConnection)

	// build peer configs
	for peerID, details := range c.Peers {
		if len(details) < 2 {
			fmt.Println("peer", peerID, "needs two endpoints (heartbeat + data)")
			os.Exit(1)
		}
		Peers[peerID] = &PeerConnection{
			HeartbeatConnectionStr: details[0],
			DialConnectionStr:      details[1],
		}
	}

	// start listeners
	go setupServer(pid)
	time.Sleep(500 * time.Millisecond)

	// connect outwards
	connectToPeers(pid)

	// heartbeat loop
	for id := range Peers {
		if id == pid {
			continue
		}
		go peerHeartbeatLoop(id)
	}

	// per-peer read loops
	for id := range Peers {
		if id == pid {
			continue
		}
		go peerReadLoop(id, Peers[id])
	}

	// start ring election
	time.Sleep(1 * time.Second)
	go leaderElection(pid)

	// REST (KV API)
	e := echo.New()
	e.POST("/kv/:key", putHandler)
	e.GET("/kv/:key", getHandler)
	e.DELETE("/kv/:key", deleteHandler)

	restAddr := c.Rest[pid]
	fmt.Println("REST server listening on", restAddr)
	e.Logger.Fatal(e.Start(restAddr))
}

////////////////////////////////////////////////////////////////////////////////
// LEADER ELECTION — Scales to N nodes
////////////////////////////////////////////////////////////////////////////////

func leaderElection(myPid string) {
	next := NextNode[myPid]
	if pc, ok := Peers[next]; ok {
		msg := fmt.Sprintf("LeaderElection|%d|%s", MyPriority, myPid)
		writeToPeer(next, pc, []byte(msg))
		fmt.Println(myPid, "sent election token:", msg, "to", next)
	}
}

func handleLeaderElection(from string, parts []string, myPid string) {
	if len(parts) < 3 {
		return
	}

	receivedPrio, _ := strconv.Atoi(parts[1])
	origin := parts[2]

	// if token returns to origin, origin is leader
	if origin == myPid {
		fmt.Printf("✔ %s is the leader (priority: %d)\n", myPid, MyPriority)
		return
	}

	// choose the higher priority (lower number = stronger)
	forwardPrio := receivedPrio
	forwardOrigin := origin

	// our priority is higher (lower number)
	if MyPriority < receivedPrio {
		forwardPrio = MyPriority
		forwardOrigin = myPid
	}

	next := NextNode[myPid]
	if pc, ok := Peers[next]; ok {
		msg := fmt.Sprintf("LeaderElection|%d|%s", forwardPrio, forwardOrigin)
		writeToPeer(next, pc, []byte(msg))
		fmt.Println("forwarded election token:", msg, "to", next)
	}
}

////////////////////////////////////////////////////////////////////////////////
// PEER COMMUNICATION
////////////////////////////////////////////////////////////////////////////////

func writeToPeer(pid string, pc *PeerConnection, data []byte) error {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	if pc.DialConnection != nil {
		_, err := pc.DialConnection.Write(data)
		return err
	}
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

	go func() {
		for {
			conn, err := hListener.Accept()
			if err != nil {
				fmt.Println("heartbeat accept err:", err)
				continue
			}
			go handshakeAccept(conn, "heartbeat")
		}
	}()

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
	buf := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return
	}

	str := string(buf[:n])
	parts := strings.SplitN(str, "|", 2)
	if len(parts) != 2 {
		conn.Close()
		return
	}
	lenPid, err := strconv.Atoi(parts[0])
	if err != nil || len(parts[1]) < lenPid {
		conn.Close()
		return
	}
	peerPid := parts[1][:lenPid]

	peersMutex.Lock()
	pc, ok := Peers[peerPid]
	peersMutex.Unlock()
	if !ok {
		conn.Close()
		return
	}

	pc.mutex.Lock()
	if typ == "heartbeat" {
		pc.IncomingHeartbeatConnection = conn
		fmt.Println("accepted heartbeat from", peerPid)
	} else {
		pc.RecvConnection = conn
		fmt.Println("accepted data connection from", peerPid)
	}
	pc.mutex.Unlock()
}

func connectToPeers(currentPid string) {
	for peerPid, pc := range Peers {
		if peerPid == currentPid {
			continue
		}

		dataConn, err := net.Dial("tcp", pc.DialConnectionStr)
		if err == nil {
			lenStr := strconv.Itoa(len(currentPid))
			_, _ = dataConn.Write([]byte(lenStr + "|" + currentPid))
			pc.mutex.Lock()
			pc.DialConnection = dataConn
			pc.mutex.Unlock()
			fmt.Println(currentPid, "connected data ->", peerPid)
		}

		hConn, err := net.Dial("tcp", pc.HeartbeatConnectionStr)
		if err == nil {
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
			hc.Write([]byte("HB|ping"))
		}
	}
}

func peerReadLoop(pid string, pc *PeerConnection) {
	for {
		pc.mutex.Lock()
		conn := pc.RecvConnection
		pc.mutex.Unlock()

		if conn == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
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

////////////////////////////////////////////////////////////////////////////////
// MESSAGE ROUTER
////////////////////////////////////////////////////////////////////////////////

func handleMessage(from string, raw string) {
	fmt.Println("received from", from, "->", raw)
	parts := strings.Split(raw, "|")

	switch parts[0] {

	case "Write":
		handleWrite(parts)

	case "Delete":
		handleDelete(parts)

	case "LeaderElection":
		handleLeaderElection(from, parts, os.Args[1])

	default:
		fmt.Println("unknown message:", raw)
	}
}

func handleWrite(parts []string) {
	if len(parts) < 5 {
		return
	}
	lenKey, _ := strconv.Atoi(parts[1])
	key := parts[2][:lenKey]

	lenVal, _ := strconv.Atoi(parts[3])
	val := parts[4][:lenVal]

	kvMutex.Lock()
	MapKV[key] = val
	kvMutex.Unlock()

	fmt.Println("applied Write", key, val)
}

func handleDelete(parts []string) {
	if len(parts) < 3 {
		return
	}
	lenKey, _ := strconv.Atoi(parts[1])
	key := parts[2][:lenKey]

	kvMutex.Lock()
	delete(MapKV, key)
	kvMutex.Unlock()

	fmt.Println("applied Delete", key)
}

////////////////////////////////////////////////////////////////////////////////
// REST API HANDLERS (unchanged)
////////////////////////////////////////////////////////////////////////////////

func getHandler(c echo.Context) error {
	key := c.Param("key")
	kvMutex.RLock()
	val, ok := MapKV[key]
	kvMutex.RUnlock()
	if !ok {
		return c.NoContent(404)
	}
	c.Response().Header().Set("Content-Length", strconv.Itoa(len(val)))
	return c.String(200, val)
}

func putHandler(c echo.Context) error {
	key := c.Param("key")

	body := struct {
		Value string `json:"value"`
	}{}

	if err := c.Bind(&body); err != nil {
		return c.NoContent(400)
	}

	kvMutex.Lock()
	MapKV[key] = body.Value
	kvMutex.Unlock()

	lenKey := strconv.Itoa(len(key))
	lenVal := strconv.Itoa(len(body.Value))
	msg := "Write|" + lenKey + "|" + key + "|" + lenVal + "|" + body.Value

	for pid, pc := range Peers {
		if pid == os.Args[1] {
			continue
		}
		_ = writeToPeer(pid, pc, []byte(msg))
	}

	return c.String(200, "Inserted successfully")
}

func deleteHandler(c echo.Context) error {
	key := c.Param("key")

	kvMutex.Lock()
	delete(MapKV, key)
	kvMutex.Unlock()

	lenKey := strconv.Itoa(len(key))
	msg := "Delete|" + lenKey + "|" + key

	for pid, pc := range Peers {
		if pid == os.Args[1] {
			continue
		}
		_ = writeToPeer(pid, pc, []byte(msg))
	}
	return c.NoContent(200)
}
