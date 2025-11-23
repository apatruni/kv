package main

import (
	"os"
	"strconv"

	"github.com/labstack/echo"
)

// REST handlers

func getFn(c echo.Context) error {
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

func putFn(c echo.Context) error {
	var kv KV
	if err := c.Bind(&kv); err != nil {
		return c.NoContent(400)
	}
	kvMutex.Lock()
	MapKV[kv.Key] = kv.Value
	kvMutex.Unlock()

	// replicate to peers
	lenKey := strconv.Itoa(len(kv.Key))
	lenVal := strconv.Itoa(len(kv.Value))
	msg := "Write|" + lenKey + "|" + kv.Key + "|" + lenVal + "|" + kv.Value

	for pid, pc := range Peers {
		// don't send to self
		if pid == os.Args[1] {
			continue
		}
		_ = writeToPeer(pid, pc, []byte(msg))
	}

	return c.String(200, "Inserted successfully")
}

func deleteFn(c echo.Context) error {
	var kv KV
	if err := c.Bind(&kv); err != nil {
		return c.NoContent(400)
	}
	kvMutex.Lock()
	delete(MapKV, kv.Key)
	kvMutex.Unlock()

	lenKey := strconv.Itoa(len(kv.Key))
	msg := "Delete|" + lenKey + "|" + kv.Key
	for pid, pc := range Peers {
		if pid == os.Args[1] {
			continue
		}
		_ = writeToPeer(pid, pc, []byte(msg))
	}
	return c.NoContent(200)
}
