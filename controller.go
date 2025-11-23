package main

import (
	"os"
	"strconv"

	"github.com/labstack/echo"
)

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

	// local write
	kvMutex.Lock()
	MapKV[key] = body.Value
	kvMutex.Unlock()

	// replicate
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
