package gcounter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var counter = "counter"

type add struct {
	Delta int
}

type counterNode struct {
	mu          sync.Mutex
	currDelta   int
	globalValue int

	kv      *maelstrom.KV
	timeout time.Duration
}

func (c *counterNode) tryRead() {
	ctx, cancel := context.WithTimeout(context.TODO(), c.timeout)
	defer cancel()
	newInt, err := c.kv.ReadInt(ctx, counter)

	rpcErr := &maelstrom.RPCError{}

	if err != nil && !(errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist) {
		log.Printf("reading value: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if newInt > c.globalValue {
		c.globalValue = newInt
	}
}

func (c *counterNode) add(delta int) {
	c.tryRead()
	c.mu.Lock()
	c.currDelta += delta
	deltaToUse := c.currDelta
	globalValue := c.globalValue
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	err := c.kv.CompareAndSwap(ctx, counter, globalValue, globalValue+deltaToUse, true)
	if err == nil {
		c.mu.Lock()
		c.currDelta -= deltaToUse
		c.globalValue += deltaToUse
		c.mu.Unlock()
	} else {
		log.Printf("CAS failed: %v\n", err)
	}
}

func (c *counterNode) read() int {
	c.tryRead()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currDelta + c.globalValue

}

func RunGcounter() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	c := &counterNode{
		kv:      kv,
		timeout: time.Millisecond * 500,
	}

	node.Handle("add", func(msg maelstrom.Message) error {
		var addMsg add
		if err := json.Unmarshal(msg.Body, &addMsg); err != nil {
			return fmt.Errorf("unmarshalling: %v", err)
		}

		c.add(addMsg.Delta)

		return node.Reply(msg, map[string]string{
			"type": "add_ok",
		})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		return node.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": c.read(),
		})
	})

	go func() {
		i := 0
		for {
			time.Sleep(time.Second)
			log.Printf("Current State %d: %d %d \n", i, c.globalValue, c.currDelta)
			c.add(0)
			i++
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatalf("Unable to run: %v", err)
	}
}
