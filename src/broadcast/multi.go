package broadcast

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastHandler struct {
	node *maelstrom.Node
	seen sync.Map

	mu             sync.Mutex
	enableBatching bool
	currBatch      []int
}

func (b *BroadcastHandler) sendShare(neighbor string, body json.RawMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	if _, err := b.node.SyncRPC(ctx, neighbor, body); err != nil {
		log.Printf("Failed to send to neighbor: %s\n", neighbor)
		b.sendShare(neighbor, body)
	}
	cancel()
}

func (b *BroadcastHandler) batchSend() {
	getBatch := func(prevTime time.Time) []int {
		b.mu.Lock()
		defer b.mu.Unlock()

		if len(b.currBatch) == 0 {
			return []int{}
		} else if len(b.currBatch) < 4 && time.Since(prevTime) < time.Millisecond*1500 {
			// if the batch is small, wait a bit
			return []int{}
		}

		currBatchCopy := make([]int, len(b.currBatch))
		copy(currBatchCopy, b.currBatch)

		b.currBatch = []int{}

		log.Printf("Preparing batch: %v", currBatchCopy)
		return currBatchCopy
	}

	prev := time.Now()
	for {
		time.Sleep(time.Millisecond * 200)

		output := map[string]any{
			"type": "share",
		}

		batch := getBatch(prev)
		if len(batch) == 0 {
			continue
		}
		log.Printf("Batch size: %v, last batch was: %v\n", len(batch), time.Since(prev))

		output["messages"] = batch
		msg, err := json.Marshal(output)
		if err != nil {
			log.Fatalf("error marshalling: %v\n", err)
			continue
		}

		neighbors := b.node.NodeIDs()
		for _, n := range neighbors {
			if n != b.node.ID() {
				go b.sendShare(n, msg)
			}
		}
		prev = time.Now()
	}

}

func (b *BroadcastHandler) handleBroadcast(msg maelstrom.Message) error {
	type broadcast struct {
		Message int
	}
	var body broadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshaling body: %w", err)
	}

	_, exists := b.seen.LoadOrStore(body.Message, struct{}{})

	if b.enableBatching {
		b.mu.Lock()
		b.currBatch = append(b.currBatch, body.Message)
		b.mu.Unlock()

	} else {
		isNode := slices.Contains(b.node.NodeIDs(), msg.Src) // should always be true

		if !exists && !isNode {
			newMsg, err := json.Marshal(map[string]any{
				"type":    "share",
				"message": body.Message,
			})
			if err != nil {
				return fmt.Errorf("new serialized message: %v", err)
			}

			neighbors := b.node.NodeIDs()
			for _, n := range neighbors {
				if n != msg.Src && n != b.node.ID() {
					go b.sendShare(n, newMsg)
				}
			}
		}
	}

	return b.node.Reply(msg, map[string]string{
		"type": "broadcast_ok",
	})
}

func (b *BroadcastHandler) handleShare(msg maelstrom.Message) error {
	type broadcast struct {
		Message  int
		Messages []int
	}
	var body broadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshaling body: %w", err)
	}

	if len(body.Messages) > 0 {
		for _, msg := range body.Messages {
			b.seen.Store(msg, struct{}{})
		}
		if body.Message != 0 {
			log.Fatalf("when `messages` is set, no `message` expected for :%v", msg.Body)
		}
	} else {
		b.seen.Store(msg, struct{}{})
	}

	return b.node.Reply(msg, map[string]string{})
}

func (b *BroadcastHandler) handleRead(msg maelstrom.Message) error {
	resp := map[string]any{
		"type": "read_ok",
	}

	var values []int

	b.seen.Range(func(key, _ any) bool {
		values = append(values, (key).(int))
		return true
	})

	resp["messages"] = values
	return b.node.Reply(msg, resp)
}

func (b *BroadcastHandler) handleTopoloy(msg maelstrom.Message) error {
	return b.node.Reply(msg, map[string]string{
		"type": "topology_ok",
	})
}

func NewHandler(n *maelstrom.Node, batched bool) *BroadcastHandler {
	b := &BroadcastHandler{
		node:           n,
		mu:             sync.Mutex{},
		seen:           sync.Map{},
		enableBatching: batched,
		currBatch:      []int{},
	}

	if b.enableBatching {
		go b.batchSend()
	}

	return b
}

func RunMulti(batched bool) {
	node := maelstrom.NewNode()
	h := NewHandler(node, batched)

	node.Handle("broadcast", h.handleBroadcast)
	node.Handle("read", h.handleRead)

	node.Handle("topology", h.handleTopoloy)

	node.Handle("share", h.handleShare)

	if err := node.Run(); err != nil {
		log.Fatalf("Unable to run: %v", err)
	}
}
