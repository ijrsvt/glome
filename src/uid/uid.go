package uid

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var idSize = 128

func RunMain() {
	node := maelstrom.NewNode()
	node.Handle("generate", func(req maelstrom.Message) error {
		id := make([]byte, idSize)
		if n, err := rand.Read(id); err != nil {
			return fmt.Errorf("gen rand: %v", err)
		} else if n != idSize {
			return errors.New("unable to generate entropy")
		}

		base64.StdEncoding.EncodeToString(id)

		return node.Reply(req, map[string]string{
			"type": "generate_ok",
			"id":   base64.StdEncoding.EncodeToString(id),
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("Error while running: %v", err)
	}
}
