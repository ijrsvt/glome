package main

import (
	"flag"
	"log"

	"github.com/ijrsvt/glome/src/broadcast"
	"github.com/ijrsvt/glome/src/echo"
	"github.com/ijrsvt/glome/src/uid"
)

func main() {
	name := flag.String("program", "", "Program to run")
	flag.Parse()

	switch *name {
	case "echo":
		echo.RunEcho()

	case "uid":
		uid.RunMain()

	case "broadcast-simple":
		broadcast.RunSimple()

	case "broadcast":
		broadcast.RunMulti()

	default:
		log.Fatalf("Unexpected argument : %s", *name)
	}

}
