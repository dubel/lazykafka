package main

import (
	"log"

	"github.com/dubel/lazykafka/ui"
)

func main() {
	app := ui.NewApp()
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
