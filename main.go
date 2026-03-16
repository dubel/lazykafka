package main

import (
	"log"

	"github.com/dubel/lazykafka/config"
	"github.com/dubel/lazykafka/ui"
)

func main() {
	// Setup daily log file with rotation
	f, err := config.SetupLogging()
	if err == nil {
		log.SetOutput(f)
		defer f.Close()
		log.Println("LazyKafka started")
	}

	app := ui.NewApp()
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
