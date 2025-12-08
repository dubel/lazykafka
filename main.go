package main

import (
	"log"
	"os"

	"github.com/dubel/lazykafka/ui"
)

func main() {
	// Setup logging to file in current directory
	logFile := "lazykafka.log"

	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
