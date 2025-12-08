package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/dubel/lazykafka/ui"
)

func main() {
	// Setup logging to file
	logDir := ".lazykafka"
	os.MkdirAll(logDir, 0755)
	logFile := filepath.Join(logDir, "app.log")

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
