package main

import (
	_ "embed"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/dubel/lazykafka/config"
	"github.com/dubel/lazykafka/ui"
)

// version is injected at release build time via ldflags:
//
//	-X main.version={{.Version}}
//
// For local builds it falls back to the embedded current-version file.
var version string

//go:embed current-version
var embeddedVersion string

// getVersion returns the version string, preferring the ldflags-injected value
// and falling back to the embedded current-version file for local dev builds.
func getVersion() string {
	if v := strings.TrimSpace(version); v != "" {
		return v
	}
	return strings.TrimSpace(embeddedVersion)
}

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(getVersion())
		return
	}

	// Setup daily log file with rotation
	f, err := config.SetupLogging()
	if err == nil {
		log.SetOutput(f)
		defer f.Close()
		log.Println("LazyKafka started")
	}

	app := ui.NewApp(getVersion())
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
