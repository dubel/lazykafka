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

// commit and date are injected at release build time via ldflags
// (-X main.commit={{.Commit}} -X main.date={{.Date}}); empty for local builds.
var (
	commit string
	date   string
)

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

// versionInfo returns the --version output: the version plus commit and build
// date when they were injected at release build time.
func versionInfo() string {
	info := getVersion()
	if commit != "" {
		info += " (commit " + commit
		if date != "" {
			info += ", built " + date
		}
		info += ")"
	}
	return info
}

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(versionInfo())
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
