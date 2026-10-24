package ui

import (
	"context"
	"fmt"
	"log"
	"os"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dubel/lazykafka/config"
	"github.com/dubel/lazykafka/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type App struct {
	TviewApp *tview.Application
	Grid     *tview.Grid
	Config   *config.Config
	Settings *config.Settings

	BrokersPanel *BrokersPanel
	TopicsPanel  *TopicsPanel
	ConsumePanel *ConsumePanel
	ProducePanel *ProducePanel

	// manager is the current connection (nil when disconnected). It is read
	// lock-free from any goroutine, including the UI goroutine; a closed
	// manager just returns kafka.ErrManagerClosed, so no lock is held across
	// network calls.
	manager            atomic.Pointer[kafka.Manager]
	InModal            bool
	Version            string
	healthMu           sync.Mutex // guards healthCheckCancel; connect runs off the UI goroutine
	healthCheckCancel  context.CancelFunc
}

// healthCheckFailuresBeforeDisconnect avoids tearing down the session on a
// single slow or dropped metadata request.
const healthCheckFailuresBeforeDisconnect = 3

func NewApp(version string) *App {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Printf("NewApp: ERROR - could not load brokers, starting with an empty list: %v", err)
	}
	if err != nil || cfg == nil {
		cfg = &config.Config{Brokers: []config.Broker{}}
	}

	settings, err := config.LoadSettings()
	if err != nil {
		// Fallback defaults if load fails (though LoadSettings handles file not found)
		settings = &config.Settings{ShowSplashOnStartup: true}
	}

	app := &App{
		TviewApp: tview.NewApplication(),
		Config:   cfg,
		Settings: settings,
		Version:  version,
	}

	app.BrokersPanel = NewBrokersPanel(app)
	app.TopicsPanel = NewTopicsPanel(app)
	app.ConsumePanel = NewConsumePanel(app)
	app.ProducePanel = NewProducePanel(app)

	// Create footer with centered help text
	footer := tview.NewTextView().
		SetTextAlign(tview.AlignCenter).
		SetText("(esc) to exit | (F5) logs | (F6) splash")

	app.Grid = tview.NewGrid().
		SetRows(0, 0, 1).
		SetColumns(0, 0).
		SetBorders(true).
		AddItem(app.BrokersPanel.View, 0, 0, 1, 1, 0, 0, true).
		AddItem(app.TopicsPanel.View, 0, 1, 1, 1, 0, 0, false).
		AddItem(app.ConsumePanel.View, 1, 0, 1, 1, 0, 0, false).
		AddItem(app.ProducePanel.View, 1, 1, 1, 1, 0, 0, false).
		AddItem(footer, 2, 0, 1, 2, 0, 0, false)

	if app.Settings.ShowSplashOnStartup {
		splash := NewSplashPanel(app, app.Settings)
		app.TviewApp.SetRoot(splash.Flex, true).EnableMouse(true)
		app.InModal = true
	} else {
		app.TviewApp.SetRoot(app.Grid, true).EnableMouse(true)
	}

	app.setupNavigation()

	return app
}

// Manager returns the current Kafka connection, or nil when not connected.
func (a *App) Manager() *kafka.Manager {
	return a.manager.Load()
}

func (a *App) Run() error {
	return a.TviewApp.Run()
}

func (a *App) setupNavigation() {
	a.TviewApp.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Only handle navigation when not in a modal
		if a.InModal {
			return event
		}

		// Tab cycling
		if event.Key() == tcell.KeyTab {
			focus := a.TviewApp.GetFocus()
			// Check if focus is on any component within the panels
			if focus == a.BrokersPanel.View || focus == a.BrokersPanel.ListView {
				a.TviewApp.SetFocus(a.TopicsPanel.View)
			} else if focus == a.TopicsPanel.View || focus == a.TopicsPanel.ListView {
				a.TviewApp.SetFocus(a.ConsumePanel.View)
			} else if focus == a.ConsumePanel.View || focus == a.ConsumePanel.TextView {
				a.TviewApp.SetFocus(a.ProducePanel.View)
			} else if focus == a.ProducePanel.View || focus == a.ProducePanel.TextArea {
				a.TviewApp.SetFocus(a.BrokersPanel.View)
			} else {
				a.TviewApp.SetFocus(a.BrokersPanel.View)
			}
			return nil
		}

		// Direct navigation
		switch event.Key() {
		case tcell.KeyF1:
			a.TviewApp.SetFocus(a.BrokersPanel.View)
			return nil
		case tcell.KeyF2:
			a.TviewApp.SetFocus(a.TopicsPanel.View)
			return nil
		case tcell.KeyF3:
			a.TviewApp.SetFocus(a.ConsumePanel.View)
			return nil
		case tcell.KeyF4:
			a.TviewApp.SetFocus(a.ProducePanel.View)
			return nil
		}

		// Log viewer
		if event.Key() == tcell.KeyF5 {
			a.showLogViewer()
			return nil
		}

		// Splash screen
		if event.Key() == tcell.KeyF6 {
			a.showSplashScreen()
			return nil
		}

		// Escape: if Topics search is active, clear it instead of exiting.
		if event.Key() == tcell.KeyEscape {
			focus := a.TviewApp.GetFocus()

			// If the search input is focused, hide it and return to the list.
			if focus == a.TopicsPanel.SearchInput {
				a.TopicsPanel.MainFlex.ResizeItem(a.TopicsPanel.SearchInput, 0, 0)
				a.TviewApp.SetFocus(a.TopicsPanel.ListView)
				return nil
			}

			// If the topics list is focused and a filter is active, clear the filter.
			if (focus == a.TopicsPanel.View || focus == a.TopicsPanel.ListView) && a.TopicsPanel.SearchQuery != "" {
				a.TopicsPanel.SearchQuery = ""
				a.TopicsPanel.rebuildList()
				a.TopicsPanel.Footer.SetText(a.TopicsPanel.defaultFooterText())
				return nil
			}

			a.TviewApp.Stop()
			return nil
		}

		return event
	})
}

func (a *App) ConnectToBroker(broker string) error {
	log.Printf("ConnectToBroker: Starting connection to broker: %s", broker)

	// Cancel any running health-check before replacing the connection.
	a.stopHealthCheck()

	// Stop any active consumption
	a.ConsumePanel.StopConsuming()
	log.Println("ConnectToBroker: Stopped any active consumption")

	// Clear the consume panel
	a.TviewApp.QueueUpdateDraw(func() {
		a.ConsumePanel.TextView.Clear()
		a.ConsumePanel.CurrentTopic = ""
	})
	log.Println("ConnectToBroker: Cleared consume panel")

	// Clear the produce panel
	a.TviewApp.QueueUpdateDraw(func() {
		a.ProducePanel.TextArea.SetText("", true)
		a.ProducePanel.CurrentTopic = ""
		if flex, ok := a.ProducePanel.View.(*tview.Flex); ok {
			flex.SetTitle("Produce (F4)")
		}
	})
	log.Println("ConnectToBroker: Cleared produce panel")

	// Detach the old connection first so nothing new starts on it. Close does
	// not wait for operations still running against a hung broker.
	if old := a.manager.Swap(nil); old != nil {
		log.Println("ConnectToBroker: Closing existing Kafka manager")
		old.Close()
	}

	log.Printf("ConnectToBroker: Creating new Kafka manager for broker: %s", broker)
	manager, err := kafka.NewManager([]string{broker})
	if err != nil {
		log.Printf("ConnectToBroker: ERROR - Failed to create Kafka manager: %v", err)
		return err
	}

	// A concurrent connect may have stored its manager meanwhile; last one wins.
	if old := a.manager.Swap(manager); old != nil {
		old.Close()
	}

	// Monitor the new connection in the background.
	a.startHealthCheck(broker, manager)

	log.Println("ConnectToBroker: Successfully connected, loading topics")
	a.TopicsPanel.LoadTopics()
	log.Println("ConnectToBroker: Connection complete")
	return nil
}

func (a *App) showLogViewer() {
	logFile, err := config.GetLogFilePath()
	if err != nil {
		logFile = "lazykafka.log" // fallback
	}

	// Read log file
	content, err := os.ReadFile(logFile)
	logText := string(content)
	if err != nil {
		logText = "No log file found or error reading log: " + err.Error()
	}

	textView := tview.NewTextView().
		SetText(logText).
		SetScrollable(true).
		SetDynamicColors(true)

	textView.ScrollToEnd()

	textView.SetBorder(true).
		SetTitle("Application Log (press F5 or Esc to close)").
		SetTitleAlign(tview.AlignCenter)

	// Handle key presses
	textView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape || event.Key() == tcell.KeyF5 {
			a.InModal = false
			a.TviewApp.SetRoot(a.Grid, true)
			return nil
		}
		return event
	})

	a.InModal = true
	a.TviewApp.SetRoot(textView, true)
}

func (a *App) showSplashScreen() {
	splash := NewSplashPanel(a, a.Settings)
	a.InModal = true
	a.TviewApp.SetRoot(splash.Flex, true)
}

// startHealthCheck launches a background goroutine that pings the connected
// broker every 5 seconds.  On the first failure it calls handleConnectionLost.
func (a *App) startHealthCheck(brokerAddress string, mgr *kafka.Manager) {
	a.stopHealthCheck() // ensure no previous check is still running

	ctx, cancel := context.WithCancel(context.Background())
	a.healthMu.Lock()
	a.healthCheckCancel = cancel
	a.healthMu.Unlock()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		failures := 0
		for {
			select {
			case <-ticker.C:
				if a.Manager() != mgr {
					return // disconnected or replaced, nothing to monitor
				}
				if err := mgr.CheckConnection(); err != nil {
					if errors.Is(err, kafka.ErrManagerClosed) {
						return
					}
					failures++
					log.Printf("HealthCheck: check %d/%d for %s failed: %v",
						failures, healthCheckFailuresBeforeDisconnect, brokerAddress, err)
					if failures >= healthCheckFailuresBeforeDisconnect {
						log.Printf("HealthCheck: connection to %s lost", brokerAddress)
						a.handleConnectionLost(brokerAddress, mgr)
						return
					}
					continue
				}
				failures = 0
			case <-ctx.Done():
				return
			}
		}
	}()
}

// stopHealthCheck cancels any running health-check goroutine.
func (a *App) stopHealthCheck() {
	a.healthMu.Lock()
	defer a.healthMu.Unlock()
	if a.healthCheckCancel != nil {
		a.healthCheckCancel()
		a.healthCheckCancel = nil
	}
}

// handleConnectionLost tears down the broken connection, clears all dependent
// UI state, and shows a dismissible modal alerting the user.
func (a *App) handleConnectionLost(brokerAddress string, mgr *kafka.Manager) {
	// Only tear down the connection that actually failed: if the user has
	// reconnected meanwhile, the new manager must stay.
	if !a.manager.CompareAndSwap(mgr, nil) {
		return
	}
	mgr.Close()

	// Stop any active consumption (safe to call from a goroutine).
	a.ConsumePanel.StopConsuming()

	a.TviewApp.QueueUpdateDraw(func() {
		// --- Topics panel ---
		a.TopicsPanel.ListView.Clear()
		a.TopicsPanel.AllTopics = []string{}
		a.TopicsPanel.Topics = []string{}
		a.TopicsPanel.SelectedIndex = -1
		a.TopicsPanel.SelectedTopic = ""
		a.TopicsPanel.SearchQuery = ""
		a.TopicsPanel.Footer.SetText(a.TopicsPanel.defaultFooterText())

		// --- Consume panel ---
		a.ConsumePanel.TextView.Clear()
		a.ConsumePanel.CurrentTopic = ""
		if flex, ok := a.ConsumePanel.View.(*tview.Flex); ok {
			flex.SetTitle("Consume (F3)")
		}

		// --- Produce panel ---
		a.ProducePanel.TextArea.SetText("", true)
		a.ProducePanel.CurrentTopic = ""
		if flex, ok := a.ProducePanel.View.(*tview.Flex); ok {
			flex.SetTitle("Produce (F4)")
		}

		// --- Brokers panel ---
		a.BrokersPanel.ConnectedAddress = ""
		a.BrokersPanel.refreshList()

		// Show the modal unless another modal is already on screen.
		if !a.InModal {
			a.showConnectionLostModal(brokerAddress)
		}
	})
}

// showConnectionLostModal renders a small centered modal over a dark background.
// It is dismissed by any key press, after which focus returns to the Brokers panel.
func (a *App) showConnectionLostModal(brokerAddress string) {
	message := fmt.Sprintf(
		"[red]Connection to broker\n\n[white::b]%s[red::-]\n\nhas been lost.\n\n"+
			"[white]Please re-connect via the Brokers panel.\n\n"+
			"[gray](press any key to dismiss)",
		tview.Escape(brokerAddress),
	)

	textView := tview.NewTextView().
		SetText(message).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	textView.SetBorder(true).
		SetTitle("  ⚠  Connection Lost  ⚠  ").
		SetTitleAlign(tview.AlignCenter).
		SetBorderColor(tcell.ColorRed)

	textView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		a.InModal = false
		a.TviewApp.SetRoot(a.Grid, true)
		a.TviewApp.SetFocus(a.BrokersPanel.View)
		return nil
	})

	// Centre the box: proportional spacers on all four sides.
	modal := tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(textView, 11, 0, true).
			AddItem(nil, 0, 1, false),
			62, 0, true).
		AddItem(nil, 0, 1, false)

	a.InModal = true
	a.TviewApp.SetRoot(modal, true)
}

