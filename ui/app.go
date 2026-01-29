package ui

import (
	"log"
	"os"
	"sync"

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

	KafkaManager *kafka.Manager
	ManagerMutex sync.RWMutex
	InModal      bool
}

func NewApp() *App {
	cfg, err := config.LoadConfig()
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
	}

	app.BrokersPanel = NewBrokersPanel(app)
	app.TopicsPanel = NewTopicsPanel(app)
	app.ConsumePanel = NewConsumePanel(app)
	app.ProducePanel = NewProducePanel(app)

	// Create footer with centered help text
	footer := tview.NewTextView().
		SetTextAlign(tview.AlignCenter).
		SetText("(esc) to exit | (l)ogs toggle")

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
			} else if focus == a.ProducePanel.View || focus == a.ProducePanel.InputField {
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
		if event.Rune() == 'l' {
			a.showLogViewer()
			return nil
		}

		return event
	})
}

func (a *App) ConnectToBroker(broker string) error {
	log.Printf("ConnectToBroker: Starting connection to broker: %s", broker)

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
		a.ProducePanel.InputField.SetText("")
		a.ProducePanel.CurrentTopic = ""
		if flex, ok := a.ProducePanel.View.(*tview.Flex); ok {
			flex.SetTitle("Produce (F4)")
		}
	})
	log.Println("ConnectToBroker: Cleared produce panel")

	a.ManagerMutex.Lock()
	if a.KafkaManager != nil {
		log.Println("ConnectToBroker: Closing existing Kafka manager")
		a.KafkaManager.Close()
		a.KafkaManager = nil
	}
	a.ManagerMutex.Unlock()

	log.Printf("ConnectToBroker: Creating new Kafka manager for broker: %s", broker)
	manager, err := kafka.NewManager([]string{broker})
	if err != nil {
		log.Printf("ConnectToBroker: ERROR - Failed to create Kafka manager: %v", err)
		return err
	}

	a.ManagerMutex.Lock()
	a.KafkaManager = manager
	a.ManagerMutex.Unlock()

	log.Println("ConnectToBroker: Successfully connected, loading topics")
	a.TopicsPanel.LoadTopics()
	log.Println("ConnectToBroker: Connection complete")
	return nil
}

func (a *App) showLogViewer() {
	logFile := "lazykafka.log"

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
		SetTitle("Application Log (press 'l' or Esc to close)").
		SetTitleAlign(tview.AlignCenter)

	// Handle key presses
	textView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape || event.Rune() == 'l' {
			a.InModal = false
			a.TviewApp.SetRoot(a.Grid, true)
			return nil
		}
		return event
	})

	a.InModal = true
	a.TviewApp.SetRoot(textView, true)
}
