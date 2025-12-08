package ui

import (
	"github.com/dubel/lazykafka/config"
	"github.com/dubel/lazykafka/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type App struct {
	TviewApp *tview.Application
	Grid     *tview.Grid
	Config   *config.Config

	BrokersPanel *BrokersPanel
	TopicsPanel  *TopicsPanel
	ConsumePanel *ConsumePanel
	ProducePanel *ProducePanel

	KafkaManager *kafka.Manager
	InModal      bool
}

func NewApp() *App {
	cfg, err := config.LoadConfig()
	if err != nil || cfg == nil {
		cfg = &config.Config{Brokers: []config.Broker{}}
	}

	app := &App{
		TviewApp: tview.NewApplication(),
		Config:   cfg,
	}

	app.BrokersPanel = NewBrokersPanel(app)
	app.TopicsPanel = NewTopicsPanel(app)
	app.ConsumePanel = NewConsumePanel(app)
	app.ProducePanel = NewProducePanel(app)

	app.Grid = tview.NewGrid().
		SetRows(0, 0).
		SetColumns(0, 0).
		SetBorders(true).
		AddItem(app.BrokersPanel.View, 0, 0, 1, 1, 0, 0, true).
		AddItem(app.TopicsPanel.View, 0, 1, 1, 1, 0, 0, false).
		AddItem(app.ConsumePanel.View, 1, 0, 1, 1, 0, 0, false).
		AddItem(app.ProducePanel.View, 1, 1, 1, 1, 0, 0, false)

	app.TviewApp.SetRoot(app.Grid, true).EnableMouse(true)
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
			if focus == a.BrokersPanel.View {
				a.TviewApp.SetFocus(a.TopicsPanel.View)
			} else if focus == a.TopicsPanel.View {
				a.TviewApp.SetFocus(a.ConsumePanel.View)
			} else if focus == a.ConsumePanel.View {
				a.TviewApp.SetFocus(a.ProducePanel.View)
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

		return event
	})
}

func (a *App) ConnectToBroker(broker string) error {
	if a.KafkaManager != nil {
		a.KafkaManager.Close()
	}

	manager, err := kafka.NewManager([]string{broker})
	if err != nil {
		return err
	}

	a.KafkaManager = manager
	a.TopicsPanel.LoadTopics()
	return nil
}
