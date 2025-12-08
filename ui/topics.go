package ui

import (
	"time"

	"github.com/rivo/tview"
)

type TopicsPanel struct {
	App  *App
	View *tview.List
}

func NewTopicsPanel(app *App) *TopicsPanel {
	p := &TopicsPanel{
		App:  app,
		View: tview.NewList(),
	}
	p.View.SetBorder(true).SetTitle("Topics (F2)")

	p.View.SetSelectedFunc(func(i int, s1, s2 string, r rune) {
		app.ConsumePanel.StartConsuming(s1)
		app.ProducePanel.SetTopic(s1)
		app.TviewApp.SetFocus(app.ConsumePanel.View)
	})

	// Background refresh
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			app.TviewApp.QueueUpdateDraw(func() {
				p.LoadTopics()
			})
		}
	}()

	return p
}

func (p *TopicsPanel) LoadTopics() {
	if p.App.KafkaManager == nil {
		return
	}

	topics, err := p.App.KafkaManager.Client.ListTopics()
	if err != nil {
		// Handle error
		return
	}

	p.View.Clear()
	for _, t := range topics {
		p.View.AddItem(t, "", 0, nil)
	}
}
