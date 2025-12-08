package ui

import (
	"time"

	"github.com/gdamore/tcell/v2"
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
	p.View.SetBorder(true).
		SetTitle("Topics (F2)").
		SetBorderColor(tcell.ColorWhite)

	p.View.SetFocusFunc(func() {
		p.View.SetBorderColor(tcell.ColorGreen)
	})
	p.View.SetBlurFunc(func() {
		p.View.SetBorderColor(tcell.ColorWhite)
	})

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
			p.LoadTopics()
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

	p.App.TviewApp.QueueUpdateDraw(func() {
		p.View.Clear()
		for _, t := range topics {
			p.View.AddItem(t, "", 0, nil)
		}
	})
}
