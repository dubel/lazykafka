package ui

import (
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type TopicsPanel struct {
	App      *App
	View     tview.Primitive
	ListView *tview.List
}

func NewTopicsPanel(app *App) *TopicsPanel {
	listView := tview.NewList()

	footer := tview.NewTextView().
		SetText("(a)dd | (r)emove | re(f)resh | (enter) to select").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(listView, 0, 1, true).
		AddItem(footer, 1, 0, false)

	flex.SetBorder(true).
		SetTitle("Topics (F2)").
		SetBorderColor(tcell.ColorWhite)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	// Set focus handlers on the ListView to update the Flex border
	listView.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	listView.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p := &TopicsPanel{
		App:      app,
		View:     flex,
		ListView: listView,
	}

	p.ListView.SetSelectedFunc(func(i int, s1, s2 string, r rune) {
		app.ConsumePanel.StartConsuming(s1)
		app.ProducePanel.SetTopic(s1)
		app.TviewApp.SetFocus(app.ConsumePanel.View)
	})

	// Background refresh
	go func() {
		ticker := time.NewTicker(300 * time.Second)
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
		p.ListView.Clear()
		for _, t := range topics {
			p.ListView.AddItem(t, "", 0, nil)
		}
	})
}
