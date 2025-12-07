package ui

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ConsumePanel struct {
	App           *App
	View          *tview.TextView
	CurrentTopic  string
	FromBeginning bool
	StopChan      chan struct{}
}

func NewConsumePanel(app *App) *ConsumePanel {
	p := &ConsumePanel{
		App:  app,
		View: tview.NewTextView(),
	}
	p.View.SetBorder(true).SetTitle("Consume [3]")
	p.View.SetScrollable(true)
	p.View.SetDynamicColors(true)
	p.View.SetChangedFunc(func() {
		app.TviewApp.Draw()
	})

	p.View.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == 't' {
			p.FromBeginning = !p.FromBeginning
			status := "New Messages"
			if p.FromBeginning {
				status = "From Beginning"
			}
			p.View.SetTitle(fmt.Sprintf("Consume [3] (%s)", status))
			// Restart consumption if topic is selected
			if p.CurrentTopic != "" {
				p.StartConsuming(p.CurrentTopic)
			}
			return nil
		}
		return event
	})

	return p
}

func (p *ConsumePanel) StartConsuming(topic string) {
	if p.StopChan != nil {
		close(p.StopChan)
		p.StopChan = nil
	}

	p.CurrentTopic = topic
	p.View.Clear()
	p.View.SetTitle(fmt.Sprintf("Consume [3] - %s", topic))

	if p.App.KafkaManager == nil {
		return
	}

	p.StopChan = make(chan struct{})
	messages := make(chan string)
	errors := make(chan error)

	go p.App.KafkaManager.ConsumeTopic(topic, p.FromBeginning, messages, errors, p.StopChan)

	go func() {
		for {
			select {
			case msg := <-messages:
				p.App.TviewApp.QueueUpdateDraw(func() {
					fmt.Fprintln(p.View, msg)
				})
			case err := <-errors:
				p.App.TviewApp.QueueUpdateDraw(func() {
					fmt.Fprintf(p.View, "[red]Error: %v[white]\n", err)
				})
			case <-p.StopChan:
				return
			}
		}
	}()
}
