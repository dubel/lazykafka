package ui

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ConsumePanel struct {
	App           *App
	View          tview.Primitive
	TextView      *tview.TextView
	CurrentTopic  string
	FromBeginning bool
	StopChan      chan struct{}
}

func NewConsumePanel(app *App) *ConsumePanel {
	textView := tview.NewTextView()
	textView.SetScrollable(true)
	textView.SetDynamicColors(true)
	textView.SetChangedFunc(func() {
		app.TviewApp.Draw()
	})

	footer := tview.NewTextView().
		SetText("(t)oggle from beginning").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(textView, 0, 1, true).
		AddItem(footer, 1, 0, false)

	flex.SetBorder(true).SetTitle("Consume (F3)")

	p := &ConsumePanel{
		App:      app,
		View:     flex,
		TextView: textView,
	}

	p.TextView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == 't' {
			p.FromBeginning = !p.FromBeginning
			status := "New Messages"
			if p.FromBeginning {
				status = "From Beginning"
			}
			flex.SetTitle(fmt.Sprintf("Consume (F3) (%s)", status))
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
	p.TextView.Clear()
	p.TextView.SetTitle(fmt.Sprintf("Consume (F3) - %s", topic))

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
					fmt.Fprintln(p.TextView, msg)
				})
			case err := <-errors:
				p.App.TviewApp.QueueUpdateDraw(func() {
					fmt.Fprintf(p.TextView, "[red]Error: %v[white]\n", err)
				})
			case <-p.StopChan:
				return
			}
		}
	}()
}
