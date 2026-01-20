package ui

import (
	"context"
	"fmt"
	"log"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ConsumePanel struct {
	App           *App
	View          tview.Primitive
	TextView      *tview.TextView
	CurrentTopic  string
	FromBeginning bool
	cancelFunc    context.CancelFunc // Replaces StopChan for cleaner cancellation
}

func NewConsumePanel(app *App) *ConsumePanel {
	textView := tview.NewTextView()
	textView.SetScrollable(true)
	textView.SetDynamicColors(true)
	textView.SetChangedFunc(func() {
		textView.ScrollToEnd()
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

	flex.SetBorder(true).
		SetTitle("Consume (F3)").
		SetBorderColor(tcell.ColorWhite)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p := &ConsumePanel{
		App:      app,
		View:     flex,
		TextView: textView,
	}

	// Set focus handlers on the TextView to update the Flex border
	p.TextView.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	p.TextView.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

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
	log.Printf("StartConsuming: Starting consumption for topic: %s, FromBeginning: %v", topic, p.FromBeginning)

	// Cancel any previous consumption
	if p.cancelFunc != nil {
		log.Println("StartConsuming: Cancelling previous consumption")
		p.cancelFunc()
		p.cancelFunc = nil
	}

	p.CurrentTopic = topic
	p.TextView.Clear()
	p.TextView.SetTitle(fmt.Sprintf("Consume (F3) - %s", topic))

	p.App.ManagerMutex.RLock()
	if p.App.KafkaManager == nil {
		p.App.ManagerMutex.RUnlock()
		log.Println("StartConsuming: ERROR - KafkaManager is nil, cannot consume")
		return
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel

	messages := make(chan string)
	errors := make(chan error)

	go p.App.KafkaManager.ConsumeTopic(ctx, topic, p.FromBeginning, messages, errors)
	p.App.ManagerMutex.RUnlock()

	log.Printf("StartConsuming: Started consuming messages from topic: %s", topic)

	go func() {
		messageCount := 0
		for {
			select {
			case msg := <-messages:
				messageCount++
				if messageCount%100 == 0 {
					log.Printf("StartConsuming: Received %d messages from topic: %s", messageCount, topic)
				}
				p.App.TviewApp.QueueUpdateDraw(func() {
					fmt.Fprintln(p.TextView, msg)
				})
			case err := <-errors:
				log.Printf("StartConsuming: ERROR - Consumption error for topic %s: %v", topic, err)
				p.App.TviewApp.QueueUpdateDraw(func() {
					fmt.Fprintf(p.TextView, "[red]Error: %v[white]\n", err)
				})
			case <-ctx.Done():
				log.Printf("StartConsuming: Stopped consuming from topic: %s (received %d messages)", topic, messageCount)
				return
			}
		}
	}()
}

// StopConsuming cancels any active message consumption
func (p *ConsumePanel) StopConsuming() {
	if p.cancelFunc != nil {
		log.Println("StopConsuming: Cancelling active consumption")
		p.cancelFunc()
		p.cancelFunc = nil
	}
}
