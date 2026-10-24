package ui

import (
	"errors"
	"log"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

var errNotConnected = errors.New("not connected to a broker")

type ProducePanel struct {
	App          *App
	View         tview.Primitive
	TextArea     *tview.TextArea
	Footer       *tview.TextView
	CurrentTopic string
	ResetTimer   *time.Timer
}

func NewProducePanel(app *App) *ProducePanel {
	textArea := tview.NewTextArea()
	textArea.SetPlaceholder("Type (or paste) your message here...")
	textArea.SetWrap(true)
	textArea.SetWordWrap(true)

	footer := tview.NewTextView().
		SetText("(alt+enter/ctrl+s) send | (ctrl+p) publish file").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(textArea, 0, 1, true).
		AddItem(footer, 1, 0, false)

	flex.SetBorder(true).
		SetTitle("Produce (F4)").
		SetBorderColor(tcell.ColorWhite)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	// Set focus handlers on the TextArea to update the Flex border
	textArea.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	textArea.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p := &ProducePanel{
		App:      app,
		View:     flex,
		TextArea: textArea,
		Footer:   footer,
	}

	p.TextArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Ctrl+P to open file picker
		if event.Key() == tcell.KeyCtrlP {
			p.openFilePicker()
			return nil
		}

		// Alt+Enter or Ctrl+S to send message
		isAltEnter := event.Key() == tcell.KeyEnter && event.Modifiers()&tcell.ModAlt != 0
		isCtrlS := event.Key() == tcell.KeyCtrlS
		if isAltEnter || isCtrlS {
			text := p.TextArea.GetText()
			topic := p.CurrentTopic

			if topic == "" || p.App.Manager() == nil {
				p.showTemporaryMessage("[red]No broker and topic selected - we need to connect first!", 5*time.Second)

				if topic == "" {
					log.Println("ProduceMessage: No topic selected, ignoring")
				} else {
					log.Println("ProduceMessage: ERROR - not connected")
				}
				return nil
			}

			if text == "" {
				log.Println("ProduceMessage: Empty message, ignoring")
				return nil
			}

			// Send off the UI goroutine: a slow or unreachable broker must not freeze input.
			p.showTemporaryMessage("[yellow]Sending...", 15*time.Second)
			go func() {
				mgr := p.App.Manager()
				err := errNotConnected
				if mgr != nil {
					log.Printf("ProduceMessage: Sending %d bytes to topic '%s'", len(text), topic)
					err = mgr.ProduceMessage(topic, text)
				}

				p.App.TviewApp.QueueUpdateDraw(func() {
					if err != nil {
						log.Printf("ProduceMessage: ERROR - Failed to produce message to topic '%s': %v", topic, err)
						p.showTemporaryMessage("[red]Failed to send message!", 5*time.Second)
						return
					}
					log.Printf("ProduceMessage: Successfully sent message to topic '%s'", topic)
					// Keep anything the user typed while the send was in flight.
					if p.TextArea.GetText() == text {
						p.TextArea.SetText("", true)
					}
					p.showTemporaryMessage("[green]Message sent!", 2*time.Second)
				})
			}()
			return nil
		}
		return event
	})

	return p
}

func (p *ProducePanel) showTemporaryMessage(msg string, duration time.Duration) {
	p.Footer.SetText(msg)

	if p.ResetTimer != nil {
		p.ResetTimer.Stop()
	}

	p.ResetTimer = time.AfterFunc(duration, func() {
		p.App.TviewApp.QueueUpdateDraw(func() {
			p.Footer.SetText("(alt+enter/ctrl+s) send | (ctrl+p) publish file")
		})
	})
}

func (p *ProducePanel) SetTopic(topic string) {
	p.CurrentTopic = topic
	if flex, ok := p.View.(*tview.Flex); ok {
		flex.SetTitle("Produce (F4) - " + topic)
	}
}
