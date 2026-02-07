package ui

import (
	"log"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

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
	textArea.SetPlaceholder("Type your message here...")
	textArea.SetWrap(true)
	textArea.SetWordWrap(true)

	footer := tview.NewTextView().
		SetText("(alt+enter) or (ctrl+s) to send").
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
		// Alt+Enter or Ctrl+S to send message
		isAltEnter := event.Key() == tcell.KeyEnter && event.Modifiers()&tcell.ModAlt != 0
		isCtrlS := event.Key() == tcell.KeyCtrlS
		if isAltEnter || isCtrlS {
			text := p.TextArea.GetText()

			p.App.ManagerMutex.RLock()
			if p.CurrentTopic == "" || p.App.KafkaManager == nil {
				p.App.ManagerMutex.RUnlock()

				p.showTemporaryMessage("[red]No broker and topic selected - we need to connect first!", 5*time.Second)

				if p.CurrentTopic == "" {
					log.Println("ProduceMessage: No topic selected, ignoring")
				} else if p.App.KafkaManager == nil {
					log.Println("ProduceMessage: ERROR - KafkaManager is nil")
				}
				return nil
			}

			if text == "" {
				p.App.ManagerMutex.RUnlock()
				log.Println("ProduceMessage: Empty message, ignoring")
				return nil
			}

			log.Printf("ProduceMessage: Sending message to topic '%s': %s", p.CurrentTopic, text)
			err := p.App.KafkaManager.ProduceMessage(p.CurrentTopic, text)
			p.App.ManagerMutex.RUnlock()

			if err != nil {
				log.Printf("ProduceMessage: ERROR - Failed to produce message to topic '%s': %v", p.CurrentTopic, err)
				p.showTemporaryMessage("[red]Failed to send message!", 5*time.Second)
			} else {
				log.Printf("ProduceMessage: Successfully sent message to topic '%s'", p.CurrentTopic)
				p.TextArea.SetText("", true)
				p.showTemporaryMessage("[green]Message sent!", 2*time.Second)
			}
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
			p.Footer.SetText("(alt+enter) or (ctrl+s) to send")
		})
	})
}

func (p *ProducePanel) SetTopic(topic string) {
	p.CurrentTopic = topic
	if flex, ok := p.View.(*tview.Flex); ok {
		flex.SetTitle("Produce (F4) - " + topic)
	}
}
