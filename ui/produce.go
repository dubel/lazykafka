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
	InputField   *tview.InputField
	Footer       *tview.TextView
	CurrentTopic string
	ResetTimer   *time.Timer
}

func NewProducePanel(app *App) *ProducePanel {
	inputField := tview.NewInputField()
	inputField.SetLabel("Message: ")

	footer := tview.NewTextView().
		SetText("(enter) to send a message").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(inputField, 0, 1, true).
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

	// Set focus handlers on the InputField to update the Flex border
	inputField.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	inputField.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p := &ProducePanel{
		App:        app,
		View:       flex,
		InputField: inputField,
		Footer:     footer,
	}

	p.InputField.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			text := p.InputField.GetText()

			p.App.ManagerMutex.RLock()
			if p.CurrentTopic == "" || p.App.KafkaManager == nil {
				p.App.ManagerMutex.RUnlock()

				p.showTemporaryMessage("[red]No broker and topic selected - we need to connect first!", 5*time.Second)

				if p.CurrentTopic == "" {
					log.Println("ProduceMessage: No topic selected, ignoring")
				} else if p.App.KafkaManager == nil {
					log.Println("ProduceMessage: ERROR - KafkaManager is nil")
				}
				return
			}

			if text == "" {
				p.App.ManagerMutex.RUnlock()
				log.Println("ProduceMessage: Empty message, ignoring")
				return
			}

			log.Printf("ProduceMessage: Sending message to topic '%s': %s", p.CurrentTopic, text)
			err := p.App.KafkaManager.ProduceMessage(p.CurrentTopic, text)
			p.App.ManagerMutex.RUnlock()

			if err != nil {
				log.Printf("ProduceMessage: ERROR - Failed to produce message to topic '%s': %v", p.CurrentTopic, err)
				p.showTemporaryMessage("[red]Failed to send message!", 5*time.Second)
			} else {
				log.Printf("ProduceMessage: Successfully sent message to topic '%s'", p.CurrentTopic)
				p.InputField.SetText("")
				p.showTemporaryMessage("[green]Message sent!", 2*time.Second)
			}
		}
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
			p.Footer.SetText("(enter) to send a message")
		})
	})
}

func (p *ProducePanel) SetTopic(topic string) {
	p.CurrentTopic = topic
	if flex, ok := p.View.(*tview.Flex); ok {
		flex.SetTitle("Produce (F4) - " + topic)
	}
}
