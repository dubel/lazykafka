package ui

import (
	"log"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ProducePanel struct {
	App          *App
	View         tview.Primitive
	InputField   *tview.InputField
	CurrentTopic string
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
	}

	p.InputField.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			text := p.InputField.GetText()

			p.App.ManagerMutex.RLock()
			if text == "" || p.CurrentTopic == "" || p.App.KafkaManager == nil {
				if text == "" {
					log.Println("ProduceMessage: Empty message, ignoring")
				} else if p.CurrentTopic == "" {
					log.Println("ProduceMessage: No topic selected, ignoring")
				} else if p.App.KafkaManager == nil {
					log.Println("ProduceMessage: ERROR - KafkaManager is nil")
				}
				p.App.ManagerMutex.RUnlock()
				return
			}

			log.Printf("ProduceMessage: Sending message to topic '%s': %s", p.CurrentTopic, text)
			err := p.App.KafkaManager.ProduceMessage(p.CurrentTopic, text)
			p.App.ManagerMutex.RUnlock()

			if err != nil {
				log.Printf("ProduceMessage: ERROR - Failed to produce message to topic '%s': %v", p.CurrentTopic, err)
			} else {
				log.Printf("ProduceMessage: Successfully sent message to topic '%s'", p.CurrentTopic)
				p.InputField.SetText("")
			}
		}
	})

	return p
}

func (p *ProducePanel) SetTopic(topic string) {
	p.CurrentTopic = topic
	if flex, ok := p.View.(*tview.Flex); ok {
		flex.SetTitle("Produce (F4) - " + topic)
	}
}
