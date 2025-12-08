package ui

import (
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
			if text == "" || p.CurrentTopic == "" || p.App.KafkaManager == nil {
				return
			}

			err := p.App.KafkaManager.ProduceMessage(p.CurrentTopic, text)
			if err != nil {
				// Show error (maybe in consume panel or a modal)
			} else {
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
