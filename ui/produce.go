package ui

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ProducePanel struct {
	App          *App
	View         *tview.InputField
	CurrentTopic string
}

func NewProducePanel(app *App) *ProducePanel {
	p := &ProducePanel{
		App:  app,
		View: tview.NewInputField(),
	}
	p.View.SetBorder(true).SetTitle("Produce (F4)")
	p.View.SetLabel("Message: ")

	p.View.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			text := p.View.GetText()
			if text == "" || p.CurrentTopic == "" || p.App.KafkaManager == nil {
				return
			}

			err := p.App.KafkaManager.ProduceMessage(p.CurrentTopic, text)
			if err != nil {
				// Show error (maybe in consume panel or a modal)
			} else {
				p.View.SetText("")
			}
		}
	})

	return p
}

func (p *ProducePanel) SetTopic(topic string) {
	p.CurrentTopic = topic
	p.View.SetTitle("Produce (F4) - " + topic)
}
