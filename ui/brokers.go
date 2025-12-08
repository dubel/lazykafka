package ui

import (
	"github.com/dubel/lazykafka/config"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type BrokersPanel struct {
	App  *App
	View *tview.List
}

func NewBrokersPanel(app *App) *BrokersPanel {
	p := &BrokersPanel{
		App:  app,
		View: tview.NewList(),
	}
	p.View.SetBorder(true).SetTitle("Brokers (F1)")

	p.refreshList()

	p.View.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyDelete || event.Rune() == 'r' {
			p.removeCurrentBroker()
			return nil
		}
		return event
	})

	return p
}

func (p *BrokersPanel) refreshList() {
	p.View.Clear()

	// Load brokers from config
	for i, b := range p.App.Config.Brokers {
		// Capture variable for closure
		broker := b
		index := i

		displayText := broker.Address
		if broker.Alias != "" {
			displayText += " [" + broker.Alias + "]"
		}

		p.View.AddItem(displayText, "", 0, func() {
			// On select, connect to broker
			p.resetStatuses()

			statusText := broker.Address
			if broker.Alias != "" {
				statusText += " [" + broker.Alias + "]"
			}
			p.View.SetItemText(index, statusText+" (connecting...)", "")

			go func() {
				err := p.App.ConnectToBroker(broker.Address)
				p.App.TviewApp.QueueUpdateDraw(func() {
					if err != nil {
						p.View.SetItemText(index, statusText+" (failed to connect !!)", "")
					} else {
						p.View.SetItemText(index, statusText+" (connected)", "")
						p.App.TviewApp.SetFocus(p.App.TopicsPanel.View)
					}
				})
			}()
		})
	}

	p.View.AddItem("Add New", "Add a new broker", 'a', func() {
		p.showAddBrokerModal()
	})
}

func (p *BrokersPanel) showAddBrokerModal() {
	form := tview.NewForm().
		AddInputField("Broker Address", "localhost:9092", 20, nil, nil).
		AddInputField("Alias (optional)", "", 20, nil, nil)

	form.AddButton("Save", func() {
		addr := form.GetFormItem(0).(*tview.InputField).GetText()
		alias := form.GetFormItem(1).(*tview.InputField).GetText()

		if addr != "" {
			newBroker := config.Broker{
				Address: addr,
				Alias:   alias,
			}
			p.App.Config.Brokers = append(p.App.Config.Brokers, newBroker)
			config.SaveConfig(p.App.Config)
			p.refreshList()

			// Auto-connect to the new broker
			// The new broker is at the end of the list (before "Add New")
			newIdx := len(p.App.Config.Brokers) - 1
			p.View.SetCurrentItem(newIdx)
			// Trigger the selected function manually?
			// tview doesn't expose a way to trigger the selected function programmatically easily without calling it directly.
			// But we can just replicate the connect logic or let the user press enter.
			// For better UX, let's just highlight it. The user can press Enter.
		}
		p.App.TviewApp.SetRoot(p.App.Grid, true)
	}).
		AddButton("Cancel", func() {
			p.App.TviewApp.SetRoot(p.App.Grid, true)
		})

	form.SetBorder(true).SetTitle("Add Broker").SetTitleAlign(tview.AlignCenter)
	p.App.TviewApp.SetRoot(form, true)
}

func (p *BrokersPanel) removeCurrentBroker() {
	idx := p.View.GetCurrentItem()
	if idx < len(p.App.Config.Brokers) {
		// Remove from config using index
		p.App.Config.Brokers = append(p.App.Config.Brokers[:idx], p.App.Config.Brokers[idx+1:]...)
		config.SaveConfig(p.App.Config)
		p.refreshList()
	}
}

func (p *BrokersPanel) resetStatuses() {
	for i := 0; i < len(p.App.Config.Brokers); i++ {
		b := p.App.Config.Brokers[i]
		text := b.Address
		if b.Alias != "" {
			text += " [" + b.Alias + "]"
		}
		p.View.SetItemText(i, text, "")
	}
}
