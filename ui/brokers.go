package ui

import (
	"github.com/dubel/lazykafka/config"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type BrokersPanel struct {
	App      *App
	View     tview.Primitive
	ListView *tview.List
}

func NewBrokersPanel(app *App) *BrokersPanel {
	listView := tview.NewList()

	footer := tview.NewTextView().
		SetText("(a)dd | (r)emove | (enter) to connect").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(listView, 0, 1, true).
		AddItem(footer, 1, 0, false)

	flex.SetBorder(true).
		SetTitle("Brokers (F1)").
		SetBorderColor(tcell.ColorWhite).
		SetBorderAttributes(tcell.AttrNone)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p := &BrokersPanel{
		App:      app,
		View:     flex,
		ListView: listView,
	}

	p.refreshList()

	// Set focus handlers on the ListView to update the Flex border
	p.ListView.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	p.ListView.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p.ListView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyDelete || event.Rune() == 'r' {
			p.removeCurrentBroker()
			return nil
		}
		return event
	})

	return p
}

func (p *BrokersPanel) refreshList() {
	p.ListView.Clear()

	// Load brokers from config
	for i, b := range p.App.Config.Brokers {
		// Capture variable for closure
		broker := b
		index := i

		displayText := broker.Address
		if broker.Alias != "" {
			displayText += " (" + broker.Alias + ")"
		}

		p.ListView.AddItem(displayText, "", 0, func() {
			// On select, connect to broker
			p.resetStatuses()

			statusText := broker.Address
			if broker.Alias != "" {
				statusText += " (" + broker.Alias + ")"
			}
			p.ListView.SetItemText(index, statusText+" (connecting...)", "")

			go func() {
				err := p.App.ConnectToBroker(broker.Address)
				p.App.TviewApp.QueueUpdateDraw(func() {
					if err != nil {
						p.ListView.SetItemText(index, statusText+" **failed to connect!!**)", "")
					} else {
						p.ListView.SetItemText(index, statusText+" >>connected<<)", "")
						p.App.TviewApp.SetFocus(p.App.TopicsPanel.View)
					}
				})
			}()
		})
	}

	p.ListView.AddItem("Add New", "Add a new broker", 'a', func() {
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
			p.ListView.SetCurrentItem(newIdx)
			// Trigger the selected function manually?
			// tview doesn't expose a way to trigger the selected function programmatically easily without calling it directly.
			// But we can just replicate the connect logic or let the user press enter.
			// For better UX, let's just highlight it. The user can press Enter.
		}
		p.App.InModal = false
		p.App.TviewApp.SetRoot(p.App.Grid, true)
	}).
		AddButton("Cancel", func() {
			p.App.InModal = false
			p.App.TviewApp.SetRoot(p.App.Grid, true)
		})

	// Enable Tab and arrow key navigation
	form.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyDown, tcell.KeyTab:
			// Move to next field/button
			return tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone)
		case tcell.KeyUp, tcell.KeyBacktab:
			// Move to previous field/button
			return tcell.NewEventKey(tcell.KeyBacktab, 0, tcell.ModNone)
		}
		return event
	})

	form.SetBorder(true).SetTitle("Add Broker").SetTitleAlign(tview.AlignCenter)
	p.App.InModal = true
	p.App.TviewApp.SetRoot(form, true)
}

func (p *BrokersPanel) removeCurrentBroker() {
	idx := p.ListView.GetCurrentItem()
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
		p.ListView.SetItemText(i, text, "")
	}
}
