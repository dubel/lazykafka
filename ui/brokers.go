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
	p.View.SetBorder(true).SetTitle("Brokers [1]")

	// Load brokers from config
	for _, b := range app.Config.Brokers {
		p.View.AddItem(b, "", 0, func() {
			// On select, connect to broker
			idx := p.View.GetCurrentItem()
			broker, _ := p.View.GetItemText(idx)
			if err := p.App.ConnectToBroker(broker); err != nil {
				// Show error modal (simplified for now)
			} else {
				p.App.TviewApp.SetFocus(p.App.TopicsPanel.View)
			}
		})
	}

	p.View.AddItem("Add New", "Add a new broker", 'a', func() {
		p.showAddBrokerModal()
	})

	p.View.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyDelete || event.Rune() == 'r' {
			p.removeCurrentBroker()
			return nil
		}
		return event
	})

	return p
}

func (p *BrokersPanel) showAddBrokerModal() {
	form := tview.NewForm().
		AddInputField("Broker Address", "localhost:9092", 20, nil, nil)

	form.AddButton("Save", func() {
		addr := form.GetFormItem(0).(*tview.InputField).GetText()
		if addr != "" {
			p.App.Config.Brokers = append(p.App.Config.Brokers, addr)
			config.SaveConfig(p.App.Config)

			// Insert before "Add New"
			count := p.View.GetItemCount()
			p.View.InsertItem(count-1, addr, "", 0, func() {
				if err := p.App.ConnectToBroker(addr); err == nil {
					p.App.TviewApp.SetFocus(p.App.TopicsPanel.View)
				}
			})
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
	count := p.View.GetItemCount()
	if idx < count-1 { // Don't remove "Add New"
		// Remove from config
		broker, _ := p.View.GetItemText(idx)
		newBrokers := []string{}
		for _, b := range p.App.Config.Brokers {
			if b != broker {
				newBrokers = append(newBrokers, b)
			}
		}
		p.App.Config.Brokers = newBrokers
		config.SaveConfig(p.App.Config)

		p.View.RemoveItem(idx)
	}
}
