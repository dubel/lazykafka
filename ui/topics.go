package ui

import (
	"log"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type TopicsPanel struct {
	App      *App
	View     tview.Primitive
	ListView *tview.List
}

func NewTopicsPanel(app *App) *TopicsPanel {
	listView := tview.NewList()

	footer := tview.NewTextView().
		SetText("(a)dd | (r)emove | re(f)resh | (enter) to select").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(listView, 0, 1, true).
		AddItem(footer, 1, 0, false)

	flex.SetBorder(true).
		SetTitle("Topics (F2)").
		SetBorderColor(tcell.ColorWhite)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	// Set focus handlers on the ListView to update the Flex border
	listView.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	listView.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p := &TopicsPanel{
		App:      app,
		View:     flex,
		ListView: listView,
	}

	p.ListView.SetSelectedFunc(func(i int, s1, s2 string, r rune) {
		app.ConsumePanel.StartConsuming(s1)
		app.ProducePanel.SetTopic(s1)
		app.TviewApp.SetFocus(app.ConsumePanel.View)
	})

	// Handle key presses for add/remove/refresh
	p.ListView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Rune() {
		case 'a':
			log.Println("Creating topic...")
			p.showCreateTopicModal()
			return nil
		case 'r':
			log.Println("Deleting topic...")
			p.deleteSelectedTopic()
			return nil
		case 'f':
			log.Println("Refreshing topics...")
			go p.LoadTopics()
			return nil
		}
		return event
	})

	// Background refresh
	go func() {
		ticker := time.NewTicker(300 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			p.LoadTopics()
		}
	}()

	return p
}

func (p *TopicsPanel) LoadTopics() {
	log.Println("LoadTopics: Starting refresh")
	if p.App.KafkaManager == nil {
		log.Println("LoadTopics: KafkaManager is nil")
		return
	}

	log.Println("LoadTopics: Refreshing metadata")
	if err := p.App.KafkaManager.Client.RefreshMetadata(); err != nil {
		log.Printf("LoadTopics: Warning - Failed to refresh metadata: %v", err)
	}

	log.Println("LoadTopics: Listing topics from client")
	topics, err := p.App.KafkaManager.Client.ListTopics()
	if err != nil {
		log.Println("LoadTopics: Failed to list topics:", err)
		return
	}
	log.Printf("LoadTopics: Found %d topics", len(topics))

	p.App.TviewApp.QueueUpdateDraw(func() {
		log.Println("LoadTopics: Updating UI list")
		p.ListView.Clear()
		for _, t := range topics {
			p.ListView.AddItem(t, "", 0, nil)
		}
		log.Println("LoadTopics: UI update complete")
	})
	log.Println("LoadTopics: Queued UI update")
}

func (p *TopicsPanel) showCreateTopicModal() {
	if p.App.KafkaManager == nil {
		return
	}

	form := tview.NewForm().
		AddInputField("Topic Name", "", 30, nil, nil)

	form.AddButton("Create", func() {
		topicName := form.GetFormItem(0).(*tview.InputField).GetText()
		log.Printf("UI: Create button clicked for topic: %s", topicName)

		// Close modal immediately
		p.App.InModal = false
		p.App.TviewApp.SetRoot(p.App.Grid, true)
		p.App.TviewApp.SetFocus(p.View)

		if topicName != "" {
			go func() {
				log.Println("UI: Calling KafkaManager.CreateTopic (background)")
				err := p.App.KafkaManager.CreateTopic(topicName)
				if err != nil {
					log.Printf("UI: Failed to create topic: %v", err)
					p.App.TviewApp.QueueUpdateDraw(func() {
						// Optionally show error modal here
					})
				} else {
					log.Printf("UI: Successfully created topic: %s", topicName)
					p.LoadTopics()
				}
			}()
		}
	})

	form.AddButton("Cancel", func() {
		log.Println("UI: Create topic cancelled")
		p.App.InModal = false
		p.App.TviewApp.SetRoot(p.App.Grid, true)
		p.App.TviewApp.SetFocus(p.View)
	})

	form.SetBorder(true).SetTitle("Create New Topic").SetTitleAlign(tview.AlignCenter)

	// Center the form
	flex := tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(form, 10, 1, true).
			AddItem(nil, 0, 1, false), 40, 1, true).
		AddItem(nil, 0, 1, false)

	p.App.InModal = true
	p.App.TviewApp.SetRoot(flex, true)
}

func (p *TopicsPanel) deleteSelectedTopic() {
	idx := p.ListView.GetCurrentItem()
	if idx == -1 {
		return
	}

	topicName, _ := p.ListView.GetItemText(idx)
	log.Printf("UI: Requesting deletion of topic: %s", topicName)

	go func() {
		if p.App.KafkaManager != nil {
			err := p.App.KafkaManager.DeleteTopic(topicName)
			if err != nil {
				log.Printf("UI: Failed to delete topic %s: %v", topicName, err)
			} else {
				log.Printf("UI: Successfully deleted topic: %s", topicName)
				p.LoadTopics()
			}
		}
	}()
}
