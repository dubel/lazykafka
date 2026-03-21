package ui

import (
	"log"
	"sort"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type TopicsPanel struct {
	App           *App
	View          tview.Primitive
	ListView      *tview.List
	Footer        *tview.TextView
	SearchInput   *tview.InputField
	MainFlex      *tview.Flex
	ResetTimer    *time.Timer
	AllTopics     []string // full sorted list from Kafka
	Topics        []string // displayed (filtered) list
	SelectedTopic string   // name of selected topic, stable across reloads/filter changes
	SelectedIndex int      // index into Topics (displayed list)
	SearchQuery   string   // active filter text; empty = no filter
	dropNextRune  bool     // swallow first rune after search input gets focus
}

func NewTopicsPanel(app *App) *TopicsPanel {
	listView := tview.NewList()

	footer := tview.NewTextView().
		SetText("(/) search | (a)dd | (r)emove | re(f)resh | (enter) to select").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	searchInput := tview.NewInputField().
		SetLabel("🔍 Search: ").
		SetLabelColor(tcell.ColorTeal).
		SetFieldWidth(0)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(searchInput, 0, 0, false). // hidden initially (height 0)
		AddItem(listView, 0, 1, true).
		AddItem(footer, 2, 0, false)

	flex.SetBorder(true).
		SetTitle("Topics (F2)").
		SetBorderColor(tcell.ColorWhite)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	// listView blur handled below after p is created (needs p in scope)

	p := &TopicsPanel{
		App:           app,
		View:          flex,
		ListView:      listView,
		Footer:        footer,
		SearchInput:   searchInput,
		MainFlex:      flex,
		SelectedIndex: -1,
	}

	// Restore green highlight when panel loses focus — hover is gone so the tag must carry the colour (green).
	p.ListView.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
		if p.SelectedIndex >= 0 && p.SelectedIndex < len(p.Topics) {
			p.ListView.SetItemText(p.SelectedIndex,
				"[white:green]"+p.Topics[p.SelectedIndex]+" >> selected <<[-:-:-]", "")
		}
	})

	// On focus-in the cursor may already be sitting on the selected item — sync immediately.
	p.ListView.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
		if p.SelectedIndex >= 0 && p.SelectedIndex < len(p.Topics) {
			if p.ListView.GetCurrentItem() == p.SelectedIndex {
				p.ListView.SetItemText(p.SelectedIndex, p.Topics[p.SelectedIndex]+" >> selected <<", "")
			} else {
				p.ListView.SetItemText(p.SelectedIndex,
					"[white:green]"+p.Topics[p.SelectedIndex]+" >> selected <<[-:-:-]", "")
			}
		}
	})

	// As the cursor moves: strip highlight when landing on selected item so the normal
	// hover highlight shows; restore green highlight as soon as the cursor moves elsewhere.
	p.ListView.SetChangedFunc(func(index int, mainText, secondaryText string, shortcut rune) {
		if p.SelectedIndex < 0 || p.SelectedIndex >= len(p.Topics) {
			return
		}
		// Guard against mid-rebuild calls where the list has fewer items than p.Topics.
		if p.SelectedIndex >= p.ListView.GetItemCount() {
			return
		}
		if index == p.SelectedIndex {
			p.ListView.SetItemText(p.SelectedIndex, p.Topics[p.SelectedIndex]+" >> selected <<", "")
		} else {
			p.ListView.SetItemText(p.SelectedIndex,
				"[white:green]"+p.Topics[p.SelectedIndex]+" >> selected <<[-:-:-]", "")
		}
	})

	p.ListView.SetSelectedFunc(func(i int, s1, s2 string, r rune) {
		topicName := p.getTopicName(i)
		p.selectTopic(i)
		app.ConsumePanel.StartConsuming(topicName)
		app.ProducePanel.SetTopic(topicName)
		app.TviewApp.SetFocus(app.ConsumePanel.View)
	})

	// Handle key presses for add/remove/refresh/search
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
		case '/':
			p.showSearchInput()
			return nil
		}
		return event
	})


	// Swallow the '/' rune that leaks into the search input when focus
	// switches mid-event-dispatch, then force-clear the field.
	searchInput.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if p.dropNextRune {
			p.dropNextRune = false
			p.SearchInput.SetText("")
			return nil
		}
		return event
	})

	// Search input: Enter applies filter, Escape cancels without changing filter.
	searchInput.SetDoneFunc(func(key tcell.Key) {
		switch key {
		case tcell.KeyEnter:
			p.SearchQuery = strings.TrimSpace(searchInput.GetText())
			// Already in the UI goroutine — call directly, never QueueUpdateDraw here.
			p.MainFlex.ResizeItem(p.SearchInput, 0, 0)
			p.rebuildList()
			p.Footer.SetText(p.defaultFooterText())
			p.App.TviewApp.SetFocus(p.ListView)
		case tcell.KeyEscape:
			p.MainFlex.ResizeItem(p.SearchInput, 0, 0)
			p.App.TviewApp.SetFocus(p.ListView)
		}
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

// showSearchInput reveals the search bar and focuses it.
// The '/' rune is already consumed by ListView's InputCapture (returns nil),
// so it will not leak into the search input field.
func (p *TopicsPanel) showSearchInput() {
	p.SearchInput.SetText("")
	p.dropNextRune = true
	p.MainFlex.ResizeItem(p.SearchInput, 1, 0)
	p.App.TviewApp.SetFocus(p.SearchInput)
}

// defaultFooterText returns the footer text reflecting current filter state.
func (p *TopicsPanel) defaultFooterText() string {
	if p.SearchQuery != "" {
		return `[teal]Filter: "` + p.SearchQuery + `"[-] | (/) new | (ESC) clear` + "\n" +
			`(a)dd | (r)emove | re(f)resh | (enter) to select`
	}
	return "(/) search | (a)dd | (r)emove | re(f)resh | (enter) to select"
}

// filteredTopics returns AllTopics filtered by SearchQuery (case-insensitive contains).
func (p *TopicsPanel) filteredTopics() []string {
	if p.SearchQuery == "" {
		return p.AllTopics
	}
	q := strings.ToLower(p.SearchQuery)
	var result []string
	for _, t := range p.AllTopics {
		if strings.Contains(strings.ToLower(t), q) {
			result = append(result, t)
		}
	}
	return result
}

// rebuildList re-renders the list view from AllTopics + current SearchQuery.
// Must be called from the UI goroutine (inside QueueUpdateDraw or equivalent).
func (p *TopicsPanel) rebuildList() {
	filtered := p.filteredTopics()

	// Reset SelectedIndex before clearing to avoid stale-index panics in SetChangedFunc.
	p.SelectedIndex = -1
	p.ListView.Clear()
	p.Topics = filtered

	newSelectedIndex := -1
	for i, t := range filtered {
		if t == p.SelectedTopic {
			newSelectedIndex = i
		}
		displayName := t
		if i == newSelectedIndex {
			displayName = "[white:green]" + t + " >> selected <<[-:-:-]"
		}
		p.ListView.AddItem(displayName, "", 0, nil)
	}
	p.SelectedIndex = newSelectedIndex
}

func (p *TopicsPanel) showTemporaryMessage(msg string, duration time.Duration) {
	p.Footer.SetText(msg)

	if p.ResetTimer != nil {
		p.ResetTimer.Stop()
	}

	p.ResetTimer = time.AfterFunc(duration, func() {
		p.App.TviewApp.QueueUpdateDraw(func() {
			p.ResetTimer = nil
			p.Footer.SetText(p.defaultFooterText())
		})
	})
}

func (p *TopicsPanel) getTopicName(index int) string {
	if index >= 0 && index < len(p.Topics) {
		return p.Topics[index]
	}
	return ""
}

func (p *TopicsPanel) selectTopic(index int) {
	// Remove highlight from previously selected topic
	if p.SelectedIndex >= 0 && p.SelectedIndex < len(p.Topics) {
		p.ListView.SetItemText(p.SelectedIndex, p.Topics[p.SelectedIndex], "")
	}
	p.SelectedIndex = index
	if index >= 0 && index < len(p.Topics) {
		p.SelectedTopic = p.Topics[index]
		// Cursor is on this item right now — use plain text so the normal hover
		// highlight is visible. SetBlurFunc will restore green highlight when focus leaves.
		p.ListView.SetItemText(index, p.Topics[index]+" >> selected <<", "")
	} else {
		p.SelectedTopic = ""
	}
}

func (p *TopicsPanel) LoadTopics() {
	log.Println("LoadTopics: Starting refresh")

	p.App.ManagerMutex.RLock()
	if p.App.KafkaManager == nil {
		p.App.ManagerMutex.RUnlock()
		log.Println("LoadTopics: KafkaManager is nil")
		p.App.TviewApp.QueueUpdateDraw(func() {
			p.showTemporaryMessage("[red]Cannot refresh Topics if we are not connected!", 5*time.Second)
		})
		return
	}
	// Keep lock held while using client

	log.Println("LoadTopics: Refreshing metadata")
	if err := p.App.KafkaManager.Client.RefreshMetadata(); err != nil {
		log.Printf("LoadTopics: Warning - Failed to refresh metadata: %v", err)
	}

	log.Println("LoadTopics: Listing topics from client")
	topics, err := p.App.KafkaManager.Client.ListTopics()
	p.App.ManagerMutex.RUnlock() // Unlock after network call

	if err != nil {
		log.Println("LoadTopics: Failed to list topics:", err)
		return
	}

	sort.Strings(topics)
	log.Printf("LoadTopics: Found %d topics", len(topics))

	p.App.TviewApp.QueueUpdateDraw(func() {
		log.Println("LoadTopics: Updating UI list")
		p.AllTopics = topics
		p.rebuildList()
		log.Println("LoadTopics: UI update complete")
	})
	log.Println("LoadTopics: Queued UI update")
}

func (p *TopicsPanel) showCreateTopicModal() {
	p.App.ManagerMutex.RLock()
	if p.App.KafkaManager == nil {
		p.App.ManagerMutex.RUnlock()
		p.showTemporaryMessage("[red]Cannot alter Topics if we are not connected!", 5*time.Second)
		return
	}
	p.App.ManagerMutex.RUnlock()

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

				p.App.ManagerMutex.RLock()
				if p.App.KafkaManager == nil {
					p.App.ManagerMutex.RUnlock()
					log.Println("UI: KafkaManager is nil, cannot create topic")
					p.App.TviewApp.QueueUpdateDraw(func() {
						p.showTemporaryMessage("[red]Cannot alter Topics if we are not connected!", 5*time.Second)
					})
					return
				}
				err := p.App.KafkaManager.CreateTopic(topicName)
				p.App.ManagerMutex.RUnlock()

				if err != nil {
					log.Printf("UI: Failed to create topic: %v", err)
					p.App.TviewApp.QueueUpdateDraw(func() {
						p.showTemporaryMessage("[red]Failed to create topic!", 5*time.Second)
					})
				} else {
					log.Printf("UI: Successfully created topic: %s", topicName)
					p.LoadTopics()
					p.App.TviewApp.QueueUpdateDraw(func() {
						p.showTemporaryMessage("[green]Topic created!", 2*time.Second)
					})
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

	form.SetBorder(true).SetTitle("Create New Topic").SetTitleAlign(tview.AlignCenter)

	p.App.InModal = true
	p.App.TviewApp.SetRoot(form, true)
}

func (p *TopicsPanel) deleteSelectedTopic() {
	p.App.ManagerMutex.RLock()
	if p.App.KafkaManager == nil {
		p.App.ManagerMutex.RUnlock()
		p.showTemporaryMessage("[red]Cannot alter Topics if we are not connected!", 5*time.Second)
		return
	}
	p.App.ManagerMutex.RUnlock()

	if p.ListView.GetItemCount() == 0 {
		return
	}
	idx := p.ListView.GetCurrentItem()
	if idx == -1 {
		return
	}

	topicName := p.getTopicName(idx)
	if topicName == "" {
		return
	}
	log.Printf("UI: Requesting deletion of topic: %s", topicName)

	go func() {
		p.App.ManagerMutex.RLock()
		if p.App.KafkaManager != nil {
			err := p.App.KafkaManager.DeleteTopic(topicName)
			p.App.ManagerMutex.RUnlock()

			if err != nil {
				log.Printf("UI: Failed to delete topic %s: %v", topicName, err)
				p.App.TviewApp.QueueUpdateDraw(func() {
					p.showTemporaryMessage("[red]Failed to delete topic!", 5*time.Second)
				})
			} else {
				log.Printf("UI: Successfully deleted topic: %s", topicName)
				p.LoadTopics()
				p.App.TviewApp.QueueUpdateDraw(func() {
					p.showTemporaryMessage("[green]Topic deleted!", 2*time.Second)
				})
			}
		} else {
			p.App.ManagerMutex.RUnlock()
			// Should not be reached if initial check passes, but good for safety
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showTemporaryMessage("[red]Cannot alter Topics if we are not connected!", 5*time.Second)
			})
		}
	}()
}
