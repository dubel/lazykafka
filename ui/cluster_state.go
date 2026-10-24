package ui

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/dubel/lazykafka/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// clusterStateTimeout bounds how long the loading screen waits for the broker.
const clusterStateTimeout = 15 * time.Second

// showClusterState opens a full-screen view with cluster health and consumer group lag.
func (p *BrokersPanel) showClusterState() {
	p.showClusterStateOnPage(0)
}

// showClusterStateOnPage is like showClusterState but opens a specific tab.
// Page indices: 0 = topics, 1 = groups, 2 = config.
func (p *BrokersPanel) showClusterStateOnPage(initialPage int) {
	p.App.InModal = true

	// Only the latest request may render: Esc or a refresh cancels the previous
	// one so a late result cannot replace whatever is on screen by then.
	if p.clusterCancel != nil {
		p.clusterCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.clusterCancel = cancel

	loading := tview.NewTextView().
		SetText("\n\n  Loading cluster state...").
		SetDynamicColors(true)
	loading.SetBorder(true).SetTitle("  Cluster State  ").SetTitleAlign(tview.AlignCenter)
	loading.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			cancel()
			p.App.InModal = false
			p.App.TviewApp.SetRoot(p.App.Grid, true)
			p.App.TviewApp.SetFocus(p.App.BrokersPanel.View)
			return nil
		}
		return event
	})
	p.App.TviewApp.SetRoot(loading, true)

	go func() {
		mgr := p.App.Manager()

		if mgr == nil {
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showClusterError("Not connected to any broker.")
			})
			return
		}

		type result struct {
			state *kafka.ClusterState
			err   error
		}
		done := make(chan result, 1)
		go func() {
			state, err := mgr.GetClusterState()
			done <- result{state, err}
		}()

		var res result
		select {
		case res = <-done:
		case <-time.After(clusterStateTimeout):
			res.err = fmt.Errorf("timed out after %v", clusterStateTimeout)
		case <-ctx.Done():
			return
		}

		p.App.TviewApp.QueueUpdateDraw(func() {
			if ctx.Err() != nil {
				return // user left or refreshed while the result was queued
			}
			if res.err != nil {
				p.showClusterError("Error: " + tview.Escape(res.err.Error()))
				return
			}
			p.renderClusterState(res.state, initialPage)
		})
	}()
}

func (p *BrokersPanel) showClusterError(msg string) {
	tv := tview.NewTextView().
		SetText(msg + "\n\n[gray](press Esc to close)").
		SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter)
	tv.SetBorder(true).SetTitle("  Cluster State  ").SetBorderColor(tcell.ColorRed)
	tv.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			p.App.InModal = false
			p.App.TviewApp.SetRoot(p.App.Grid, true)
			p.App.TviewApp.SetFocus(p.App.BrokersPanel.View)
			return nil
		}
		return event
	})
	p.App.TviewApp.SetRoot(tv, true)
}

func (p *BrokersPanel) renderClusterState(state *kafka.ClusterState, initialPage ...int) {
	// Determine which tab to show initially (default: topics = 0).
	startPage := 0
	if len(initialPage) > 0 && initialPage[0] >= 0 && initialPage[0] < 3 {
		startPage = initialPage[0]
	}
	// ── Broker table ──
	brokerTable := tview.NewTable().
		SetBorders(false).
		SetSelectable(false, false)

	headers := []string{"ID", "Address", "Controller", "Leaders", "Replicas"}
	for i, h := range headers {
		brokerTable.SetCell(0, i, tview.NewTableCell(h).
			SetTextColor(tcell.ColorYellow).
			SetAttributes(tcell.AttrBold).
			SetSelectable(false))
	}

	sort.Slice(state.Brokers, func(i, j int) bool {
		return state.Brokers[i].ID < state.Brokers[j].ID
	})

	for r, b := range state.Brokers {
		ctrl := ""
		if b.IsController {
			ctrl = "✓"
		}
		brokerTable.SetCell(r+1, 0, tview.NewTableCell(fmt.Sprintf("%d", b.ID)))
		brokerTable.SetCell(r+1, 1, tview.NewTableCell(tview.Escape(b.Address)))
		brokerTable.SetCell(r+1, 2, tview.NewTableCell(ctrl).SetTextColor(tcell.ColorGreen))
		brokerTable.SetCell(r+1, 3, tview.NewTableCell(fmt.Sprintf("%d", b.LeaderCount)))
		brokerTable.SetCell(r+1, 4, tview.NewTableCell(fmt.Sprintf("%d", b.ReplicaCount)))
	}

	brokerBox := tview.NewFlex().SetDirection(tview.FlexRow).AddItem(brokerTable, 0, 1, false)
	brokerBox.SetBorder(true).SetTitle(" Brokers ")

	// ── Health bar ──
	urpColor := "[green]"
	if state.UnderReplicatedCount > 0 {
		urpColor = "[red]"
	}
	offColor := "[green]"
	if state.OfflinePartitionCount > 0 {
		offColor = "[red]"
	}

	healthText := fmt.Sprintf(
		" Topics: [white]%d[-]  |  Partitions: [white]%d[-]  |  Under-replicated: %s%d[-]  |  Offline: %s%d[-]  |  Controller: [white]%d[-]",
		state.TotalTopics, state.TotalPartitions,
		urpColor, state.UnderReplicatedCount,
		offColor, state.OfflinePartitionCount,
		state.ControllerID,
	)
	healthBar := tview.NewTextView().
		SetText(healthText).
		SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter)
	healthBar.SetBorder(true).SetTitle(" Cluster Health ")

	// ── Topic offsets table ──
	topicTable := tview.NewTable().
		SetBorders(false).
		SetSelectable(len(state.TopicOffsets) > 0, false).
		SetFixed(1, 0)

	tHeaders := []string{"Topic", "Partitions", "Messages", "Newest Offset"}
	for i, h := range tHeaders {
		topicTable.SetCell(0, i, tview.NewTableCell(h).
			SetTextColor(tcell.ColorYellow).
			SetAttributes(tcell.AttrBold).
			SetSelectable(false))
	}

	// Sort topics by message count descending
	sort.Slice(state.TopicOffsets, func(i, j int) bool {
		return state.TopicOffsets[i].TotalMessages > state.TopicOffsets[j].TotalMessages
	})

	for r, ti := range state.TopicOffsets {
		msgColor := tcell.ColorWhite
		if ti.TotalMessages > 0 {
			msgColor = tcell.ColorGreen
		}
		topicTable.SetCell(r+1, 0, tview.NewTableCell(ti.Name))
		topicTable.SetCell(r+1, 1, tview.NewTableCell(fmt.Sprintf("%d", ti.Partitions)))
		topicTable.SetCell(r+1, 2, tview.NewTableCell(fmt.Sprintf("%d", ti.TotalMessages)).SetTextColor(msgColor))
		topicTable.SetCell(r+1, 3, tview.NewTableCell(fmt.Sprintf("%d", ti.NewestOffset)))
	}

	if len(state.TopicOffsets) == 0 {
		topicTable.SetCell(1, 0, tview.NewTableCell("No topics found").
			SetTextColor(tcell.ColorGray))
	}

	topicTable.SetBorder(true).SetTitle(" Topic Offsets (sorted by messages) ")

	// ── Consumer groups table ──
	hasGroups := len(state.ConsumerGroups) > 0

	groupTable := tview.NewTable().
		SetBorders(false).
		SetSelectable(hasGroups, false).
		SetFixed(1, 0)

	gHeaders := []string{"Group", "State", "Members", "Total Lag", "Top Lagging Topic"}
	for i, h := range gHeaders {
		groupTable.SetCell(0, i, tview.NewTableCell(h).
			SetTextColor(tcell.ColorYellow).
			SetAttributes(tcell.AttrBold).
			SetSelectable(false))
	}

	sort.Slice(state.ConsumerGroups, func(i, j int) bool {
		return state.ConsumerGroups[i].TotalLag > state.ConsumerGroups[j].TotalLag
	})

	for r, g := range state.ConsumerGroups {
		stateColor := tcell.ColorWhite
		switch g.State {
		case "Stable":
			stateColor = tcell.ColorGreen
		case "Empty":
			stateColor = tcell.ColorGray
		case "Dead":
			stateColor = tcell.ColorRed
		case "PreparingRebalance", "CompletingRebalance":
			stateColor = tcell.ColorOrange
		}

		lagColor := tcell.ColorWhite
		if g.TotalLag > 10000 {
			lagColor = tcell.ColorRed
		} else if g.TotalLag > 1000 {
			lagColor = tcell.ColorYellow
		}

		topTopic := "-"
		var topLag int64
		for t, l := range g.TopicLags {
			if l > topLag {
				topLag = l
				topTopic = fmt.Sprintf("%s (%d)", t, l)
			}
		}

		groupTable.SetCell(r+1, 0, tview.NewTableCell(tview.Escape(g.Name)))
		groupTable.SetCell(r+1, 1, tview.NewTableCell(tview.Escape(g.State)).SetTextColor(stateColor))
		groupTable.SetCell(r+1, 2, tview.NewTableCell(fmt.Sprintf("%d", g.Members)))
		groupTable.SetCell(r+1, 3, tview.NewTableCell(fmt.Sprintf("%d", g.TotalLag)).SetTextColor(lagColor))
		groupTable.SetCell(r+1, 4, tview.NewTableCell(topTopic))
	}

	if !hasGroups {
		groupTable.SetCell(1, 0, tview.NewTableCell("No consumer groups found (built-in consumer uses partition mode, not group mode)").
			SetTextColor(tcell.ColorGray))
	}

	groupTable.SetBorder(true).SetTitle(" Consumer Groups (sorted by lag) ")

	// ── Cluster config table ──
	configTable := tview.NewTable().
		SetBorders(false).
		SetSelectable(len(state.ClusterConfig) > 0, false).
		SetFixed(1, 0)

	cHeaders := []string{"Config Key", "Value", "Source"}
	for i, h := range cHeaders {
		configTable.SetCell(0, i, tview.NewTableCell(h).
			SetTextColor(tcell.ColorYellow).
			SetAttributes(tcell.AttrBold).
			SetSelectable(false))
	}

	// Sort config entries alphabetically
	sort.Slice(state.ClusterConfig, func(i, j int) bool {
		return state.ClusterConfig[i].Key < state.ClusterConfig[j].Key
	})

	for r, ce := range state.ClusterConfig {
		source := "[yellow]custom[-]"
		sourceColor := tcell.ColorYellow
		if ce.IsDefault {
			source = "default"
			sourceColor = tcell.ColorGray
		}
		_ = source // we use sourceColor on the cell directly

		configTable.SetCell(r+1, 0, tview.NewTableCell(ce.Key))
		configTable.SetCell(r+1, 1, tview.NewTableCell(tview.Escape(ce.Value)).SetTextColor(tcell.ColorWhite))
		configTable.SetCell(r+1, 2, tview.NewTableCell(func() string {
			if ce.IsDefault {
				return "default"
			}
			return "custom"
		}()).SetTextColor(sourceColor))
	}

	if len(state.ClusterConfig) == 0 {
		configTable.SetCell(1, 0, tview.NewTableCell("Could not retrieve cluster config").
			SetTextColor(tcell.ColorGray))
	}

	configTable.SetBorder(true).SetTitle(fmt.Sprintf(" Cluster Config (from controller broker %d) ", state.ControllerID))

	// ── Tab-switchable bottom section ──
	pageNames := []string{"topics", "groups", "config"}
	pageTables := []*tview.Table{topicTable, groupTable, configTable}

	pages := tview.NewPages().
		AddPage("topics", topicTable, true, startPage == 0).
		AddPage("groups", groupTable, true, startPage == 1).
		AddPage("config", configTable, true, startPage == 2)

	currentPageIdx := startPage
	pages.SwitchToPage(pageNames[currentPageIdx])

	// Keep a reference to the groups data for delete operations.
	groups := state.ConsumerGroups

	// ── Footer ── (declared early so the updateFooter closure can reference it)
	footer := tview.NewTextView().
		SetText("(Esc) close | (r) refresh | (Tab) cycle: offsets → groups → config | (↑/↓) scroll").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	// Helper: update footer text based on the active page.
	updateFooter := func() {
		base := "(Esc) close | (r) refresh | (Tab) cycle: offsets → groups → config | (↑/↓) scroll"
		if pageNames[currentPageIdx] == "groups" && hasGroups {
			base = "(Esc) close | (r) remove group | (f) refresh | (Tab) cycle: offsets → groups → config | (↑/↓) scroll"
		}
		footer.SetText(base)
	}

	// Shared input handler for all three tables.
	makeInputCapture := func() func(event *tcell.EventKey) *tcell.EventKey {
		return func(event *tcell.EventKey) *tcell.EventKey {
			if event.Key() == tcell.KeyEscape {
				p.App.InModal = false
				p.App.TviewApp.SetRoot(p.App.Grid, true)
				p.App.TviewApp.SetFocus(p.App.BrokersPanel.View)
				return nil
			}
			// 'r' is context-sensitive: on the groups page it removes,
			// on all other pages it refreshes the cluster state view.
			if event.Rune() == 'r' {
				if pageNames[currentPageIdx] == "groups" && hasGroups {
					p.deleteSelectedConsumerGroup(groupTable, groups, state)
					return nil
				}
				p.showClusterState()
				return nil
			}
			// 'f' always refreshes (available on all pages, required on groups
			// page where 'r' is used for remove).
			if event.Rune() == 'f' {
				p.showClusterState()
				return nil
			}
			if event.Key() == tcell.KeyTab {
				currentPageIdx = (currentPageIdx + 1) % len(pageNames)
				pages.SwitchToPage(pageNames[currentPageIdx])
				p.App.TviewApp.SetFocus(pageTables[currentPageIdx])
				updateFooter()
				return nil
			}
			if event.Key() == tcell.KeyBacktab {
				currentPageIdx = (currentPageIdx + len(pageNames) - 1) % len(pageNames)
				pages.SwitchToPage(pageNames[currentPageIdx])
				p.App.TviewApp.SetFocus(pageTables[currentPageIdx])
				updateFooter()
				return nil
			}
			return event
		}
	}

	capture := makeInputCapture()
	topicTable.SetInputCapture(capture)
	groupTable.SetInputCapture(capture)
	configTable.SetInputCapture(capture)

	updateFooter()

	// ── Layout ──
	brokerRows := len(state.Brokers) + 3
	if brokerRows < 4 {
		brokerRows = 4
	}

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(brokerBox, brokerRows, 0, false).
		AddItem(healthBar, 3, 0, false).
		AddItem(pages, 0, 1, true).
		AddItem(footer, 1, 0, false)

	p.App.TviewApp.SetRoot(layout, true)
	p.App.TviewApp.SetFocus(pageTables[currentPageIdx])
}

// deleteSelectedConsumerGroup handles the 'r' key on the groups page.
// It checks for active members and shows a confirmation modal before deleting.
func (p *BrokersPanel) deleteSelectedConsumerGroup(groupTable *tview.Table, groups []kafka.ConsumerGroupInfo, state *kafka.ClusterState) {
	row, _ := groupTable.GetSelection()
	// Row 0 is the header; data rows start at 1.
	idx := row - 1
	if idx < 0 || idx >= len(groups) {
		return
	}
	group := groups[idx]

	// Active member guard — Kafka rejects deletion of groups with active consumers.
	if group.Members > 0 {
		log.Printf("UI: Cannot delete consumer group '%s' — %d active member(s)", group.Name, group.Members)
		p.showClusterMessage(
			fmt.Sprintf("[red]Cannot delete group '[white]%s[red]'\n\nGroup has %d active member(s).\nStop all consumers in this group before deleting.\n\n[gray](press any key to dismiss)",
				tview.Escape(group.Name), group.Members),
			"  ⚠  Active Members  ⚠  ",
			tcell.ColorRed,
			state,
		)
		return
	}

	// Confirmation modal.
	log.Printf("UI: Requesting deletion of consumer group: %s", group.Name)
	message := fmt.Sprintf(
		"Delete consumer group\n\n[white::b]%s[-::-]\n\n[gray]State: %s | Lag: %d\n\n[white](Enter) confirm | (Esc) cancel",
		tview.Escape(group.Name), tview.Escape(group.State), group.TotalLag,
	)

	textView := tview.NewTextView().
		SetText(message).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	textView.SetBorder(true).
		SetTitle("  Delete Consumer Group  ").
		SetTitleAlign(tview.AlignCenter).
		SetBorderColor(tcell.ColorRed)

	textView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			// Cancel — return to the cluster state view on the groups page.
			p.renderClusterState(state, 1)
			return nil
		case tcell.KeyEnter:
			// Confirm — perform the delete.
			p.executeDeleteConsumerGroup(group.Name)
			return nil
		}
		return event
	})

	// Centre the confirmation box.
	modal := tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(textView, 10, 0, true).
			AddItem(nil, 0, 1, false),
			50, 0, true).
		AddItem(nil, 0, 1, false)

	p.App.TviewApp.SetRoot(modal, true)
}

// executeDeleteConsumerGroup performs the async group deletion and refreshes the view.
func (p *BrokersPanel) executeDeleteConsumerGroup(groupName string) {
	// Show a brief "deleting..." message.
	loading := tview.NewTextView().
		SetText(fmt.Sprintf("\n\n  Deleting consumer group '%s'...", tview.Escape(groupName))).
		SetDynamicColors(true)
	loading.SetBorder(true).SetTitle("  Cluster State  ").SetTitleAlign(tview.AlignCenter)
	p.App.TviewApp.SetRoot(loading, true)

	go func() {
		mgr := p.App.Manager()

		if mgr == nil {
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showClusterError("Not connected to any broker.")
			})
			return
		}

		err := mgr.DeleteConsumerGroup(groupName)
		if err != nil {
			log.Printf("UI: Failed to delete consumer group %s: %v", groupName, err)
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showClusterError(fmt.Sprintf("[red]Failed to delete consumer group '[white]%s[red]':\n\n%s\n\n[gray](press Esc to close)", tview.Escape(groupName), tview.Escape(err.Error())))
			})
			return
		}

		log.Printf("UI: Successfully deleted consumer group: %s", groupName)
		// Refresh the cluster state view on the groups page to reflect the deletion.
		p.App.TviewApp.QueueUpdateDraw(func() {
			p.showClusterStateOnPage(1)
		})
	}()
}

// showClusterMessage displays a dismissible message modal that returns to the cluster state view.
func (p *BrokersPanel) showClusterMessage(message, title string, borderColor tcell.Color, state *kafka.ClusterState) {
	textView := tview.NewTextView().
		SetText(message).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	textView.SetBorder(true).
		SetTitle(title).
		SetTitleAlign(tview.AlignCenter).
		SetBorderColor(borderColor)

	textView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		p.renderClusterState(state, 1)
		return nil
	})

	modal := tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(textView, 10, 0, true).
			AddItem(nil, 0, 1, false),
			60, 0, true).
		AddItem(nil, 0, 1, false)

	p.App.TviewApp.SetRoot(modal, true)
}
