package ui

import (
	"fmt"
	"sort"

	"github.com/dubel/lazykafka/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// showClusterState opens a full-screen view with cluster health and consumer group lag.
func (p *BrokersPanel) showClusterState() {
	p.App.InModal = true

	loading := tview.NewTextView().
		SetText("\n\n  Loading cluster state...").
		SetDynamicColors(true)
	loading.SetBorder(true).SetTitle("  Cluster State  ").SetTitleAlign(tview.AlignCenter)
	loading.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			p.App.InModal = false
			p.App.TviewApp.SetRoot(p.App.Grid, true)
			p.App.TviewApp.SetFocus(p.App.BrokersPanel.View)
			return nil
		}
		return event
	})
	p.App.TviewApp.SetRoot(loading, true)

	go func() {
		p.App.ManagerMutex.RLock()
		mgr := p.App.KafkaManager
		p.App.ManagerMutex.RUnlock()

		if mgr == nil {
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showClusterError("Not connected to any broker.")
			})
			return
		}

		state, err := mgr.GetClusterState()
		if err != nil {
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showClusterError(fmt.Sprintf("Error: %v", err))
			})
			return
		}

		p.App.TviewApp.QueueUpdateDraw(func() {
			p.renderClusterState(state)
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

func (p *BrokersPanel) renderClusterState(state *kafka.ClusterState) {
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
		brokerTable.SetCell(r+1, 1, tview.NewTableCell(b.Address))
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

		groupTable.SetCell(r+1, 0, tview.NewTableCell(g.Name))
		groupTable.SetCell(r+1, 1, tview.NewTableCell(g.State).SetTextColor(stateColor))
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
		configTable.SetCell(r+1, 1, tview.NewTableCell(ce.Value).SetTextColor(tcell.ColorWhite))
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
		AddPage("topics", topicTable, true, true).
		AddPage("groups", groupTable, true, false).
		AddPage("config", configTable, true, false)

	currentPageIdx := 0

	// Shared input handler for all three tables.
	makeInputCapture := func() func(event *tcell.EventKey) *tcell.EventKey {
		return func(event *tcell.EventKey) *tcell.EventKey {
			if event.Key() == tcell.KeyEscape {
				p.App.InModal = false
				p.App.TviewApp.SetRoot(p.App.Grid, true)
				p.App.TviewApp.SetFocus(p.App.BrokersPanel.View)
				return nil
			}
			if event.Rune() == 'r' {
				p.showClusterState()
				return nil
			}
			if event.Key() == tcell.KeyTab {
				currentPageIdx = (currentPageIdx + 1) % len(pageNames)
				pages.SwitchToPage(pageNames[currentPageIdx])
				p.App.TviewApp.SetFocus(pageTables[currentPageIdx])
				return nil
			}
			if event.Key() == tcell.KeyBacktab {
				currentPageIdx = (currentPageIdx + len(pageNames) - 1) % len(pageNames)
				pages.SwitchToPage(pageNames[currentPageIdx])
				p.App.TviewApp.SetFocus(pageTables[currentPageIdx])
				return nil
			}
			return event
		}
	}

	capture := makeInputCapture()
	topicTable.SetInputCapture(capture)
	groupTable.SetInputCapture(capture)
	configTable.SetInputCapture(capture)

	// ── Footer ──
	footer := tview.NewTextView().
		SetText("(Esc) close | (r) refresh | (Tab) cycle: offsets → groups → config | (↑/↓) scroll").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

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
	p.App.TviewApp.SetFocus(topicTable)
}

