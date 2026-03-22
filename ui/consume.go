package ui

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ConsumePanel struct {
	App           *App
	View          tview.Primitive
	TextView      *tview.TextView
	Footer        *tview.TextView
	ResetTimer    *time.Timer
	CurrentTopic  string
	FromBeginning bool
	cancelFunc    context.CancelFunc // Replaces StopChan for cleaner cancellation
	LastMessage   string             // last consumed message, used for copy-to-clipboard
}

func NewConsumePanel(app *App) *ConsumePanel {
	textView := tview.NewTextView()
	textView.SetScrollable(true)
	textView.SetDynamicColors(true)
	textView.SetChangedFunc(func() {
		textView.ScrollToEnd()
		app.TviewApp.Draw()
	})

	footer := tview.NewTextView().
		SetText("(t)oggle from beginning | (c)opy last msg").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(textView, 0, 1, true).
		AddItem(footer, 1, 0, false)

	flex.SetBorder(true).
		SetTitle("Consume (F3)").
		SetBorderColor(tcell.ColorWhite)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p := &ConsumePanel{
		App:      app,
		View:     flex,
		TextView: textView,
		Footer:   footer,
	}

	// Set focus handlers on the TextView to update the Flex border
	p.TextView.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	p.TextView.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p.TextView.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Rune() {
		case 't':
			p.FromBeginning = !p.FromBeginning
			status := "New Messages"
			if p.FromBeginning {
				status = "From Beginning"
			}
			if p.CurrentTopic != "" {
				flex.SetTitle(fmt.Sprintf("Consume (F3) - %s (%s)", p.CurrentTopic, status))
			} else {
				flex.SetTitle(fmt.Sprintf("Consume (F3) (%s)", status))
			}
			// Restart consumption if topic is selected
			if p.CurrentTopic != "" {
				p.StartConsuming(p.CurrentTopic)
			}
			return nil
		case 'c':
			p.copyLastMessageToClipboard()
			return nil
		}
		return event
	})

	return p
}

func (p *ConsumePanel) StartConsuming(topic string) {
	log.Printf("StartConsuming: Starting consumption for topic: %s, FromBeginning: %v", topic, p.FromBeginning)

	// Cancel any previous consumption
	if p.cancelFunc != nil {
		log.Println("StartConsuming: Cancelling previous consumption")
		p.cancelFunc()
		p.cancelFunc = nil
	}

	p.CurrentTopic = topic
	p.LastMessage = ""
	p.TextView.Clear()
	status := "New Messages"
	if p.FromBeginning {
		status = "From Beginning"
	}
	if flex, ok := p.View.(*tview.Flex); ok {
		flex.SetTitle(fmt.Sprintf("Consume (F3) - %s (%s)", topic, status))
	}

	p.App.ManagerMutex.RLock()
	if p.App.KafkaManager == nil {
		p.App.ManagerMutex.RUnlock()
		log.Println("StartConsuming: ERROR - KafkaManager is nil, cannot consume")
		return
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel

	messages := make(chan string)
	errors := make(chan error)

	go p.App.KafkaManager.ConsumeTopic(ctx, topic, p.FromBeginning, messages, errors)
	p.App.ManagerMutex.RUnlock()

	log.Printf("StartConsuming: Started consuming messages from topic: %s", topic)

	go func() {
		messageCount := 0
		for {
			select {
			case msg := <-messages:
				messageCount++
				p.LastMessage = msg
				if messageCount%100 == 0 {
					log.Printf("StartConsuming: Received %d messages from topic: %s", messageCount, topic)
				}
				p.App.TviewApp.QueueUpdateDraw(func() {
					fmt.Fprintln(p.TextView, msg)
				})
			case err := <-errors:
				log.Printf("StartConsuming: ERROR - Consumption error for topic %s: %v", topic, err)
				p.App.TviewApp.QueueUpdateDraw(func() {
					fmt.Fprintf(p.TextView, "[red]Error: %v[white]\n", err)
				})
			case <-ctx.Done():
				log.Printf("StartConsuming: Stopped consuming from topic: %s (received %d messages)", topic, messageCount)
				return
			}
		}
	}()
}

// StopConsuming cancels any active message consumption
func (p *ConsumePanel) StopConsuming() {
	if p.cancelFunc != nil {
		log.Println("StopConsuming: Cancelling active consumption")
		p.cancelFunc()
		p.cancelFunc = nil
	}
}

// copyLastMessageToClipboard copies the last consumed message to the system clipboard.
func (p *ConsumePanel) copyLastMessageToClipboard() {
	if p.LastMessage == "" {
		p.showTemporaryFooter("[yellow]No messages to copy", 2*time.Second)
		return
	}

	if err := writeToClipboard(p.LastMessage); err != nil {
		log.Printf("copyLastMessageToClipboard: clipboard error: %v", err)
		p.showTemporaryFooter("[red]Failed to copy to clipboard", 2*time.Second)
		return
	}

	log.Printf("copyLastMessageToClipboard: copied %d bytes to clipboard", len(p.LastMessage))
	p.showTemporaryFooter("[green]Copied last message to clipboard!", 2*time.Second)
}

// showTemporaryFooter shows a temporary message in the footer, then restores it.
func (p *ConsumePanel) showTemporaryFooter(msg string, duration time.Duration) {
	p.Footer.SetText(msg)

	if p.ResetTimer != nil {
		p.ResetTimer.Stop()
	}

	p.ResetTimer = time.AfterFunc(duration, func() {
		p.App.TviewApp.QueueUpdateDraw(func() {
			p.ResetTimer = nil
			p.Footer.SetText("(t)oggle from beginning | (c)opy last msg")
		})
	})
}

// writeToClipboard writes text to the system clipboard using OS-specific commands.
func writeToClipboard(text string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("pbcopy")
	case "linux":
		cmd = exec.Command("xclip", "-selection", "clipboard")
	case "windows":
		cmd = exec.Command("clip")
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
	cmd.Stdin = strings.NewReader(text)
	return cmd.Run()
}

