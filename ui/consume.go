package ui

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
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
		SetBorderColor(tview.Styles.BorderColor)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tview.Styles.BorderColor)
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
		flex.SetBorderColor(tview.Styles.BorderColor)
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
// The actual clipboard write runs in a goroutine so it never blocks the UI.
func (p *ConsumePanel) copyLastMessageToClipboard() {
	if p.LastMessage == "" {
		p.showTemporaryFooter("[yellow]No messages to copy", 2*time.Second)
		return
	}

	msg := p.LastMessage // snapshot for the goroutine
	p.showTemporaryFooter("[yellow]Copying...", 10*time.Second)

	go func() {
		progName, err := writeToClipboard(msg)

		p.App.TviewApp.QueueUpdateDraw(func() {
			if err != nil {
				log.Printf("copyLastMessageToClipboard: clipboard error: %v", err)
				if errors.Is(err, exec.ErrNotFound) {
					p.showTemporaryFooter(
						"[red]No clipboard program found (install xclip, xsel, or wl-copy)",
						5*time.Second,
					)
				} else {
					p.showTemporaryFooter(
						fmt.Sprintf("[red]Clipboard failed: %v", err),
						5*time.Second,
					)
				}
				return
			}

			log.Printf("copyLastMessageToClipboard: copied %d bytes via %s", len(msg), progName)
			if progName == "OSC52" {
				p.showTemporaryFooter("[green]Copied via OSC52 (requires terminal support)", 3*time.Second)
			} else {
				p.showTemporaryFooter("[green]Copied last message to clipboard!", 2*time.Second)
			}
		})
	}()
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

// clipboardCandidate describes a clipboard program and whether it forks.
type clipboardCandidate struct {
	name  string
	args  []string
	forks bool // true if the program forks to serve clipboard (e.g. wl-copy)
}

// clipboardCandidates returns an ordered list of clipboard programs to try for the current OS.
// On Linux we try multiple backends: wl-copy (Wayland), xsel, xclip.
func clipboardCandidates() []clipboardCandidate {
	switch runtime.GOOS {
	case "darwin":
		return []clipboardCandidate{{"pbcopy", nil, false}}
	case "linux":
		return []clipboardCandidate{
			{"wl-copy", nil, true},                                       // Wayland — forks to serve clipboard
			{"xsel", []string{"--clipboard", "--input"}, false},          // X11 (xsel)
			{"xclip", []string{"-selection", "clipboard", "-in"}, false}, // X11 (xclip)
		}
	case "windows":
		return []clipboardCandidate{{"clip", nil, false}}
	default:
		return nil
	}
}

// writeToClipboard writes text to the system clipboard using OS-specific commands.
// On Linux it tries multiple clipboard backends in order, then falls back to
// OSC 52 escape sequence which works over SSH sessions.
// Returns the program name (for error reporting) and any error encountered.
func writeToClipboard(text string) (string, error) {
	candidates := clipboardCandidates()

	var lastErr error
	var lastProg string

	for _, c := range candidates {
		if _, err := exec.LookPath(c.name); err != nil {
			continue // not installed, try next
		}

		var err error
		if c.forks {
			err = startForkingClipboard(c.name, c.args, text)
		} else {
			err = runClipboardCmd(c.name, c.args, text)
		}

		if err != nil {
			lastErr = fmt.Errorf("%s: %w", c.name, err)
			lastProg = c.name
			log.Printf("writeToClipboard: %s failed: %v", c.name, lastErr)
			continue
		}

		return c.name, nil
	}

	// All native clipboard programs failed or are missing — try OSC 52.
	if err := tryOSC52(text); err != nil {
		log.Printf("writeToClipboard: OSC 52 fallback failed: %v", err)
		if lastErr != nil {
			return lastProg, lastErr
		}
		return "OSC52", err
	}

	return "OSC52", nil
}

// runClipboardCmd runs a clipboard program that exits after reading stdin (pbcopy, xsel, xclip).
func runClipboardCmd(name string, args []string, text string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = strings.NewReader(text)

	devNull, _ := os.Open(os.DevNull)
	if devNull != nil {
		cmd.Stderr = devNull
		defer devNull.Close()
	}

	return cmd.Run()
}

// startForkingClipboard handles clipboard programs like wl-copy that fork a child
// to serve the clipboard. We pipe stdin, close it, and let the process live —
// we must NOT call cmd.Wait() or the forked child will block us.
func startForkingClipboard(name string, args []string, text string) error {
	cmd := exec.Command(name, args...)

	// Get a pipe to stdin so we can write and close it
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}

	// Detach stderr/stdout so forked child doesn't hold Go pipes open
	devNull, _ := os.Open(os.DevNull)
	if devNull != nil {
		cmd.Stderr = devNull
		cmd.Stdout = devNull
	}

	if err := cmd.Start(); err != nil {
		if devNull != nil {
			devNull.Close()
		}
		return fmt.Errorf("start: %w", err)
	}

	// Write content and close stdin — wl-copy reads it, then forks to background
	if _, err := stdin.Write([]byte(text)); err != nil {
		if devNull != nil {
			devNull.Close()
		}
		return fmt.Errorf("write: %w", err)
	}
	stdin.Close()

	// Wait in a goroutine so we don't leak, but don't block the caller.
	// The parent process exits quickly once stdin is closed; the forked
	// child keeps serving the clipboard independently.
	go func() {
		_ = cmd.Wait()
		if devNull != nil {
			devNull.Close()
		}
	}()

	return nil
}

// tryOSC52 writes an OSC 52 escape sequence to the controlling terminal.
// The sequence format is: ESC ] 52 ; c ; <base64-payload> ESC \
// The terminal emulator processes this and sets the system clipboard.
func tryOSC52(text string) error {
	encoded := base64.StdEncoding.EncodeToString([]byte(text))

	// Write directly to /dev/tty so it reaches the terminal emulator
	// even while tview/tcell owns stdout.
	tty, err := os.OpenFile("/dev/tty", os.O_WRONLY, 0)
	if err != nil {
		// Fallback: try stdout (works on some systems / inside tmux)
		_, err2 := fmt.Fprintf(os.Stdout, "\033]52;c;%s\033\\", encoded)
		return err2
	}
	defer tty.Close()

	_, err = fmt.Fprintf(tty, "\033]52;c;%s\033\\", encoded)
	return err
}

