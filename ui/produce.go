package ui

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dubel/lazykafka/config"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ProducePanel struct {
	App          *App
	View         tview.Primitive
	TextArea     *tview.TextArea
	Footer       *tview.TextView
	CurrentTopic string
	ResetTimer   *time.Timer
}

func NewProducePanel(app *App) *ProducePanel {
	textArea := tview.NewTextArea()
	textArea.SetPlaceholder("Type (or paste) your message here...")
	textArea.SetWrap(true)
	textArea.SetWordWrap(true)

	footer := tview.NewTextView().
		SetText("(alt+enter/ctrl+s) send | (ctrl+p) publish file").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(textArea, 0, 1, true).
		AddItem(footer, 1, 0, false)

	flex.SetBorder(true).
		SetTitle("Produce (F4)").
		SetBorderColor(tcell.ColorWhite)

	flex.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	flex.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	// Set focus handlers on the TextArea to update the Flex border
	textArea.SetFocusFunc(func() {
		flex.SetBorderColor(tcell.ColorGreen)
	})
	textArea.SetBlurFunc(func() {
		flex.SetBorderColor(tcell.ColorWhite)
	})

	p := &ProducePanel{
		App:      app,
		View:     flex,
		TextArea: textArea,
		Footer:   footer,
	}

	p.TextArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Ctrl+P to open file picker
		if event.Key() == tcell.KeyCtrlP {
			p.openFilePicker()
			return nil
		}

		// Alt+Enter or Ctrl+S to send message
		isAltEnter := event.Key() == tcell.KeyEnter && event.Modifiers()&tcell.ModAlt != 0
		isCtrlS := event.Key() == tcell.KeyCtrlS
		if isAltEnter || isCtrlS {
			text := p.TextArea.GetText()

			p.App.ManagerMutex.RLock()
			if p.CurrentTopic == "" || p.App.KafkaManager == nil {
				p.App.ManagerMutex.RUnlock()

				p.showTemporaryMessage("[red]No broker and topic selected - we need to connect first!", 5*time.Second)

				if p.CurrentTopic == "" {
					log.Println("ProduceMessage: No topic selected, ignoring")
				} else if p.App.KafkaManager == nil {
					log.Println("ProduceMessage: ERROR - KafkaManager is nil")
				}
				return nil
			}

			if text == "" {
				p.App.ManagerMutex.RUnlock()
				log.Println("ProduceMessage: Empty message, ignoring")
				return nil
			}

			log.Printf("ProduceMessage: Sending message to topic '%s': %s", p.CurrentTopic, text)
			err := p.App.KafkaManager.ProduceMessage(p.CurrentTopic, text)
			p.App.ManagerMutex.RUnlock()

			if err != nil {
				log.Printf("ProduceMessage: ERROR - Failed to produce message to topic '%s': %v", p.CurrentTopic, err)
				p.showTemporaryMessage("[red]Failed to send message!", 5*time.Second)
			} else {
				log.Printf("ProduceMessage: Successfully sent message to topic '%s'", p.CurrentTopic)
				p.TextArea.SetText("", true)
				p.showTemporaryMessage("[green]Message sent!", 2*time.Second)
			}
			return nil
		}
		return event
	})

	return p
}

func (p *ProducePanel) showTemporaryMessage(msg string, duration time.Duration) {
	p.Footer.SetText(msg)

	if p.ResetTimer != nil {
		p.ResetTimer.Stop()
	}

	p.ResetTimer = time.AfterFunc(duration, func() {
		p.App.TviewApp.QueueUpdateDraw(func() {
			p.Footer.SetText("(alt+enter/ctrl+s) send | (ctrl+p) publish file")
		})
	})
}

func (p *ProducePanel) SetTopic(topic string) {
	p.CurrentTopic = topic
	if flex, ok := p.View.(*tview.Flex); ok {
		flex.SetTitle("Produce (F4) - " + topic)
	}
}

// openFilePicker checks connection first, then opens the file picker modal.
func (p *ProducePanel) openFilePicker() {
	p.App.ManagerMutex.RLock()
	if p.CurrentTopic == "" || p.App.KafkaManager == nil {
		p.App.ManagerMutex.RUnlock()
		p.showTemporaryMessage("[red]No broker and topic selected - we need to connect first!", 5*time.Second)
		return
	}
	p.App.ManagerMutex.RUnlock()

	// Determine starting directory: cached or home.
	startDir := p.App.Settings.LastFilePickerDir
	if startDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			startDir = "/"
		} else {
			startDir = home
		}
	}
	// Validate cached dir still exists.
	if info, err := os.Stat(startDir); err != nil || !info.IsDir() {
		home, _ := os.UserHomeDir()
		if home == "" {
			home = "/"
		}
		startDir = home
	}

	p.showFilePicker(startDir)
}

// showFilePicker renders a full-screen file picker at the given directory.
func (p *ProducePanel) showFilePicker(dir string) {
	list := tview.NewList().
		ShowSecondaryText(true).
		SetHighlightFullLine(true)

	titleText := fmt.Sprintf("📂 File Picker — %s", dir)
	list.SetBorder(true).
		SetTitle(titleText).
		SetTitleAlign(tview.AlignLeft).
		SetBorderColor(tcell.ColorTeal)

	footerHelp := tview.NewTextView().
		SetText("(enter/space) open | (backspace) parent dir | (esc) cancel").
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	// Read directory entries.
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("FilePicker: failed to read dir %s: %v", dir, err)
		p.showTemporaryMessage("[red]Cannot read directory!", 3*time.Second)
		return
	}

	type fpEntry struct {
		name  string
		isDir bool
		size  int64
	}

	var dirEntries []fpEntry
	var fileEntries []fpEntry

	for _, e := range entries {
		// Skip hidden files/dirs (starting with dot).
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		fe := fpEntry{name: e.Name(), isDir: e.IsDir(), size: info.Size()}
		if e.IsDir() {
			dirEntries = append(dirEntries, fe)
		} else {
			fileEntries = append(fileEntries, fe)
		}
	}

	sort.Slice(dirEntries, func(i, j int) bool { return dirEntries[i].name < dirEntries[j].name })
	sort.Slice(fileEntries, func(i, j int) bool { return fileEntries[i].name < fileEntries[j].name })

	// Add ".." to go up unless at root.
	if dir != "/" {
		list.AddItem("📁 ..", "Parent directory", 0, nil)
	}

	for _, d := range dirEntries {
		list.AddItem("📁 "+d.name, "Directory", 0, nil)
	}
	for _, f := range fileEntries {
		list.AddItem("📄 "+f.name, fmt.Sprintf("File — %s", formatFileSize(f.size)), 0, nil)
	}

	if list.GetItemCount() == 0 {
		list.AddItem("(empty directory)", "", 0, nil)
	}

	// Handle selection (enter).
	list.SetSelectedFunc(func(idx int, mainText, secText string, shortcut rune) {
		p.handleFilePickerSelect(dir, mainText)
	})

	// Handle key presses.
	list.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			p.App.InModal = false
			p.App.TviewApp.SetRoot(p.App.Grid, true)
			p.App.TviewApp.SetFocus(p.View)
			return nil
		case tcell.KeyBackspace, tcell.KeyBackspace2:
			if dir != "/" {
				parent := filepath.Dir(dir)
				p.showFilePicker(parent)
			}
			return nil
		}
		// Space as alternative select.
		if event.Rune() == ' ' {
			idx := list.GetCurrentItem()
			mainText, _ := list.GetItemText(idx)
			p.handleFilePickerSelect(dir, mainText)
			return nil
		}
		return event
	})

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(list, 0, 1, true).
		AddItem(footerHelp, 1, 0, false)

	p.App.InModal = true
	p.App.TviewApp.SetRoot(flex, true)
}

// handleFilePickerSelect processes a selection in the file picker list.
func (p *ProducePanel) handleFilePickerSelect(currentDir, mainText string) {
	// Strip emoji prefix to get the real name.
	cleanName := mainText
	cleanName = strings.TrimPrefix(cleanName, "📁 ")
	cleanName = strings.TrimPrefix(cleanName, "📄 ")

	if cleanName == ".." {
		parent := filepath.Dir(currentDir)
		p.showFilePicker(parent)
		return
	}

	if cleanName == "(empty directory)" {
		return
	}

	fullPath := filepath.Join(currentDir, cleanName)
	info, err := os.Stat(fullPath)
	if err != nil {
		log.Printf("FilePicker: cannot stat %s: %v", fullPath, err)
		return
	}

	if info.IsDir() {
		p.showFilePicker(fullPath)
		return
	}

	// It's a file — read and publish.
	p.publishFileContent(fullPath, currentDir)
}

// publishFileContent reads a file and sends its content to the current topic.
func (p *ProducePanel) publishFileContent(filePath, parentDir string) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("PublishFile: failed to read file %s: %v", filePath, err)
		p.App.InModal = false
		p.App.TviewApp.SetRoot(p.App.Grid, true)
		p.App.TviewApp.SetFocus(p.View)
		p.showTemporaryMessage("[red]Failed to read file!", 5*time.Second)
		return
	}

	text := string(content)
	if text == "" {
		p.App.InModal = false
		p.App.TviewApp.SetRoot(p.App.Grid, true)
		p.App.TviewApp.SetFocus(p.View)
		p.showTemporaryMessage("[yellow]File is empty, nothing to send.", 3*time.Second)
		return
	}

	// Close the modal first.
	p.App.InModal = false
	p.App.TviewApp.SetRoot(p.App.Grid, true)
	p.App.TviewApp.SetFocus(p.View)

	// Cache the directory for next time.
	p.App.Settings.LastFilePickerDir = parentDir
	if err := config.SaveSettings(p.App.Settings); err != nil {
		log.Printf("PublishFile: failed to save settings: %v", err)
	}

	// Publish in background.
	go func() {
		p.App.ManagerMutex.RLock()
		if p.App.KafkaManager == nil || p.CurrentTopic == "" {
			p.App.ManagerMutex.RUnlock()
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showTemporaryMessage("[red]No broker/topic — cannot publish!", 5*time.Second)
			})
			return
		}
		log.Printf("PublishFile: Sending file %s content to topic '%s' (%d bytes)",
			filepath.Base(filePath), p.CurrentTopic, len(text))
		err := p.App.KafkaManager.ProduceMessage(p.CurrentTopic, text)
		p.App.ManagerMutex.RUnlock()

		if err != nil {
			log.Printf("PublishFile: ERROR - Failed to produce: %v", err)
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showTemporaryMessage("[red]Failed to publish file content!", 5*time.Second)
			})
		} else {
			log.Printf("PublishFile: Successfully sent file content to topic '%s'", p.CurrentTopic)
			p.App.TviewApp.QueueUpdateDraw(func() {
				p.showTemporaryMessage(
					fmt.Sprintf("[green]File \"%s\" published!", filepath.Base(filePath)),
					3*time.Second,
				)
			})
		}
	}()
}


// formatFileSize returns a human-readable file size string.
func formatFileSize(size int64) string {
	switch {
	case size < 1024:
		return fmt.Sprintf("%d B", size)
	case size < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(size)/1024)
	case size < 1024*1024*1024:
		return fmt.Sprintf("%.1f MB", float64(size)/(1024*1024))
	default:
		return fmt.Sprintf("%.1f GB", float64(size)/(1024*1024*1024))
	}
}

