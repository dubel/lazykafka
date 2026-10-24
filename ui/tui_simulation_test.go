//go:build integration
// +build integration

package ui_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// TUI tests using tcell's SimulationScreen
// These tests verify UI behavior without needing a real terminal
//
// Run with:
//   go test -tags=integration -v ./ui/...

// TestSimulationScreen_BasicSetup demonstrates how to create a simulated screen
func TestSimulationScreen_BasicSetup(t *testing.T) {
	// Create a simulation screen (no real terminal needed)
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatalf("Failed to init simulation screen: %v", err)
	}

	// Set a reasonable size
	screen.SetSize(80, 24)

	// Create a simple tview application
	app := tview.NewApplication()
	app.SetScreen(screen)

	// Create a simple text view
	textView := tview.NewTextView().
		SetText("Hello, TUI Testing!").
		SetTextAlign(tview.AlignCenter)
	textView.SetBorder(true).SetTitle("Test")

	app.SetRoot(textView, true)

	var wg sync.WaitGroup
	wg.Add(1)

	// Run the app in a goroutine (it would normally block)
	go func() {
		defer wg.Done()
		if err := app.Run(); err != nil {
			t.Errorf("app.Run: %v", err)
		}
	}()

	// Give it a moment to render
	time.Sleep(100 * time.Millisecond)

	// Force a draw and sync
	app.QueueUpdateDraw(func() {})
	time.Sleep(50 * time.Millisecond)

	// Sync screen to get content
	screen.Sync()

	// Read what was rendered to the screen
	content := getScreenContent(screen)
	t.Logf("Screen content:\n%s", content)

	// Stop the app
	app.Stop()
	wg.Wait()

	if !strings.Contains(content, "Hello") {
		t.Error("Expected text not found on screen")
	}
}

// TestSimulationScreen_KeyboardNavigation tests keyboard input handling
func TestSimulationScreen_KeyboardNavigation(t *testing.T) {
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatalf("Failed to init simulation screen: %v", err)
	}
	screen.SetSize(80, 24)

	app := tview.NewApplication()
	app.SetScreen(screen)

	// Create a list with items
	list := tview.NewList().
		AddItem("Item 1", "First item", '1', nil).
		AddItem("Item 2", "Second item", '2', nil).
		AddItem("Item 3", "Third item", '3', nil)
	list.SetBorder(true).SetTitle("Navigation Test")

	app.SetRoot(list, true)

	// Track which item is selected
	var mu sync.Mutex
	selectedIndex := 0
	list.SetChangedFunc(func(index int, mainText, secondaryText string, shortcut rune) {
		mu.Lock()
		selectedIndex = index
		mu.Unlock()
	})

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := app.Run(); err != nil {
			t.Errorf("app.Run: %v", err)
		}
	}()

	// Wait for initial render
	time.Sleep(100 * time.Millisecond)

	// Simulate pressing Down arrow twice
	screen.InjectKey(tcell.KeyDown, 0, tcell.ModNone)
	time.Sleep(100 * time.Millisecond)
	screen.InjectKey(tcell.KeyDown, 0, tcell.ModNone)
	time.Sleep(100 * time.Millisecond)

	// Check the result
	mu.Lock()
	result := selectedIndex
	mu.Unlock()

	// Stop the app
	app.Stop()
	wg.Wait()

	// Verify we navigated to the third item (index 2)
	if result != 2 {
		t.Errorf("Expected selected index 2, got %d", result)
	}
}

// TestSimulationScreen_TextInput tests text input in a form field
func TestSimulationScreen_TextInput(t *testing.T) {
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatalf("Failed to init simulation screen: %v", err)
	}
	screen.SetSize(80, 24)

	app := tview.NewApplication()
	app.SetScreen(screen)

	// Create an input field
	inputField := tview.NewInputField().
		SetLabel("Message: ").
		SetFieldWidth(40)

	var capturedText string
	inputField.SetDoneFunc(func(key tcell.Key) {
		capturedText = inputField.GetText()
		app.Stop()
	})

	app.SetRoot(inputField, true)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := app.Run(); err != nil {
			t.Errorf("app.Run: %v", err)
		}
	}()

	// Wait for render
	time.Sleep(100 * time.Millisecond)

	// Type some text
	for _, char := range "Hello World" {
		screen.InjectKey(tcell.KeyRune, char, tcell.ModNone)
		time.Sleep(20 * time.Millisecond)
	}

	// Press Enter to submit (this triggers SetDoneFunc which calls app.Stop())
	time.Sleep(50 * time.Millisecond)
	screen.InjectKey(tcell.KeyEnter, 0, tcell.ModNone)

	wg.Wait()

	if capturedText != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%s'", capturedText)
	}
}

// TestSimulationScreen_FunctionKeys tests F-key navigation
func TestSimulationScreen_FunctionKeys(t *testing.T) {
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatalf("Failed to init simulation screen: %v", err)
	}
	screen.SetSize(120, 40)

	app := tview.NewApplication()
	app.SetScreen(screen)

	// Create panels similar to lazykafka
	panel1 := tview.NewTextView().SetText("Panel 1 - Brokers")
	panel1.SetBorder(true).SetTitle("Brokers (F1)")

	panel2 := tview.NewTextView().SetText("Panel 2 - Topics")
	panel2.SetBorder(true).SetTitle("Topics (F2)")

	grid := tview.NewGrid().
		SetRows(0).
		SetColumns(0, 0).
		AddItem(panel1, 0, 0, 1, 1, 0, 0, true).
		AddItem(panel2, 0, 1, 1, 1, 0, 0, false)

	// Track focus changes
	var mu sync.Mutex
	focusHistory := []string{}

	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyF1:
			app.SetFocus(panel1)
			mu.Lock()
			focusHistory = append(focusHistory, "F1-Panel1")
			mu.Unlock()
			return nil
		case tcell.KeyF2:
			app.SetFocus(panel2)
			mu.Lock()
			focusHistory = append(focusHistory, "F2-Panel2")
			mu.Unlock()
			return nil
		case tcell.KeyEscape:
			app.Stop()
			return nil
		}
		return event
	})

	app.SetRoot(grid, true)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := app.Run(); err != nil {
			t.Errorf("app.Run: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Press F2 to switch to panel 2
	screen.InjectKey(tcell.KeyF2, 0, tcell.ModNone)
	time.Sleep(100 * time.Millisecond)

	// Press F1 to switch back to panel 1
	screen.InjectKey(tcell.KeyF1, 0, tcell.ModNone)
	time.Sleep(100 * time.Millisecond)

	// Press Escape to exit
	screen.InjectKey(tcell.KeyEscape, 0, tcell.ModNone)

	wg.Wait()

	// Verify focus changed as expected
	mu.Lock()
	history := make([]string, len(focusHistory))
	copy(history, focusHistory)
	mu.Unlock()

	expected := []string{"F2-Panel2", "F1-Panel1"}
	if len(history) != len(expected) {
		t.Errorf("Expected %d focus changes, got %d: %v", len(expected), len(history), history)
	}
	for i, exp := range expected {
		if i < len(history) && history[i] != exp {
			t.Errorf("Focus history[%d]: expected %s, got %s", i, exp, history[i])
		}
	}
}

// TestSimulationScreen_ModalDialog tests modal dialog behavior
func TestSimulationScreen_ModalDialog(t *testing.T) {
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatalf("Failed to init simulation screen: %v", err)
	}
	screen.SetSize(80, 24)

	app := tview.NewApplication()
	app.SetScreen(screen)

	// Create a modal
	modal := tview.NewModal().
		SetText("Are you sure?").
		AddButtons([]string{"Cancel", "OK"})

	var selectedButton string
	modal.SetDoneFunc(func(buttonIndex int, buttonLabel string) {
		selectedButton = buttonLabel
		app.Stop()
	})

	app.SetRoot(modal, true)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := app.Run(); err != nil {
			t.Errorf("app.Run: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Press Tab to move to "OK" button, then Enter
	screen.InjectKey(tcell.KeyTab, 0, tcell.ModNone)
	time.Sleep(100 * time.Millisecond)
	screen.InjectKey(tcell.KeyEnter, 0, tcell.ModNone)

	wg.Wait()

	if selectedButton != "OK" {
		t.Errorf("Expected 'OK' button, got '%s'", selectedButton)
	}
}

// Helper function to read screen content as a string
func getScreenContent(screen tcell.SimulationScreen) string {
	width, height := screen.Size()
	var builder strings.Builder

	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			mainc, _, _, _ := screen.GetContent(x, y)
			if mainc == 0 {
				mainc = ' '
			}
			builder.WriteRune(mainc)
		}
		builder.WriteRune('\n')
	}

	return builder.String()
}
