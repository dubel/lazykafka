package ui

import (
	_ "embed"
	"strings"

	"github.com/dubel/lazykafka/config"
	"github.com/mattn/go-runewidth"
	"github.com/rivo/tview"
)

//go:embed assets/splash.txt
var splashArt string

type SplashPanel struct {
	App      *App
	Flex     *tview.Flex
	Settings *config.Settings
}

func NewSplashPanel(app *App, settings *config.Settings) *SplashPanel {
	s := &SplashPanel{
		App:      app,
		Settings: settings,
	}

	// Calculate approximate width/height of splash art
	lines := strings.Split(splashArt, "\n")
	height := len(lines)
	width := 0
	for _, line := range lines {
		w := runewidth.StringWidth(line)
		if w > width {
			width = w
		}
	}

	// TextView for the ASCII art
	artView := tview.NewTextView().
		SetText(splashArt).
		SetTextAlign(tview.AlignCenter).
		SetDynamicColors(true)

	// Form for settings
	form := tview.NewForm().
		AddCheckbox("Show on startup", settings.ShowSplashOnStartup, func(checked bool) {
			settings.ShowSplashOnStartup = checked
			config.SaveSettings(settings)
		}).
		AddCheckbox("Check for updates", settings.CheckForUpdates, func(checked bool) {
			settings.CheckForUpdates = checked
			config.SaveSettings(settings)
		}).
		AddButton("Continue", func() {
			s.dismiss()
		})

	form.SetButtonsAlign(tview.AlignCenter)

	// Center everything in a flex
	// We use a centered flex to make it look like a modal
	modalContent := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(artView, height+2, 0, false). // +2 for detailed spacing
		AddItem(form, 7, 0, true)             // Checkboxes + Button

	modalContent.SetBorder(true).
		SetTitle(" Welcome to LazyKafka ").
		SetTitleAlign(tview.AlignCenter)

	// Outer flex to center the modal window on screen
	s.Flex = tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(modalContent, height+12, 1, true).
			AddItem(nil, 0, 1, false), width+10, 1, true).
		AddItem(nil, 0, 1, false)

		// Handle "Enter" to dismiss if focus is not on specific form elements (optional convenience)
		// The form handles Enter on the button automatically.

	return s
}

func (s *SplashPanel) dismiss() {
	s.App.InModal = false
	s.App.TviewApp.SetRoot(s.App.Grid, true)
	s.App.TviewApp.SetFocus(s.App.BrokersPanel.View)
}
