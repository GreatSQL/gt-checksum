package logrus_test

import (
	"os"
)

type DefaultFieldHook struct {
	GetValue func() string
}

func (h *DefaultFieldHook) Levels() []Level {
	return AllLevels
}

func (h *DefaultFieldHook) Fire(e *Entry) error {
	e.Data["aDefaultField"] = h.GetValue()
	return nil
}

func ExampleDefaultFieldHook() {
	l := New()
	l.Out = os.Stdout
	l.Formatter = &TextFormatter{DisableTimestamp: true, DisableColors: true}

	l.AddHook(&DefaultFieldHook{GetValue: func() string { return "with its default value" }})
	l.Info("first log")
	// Output:
	// level=info msg="first log" aDefaultField="with its default value"
}
