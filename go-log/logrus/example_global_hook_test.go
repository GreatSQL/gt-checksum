package logrus_test

import (
	"os"
)

var (
	mystring string
)

type GlobalHook struct {
}

func (h *GlobalHook) Levels() []Level {
	return AllLevels
}

func (h *GlobalHook) Fire(e *Entry) error {
	e.Data["mystring"] = mystring
	return nil
}

func ExampleGlobalHook() {
	l := New()
	l.Out = os.Stdout
	l.Formatter = &TextFormatter{DisableTimestamp: true, DisableColors: true}
	l.AddHook(&GlobalHook{})
	mystring = "first value"
	l.Info("first log")
	mystring = "another value"
	l.Info("second log")
	// Output:
	// level=info msg="first log" mystring="first value"
	// level=info msg="second log" mystring="another value"
}
