package Tools

import (
	"fmt"
	"log"
	"os"
	"time"
)


const (
	colorReset  = "\x1b[0m"
	colorRed    = "\x1b[31m"
	colorGreen  = "\x1b[32m"
	colorYellow = "\x1b[33m"
	colorBlue   = "\x1b[34m"
)

var (
	infoLog  = log.New(os.Stdout, "", 0)
	errorLog = log.New(os.Stderr, "", 0)
)

func timestamp() string {
	return time.Now().Format("2006-01-02 15:04:05.000")
}

func Debug(msg string, args ...any) {
	msg = " msg = " + msg
	print(infoLog, colorBlue, "DEBUG", msg, args...)
}

func Info(msg string, args ...any) {
	msg = " msg = " + msg
	print(infoLog, colorGreen, "INFO", msg, args...)
}

func Warn(msg string, args ...any) {
	msg = " msg = " + msg
	print(infoLog, colorYellow, "WARN", msg, args...)
}

func Error(msg string, args ...any) {
	msg = " msg = " + msg
	print(errorLog, colorRed, "ERROR", msg, args...)
}

func print(l *log.Logger, color, level, msg string, args ...any) {
	prefix := fmt.Sprintf(
		"%s [%s%s%s] ",
		timestamp(),
		color,
		level,
		colorReset,
	)

	if len(args) > 0 {
		l.Println(prefix + fmt.Sprintf(msg, args...))
	} else {
		l.Println(prefix + msg)
	}
}
