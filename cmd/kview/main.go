// kview is a simple CLI tool for browsing kv files.
//
// Usage:
//
//	kview <filename>           # interactive mode
//	kview -l <filename>        # list mode (print all)
//	kview -l -n 20 <filename>  # list first 20 items
//
// Interactive mode:
//
//	j/↓    scroll down
//	k/↑    scroll up
//	g      jump to first
//	G      jump to last
//	/      search key (prefix match)
//	q/Esc  quit
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dacapoday/smol/kv"
	"golang.org/x/term"
)

func main() {
	listFlag := flag.Bool("l", false, "list mode (non-interactive)")
	countFlag := flag.Int("n", 0, "number of items (0 = all)")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: kview [-l] [-n count] <filename>")
		os.Exit(1)
	}

	filename := flag.Arg(0)

	if *listFlag {
		runList(filename, *countFlag)
		return
	}

	runInteractive(filename)
}

func runList(filename string, count int) {
	db, err := kv.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	iter := db.Iter()
	defer iter.Close()

	n := 0
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		if count > 0 && n >= count {
			break
		}
		fmt.Printf("%s: %s\n", display(iter.Key(), 40), display(iter.Val(), 60))
		n++
	}
}

func runInteractive(filename string) {
	db, err := kv.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	iter := db.Iter()
	defer iter.Close()
	iter.SeekFirst()

	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	v := &viewer{
		db:   db,
		iter: iter,
	}
	v.updateSize()
	v.load()

	fmt.Print("\033[?25l\033[2J") // hide cursor, clear screen once
	defer fmt.Print("\033[?25h\033[2J\033[H") // show cursor, clear screen

	reader := bufio.NewReader(os.Stdin)

	for {
		// update terminal size on each render
		if v.updateSize() {
			v.load() // reload if size changed
		}
		v.render()

		b, err := reader.ReadByte()
		if err != nil {
			break
		}

		v.status = "" // clear status on any input

		switch b {
		case 'q', 3, 27: // q, Ctrl+C, Esc
			if b == 27 && reader.Buffered() > 0 {
				// escape sequence
				b2, _ := reader.ReadByte()
				if b2 == '[' {
					b3, _ := reader.ReadByte()
					switch b3 {
					case 'A': // up
						v.up()
					case 'B': // down
						v.down()
					case '5': // page up
						reader.ReadByte()
						v.pageUp()
					case '6': // page down
						reader.ReadByte()
						v.pageDown()
					}
				}
				continue
			}
			return
		case 'j':
			v.down()
		case 'k':
			v.up()
		case 'g':
			v.first()
		case 'G':
			v.last()
		case '/':
			v.search(reader, oldState)
		}
	}
}

type item struct {
	key, val []byte
}

type viewer struct {
	db      *kv.DB
	iter    kv.DBIter
	items   []item
	width   int
	height  int
	atStart bool // no more items before first
	atEnd   bool // no more items after last
	status  string
}

// updateSize checks terminal size and returns true if changed.
func (v *viewer) updateSize() bool {
	w, h, err := term.GetSize(int(os.Stdin.Fd()))
	if err != nil {
		w, h = 80, 24
	}
	if w == v.width && h == v.height {
		return false
	}
	v.width, v.height = w, h
	return true
}

func (v *viewer) lines() int {
	return v.height - 4 // title + separator + separator + status
}

func (v *viewer) load() {
	v.items = nil
	v.atStart = false
	v.atEnd = false

	if !v.iter.Valid() {
		v.iter.SeekFirst()
		if !v.iter.Valid() {
			v.atStart = true
			v.atEnd = true
			return
		}
	}

	// load all items from current position (up to lines())
	lines := v.lines()
	for i := 0; i < lines && v.iter.Valid(); i++ {
		v.items = append(v.items, item{
			key: bytes.Clone(v.iter.Key()),
			val: bytes.Clone(v.iter.Val()),
		})
		if !v.iter.Next() {
			v.atEnd = true
			break
		}
	}

	// check boundaries and restore position
	if len(v.items) > 0 {
		v.iter.Seek(v.items[0].key)
		if !v.iter.Prev() {
			v.atStart = true
		}
		v.iter.Seek(v.items[0].key)
	}
}

func (v *viewer) down() {
	if len(v.items) == 0 {
		return
	}

	// try to get next item
	last := v.items[len(v.items)-1].key
	v.iter.Seek(last)
	if v.iter.Next() {
		// add new at bottom, remove from top
		v.items = append(v.items[1:], item{
			key: bytes.Clone(v.iter.Key()),
			val: bytes.Clone(v.iter.Val()),
		})
		v.atStart = false
		if !v.iter.Next() {
			v.atEnd = true
		}
		v.iter.Seek(v.items[0].key)
	} else if len(v.items) > 1 {
		// at end, allow scrolling until only 1 item visible
		v.items = v.items[1:]
		v.atEnd = true
	}
}

func (v *viewer) up() {
	if v.atStart || len(v.items) == 0 {
		return
	}

	// try to get prev item
	first := v.items[0].key
	v.iter.Seek(first)
	if v.iter.Prev() {
		newItem := item{
			key: bytes.Clone(v.iter.Key()),
			val: bytes.Clone(v.iter.Val()),
		}
		// only remove from bottom if screen is full
		if len(v.items) >= v.lines() {
			v.items = append([]item{newItem}, v.items[:len(v.items)-1]...)
		} else {
			v.items = append([]item{newItem}, v.items...)
		}
		v.atEnd = false
		if !v.iter.Prev() {
			v.atStart = true
		}
		v.iter.Seek(v.items[0].key)
	}
}

func (v *viewer) pageDown() {
	for i := 0; i < v.lines()-1; i++ {
		v.down()
	}
}

func (v *viewer) pageUp() {
	for i := 0; i < v.lines()-1; i++ {
		v.up()
	}
}

func (v *viewer) first() {
	v.iter.SeekFirst()
	v.load()
}

func (v *viewer) last() {
	v.iter.SeekLast()
	// back up to show a full screen
	for i := 0; i < v.lines()-1; i++ {
		if !v.iter.Prev() {
			break
		}
	}
	v.load()
}

func (v *viewer) search(reader *bufio.Reader, oldState *term.State) {
	// show search prompt
	fmt.Print("\033[?25h") // show cursor
	fmt.Printf("\033[%d;1H\033[K/", v.height)

	// read search input
	var input []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			break
		}
		if b == 27 || b == 3 { // Esc or Ctrl+C
			fmt.Print("\033[?25l")
			v.status = ""
			return
		}
		if b == 13 || b == 10 { // Enter
			break
		}
		if b == 127 || b == 8 { // Backspace
			if len(input) > 0 {
				input = input[:len(input)-1]
				fmt.Print("\b \b")
			}
			continue
		}
		if b >= 32 && b < 127 {
			input = append(input, b)
			fmt.Print(string(b))
		}
	}
	fmt.Print("\033[?25l")

	if len(input) == 0 {
		v.status = ""
		return
	}

	// search for key
	key := input
	v.iter.Seek(key)
	if v.iter.Valid() {
		v.load()
		v.status = fmt.Sprintf("jumped to: %s", display(key, 20))
	} else {
		v.status = "not found"
	}
}

func (v *viewer) render() {
	var b strings.Builder

	// move to top (no clear)
	b.WriteString("\033[H")

	// header
	b.WriteString("[ kview ]\033[K\r\n")
	b.WriteString(strings.Repeat("─", v.width))
	b.WriteString("\033[K\r\n")

	// items
	keyWidth := 32
	valWidth := v.width - keyWidth - 4
	if valWidth < 20 {
		valWidth = 20
	}

	lines := v.lines()
	for i := 0; i < lines; i++ {
		if i < len(v.items) {
			it := v.items[i]
			b.WriteString(display(it.key, keyWidth))
			b.WriteString(": ")
			b.WriteString(display(it.val, valWidth))
		} else {
			b.WriteString("~")
		}
		b.WriteString("\033[K\r\n")
	}

	// footer
	b.WriteString(strings.Repeat("─", v.width))
	b.WriteString("\033[K\r\n")

	// status line
	pos := ""
	if v.atStart && v.atEnd {
		pos = "[all]"
	} else if v.atStart {
		pos = "[top]"
	} else if v.atEnd {
		pos = "[end]"
	}

	if v.status != "" {
		b.WriteString(" ")
		b.WriteString(v.status)
		b.WriteString(" ")
		b.WriteString(pos)
	} else {
		b.WriteString(" j/k:scroll g/G:jump /:search q:quit ")
		b.WriteString(pos)
	}
	b.WriteString("\033[K")

	fmt.Print(b.String())
}

// display formats bytes for display, truncating if needed.
// Tries to show as string if printable, otherwise hex.
func display(b []byte, maxLen int) string {
	if len(b) == 0 {
		return "(empty)"
	}

	// check if printable UTF-8
	if utf8.Valid(b) && isPrintable(b) {
		runes := []rune(string(b))
		if len(runes) > maxLen-3 {
			return string(runes[:maxLen-3]) + "..."
		}
		return string(runes)
	}

	// show as hex
	hex := fmt.Sprintf("%x", b)
	if len(hex) > maxLen-3 {
		return hex[:maxLen-3] + "..."
	}
	return hex
}

func isPrintable(b []byte) bool {
	for _, r := range string(b) {
		if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}
