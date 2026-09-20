package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

var skipDirs = map[string]bool{
	".git": true, "target": true, "build": true, "out": true,
	".idea": true, ".gradle": true, "node_modules": true,
	".vscode": true, "__pycache__": true,
}

type ScanRequest struct {
	Path string `json:"path"`
}

type ScanResult struct {
	Root          string `json:"root"`
	TotalFiles    int    `json:"totalFiles"`
	TotalLines    int64  `json:"totalLines"`
	CharsInc      int64  `json:"charsInc"`
	CharsExc      int64  `json:"charsExc"`
	BlankLines    int64  `json:"blankLines"`
	NonBlankLines int64  `json:"nonBlankLines"`
	TotalSize     int64  `json:"totalSize"`
	ScanTimeMs    int64  `json:"scanTimeMs"`
	ScanTimeStr   string `json:"scanTimeStr"`
	Error         string `json:"error,omitempty"`
}

type ListResult struct {
	Current string   `json:"current"`
	Parent  string   `json:"parent"`
	Dirs    []string `json:"dirs"`
	Sep     string   `json:"sep"`
	Error   string   `json:"error,omitempty"`
}

func main() {
	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/api/scan", scanHandler)
	http.HandleFunc("/api/list", listHandler)

	port := ":8080"
	fmt.Printf("Application running at http://localhost%s\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	data, _ := os.ReadFile("index.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func listHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		path, _ = os.Getwd()
	}

	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}

	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		path, _ = os.Getwd()
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		writeJSON(w, ListResult{
			Current: path,
			Error:   err.Error(),
			Sep:     string(os.PathSeparator),
		})
		return
	}

	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry.Name())
		}
	}
	sort.Strings(dirs)

	parent := ""
	if path != "/" && path != filepath.Dir(path) {
		if !(runtime.GOOS == "windows" && len(path) <= 3) {
			parent = filepath.Dir(path)
		}
	}

	writeJSON(w, ListResult{
		Current: path,
		Parent:  parent,
		Dirs:    dirs,
		Sep:     string(os.PathSeparator),
	})
}

func scanHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req ScanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Path) == "" {
		writeJSON(w, ScanResult{Error: "no directory selected"})
		return
	}

	result, err := scanRoot(req.Path)
	if err != nil {
		result.Error = err.Error()
	}

	writeJSON(w, result)
}

func scanRoot(root string) (ScanResult, error) {
	start := time.Now()

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return ScanResult{}, err
	}

	info, err := os.Stat(absRoot)
	if err != nil {
		return ScanResult{}, fmt.Errorf("cannot access %s: %w", root, err)
	}
	if !info.IsDir() {
		return ScanResult{}, fmt.Errorf("not a directory: %s", root)
	}

	var (
		totalFiles, totalLines, charsInc, charsExc, blankLines, totalSize int64
		wg                                                                sync.WaitGroup
	)

	// ASCII whitespace lookup
	var asciiSpace [128]bool
	for _, c := range "\t\n\v\f\r " {
		asciiSpace[c] = true
	}

	// Bound concurrent I/O, reuse read buffers to keep GC pressure low
	sem := make(chan struct{}, runtime.NumCPU()*2)
	bufPool := sync.Pool{New: func() any {
		b := make([]byte, 0, 256*1024)
		return &b
	}}

	processFile := func(path string) {
		sem <- struct{}{}
		defer func() { <-sem }()

		f, err := os.Open(path)
		if err != nil {
			return
		}
		defer f.Close()

		st, err := f.Stat()
		if err != nil {
			return
		}

		n := int(st.Size())
		bp := bufPool.Get().(*[]byte)
		defer bufPool.Put(bp)
		if cap(*bp) < n {
			*bp = make([]byte, n)
		}
		buf := (*bp)[:n]
		read, _ := io.ReadFull(f, buf)
		data := buf[:read]

		var lLines, lInc, lExc, lBlank int64

		for len(data) > 0 {
			var line []byte
			if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
				line = data[:idx]
				data = data[idx+1:]
			} else {
				line = data
				data = nil
			}
			if n := len(line); n > 0 && line[n-1] == '\r' {
				line = line[:n-1]
			}

			lLines++
			var nonSpace int64
			for i := 0; i < len(line); {
				b := line[i]
				lInc++
				if b < utf8.RuneSelf {
					if !asciiSpace[b] {
						nonSpace++
					}
					i++
				} else {
					r, size := utf8.DecodeRune(line[i:])
					if !unicode.IsSpace(r) {
						nonSpace++
					}
					i += size
				}
			}
			lExc += nonSpace
			if nonSpace == 0 {
				lBlank++
			}
		}

		atomic.AddInt64(&totalFiles, 1)
		atomic.AddInt64(&totalSize, int64(read))
		atomic.AddInt64(&totalLines, lLines)
		atomic.AddInt64(&charsInc, lInc)
		atomic.AddInt64(&charsExc, lExc)
		atomic.AddInt64(&blankLines, lBlank)
	}

	var walk func(dir string)
	walk = func(dir string) {
		defer wg.Done()

		sem <- struct{}{}
		entries, err := os.ReadDir(dir)
		<-sem
		if err != nil {
			return
		}

		for _, entry := range entries {
			name := entry.Name()
			full := filepath.Join(dir, name)

			if entry.IsDir() {
				if skipDirs[name] {
					continue
				}
				wg.Add(1)
				go walk(full)
				continue
			}

			if !strings.EqualFold(filepath.Ext(name), ".java") {
				continue
			}
			wg.Add(1)
			go func(p string) {
				defer wg.Done()
				processFile(p)
			}(full)
		}
	}

	wg.Add(1)
	go walk(absRoot)
	wg.Wait()

	elapsed := time.Since(start)
	nonBlank := atomic.LoadInt64(&totalLines) - atomic.LoadInt64(&blankLines)

	return ScanResult{
		Root:          absRoot,
		TotalFiles:    int(atomic.LoadInt64(&totalFiles)),
		TotalLines:    atomic.LoadInt64(&totalLines),
		CharsInc:      atomic.LoadInt64(&charsInc),
		CharsExc:      atomic.LoadInt64(&charsExc),
		BlankLines:    atomic.LoadInt64(&blankLines),
		NonBlankLines: nonBlank,
		TotalSize:     atomic.LoadInt64(&totalSize),
		ScanTimeMs:    elapsed.Milliseconds(),
		ScanTimeStr:   fmt.Sprintf("%.2fs", elapsed.Seconds()),
	}, nil
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
