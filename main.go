package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
)

type entry struct {
	count int
	total int
	min   int
	max   int
	name  []byte
}

type result struct {
	entries map[string]entry
	buffer  []byte
}

var results chan result

func main() {
	f, err := os.Open(`../1brc/data/full.txt`)
	// f, err := os.Open(`../1brc/data/test.txt`)
	if err != nil {
		panic(err)
	}
	c := runtime.NumCPU() / 2
	run(c, 64*1024*1024, f)
}

func run(c, size int, f io.Reader) {
	var r result
	var ok bool
	var fe entry
	results = make(chan result, c)
	for i := 0; i < c; i++ {
		results <- result{
			map[string]entry{},
			make([]byte, size),
		}
	}
	read(f)
	final := map[string]entry{}
	for i := 0; i < c; i++ {
		select {
		case r = <-results:
			for name, e := range r.entries {
				fe, ok = final[name]
				if !ok {
					fe.name = []byte(name)
					fe.max = -1e6
					fe.min = 1e6
				}
				fe.total += e.total
				fe.count += e.count
				if e.max > fe.max {
					fe.max = e.max
				}
				if e.min < fe.min {
					fe.min = e.min
				}
				final[name] = fe
			}
		}
	}
	var entries []entry
	for _, e := range final {
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b entry) int {
		return bytes.Compare(a.name, b.name)
	})
	fmt.Print(`{`)
	for i, e := range entries {
		if bytes.HasPrefix(e.name, []byte(`\n`)) {
			continue
		}
		fmt.Printf(`%s=%.1f/%.1f/%.1f`, e.name, float64(e.min)/10, float64(e.total)/float64(e.count)/10, float64(e.max)/10)
		if i < len(entries)-1 {
			fmt.Print(`, `)
		}
	}
	fmt.Print(`}`)
}

func read(f io.Reader) {
	var r result
	var n int
	var off int
	var rem = make([]byte, 100)
	var err error
	for {
		select {
		case r = <-results:
			copy(r.buffer[:off], rem[:off])
			n, err = f.Read(r.buffer[off:cap(r.buffer)])
			n += off
			off = n - bytes.LastIndexByte(r.buffer[:n], '\n')
			copy(rem[:off], r.buffer[n-off:n])
			r.buffer = r.buffer[:n-off]
			go doPart(r)
			if err == io.EOF {
				return
			}
		}
	}
}

func doPart(r result) {
	var i int
	var j int
	var k int
	var temp int
	var name string
	var e entry
	var ok bool
	for i < len(r.buffer) {
		j = bytes.IndexByte(r.buffer[i:], ';') + i
		name = string(r.buffer[i:j])
		e, ok = r.entries[name]
		if !ok {
			e.max = -1e6
			e.min = 1e6
		}
		k = bytes.IndexByte(r.buffer[i:], '\n') + i
		if j > k {
			k = len(r.buffer)
		}
		temp = customStringToIntParser(r.buffer[j+1 : k])
		e.count++
		e.total += temp
		if temp < e.min {
			e.min = temp
		}
		if temp > e.max {
			e.max = temp
		}
		r.entries[name] = e
		i = k + 1
	}
	results <- r
	return
}

// https://github.com/shraddhaag/1brc/blob/17d575fd0f143aed18d285713d030a5b52b478df/main.go#L231
func customStringToIntParser(input []byte) (output int) {
	var isNegativeNumber bool
	if input[0] == '-' {
		isNegativeNumber = true
		input = input[1:]
	}

	switch len(input) {
	case 3:
		output = int(input[0])*10 + int(input[2]) - int('0')*11
	case 4:
		output = int(input[0])*100 + int(input[1])*10 + int(input[3]) - (int('0') * 111)
	}

	if isNegativeNumber {
		return -output
	}
	return
}
