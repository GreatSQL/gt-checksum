package main

import (
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type rangeSegment struct {
	start int
	end   int
}

func splitConsecutiveRanges(numbers []int) []rangeSegment {
	if len(numbers) == 0 {
		return nil
	}
	sort.Ints(numbers)
	uniq := make([]int, 0, len(numbers))
	prev := numbers[0] - 1
	for _, n := range numbers {
		if n != prev {
			uniq = append(uniq, n)
		}
		prev = n
	}
	if len(uniq) == 0 {
		return nil
	}
	var segments []rangeSegment
	start := uniq[0]
	last := uniq[0]
	for i := 1; i < len(uniq); i++ {
		if uniq[i] == last+1 {
			last = uniq[i]
			continue
		}
		segments = append(segments, rangeSegment{start: start, end: last})
		start = uniq[i]
		last = uniq[i]
	}
	segments = append(segments, rangeSegment{start: start, end: last})
	return segments
}

func normalizePlanPath(file, fixFileDir string) string {
	rel, err := filepath.Rel(fixFileDir, file)
	if err == nil && !strings.HasPrefix(rel, "..") && rel != "." {
		baseDir := filepath.Base(filepath.Clean(fixFileDir))
		joined := filepath.ToSlash(filepath.Join(baseDir, rel))
		return strings.TrimPrefix(joined, "./")
	}
	return strings.TrimPrefix(filepath.ToSlash(file), "./")
}

func buildCompressedPlanEntries(files []string, fixFileDir string) []string {
	type groupKey struct {
		dir    string
		prefix string
		suffix string
	}
	type groupValue struct {
		order   int
		numbers []int
	}
	type plainEntry struct {
		order int
		path  string
	}
	type mergedEntry struct {
		order int
		path  string
		start int
	}

	grouped := make(map[groupKey]*groupValue)
	var groupOrder []groupKey
	var plain []plainEntry
	orderCounter := 0

	for _, file := range files {
		normalized := normalizePlanPath(file, fixFileDir)
		base := filepath.Base(normalized)
		dir := filepath.ToSlash(filepath.Dir(normalized))
		if dir == "." {
			dir = ""
		}
		matches := numberedSQLFileRegex.FindStringSubmatch(base)
		if len(matches) != 4 {
			plain = append(plain, plainEntry{order: orderCounter, path: normalized})
			orderCounter++
			continue
		}
		num, err := strconv.Atoi(matches[2])
		if err != nil {
			plain = append(plain, plainEntry{order: orderCounter, path: normalized})
			orderCounter++
			continue
		}
		key := groupKey{dir: dir, prefix: matches[1], suffix: matches[3]}
		if _, ok := grouped[key]; !ok {
			grouped[key] = &groupValue{order: orderCounter}
			groupOrder = append(groupOrder, key)
			orderCounter++
		}
		grouped[key].numbers = append(grouped[key].numbers, num)
	}

	var merged []mergedEntry
	for _, key := range groupOrder {
		segments := splitConsecutiveRanges(grouped[key].numbers)
		for _, seg := range segments {
			name := fmt.Sprintf("%s(%d-%d)%s", key.prefix, seg.start, seg.end, key.suffix)
			if key.dir != "" {
				name = key.dir + "/" + name
			}
			merged = append(merged, mergedEntry{
				order: grouped[key].order,
				path:  name,
				start: seg.start,
			})
		}
	}

	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].order != merged[j].order {
			return merged[i].order < merged[j].order
		}
		if merged[i].start != merged[j].start {
			return merged[i].start < merged[j].start
		}
		return merged[i].path < merged[j].path
	})
	sort.SliceStable(plain, func(i, j int) bool {
		if plain[i].order != plain[j].order {
			return plain[i].order < plain[j].order
		}
		return plain[i].path < plain[j].path
	})

	type finalEntry struct {
		order int
		path  string
	}
	var final []finalEntry
	for _, item := range merged {
		final = append(final, finalEntry{order: item.order, path: item.path})
	}
	for _, item := range plain {
		final = append(final, finalEntry{order: item.order, path: item.path})
	}
	sort.SliceStable(final, func(i, j int) bool {
		return final[i].order < final[j].order
	})

	result := make([]string, 0, len(final))
	for _, item := range final {
		result = append(result, item.path)
	}
	return result
}

func logExecutionPlan(stageName string, files []string, fixFileDir string) {
	if len(files) == 0 {
		return
	}
	log.Printf("[%s] planned execution order (%d files):", stageName, len(files))
	entries := buildCompressedPlanEntries(files, fixFileDir)
	for idx, file := range entries {
		log.Printf("[%s] #%d %s", stageName, idx+1, file)
	}
}
