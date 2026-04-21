// Command checker audits a simulator history for user-visible
// consistency guarantees. Point it at a JSONL file produced by the
// simulator's -history-out flag:
//
//	./bin/checker -history ./history.jsonl
//
// Exits 0 iff every property held. On violations it prints a summary
// to stdout (human or JSON) and returns exit code 1.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"edge-cloud-replication/evaluation/checker"
)

func main() {
	var (
		historyPath string
		outputJSON  bool
		maxDump     int
	)
	flag.StringVar(&historyPath, "history", "", "path to JSONL history produced by cmd/simulator -history-out")
	flag.BoolVar(&outputJSON, "json", false, "emit a machine-readable JSON report on stdout")
	flag.IntVar(&maxDump, "dump", 20, "maximum violations to print in human mode (0 for unlimited)")
	flag.Parse()

	if historyPath == "" {
		fmt.Fprintln(os.Stderr, "checker: -history is required")
		os.Exit(2)
	}

	events, err := checker.LoadJSONL(historyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "checker: %v\n", err)
		os.Exit(2)
	}
	checker.SortByIssue(events)
	report := checker.Check(events)

	if outputJSON {
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			fmt.Fprintf(os.Stderr, "checker: encode: %v\n", err)
			os.Exit(2)
		}
	} else {
		printHuman(report, maxDump)
	}
	if !report.OK() {
		os.Exit(1)
	}
}

func printHuman(r checker.Report, maxDump int) {
	fmt.Println("─── offline consistency check ───────────────────────────────")
	fmt.Printf("events             : %d\n", r.Events)
	fmt.Printf("sessions           : %d\n", r.Sessions)
	fmt.Printf("sites              : %d\n", r.Sites)
	fmt.Printf("keys               : %d\n", r.Keys)
	fmt.Println()
	fmt.Printf("monotonic reads    : %s  (%d violations)\n", verdict(r.MonotonicReads), r.MonotonicReads)
	fmt.Printf("read-your-writes   : %s  (%d violations)\n", verdict(r.ReadYourWrites), r.ReadYourWrites)
	fmt.Printf("origin freshness   : %s  (%d violations)\n", verdict(r.NoStaleReadsAtOrigin), r.NoStaleReadsAtOrigin)
	fmt.Printf("convergence        : %s  (%d violations)\n", verdict(r.Convergence), r.Convergence)
	fmt.Println()
	if r.OK() {
		fmt.Println("verdict            : PASS — every property held")
		return
	}
	fmt.Println("verdict            : FAIL")

	if maxDump == 0 {
		maxDump = len(r.Violations)
	}
	if len(r.Violations) == 0 {
		return
	}
	fmt.Println()
	fmt.Println("─── violations (first", min(maxDump, len(r.Violations)), "of", len(r.Violations), ") ───")
	for i, v := range r.Violations {
		if i >= maxDump {
			fmt.Printf("... %d more suppressed\n", len(r.Violations)-maxDump)
			break
		}
		fmt.Printf("  [%d] %-24s seq=%d  %s\n", i+1, v.Property, v.Seq, v.Message)
	}
}

func verdict(n int) string {
	if n == 0 {
		return "PASS"
	}
	return "FAIL"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
