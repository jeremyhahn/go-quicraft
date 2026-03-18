// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package perfresult

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Chart layout constants.
const (
	chartTotalWidth = 1100
	chartBarArea    = 550

	chartMarginLeft  = 200
	chartMarginRight = 320
	chartMarginTop   = 70
	chartHeaderH     = 56
	chartFooterH     = 60

	chartBarHeight     = 24
	chartBarRadius     = 3
	chartBarGap        = 6
	chartScenarioGap   = 12
	chartGroupPadTop   = 4
	chartGroupPadBot   = 8
	chartSectionBorder = 1

	chartFontTitle    = 18
	chartFontScenario = 15
	chartFontData     = 13
	chartFontLegend   = 12
	chartFontSmall    = 11
)

// Color palette for systems.
var systemColors = map[string]string{
	"quicraft":   "#2196F3",
	"etcd":       "#4CAF50",
	"dragonboat": "#FF9800",
	"openraft":   "#F44336",
}

// defaultColor is used for systems not in the palette.
const defaultColor = "#9E9E9E"

// Header and styling colors.
const (
	colorHeaderBg    = "#1a1a2e"
	colorHeaderText  = "#ffffff"
	colorRowEven     = "#ffffff"
	colorRowOdd      = "#f8f9fa"
	colorGridLine    = "#e0e0e0"
	colorLabelText   = "#333333"
	colorSystemText  = "#555555"
	colorValueText   = "#444444"
	colorLatencyText = "#777777"
	colorWinner      = "#FFC107"
	colorNAText      = "#aaaaaa"
	colorSeparator   = "#dee2e6"
	colorFooterBg    = "#f8f9fa"
)

// colorFor returns the SVG fill color for a system name.
func colorFor(system string) string {
	if c, ok := systemColors[system]; ok {
		return c
	}
	return defaultColor
}

// GenerateSystemChart creates a professional SVG bar chart for a single
// system's benchmark results, showing throughput and latency per scenario.
func GenerateSystemChart(suite *BenchmarkSuite, outPath string) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return err
	}

	scenarios := collectPresentScenarios(map[string]*BenchmarkSuite{suite.System: suite})
	if len(scenarios) == 0 {
		return &ErrNoResults{}
	}

	// Compute dynamic height.
	rowCount := len(scenarios)
	contentH := chartHeaderH + chartMarginTop +
		rowCount*(chartBarHeight+chartBarGap+chartScenarioGap) +
		chartFooterH + 40
	if contentH < 400 {
		contentH = 400
	}

	color := colorFor(suite.System)
	var sb strings.Builder
	sb.Grow(8192)

	// SVG root.
	writeXMLHeader(&sb)
	writeSVGOpen(&sb, chartTotalWidth, contentH)

	// Background.
	writeRect(&sb, 0, 0, chartTotalWidth, contentH, colorRowEven, 0)

	// Header band.
	writeRect(&sb, 0, 0, chartTotalWidth, chartHeaderH, colorHeaderBg, 0)
	writeText(&sb, chartTotalWidth/2, 36, chartFontTitle, "bold",
		"middle", colorHeaderText, escapeXML(suite.System)+" Performance")

	// Bar area boundaries.
	barLeft := chartMarginLeft
	barRight := barLeft + chartBarArea

	// Grid lines in bar area.
	gridTop := chartHeaderH + 10
	gridBot := contentH - chartFooterH - 10
	writeGridLines(&sb, barLeft, barRight, gridTop, gridBot)

	y := chartHeaderH + chartMarginTop

	for i, sc := range scenarios {
		label := scenarioLabel(sc)
		result, ok := suite.Results[sc]

		// Alternating row background.
		rowH := chartBarHeight + chartBarGap + chartScenarioGap
		rowBg := colorRowEven
		if i%2 == 1 {
			rowBg = colorRowOdd
		}
		writeRect(&sb, 0, y-chartGroupPadTop, chartTotalWidth, rowH, rowBg, 0)

		// Scenario label (left side).
		textY := y + chartBarHeight - 5
		writeText(&sb, barLeft-12, textY, chartFontScenario, "normal",
			"end", colorLabelText, escapeXML(label))

		if !ok {
			writeText(&sb, barLeft+8, textY, chartFontData, "normal",
				"start", colorNAText, "N/A")
			y += rowH
			continue
		}

		// Compute bar width using per-scenario max (just this system, so full width).
		localMax := result.OpsPerSec
		barW := scaleBar(result.OpsPerSec, localMax, chartBarArea)

		// Bar.
		writeRoundedRect(&sb, barLeft, y, barW, chartBarHeight, chartBarRadius, color, "0.9")

		// Throughput value.
		valueX := barRight + 14
		writeText(&sb, valueX, textY, chartFontData, "bold",
			"start", colorValueText, escapeXML(FormatOps(result.OpsPerSec))+" ops/s")

		// Latency value.
		if result.Latency.P50Ns > 0 {
			latencyX := valueX + 140
			writeText(&sb, latencyX, textY, chartFontSmall, "normal",
				"start", colorLatencyText,
				"P50: "+escapeXML(FormatDurationNs(result.Latency.P50Ns)))
		}

		// Section separator.
		sepY := y + chartBarHeight + chartBarGap + chartScenarioGap - chartSectionBorder
		writeLine(&sb, 20, sepY, chartTotalWidth-20, sepY, colorSeparator, 1)

		y += rowH
	}

	// Footer.
	footerY := contentH - chartFooterH
	writeRect(&sb, 0, footerY, chartTotalWidth, chartFooterH, colorFooterBg, 0)
	writeLine(&sb, 0, footerY, chartTotalWidth, footerY, colorSeparator, 1)

	// Legend swatch.
	legendY := footerY + 22
	writeRect(&sb, chartMarginLeft, legendY, 14, 14, color, chartBarRadius)
	writeText(&sb, chartMarginLeft+20, legendY+12, chartFontLegend, "normal",
		"start", colorLabelText, escapeXML(suite.System))

	writeSVGClose(&sb)

	return os.WriteFile(outPath, []byte(sb.String()), 0644)
}

// GenerateComparisonChart creates a professional SVG grouped bar chart
// comparing multiple systems side-by-side with per-scenario scaling,
// throughput bars, latency data, and winner indicators.
func GenerateComparisonChart(suites map[string]*BenchmarkSuite, outPath string) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return err
	}

	scenarios := collectPresentScenarios(suites)
	if len(scenarios) == 0 {
		return &ErrNoResults{}
	}

	systems := orderedSystemKeys(suites)
	systemCount := len(systems)

	// Compute dynamic height: header + per-scenario groups + legend footer.
	groupH := scenarioGroupHeight(systemCount)
	contentH := chartHeaderH + chartMarginTop +
		len(scenarios)*groupH +
		chartFooterH + 20
	if contentH < 500 {
		contentH = 500
	}

	var sb strings.Builder
	sb.Grow(32768)

	// SVG root.
	writeXMLHeader(&sb)
	writeSVGOpen(&sb, chartTotalWidth, contentH)

	// Full background.
	writeRect(&sb, 0, 0, chartTotalWidth, contentH, colorRowEven, 0)

	// Header band.
	writeRect(&sb, 0, 0, chartTotalWidth, chartHeaderH, colorHeaderBg, 0)
	title := "Performance Comparison: " + strings.Join(systems, " vs ")
	writeText(&sb, chartTotalWidth/2, 36, chartFontTitle, "bold",
		"middle", colorHeaderText, escapeXML(title))

	barLeft := chartMarginLeft
	barRight := barLeft + chartBarArea

	// Grid lines.
	gridTop := chartHeaderH + 10
	gridBot := contentH - chartFooterH - 10
	writeGridLines(&sb, barLeft, barRight, gridTop, gridBot)

	y := chartHeaderH + chartMarginTop

	for scIdx, sc := range scenarios {
		label := scenarioLabel(sc)
		grpH := scenarioGroupHeight(systemCount)

		// Alternating group background.
		grpBg := colorRowEven
		if scIdx%2 == 1 {
			grpBg = colorRowOdd
		}
		writeRect(&sb, 0, y-chartGroupPadTop, chartTotalWidth, grpH, grpBg, 0)

		// Scenario label.
		writeText(&sb, 14, y+chartFontScenario, chartFontScenario, "bold",
			"start", colorLabelText, escapeXML(label))
		y += chartFontScenario + 8

		// Per-scenario max for independent scaling.
		scenarioMax := scenarioMaxOps(suites, sc)

		// Find throughput and latency winners for this scenario.
		tWinner, lWinner, speedups := scenarioWinners(suites, systems, sc)

		// Render each system bar within this scenario group.
		for _, sys := range systems {
			suite, hasSuite := suites[sys]
			if !hasSuite {
				writeSystemNARow(&sb, sys, barLeft, y)
				y += chartBarHeight + chartBarGap
				continue
			}

			result, hasResult := suite.Results[sc]
			if !hasResult {
				writeSystemNARow(&sb, sys, barLeft, y)
				y += chartBarHeight + chartBarGap
				continue
			}

			sysColor := colorFor(sys)
			barW := scaleBar(result.OpsPerSec, scenarioMax, chartBarArea)
			textY := y + chartBarHeight - 6

			// System name label.
			writeText(&sb, barLeft-12, textY, chartFontData, "normal",
				"end", colorSystemText, escapeXML(sys))

			// Bar.
			writeRoundedRect(&sb, barLeft, y, barW, chartBarHeight, chartBarRadius, sysColor, "0.9")

			// Throughput value.
			valueX := barRight + 14
			opsStr := FormatOps(result.OpsPerSec) + " ops/s"
			writeText(&sb, valueX, textY, chartFontData, "bold",
				"start", colorValueText, escapeXML(opsStr))

			// P50 latency.
			latencyX := valueX + 130
			if result.Latency.P50Ns > 0 {
				writeText(&sb, latencyX, textY, chartFontSmall, "normal",
					"start", colorLatencyText, escapeXML(FormatDurationNs(result.Latency.P50Ns)))
			}

			// Winner marker and speedup annotation.
			markerX := latencyX + 70
			isThroughputWinner := sys == tWinner && len(systems) > 1
			isLatencyWinner := sys == lWinner && len(systems) > 1

			if isThroughputWinner || isLatencyWinner {
				writeText(&sb, markerX, textY, chartFontData, "bold",
					"start", colorWinner, starMarker)
				// Speedup annotation.
				if speedup, ok := speedups[sys]; ok && speedup > 1.1 {
					annot := "(" + formatSpeedup(speedup) + "x)"
					writeText(&sb, markerX+16, textY, chartFontSmall, "normal",
						"start", colorWinner, escapeXML(annot))
				}
			}

			y += chartBarHeight + chartBarGap
		}

		// Section separator line.
		sepY := y + chartGroupPadBot - chartSectionBorder
		writeLine(&sb, 20, sepY, chartTotalWidth-20, sepY, colorSeparator, 1)
		y += chartGroupPadBot + chartScenarioGap
	}

	// Footer with legend.
	footerY := contentH - chartFooterH
	writeRect(&sb, 0, footerY, chartTotalWidth, chartFooterH, colorFooterBg, 0)
	writeLine(&sb, 0, footerY, chartTotalWidth, footerY, colorSeparator, 1)

	legendY := footerY + 22
	legendX := chartMarginLeft
	for _, sys := range systems {
		sysColor := colorFor(sys)
		writeRect(&sb, legendX, legendY, 14, 14, sysColor, chartBarRadius)
		writeText(&sb, legendX+20, legendY+12, chartFontLegend, "normal",
			"start", colorLabelText, escapeXML(sys))
		legendX += 140
	}

	writeSVGClose(&sb)

	return os.WriteFile(outPath, []byte(sb.String()), 0644)
}

// starMarker is the Unicode star used to indicate the winner.
const starMarker = "\xe2\x98\x85"

// scenarioGroupHeight returns the pixel height for one scenario group
// with the given number of system bars.
func scenarioGroupHeight(systemCount int) int {
	return chartFontScenario + 8 +
		systemCount*(chartBarHeight+chartBarGap) +
		chartGroupPadBot + chartScenarioGap
}

// scenarioLabel returns the human-readable label for a scenario key.
func scenarioLabel(sc string) string {
	if label, ok := ScenarioLabels[sc]; ok {
		return label
	}
	return sc
}

// scenarioMaxOps returns the maximum ops/sec across all suites for a
// single scenario. This provides per-scenario independent scaling.
func scenarioMaxOps(suites map[string]*BenchmarkSuite, scenario string) float64 {
	var maxOps float64
	for _, suite := range suites {
		if r, ok := suite.Results[scenario]; ok && r.OpsPerSec > maxOps {
			maxOps = r.OpsPerSec
		}
	}
	return maxOps
}

// scenarioWinners determines the throughput winner, latency winner, and
// speedup ratios for a single scenario across all systems.
func scenarioWinners(
	suites map[string]*BenchmarkSuite,
	systems []string,
	scenario string,
) (throughputWinner string, latencyWinner string, speedups map[string]float64) {

	speedups = make(map[string]float64)

	var bestOps float64
	var bestLatency int64
	var minOps float64
	firstOps := true
	firstLat := true

	for _, sys := range systems {
		suite, ok := suites[sys]
		if !ok {
			continue
		}
		r, ok := suite.Results[scenario]
		if !ok {
			continue
		}

		// Throughput: higher is better.
		if r.OpsPerSec > 0 {
			if firstOps || r.OpsPerSec > bestOps {
				bestOps = r.OpsPerSec
				throughputWinner = sys
			}
			if firstOps || r.OpsPerSec < minOps {
				minOps = r.OpsPerSec
			}
			firstOps = false
		}

		// Latency: lower P50 is better.
		if r.Latency.P50Ns > 0 {
			if firstLat || r.Latency.P50Ns < bestLatency {
				bestLatency = r.Latency.P50Ns
				latencyWinner = sys
			}
			firstLat = false
		}
	}

	// Compute speedup ratios relative to slowest.
	if minOps > 0 {
		for _, sys := range systems {
			suite, ok := suites[sys]
			if !ok {
				continue
			}
			r, ok := suite.Results[scenario]
			if !ok {
				continue
			}
			if r.OpsPerSec > 0 {
				speedups[sys] = r.OpsPerSec / minOps
			}
		}
	}

	return throughputWinner, latencyWinner, speedups
}

// formatSpeedup formats a speedup ratio to two decimal places.
func formatSpeedup(ratio float64) string {
	return strconv.FormatFloat(ratio, 'f', 2, 64)
}

// collectPresentScenarios returns scenario keys where at least one system
// has a result, in AllScenarios order.
func collectPresentScenarios(suites map[string]*BenchmarkSuite) []string {
	var result []string
	for _, sc := range AllScenarios {
		for _, suite := range suites {
			if _, ok := suite.Results[sc]; ok {
				result = append(result, sc)
				break
			}
		}
	}
	return result
}

// findMaxOps returns the maximum ops/sec across all systems and scenarios.
func findMaxOps(suites map[string]*BenchmarkSuite, scenarios []string) float64 {
	var maxOps float64
	for _, suite := range suites {
		for _, sc := range scenarios {
			if r, ok := suite.Results[sc]; ok && r.OpsPerSec > maxOps {
				maxOps = r.OpsPerSec
			}
		}
	}
	return maxOps
}

// orderedSystemKeys returns system keys in preferred display order.
func orderedSystemKeys(suites map[string]*BenchmarkSuite) []string {
	preferred := []string{"quicraft", "etcd", "dragonboat", "openraft"}
	var result []string
	seen := make(map[string]bool)

	for _, sys := range preferred {
		if _, ok := suites[sys]; ok {
			result = append(result, sys)
			seen[sys] = true
		}
	}
	for sys := range suites {
		if !seen[sys] {
			result = append(result, sys)
		}
	}
	return result
}

// scaleBar converts an ops/sec value to a pixel width proportional
// to the maximum value within the available width.
func scaleBar(ops, maxOps float64, maxWidth int) int {
	if maxOps == 0 || ops == 0 {
		return 0
	}
	w := int(math.Round(ops / maxOps * float64(maxWidth)))
	if w < 1 {
		w = 1
	}
	return w
}

// escapeXML escapes special XML characters for safe SVG text content.
func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	return s
}

// SVG primitive writers.

// writeXMLHeader writes the XML declaration.
func writeXMLHeader(sb *strings.Builder) {
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
}

// writeSVGOpen writes the opening SVG element with dimensions.
func writeSVGOpen(sb *strings.Builder, width, height int) {
	sb.WriteString(`<svg xmlns="http://www.w3.org/2000/svg" `)
	sb.WriteString(`width="`)
	sb.WriteString(itoa(width))
	sb.WriteString(`" height="`)
	sb.WriteString(itoa(height))
	sb.WriteString(`" viewBox="0 0 `)
	sb.WriteString(itoa(width))
	sb.WriteString(` `)
	sb.WriteString(itoa(height))
	sb.WriteString(`" font-family="Arial, Helvetica, sans-serif">` + "\n")
}

// writeSVGClose writes the closing SVG tag.
func writeSVGClose(sb *strings.Builder) {
	sb.WriteString("</svg>\n")
}

// writeRect writes an SVG rect element.
func writeRect(sb *strings.Builder, x, y, w, h int, fill string, rx int) {
	sb.WriteString(`  <rect x="`)
	sb.WriteString(itoa(x))
	sb.WriteString(`" y="`)
	sb.WriteString(itoa(y))
	sb.WriteString(`" width="`)
	sb.WriteString(itoa(w))
	sb.WriteString(`" height="`)
	sb.WriteString(itoa(h))
	sb.WriteString(`" fill="`)
	sb.WriteString(fill)
	sb.WriteString(`"`)
	if rx > 0 {
		sb.WriteString(` rx="`)
		sb.WriteString(itoa(rx))
		sb.WriteString(`"`)
	}
	sb.WriteString("/>\n")
}

// writeRoundedRect writes an SVG rect with rounded corners and opacity.
func writeRoundedRect(sb *strings.Builder, x, y, w, h, rx int, fill, opacity string) {
	sb.WriteString(`  <rect x="`)
	sb.WriteString(itoa(x))
	sb.WriteString(`" y="`)
	sb.WriteString(itoa(y))
	sb.WriteString(`" width="`)
	sb.WriteString(itoa(w))
	sb.WriteString(`" height="`)
	sb.WriteString(itoa(h))
	sb.WriteString(`" rx="`)
	sb.WriteString(itoa(rx))
	sb.WriteString(`" fill="`)
	sb.WriteString(fill)
	sb.WriteString(`" opacity="`)
	sb.WriteString(opacity)
	sb.WriteString(`"/>` + "\n")
}

// writeText writes an SVG text element.
func writeText(sb *strings.Builder, x, y, fontSize int, fontWeight, anchor, fill, content string) {
	sb.WriteString(`  <text x="`)
	sb.WriteString(itoa(x))
	sb.WriteString(`" y="`)
	sb.WriteString(itoa(y))
	sb.WriteString(`" font-size="`)
	sb.WriteString(itoa(fontSize))
	sb.WriteString(`" font-weight="`)
	sb.WriteString(fontWeight)
	sb.WriteString(`" text-anchor="`)
	sb.WriteString(anchor)
	sb.WriteString(`" fill="`)
	sb.WriteString(fill)
	sb.WriteString(`">`)
	sb.WriteString(content)
	sb.WriteString("</text>\n")
}

// writeLine writes an SVG line element.
func writeLine(sb *strings.Builder, x1, y1, x2, y2 int, stroke string, width int) {
	sb.WriteString(`  <line x1="`)
	sb.WriteString(itoa(x1))
	sb.WriteString(`" y1="`)
	sb.WriteString(itoa(y1))
	sb.WriteString(`" x2="`)
	sb.WriteString(itoa(x2))
	sb.WriteString(`" y2="`)
	sb.WriteString(itoa(y2))
	sb.WriteString(`" stroke="`)
	sb.WriteString(stroke)
	sb.WriteString(`" stroke-width="`)
	sb.WriteString(itoa(width))
	sb.WriteString(`"/>` + "\n")
}

// writeDashedLine writes an SVG dashed line element.
func writeDashedLine(sb *strings.Builder, x1, y1, x2, y2 int, stroke string) {
	sb.WriteString(`  <line x1="`)
	sb.WriteString(itoa(x1))
	sb.WriteString(`" y1="`)
	sb.WriteString(itoa(y1))
	sb.WriteString(`" x2="`)
	sb.WriteString(itoa(x2))
	sb.WriteString(`" y2="`)
	sb.WriteString(itoa(y2))
	sb.WriteString(`" stroke="`)
	sb.WriteString(stroke)
	sb.WriteString(`" stroke-width="1" stroke-dasharray="4,4" opacity="0.5"/>` + "\n")
}

// writeGridLines draws subtle vertical dotted grid lines at 25%, 50%, 75%,
// and 100% of the bar area width.
func writeGridLines(sb *strings.Builder, barLeft, barRight, gridTop, gridBot int) {
	barWidth := barRight - barLeft
	for _, pct := range []int{25, 50, 75, 100} {
		x := barLeft + barWidth*pct/100
		writeDashedLine(sb, x, gridTop, x, gridBot, colorGridLine)
	}
}

// writeSystemNARow writes a system row with N/A when no result exists.
func writeSystemNARow(sb *strings.Builder, system string, barLeft, y int) {
	textY := y + chartBarHeight - 6
	writeText(sb, barLeft-12, textY, chartFontData, "normal",
		"end", colorSystemText, escapeXML(system))
	writeText(sb, barLeft+8, textY, chartFontData, "normal",
		"start", colorNAText, "N/A")
}

// itoa converts an int to its string representation without allocating
// through fmt.Sprintf.
func itoa(n int) string {
	return strconv.Itoa(n)
}
