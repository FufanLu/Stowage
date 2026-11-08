---
name: Stowage Console
description: The cluster as a continuous-form greenbar printout, read by operators during an incident.
colors:
  paper: "#fbfcf8"
  paper-2: "#f2f5ef"
  band: "#e4f0e6"
  band-2: "#d3e6d7"
  ink: "#18201a"
  ink-2: "#47554a"
  ink-3: "#5f6d62"
  rule: "#a9c1ad"
  perf: "#c6d6c8"
  hole: "#e9ede6"
  hole-edge: "#c4cfc5"
  lead: "#17663a"
  lead-wash: "#cfe8d6"
  fault: "#b0251c"
  fault-wash: "#f8dcd8"
  warn: "#8a5a00"
  sel: "#b9dcc2"
typography:
  wordmark:
    fontFamily: "JetBrains Mono Variable, ui-monospace, SF Mono, Menlo, monospace"
    fontSize: "15px"
    fontWeight: 700
    letterSpacing: "0.18em"
  headline:
    fontFamily: "Archivo Variable, Helvetica Neue, Arial, sans-serif"
    fontSize: "15px"
    fontWeight: 700
    letterSpacing: "0.01em"
    fontVariation: "\"wdth\" 92"
  body:
    fontFamily: "Archivo Variable, Helvetica Neue, Arial, sans-serif"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: 1.45
    fontVariation: "\"wdth\" 92"
    fontFeature: "\"tnum\""
  label:
    fontFamily: "Archivo Variable, Helvetica Neue, Arial, sans-serif"
    fontSize: "12px"
    fontWeight: 400
    fontVariation: "\"wdth\" 92"
  data:
    fontFamily: "JetBrains Mono Variable, ui-monospace, SF Mono, Menlo, monospace"
    fontSize: "12.5px"
    fontWeight: 400
    lineHeight: "1.75rem"
  data-small:
    fontFamily: "JetBrains Mono Variable, ui-monospace, SF Mono, Menlo, monospace"
    fontSize: "11.5px"
    fontWeight: 400
rounded:
  mark: "2px"
  control: "3px"
spacing:
  hair: "2px"
  xs: "6px"
  sm: "8px"
  md: "12px"
  lg: "18px"
  gutter-sm: "16px"
  gutter: "28px"
  section: "30px"
  row: "1.75rem"
components:
  button:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: "6px 13px"
  button-hover:
    backgroundColor: "{colors.band}"
  button-primary:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper}"
    rounded: "{rounded.control}"
    padding: "6px 13px"
  button-primary-hover:
    backgroundColor: "{colors.lead}"
    textColor: "{colors.paper}"
  button-armed:
    backgroundColor: "{colors.fault}"
    textColor: "#ffffff"
    rounded: "{rounded.control}"
    padding: "6px 13px"
  tab:
    textColor: "{colors.ink-2}"
    rounded: "{rounded.control}"
    padding: "7px 14px 9px"
  tab-selected:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
  input:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.data}"
    rounded: "{rounded.mark}"
    padding: "5px 8px"
  listing-row:
    textColor: "{colors.ink}"
    typography: "{typography.data}"
    height: "{spacing.row}"
    padding: "0 12px"
  listing-row-banded:
    backgroundColor: "{colors.band}"
  listing-row-selected:
    backgroundColor: "{colors.sel}"
  cellmark-leader:
    backgroundColor: "{colors.lead}"
    textColor: "{colors.paper}"
    rounded: "{rounded.mark}"
    width: "22px"
    height: "18px"
---

# Design System: Stowage Console

## Overview

**Creative North Star: "The Greenbar Listing"**

The console is a continuous-form line-printer listing. The key space is one long printout on white stock with pale green bands, the page sits between tractor-feed margins with punched holes, sections are torn apart at perforations, and data is struck in carbon ink. Ranges are the pages of the listing; nodes are the carriage columns across the top. It deliberately refuses the graphite dev console with neon charts that most database consoles ship.

Density is high and calm: small type, tight rows, almost no chrome. Every visible line is armature, not ornament. Greenbar bands exist to carry the eye along a row, perforations exist to mark a section break, rules exist to bound a table. Colour carries state and nothing else: ledger green means "leads" or "healthy", ribbon red means "fault" (no leader, node down, destructive action armed). Everything else is ink on paper.

Under low light the same paper turns dark: a dark theme follows `prefers-color-scheme` and can be forced with `data-theme="dark"` or `data-theme="light"` on the root. It is the same listing read under a desk lamp, not a separate "dark mode" style: every token keeps its role, only its value changes.

**Key Characteristics:**
- Paper ground, greenbar banding, carbon ink; colour reserved for leader (green) and fault (red).
- Line-printer mono for every number, key, identifier and plan; a narrowed grotesk for labels and prose.
- Tractor-feed margins and dashed perforations frame the sheet on wide screens.
- Flat surfaces bounded by hairlines; one soft ambient shadow lifts the sheet off the desk.
- Light and dark are the same paper under different light.

## Colors

A near-monochrome paper-and-ink palette tinted green throughout, with exactly two signal inks.

### Primary
- **Ledger Green** (`lead`): the ink of leadership and health. Leader cells in the matrix, the leader column in listings, "up" in node cells, the focus ring, the caret, the selected-span underline, plan time bars, links, and the primary button's hover. Its wash (`lead-wash`) is reserved for green-tinted fills.

### Secondary
- **Ribbon Red** (`fault`): faults only. Range with no leader, node down or unreachable, stale refresh age, plan misestimates, error codes, and the armed destructive-SQL button. Its wash (`fault-wash`) fills fault spans, error boxes and banners.
- **Carbon Amber** (`warn`): defined for warnings; currently unused by any component.

### Neutral
- **Listing Stock** (`paper`): the sheet, tables, inputs, the sticky carriage header.
- **Desk** (`paper-2`): the page behind the sheet, visible beyond the 1480px maximum.
- **Greenbar** (`band`): row banding, hover fills, empty-state code chips. **Deep Greenbar** (`band-2`): hover on already-banded rows and spans.
- **Carbon** (`ink`): all data, table frames, the carriage baseline, the primary button fill. **Faded Carbon** (`ink-2`): secondary text, headers of listings, prose ledes. **Ghost Carbon** (`ink-3`): sources, axis labels, units, nulls.
- **Ruled Line** (`rule`): inner table and strip dividers, plan-tree connectors, scrollbar thumb. **Perforation** (`perf`): dashed section breaks, tractor borders, cell separators.
- **Punch Hole** (`hole`, `hole-edge`): the tractor-feed holes only.
- **Selection** (`sel`): the selected range, the expanded listing row, text selection.

Dark values live in the sidecar (`colorMeta.<token>.dark`) and in `styles.css`; they keep every role above.

### Named Rules
**The Two Inks Rule.** Green means leads or healthy; red means fault. No other hue appears, and neither is ever used for decoration or emphasis.

**The Same Paper Rule.** The dark theme is the paper under low light, not a restyle. Add a token to both themes or to neither; never branch a component on the theme.

## Typography

**Display Font:** none; the system has no display tier.
**Body Font:** Archivo Variable at width 92 (with Helvetica Neue, Arial)
**Label/Mono Font:** JetBrains Mono Variable (with ui-monospace, SF Mono, Menlo)

**Character:** A narrowed working grotesk for words, a line-printer mono for anything a machine produced. Tabular numerals everywhere, ligatures off in mono so `->` and `!=` print as typed.

### Hierarchy
- **Wordmark** (mono 700, 15px, 0.18em, uppercase): "STOWAGE" in the carriage only.
- **Headline** (700, 15px): section titles ("Key space", "Ranges", "SQL"). Same size as the wordmark; hierarchy comes from weight and position, not scale.
- **Body** (400, 14px/1.45): prose; ledes at 13px in `ink-2`, capped at 72ch.
- **Tab / Button** (500 and 600, 13px): view tabs (650 when selected) and buttons.
- **Label** (400, 12px, `ink-3`): section source lines, legend, detail-list terms. Listing headers are 600 at 11.5px in `ink-2`, sentence case.
- **Data** (mono 400, 12.5px): every listing cell, key, identifier, SQL text and plan line. Smaller data (11-12px) for span cells, node cells, matrix heads and region labels down to 10.5px.

### Named Rules
**The Machine Ink Rule.** If the cluster produced it (a number, key, id, node name, statement, plan), it is set in mono. If a person wrote it (a label, a heading, an explanation), it is set in the grotesk.

**The Flat Scale Rule.** No type above 15px. A section is found by its rule and its weight, never by a large heading.

## Layout

The sheet is a three-column grid: a 30px tractor margin, the content, a 30px tractor margin, centred to a 1480px maximum on the `paper-2` desk. Content pads 28px each side and 64px at the foot. The carriage header is sticky, sits on `paper`, and ends in a 1px ink baseline that the selected folder tab breaks into.

Sections start 30px apart; a section head is title plus source line on one baseline with 10px below. A dashed perforation hairline separates groups of sections and runs edge to edge through the content padding. Listing rows are 1.75rem high, the same pitch as the SQL editor's banded lines and the loading skeleton, so every greenbar surface shares one row rhythm.

At 760px and below the tractor margins are hidden, the gutter drops to 16px, node cells become a three-column grid on their own line with abbreviated text ("2 L · 8 ms"), the tabs take a full-width row and share it equally, secondary listing columns hide (Statements folds its metrics under the query text), and horizontally scrolling strips, matrices and listings fade out on the right edge as a scroll cue.

### Named Rules
**The Tabs In The Carriage Rule.** View tabs sit in the carriage header as folder tabs, not on the tractor margin. The margins are hidden at 760px and below, so anything placed on them would vanish on a phone.

**The Armature Rule.** A visible grid line must measure or separate something real. Greenbar bands carry row scanning and perforations mark section breaks. A decorative column ruler was built across the top of the sheet, measured nothing, and was removed at review; do not bring it back.

## Elevation & Depth

Flat. Depth is carried by hairlines and banding, not by shadow. The single exception is the sheet itself, lifted off the desk by a soft ambient shadow so the page reads as paper. Selection on the key-space strip is shown with an inset 3px ledger-green underline, not a lift.

### Shadow Vocabulary
- **Sheet lift** (`box-shadow: 0 1px 3px rgb(0 0 0 / 0.06), 0 8px 28px rgb(0 0 0 / 0.05)`): the sheet only.
- **Selected span underline** (`box-shadow: inset 0 -3px 0 var(--lead)`): the selected range in the key-space strip.
- **Editor focus** (`box-shadow: inset 0 0 0 2px var(--lead)`): the SQL textarea when focused.

### Named Rules
**The One Lift Rule.** Only the sheet casts a shadow. Panels, buttons and tables stay flat on the paper.

## Shapes

Square paper. Corners are 2px on marks and inputs and 3px on buttons and the top of folder tabs; tables, the strip, the editor, error boxes and banners are square. Borders do the shaping: 1px solid ink frames a whole object (strip, editor, listing top and bottom), 1px `rule` divides inside it, 1px dashed `perf` marks a tear (perforations, detail-row bottoms, history rows, tractor edges). Dashed red outlines mean an orphaned replica.

## Components

### Buttons
Plain, inked, a little tactile.
- **Shape:** gently squared (3px), 1px ink border.
- **Default:** paper fill, ink text, 600 at 13px, 6px 13px padding. Hover fills greenbar; active nudges down 1px; disabled at 45% opacity.
- **Primary (Run):** ink fill, paper text; hover turns ledger green.
- **Armed:** ribbon-red fill with white text, shown after the first press of a destructive statement (DROP, DELETE and the like) with a red guard line "Run again within 4 s to confirm". EXPLAIN ANALYZE is disabled for destructive statements.

### Inputs / Fields
- **Credential inputs:** paper fill, 1px `rule` border, 2px corners, mono 12.5px. Focus: 2px ledger-green outline and green border.
- **SQL editor:** a 1px ink frame around a textarea ruled in greenbar at the 1.75rem row pitch (bands scroll with the text), mono 13px, green caret, toolbar below a `rule` line.

### Navigation
- **Folder tabs** (Cluster, SQL, Statements) in the carriage. Unselected: no border, `ink-2`; hover greenbar. Selected: paper fill, 1px ink border on three sides with a 3px top radius, sitting 1px over the carriage baseline so it opens into the page. Arrow keys move between tabs; the view is kept in the URL hash.
- **Node cells:** a ruled line of mono cells at the carriage's right, `n1 up` with leading count and fetch rtt underneath; a down node turns red. The refresh age ("read 1 s ago") turns red when older than 6 s or unreachable.

### Greenbar Listing (signature)
Full-width table bounded by ink rules top and bottom, sticky sentence-case header over a `rule` line, mono cells at 1.75rem. Bands come in pairs (two rows banded, two plain) like real greenbar stock; expandable listings band per record instead. Numeric columns right-align. Rows that open show `sel` when expanded and a detail block of term/value pairs closed by a dashed tear.

### Key-Space Strip (signature)
One framed row of range cells in key order, widths by the square root of bytes (74px minimum), alternating greenbar, each cell printing range id, leader node in green (or "no leader" in red on a fault wash), start key and size. Region labels above, separated by ghost rules, name the key prefix (sql rows 0x02, documents 0x03). Selecting a cell syncs with the listing.

### Leader Grid
Nodes by ranges, banded per node. A filled green "L" mark is the leader, a hollow ghost-ink mark is a follower, a dashed red mark is a replica of a range with no leader; per-node stats (leading, replicas, queued, fsync p99, kv requests) sit to the right. It is a presence grid: it shows who holds what, nothing more. Shading cells by request load (a heat encoding) is an open idea, not built; do not imply it exists.

### Plan Tree
EXPLAIN output as an indented printout: operator in bold mono, detail lines in `ink-2`, connector lines in `rule`, actual rows beside estimates on the right, misestimates in red (×n), and a 3px ledger-green time bar scaled to the slowest node. A raw-text toggle shows the server's text.

### States
Loading is a banded skeleton that steps in opacity (steps, not a smooth pulse; off under reduced motion). Errors and banners are square boxes with a red border and fault-wash fill, the code in red mono. Empty states are a plain `ink-2` sentence.

## Do's and Don'ts

### Do:
- **Do** set every machine-produced value in JetBrains Mono with tabular numerals and ligatures off.
- **Do** keep greenbar row pitch at 1.75rem across listings, the editor and skeletons.
- **Do** use ledger green only for leader or healthy, and ribbon red only for faults or armed destructive actions.
- **Do** keep view navigation in the carriage header so it survives the hidden tractor margins at 760px and below.
- **Do** define every new colour token in both the light and the dark theme.
- **Do** hold destructive SQL behind a second press, separate from Run.

### Don't:
- **Don't** add grid lines, rulers or scales that measure nothing; the removed column ruler is the precedent.
- **Don't** add shadows beyond the sheet lift, or rounded cards.
- **Don't** set type above 15px or add a display tier.
- **Don't** use colour for decoration, category or emphasis; a third signal hue needs a real state to carry.
- **Don't** shade the leader grid by load or otherwise imply heat data until that encoding is actually built from live metrics.
- **Don't** show numbers the cluster did not report; missing values print as an en dash.
