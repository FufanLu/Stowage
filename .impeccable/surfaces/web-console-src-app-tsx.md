---
version: 1
slug: "web-console-src-app-tsx"
primary_target: "web/console/src/App.tsx"
related_targets: []
---

# Surface brief: Stowage console

Scope: web/console, the whole operations console (Overview, Ranges, SQL, Statements). Mode: Operate.
Audience: operators and SREs during an incident or a demo, a terminal and Grafana open beside it. Job: see where the key space lives, who leads each range, what a plan does, which statements cost most.
Constraints: live data only from /status, /metrics, /api/nodes, /api/sql. No invented numbers. Unattended run: the owner asked for autonomous work, so the assigned direction was built without a decision round.

## Direction contract

THESIS: The cluster is a continuous-form printout: the key space is one long listing on greenbar paper, ranges are its pages, nodes are the carriage columns. It refuses the graphite dev-console with neon charts that every database console ships.

OWN-WORLD: Paper white ground, pale greenbar bands (#e4f1e6 family) every other row, tractor-feed margins with punched holes on the page edge, perforation hairlines between sections, carbon-black ink for data, one ribbon-red for faults (no leader, node down) and one ledger-green ink for leaders. Line-printer mono for every number, key, identifier and plan; a plain grotesk for labels and prose.

STORY: The operator sees at a glance whether every range has a leader and every node answers, reads the key space left to right as SQL tables (0x02) then documents (0x03), opens a range to see its replicas and term, runs EXPLAIN ANALYZE and reads the plan as an indented printout with actual rows beside estimates.

FIRST VIEWPORT: A thin carriage header (Stowage, view tabs, cluster health as a ruled line of node cells n1 n2 n3 with ok/fault ink and fetch rtt, refreshed age). Below, the key-space strip at full width: one band per range, width by the square root of bytes (min width, size printed in the cell), leader node in each cell, catalog/SQL/doc regions labeled from the key prefix, faults in ribbon red. Under it, the greenbar range listing (id, span, leader, term, replicas, generation, bytes). Tabs sit in the carriage as folder tabs (Cluster, SQL, Statements), because the tractor margins disappear on phones.

FORM: Greenbar continuous-form line-printer listing, candidate 7 of 7 (rack panel, interlocking diagram, FC bin map, oscilloscope, tape, blueprint, greenbar). Seed key 20601f69. Raises: from the Crouwel specimen, the visible grid is real armature, never ornament: greenbar bands carry row scanning and perforations mark section breaks (a decorative column ruler was built, measured nothing, and was removed at review); from the dark developer console, destructive SQL (DROP, DELETE) is held behind an explicit second key press, isolated from Run. Declined: orienteering map, airport signage, streaming wall, split-flap (all lose on product clarity).

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance
