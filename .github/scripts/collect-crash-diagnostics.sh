#!/usr/bin/env bash
#
# Turn whatever a crashed test left behind into a stack in the job log.
#
#   .github/scripts/collect-crash-diagnostics.sh <output-dir> [since-epoch]
#
# A test process that dies on a signal tells the job almost nothing. chdb does
# not install deadly-signal handlers, so there is no ClickHouse stack on stderr;
# pytest reports the child as `exit -6` with both streams empty; and the core, if
# one was even written, is a multi-gigabyte artifact nobody downloads. A release
# has already been held up by a SIGABRT whose only evidence was the number -6.
#
# What actually locates such a crash is a symbolicated backtrace, and both
# platforms can produce one on the runner, where the binary, its debug info and
# the crash artifact are all still sitting next to each other. That is the only
# moment they are together: the dSYM and the .debug files are packaged and
# uploaded later, and their UUIDs match this build and no other.
#
# So this prints backtraces into the log rather than shipping evidence elsewhere,
# and writes only text -- never the core itself -- to the output directory.
#
# Cost when nothing crashed is a `find` over two empty directories, so callers
# run it unconditionally rather than guessing which steps can crash.
#
# The since-epoch argument matters on self-hosted runners, where /tmp/core and
# the crash-report directories survive between jobs. Without it a green run
# reports the previous job's crash and the next reader chases a ghost.

set -uo pipefail

OUT="${1:-crash-diagnostics}"
SINCE="${2:-0}"

mkdir -p "$OUT"
SUMMARY="$OUT/summary.txt"
: > "$SUMMARY"

log() { echo "$@" | tee -a "$SUMMARY"; }

# macOS ships no timeout(1). A debugger that wedges on a corrupt core would
# otherwise hold the job until the runner kills it, so bound it where we can
# and accept the risk where we cannot.
if command -v timeout >/dev/null 2>&1; then
	bounded() { timeout "$@"; }
elif command -v gtimeout >/dev/null 2>&1; then
	bounded() { gtimeout "$@"; }
else
	bounded() { shift; "$@"; }
fi

# `find -newermt @epoch` is GNU-only; -newer against a stamp file works on both.
STAMP=$(mktemp)
if [ "$SINCE" != "0" ]; then
	# BSD touch takes -t, GNU takes -d; try the portable date -r form first.
	touch -t "$(date -r "$SINCE" +%Y%m%d%H%M.%S 2>/dev/null || date -d "@$SINCE" +%Y%m%d%H%M.%S)" "$STAMP" 2>/dev/null \
		|| SINCE=0
fi
# NUL-delimited: a path with a space in it is one path, not two words that
# each fail to open.
newer_than_stamp() {
	if [ "$SINCE" = "0" ]; then
		find "$1" -maxdepth 1 -type f -name "$2" -print0 2>/dev/null
	else
		find "$1" -maxdepth 1 -type f -name "$2" -newer "$STAMP" -print0 2>/dev/null
	fi
}

log "=== crash diagnostics ==="
log "uname: $(uname -a)"
if [ "$SINCE" = 0 ]; then
	log "since: NO CUTOFF -- the caller recorded none, or the step that records"
	log "       it was skipped. Everything below may predate this job; check the"
	log "       mtime printed with each artifact before believing it."
else
	log "since: $SINCE ($(date -r "$SINCE" 2>/dev/null || date -d "@$SINCE" 2>/dev/null))"
fi

###############################################################################
# Symbol search path
#
# The wheel installs the extension into site-packages, while the split debug
# info stays in the workspace beside the build that produced it. gdb resolves
# .gnu_debuglink relative to the binary's own directory and finds nothing
# there, so a backtrace through the engine came out as bare addresses. Indexing
# the .debug files by build id gives gdb a place to look that does not depend
# on where the library was loaded from. lldb is told the same directory, since
# a runner has no Spotlight index to find a dSYM by UUID.
###############################################################################

# Outside $OUT, and symlinks rather than copies: $OUT is uploaded as an
# artifact, and a split symbol file for this engine runs to gigabytes, so
# copying them here would ship the debug info as a "diagnostic" and could
# exhaust the artifact before the text report got published.
DBGROOT=$(mktemp -d)
for dbg in *.debug; do
	[ -e "$dbg" ] || continue
	id=$(readelf -n "$dbg" 2>/dev/null | sed -n 's/.*Build ID: \([0-9a-f]*\).*/\1/p' | head -1)
	[ -n "$id" ] || continue
	mkdir -p "$DBGROOT/.build-id/${id:0:2}"
	ln -sf "$PWD/$dbg" "$DBGROOT/.build-id/${id:0:2}/${id:2}.debug" 2>/dev/null || true
	log "indexed $dbg as build id $id"
done
SYMPATHS="$PWD"
for d in *.dSYM; do
	[ -e "$d" ] || continue
	log "dSYM in workspace: $d"
done

###############################################################################
# Cores
###############################################################################

case "$(uname -s)" in
Darwin) CORE_DIRS="tmp/core /cores"; PREFER="lldb gdb" ;;
*) CORE_DIRS="/tmp/core /var/lib/systemd/coredump"; PREFER="gdb lldb" ;;
esac

# Preference, not availability: gdb is the one that reads the build-id index
# above, and a Linux runner that once ran in debug mode still has an lldb that
# would be picked first and would ignore it.
DEBUGGER=
for d in $PREFER; do
	if command -v "$d" >/dev/null 2>&1; then
		DEBUGGER=$d
		break
	fi
done
log "debugger: ${DEBUGGER:-none available}"

found_core=0
for d in $CORE_DIRS; do
	[ -d "$d" ] || continue
	while IFS= read -r -d '' c; do
		found_core=1
		log ""
		log "--- core: $c ($(du -h "$c" 2>/dev/null | cut -f1), written $(date -r "$c" 2>/dev/null))"
		# The core does not name its executable, and a wrong one yields a
		# backtrace of plausible nonsense, so ask the file itself.
		exe=$(file "$c" 2>/dev/null | sed -n "s/.*execfn: '\([^']*\)'.*/\1/p")
		[ -n "$exe" ] || exe=$(file "$c" 2>/dev/null | sed -n "s/.*from '\([^' ]*\).*/\1/p")
		log "    executable: ${exe:-unknown}"
		# Both debuggers take the core through a flag. Passing it as a bare
		# argument makes it the executable instead, and the result is not an
		# error but an empty backtrace.
		if [ "$DEBUGGER" = lldb ]; then
			log "    (lldb) thread backtrace all"
			# The search path has to be set before the target exists, so the
			# core is opened by a command rather than by lldb's -c flag.
			create="target create --core \"$c\""
			[ -n "$exe" ] && create="$create \"$exe\""
			bounded 900 lldb -b \
				-o "settings set target.debug-file-search-paths $SYMPATHS" \
				-o "$create" \
				-o "thread backtrace all" -o quit 2>&1 | tee -a "$SUMMARY"
		elif [ "$DEBUGGER" = gdb ]; then
			log "    (gdb) thread apply all bt"
			args=(-batch -nx -ex "set debug-file-directory $DBGROOT")
			args+=(-ex "thread apply all bt" -ex quit)
			[ -n "$exe" ] && args+=("$exe")
			args+=(-c "$c")
			bounded 900 gdb "${args[@]}" 2>&1 | tee -a "$SUMMARY"
		else
			log "    no lldb or gdb on this runner; core left for the upload step"
		fi
	done < <(newer_than_stamp "$d" 'core*')
done
[ "$found_core" = 1 ] || log "no core files under: $CORE_DIRS"

###############################################################################
# macOS crash reports
###############################################################################

if [ "$(uname -s)" = Darwin ]; then
	found_ips=0
	for d in "$HOME/Library/Logs/DiagnosticReports" /Library/Logs/DiagnosticReports; do
		[ -d "$d" ] || continue
		while IFS= read -r -d '' f; do
			found_ips=1
			log "--- crash report: $f (written $(date -r "$f" 2>/dev/null))"
			cp "$f" "$OUT/" 2>/dev/null || sudo cp "$f" "$OUT/" 2>/dev/null || true
		done < <(newer_than_stamp "$d" '*.ips')
	done
	if [ "$found_ips" = 1 ]; then
		python3 "$(dirname "$0")/symbolicate-ips.py" "$OUT" 2>&1 | tee -a "$SUMMARY"
	else
		log "no crash reports in DiagnosticReports"
	fi
fi

###############################################################################
# Recent logs
#
# Bounded on purpose. A crash usually has a server or test log beside it that
# says what the process was doing, and the ones worth reading are the ones
# written in the same window as the crash.
###############################################################################

log ""
log "--- recent logs (last 200 lines of up to 10 files touched since the cutoff)"
n=0
while [ "$n" -lt 10 ] && IFS= read -r -d '' f; do
	n=$((n + 1))
	log ""
	log "  === $f"
	tail -200 "$f" 2>/dev/null | tee -a "$SUMMARY"
done < <(
	newer_than_stamp . '*.log'
	newer_than_stamp /tmp '*.log'
)
[ "$n" -gt 0 ] || log "  (none)"

rm -f "$STAMP"
log ""
log "=== crash diagnostics done; text written to $OUT ==="
exit 0
