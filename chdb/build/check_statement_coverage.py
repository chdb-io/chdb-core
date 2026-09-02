#!/usr/bin/env python3
"""Every top-level statement the engine can parse must have a classification test.

`examples/chdbDurableAbiTest.c` asserts a class for each statement shape, and a
durable control plane trusts that table: a statement the classifier has no
opinion about comes back UNKNOWN, which the caller refuses. That is the safe
direction, but only if someone notices. A hand-written table does not notice --
`ParserCopyQuery` sat in the engine unlisted while the test claimed to cover
every shape.

So the table is checked against the engine instead of trusted. The alternatives
tried by `ParserQuery::parseImpl` and `ParserQueryWithOutput::parseImpl` are the
definition of "top-level statement"; every one of them must appear in the test
table's `parser` column, or be listed below with a reason.

Run from anywhere; exits non-zero and names the gap when one appears.
"""

import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PARSER_SOURCES = [
    os.path.join(REPO, "src", "Parsers", "ParserQuery.cpp"),
    os.path.join(REPO, "src", "Parsers", "ParserQueryWithOutput.cpp"),
]
TEST_TABLE = os.path.join(REPO, "examples", "chdbDurableAbiTest.c")

# Parsers that are not a top-level statement alternative of their own.
NOT_A_STATEMENT = {
    # Recurses into itself for the EXECUTE AS / PARALLEL WITH sub-query.
    "ParserQuery",
    # An aggregate: its own alternatives are enumerated in the other file.
    "ParserQueryWithOutput",
    # Clause helpers for INTO OUTFILE / FORMAT, not statements.
    "ParserIdentifier",
    "ParserStringLiteral",
}

# Statement alternatives the test table cannot exercise, with the reason.
# Removing an entry is how you promote one back into the table.
UNREACHABLE = {
    "ParserCreateMaskingPolicy": (
        "no accepted spelling in this build -- every CREATE/ALTER MASKING POLICY "
        "form tried against libchdb parses as UNKNOWN"
    ),
}


def parsers_declared_in(path):
    """The Parser types instantiated as alternatives in a parseImpl."""
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    return set(re.findall(r"^\s+(Parser[A-Za-z0-9_]+)\s+[a-z_]+_p\b", text, re.MULTILINE))


def parsers_covered_by_table(path):
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    match = re.search(r"k_all_statements\[\]\s*=\s*\{(.*?)\n\};", text, re.DOTALL)
    if not match:
        raise SystemExit(f"{path}: could not find the k_all_statements table")
    return set(re.findall(r'\{\s*"(Parser[A-Za-z0-9_]+)"', match.group(1)))


def main():
    engine = set()
    for source in PARSER_SOURCES:
        if not os.path.exists(source):
            raise SystemExit(f"missing parser source: {source}")
        engine |= parsers_declared_in(source)
    engine -= NOT_A_STATEMENT

    covered = parsers_covered_by_table(TEST_TABLE)

    uncovered = engine - covered - set(UNREACHABLE)
    stale_exemptions = set(UNREACHABLE) - engine
    stale_rows = covered - engine

    failed = False

    if uncovered:
        failed = True
        print(
            "These statement parsers have no row in the chdbDurableAbiTest table.\n"
            "Add one asserting the class the statement must get, or add it to\n"
            "UNREACHABLE in this script with the reason it cannot be tested:",
            file=sys.stderr,
        )
        for name in sorted(uncovered):
            print(f"  {name}", file=sys.stderr)

    if stale_exemptions:
        failed = True
        print(
            "\nThese are exempted here but no longer exist in the engine; drop them:",
            file=sys.stderr,
        )
        for name in sorted(stale_exemptions):
            print(f"  {name}", file=sys.stderr)

    if stale_rows:
        failed = True
        print(
            "\nThe test table names parsers the engine no longer has; drop those rows:",
            file=sys.stderr,
        )
        for name in sorted(stale_rows):
            print(f"  {name}", file=sys.stderr)

    if failed:
        return 1

    print(
        f"statement coverage: {len(covered)} of {len(engine)} top-level parsers tested, "
        f"{len(UNREACHABLE)} exempted"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
