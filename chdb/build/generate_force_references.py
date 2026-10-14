#!/usr/bin/env python3
"""
Generate programs/local/ForceFunctionReferences.cpp from the REGISTER_FUNCTION sites in
src/Functions, or with --check fail when the committed file differs from what it would generate.

The static archive keeps only the objects an example program's link pulls in (see
build_static_lib.sh), and a REGISTER_FUNCTION object registers itself from a static initializer
that nothing references. The generated file takes the address of every registerFunction<Name>
so the link pulls each of them in.

A registration compiled under a preprocessor condition is referenced under the same condition,
so a function is kept wherever it is built and asked for nowhere it is not. Every macro such a
condition names has to be known here; an unknown one stops the generator rather than be
evaluated in a translation unit where it may mean something else.
"""
import argparse
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FUNCTIONS_DIR = PROJECT_ROOT / "src" / "Functions"
CONFIG_H_IN = PROJECT_ROOT / "src" / "Common" / "config.h.in"
OUTPUT_FILE = PROJECT_ROOT / "programs" / "local" / "ForceFunctionReferences.cpp"

# Registrations defined outside namespace DB (upstream bugs).
GLOBAL_NAMESPACE_FUNCTIONS = {
    'IsValidASCII',
}

# Registrations never referenced, with the reason. A function compiled under a condition does
# not belong here: its reference carries the condition.
EXCLUDED_FUNCTIONS = {
}

# How each macro in a registration's condition is written in the generated file, besides the
# USE_* macros of config.h, which the generated file includes. None keeps the macro as it is,
# for one that means the same there as in the function's source; a string replaces it, for one
# the generated file does not see the way the function's source does.
MACROS = {
    # The compiler's and the build's global definitions.
    '__ELF__': None,
    'OS_DARWIN': None,
    'OS_FREEBSD': None,
    'OS_LINUX': None,
    'CHDB_LITE': None,
    # A CMake option only; nothing defines it as a macro, so it reads as 0 everywhere.
    'WITH_COVERAGE_DEPTH': None,
    # Defined only by the unit tests that compile these sources on their own.
    'MD5_GTEST_UNIT_TEST': None,
    'SHA1_GTEST_UNIT_TEST': None,
    'STRING_SIMILARITY_GTEST_UNIT_TEST': None,
    # Defined by addressToSymbol.cpp itself.
    'HAS_SYMBOL_INDEX': '(defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)',
    # Defined for src/Functions only, whenever CHDB_VERSION is set, which every build script does.
    'CHDB_VERSION_STRING': '1',
}

REGISTER_RE = re.compile(r'REGISTER_FUNCTION\s*\(\s*(\w+)\s*\)')
DIRECTIVE_RE = re.compile(r'^\s*#\s*(if|ifdef|ifndef|elif|else|endif)\b(.*)$')
# defined(NAME), defined NAME, or a bare NAME.
TOKEN_RE = re.compile(r'\bdefined\s*\(\s*(\w+)\s*\)|\bdefined\s+(\w+)|\b([A-Za-z_]\w*)\b')


def config_macros():
    """The macros config.h defines to 0 or 1."""
    return set(re.findall(r'^#cmakedefine01\s+(\w+)', CONFIG_H_IN.read_text(), re.MULTILINE))


def strip_comment(text):
    text = re.sub(r'/\*.*?\*/', ' ', text)
    return ' '.join(text.split('//')[0].split())


def translate(condition, where, known_config):
    """Rewrite one #if expression for the generated file."""
    def replace(match):
        name = match.group(1) or match.group(2) or match.group(3)
        if name not in MACROS and name not in known_config:
            raise SystemExit(
                f"error: {where}: a REGISTER_FUNCTION is compiled under a condition naming '{name}', "
                f"which chdb/build/generate_force_references.py does not know. Add it to MACROS, "
                f"saying how the generated file should evaluate it.")
        replacement = MACROS.get(name)
        if replacement is not None:
            return replacement if match.group(0) == condition.strip() else f'({replacement})'
        return name if match.group(3) else f'defined({name})'

    return TOKEN_RE.sub(replace, condition)


def scan_file(path, known_config):
    """Yield (function, condition) for each REGISTER_FUNCTION in one source file."""
    relative = path.relative_to(PROJECT_ROOT)
    text = path.read_text(encoding='utf-8', errors='replace').replace('\\\n', ' ')
    # One entry per open #if: the conditions of the branches already passed, and the current
    # one (None after #else). Conditions stay raw until a registration needs them, so that
    # sources without one never have their macros checked.
    stack = []
    for lineno, line in enumerate(text.splitlines(), 1):
        match = DIRECTIVE_RE.match(line)
        if match:
            keyword, rest = match.group(1), strip_comment(match.group(2))
            where = f'{relative}:{lineno}'
            if keyword == 'if':
                stack.append(([], (rest, where)))
            elif keyword == 'ifdef':
                stack.append(([], (f'defined({rest})', where)))
            elif keyword == 'ifndef':
                stack.append(([], (f'!defined({rest})', where)))
            elif keyword == 'elif':
                previous, current = stack[-1]
                stack[-1] = (previous + [current], (rest, where))
            elif keyword == 'else':
                previous, current = stack[-1]
                stack[-1] = (previous + [current], None)
            else:
                stack.pop()
            continue
        if line.lstrip().startswith('#define'):
            continue
        for function in REGISTER_RE.findall(line):
            terms = []
            for previous, current in stack:
                terms.extend(f'!({translate(*c, known_config)})' for c in previous)
                if current is not None:
                    terms.append(translate(*current, known_config))
            terms = [t for t in terms if t != '1']
            if len(terms) > 1:
                terms = [t if t.startswith('!(') else f'({t})' for t in terms]
            yield function, ' && '.join(terms)


def collect_registrations():
    """Map each registered function to the condition it is compiled under ('' for always)."""
    known_config = config_macros()
    registrations = {}
    for path in sorted(FUNCTIONS_DIR.rglob('*.cpp')):
        for function, condition in scan_file(path, known_config):
            if function in EXCLUDED_FUNCTIONS:
                continue
            if registrations.get(function, condition) != condition:
                raise SystemExit(f"error: {function} is registered under two different conditions")
            registrations[function] = condition
    return registrations


def grouped(registrations, fmt, global_namespace):
    """One line per registration, grouped by condition, the unconditional ones first."""
    by_condition = {}
    for function in sorted(registrations):
        if (function in GLOBAL_NAMESPACE_FUNCTIONS) == global_namespace:
            by_condition.setdefault(registrations[function], []).append(fmt.format(function))
    out = []
    for condition in sorted(by_condition, key=lambda c: (c != '', c)):
        if condition:
            out.append(f'#if {condition}')
        out.extend(by_condition[condition])
        if condition:
            out.append('#endif')
    return out


def generate_code(registrations):
    code = [
        '/// Generated by chdb/build/generate_force_references.py - DO NOT EDIT MANUALLY.',
        '/// Regenerate after changing src/Functions; CI runs the generator with --check.',
        '///',
        '/// References every function registration so that the static library keeps its object:',
        '/// nothing else names a registration, which registers itself from a static initializer.',
        '/// Each reference carries the preprocessor condition its registration is compiled under.',
        '',
        '#include "config.h"',
        '',
        'namespace DB',
        '{',
        '    class FunctionFactory;',
        '}',
        '',
    ]
    global_decls = grouped(registrations, 'extern void registerFunction{}(::DB::FunctionFactory & factory);', True)
    if global_decls:
        code.append('/// Functions defined in global namespace (upstream bugs)')
        code.extend(global_decls)
        code.append('')
    code.extend([
        'namespace DB',
        '{',
        '',
        '    /// Forward declarations of function registration functions',
        '    /// These functions have external linkage and can be referenced',
    ])
    code.extend(grouped(registrations, '    extern void registerFunction{}(FunctionFactory & factory);', False))
    code.extend([
        '',
        '    /// Force references to ensure function registration functions are linked',
        '    void * ForceStaticRegistrationObjects();',
        '    void * ForceStaticRegistrationObjects()',
        '    {',
        '        /// Create function pointer array to force linking',
        '        static void (*registration_funcs[])(FunctionFactory &) =',
        '        {',
    ])
    code.extend(grouped(registrations, '            &::registerFunction{},', True))
    code.extend(grouped(registrations, '            &registerFunction{},', False))
    code.extend([
        '            nullptr',
        '        };',
        '',
        '        /// Return pointer to prevent compiler optimization',
        '        return static_cast<void *>(registration_funcs);',
        '    }',
        '}',
        '',
    ])
    return '\n'.join(code)


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument('--check', action='store_true',
                        help='fail if the committed file differs from the generated one')
    args = parser.parse_args()

    registrations = collect_registrations()
    code = generate_code(registrations)
    relative = OUTPUT_FILE.relative_to(PROJECT_ROOT)
    summary = f"{len(registrations)} registrations, {sum(1 for c in registrations.values() if c)} conditional"

    if args.check:
        current = OUTPUT_FILE.read_text(encoding='utf-8') if OUTPUT_FILE.exists() else ''
        if current != code:
            print(f"error: {relative} does not match the REGISTER_FUNCTION sites in src/Functions.\n"
                  f"Regenerate it with: python3 chdb/build/generate_force_references.py", file=sys.stderr)
            return 1
        print(f"{relative} is up to date ({summary})")
        return 0

    OUTPUT_FILE.write_text(code, encoding='utf-8')
    print(f"Generated {relative} ({summary})")
    return 0


if __name__ == '__main__':
    sys.exit(main())
