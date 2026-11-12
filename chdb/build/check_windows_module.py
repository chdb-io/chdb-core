#!/usr/bin/env python3
"""Check the exports and DLL imports of the Windows Python module and its pybind11 DLLs.

Usage: check_windows_module.py <dir with _chdb.pyd> <python version>...

- _chdb.pyd exports PyInit__chdb and the ADBC entry points, and imports only DLLs that ship with
  Windows 10 (including the UCRT), the stable-ABI python3.dll and the pybind11 stubs. In
  particular no msvcrt.dll (the module must share python.org CPython's UCRT), no MinGW runtime
  DLL (winpthread, libc++, libgcc: they are linked statically) and no versioned pythonXY.dll.
- pybind11nonlimitedapi_stubs.dll imports python3.dll and exports the pybind11 shim interface.
- pybind11nonlimitedapi_chdb_X.Y.dll imports pythonXY.dll and the stubs.
"""

import re
import subprocess
import sys
from pathlib import Path

SYSTEM_DLLS = {
    "kernel32.dll", "advapi32.dll", "ws2_32.dll", "iphlpapi.dll", "shell32.dll", "ole32.dll",
    "oleaut32.dll", "userenv.dll", "dbghelp.dll", "psapi.dll", "bcrypt.dll", "user32.dll",
    "ntdll.dll", "crypt32.dll", "secur32.dll", "shlwapi.dll", "version.dll", "dnsapi.dll",
    "ucrtbase.dll", "kernelbase.dll", "rpcrt4.dll", "mswsock.dll", "gdi32.dll",
}


def run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def imports(path):
    return [m.lower() for m in re.findall(r"DLL Name:\s*(\S+)", run("llvm-objdump-21", "-p", str(path)))]


def exports(path):
    return set(re.findall(r"Name:\s*(\S+)", run("llvm-readobj-21", "--coff-exports", str(path))))


def is_system(dll):
    return dll in SYSTEM_DLLS or dll.startswith("api-ms-win-")


def main():
    root = Path(sys.argv[1])
    versions = sys.argv[2:]
    errors = []

    def check(cond, message):
        if not cond:
            errors.append(message)

    pyd = root / "_chdb.pyd"
    pyd_exports = exports(pyd)
    for name in ("PyInit__chdb", "chdb_adbc_init", "AdbcDriverInit"):
        check(name in pyd_exports, f"{pyd.name} does not export {name}")
    for dll in imports(pyd):
        check(is_system(dll) or dll in ("python3.dll", "pybind11nonlimitedapi_stubs.dll"),
              f"{pyd.name} imports {dll}")
    check("python3.dll" in imports(pyd), f"{pyd.name} does not import python3.dll")

    stubs = root / "pybind11nonlimitedapi_stubs.dll"
    for dll in imports(stubs):
        check(is_system(dll) or dll == "python3.dll", f"{stubs.name} imports {dll}")
    check(any(e.startswith("_ZN8pybind11") for e in exports(stubs)), f"{stubs.name} exports no pybind11 symbols")

    for version in versions:
        shim = root / f"pybind11nonlimitedapi_chdb_{version}.dll"
        check(shim.exists(), f"{shim.name} is missing")
        if not shim.exists():
            continue
        expected = f"python{version.replace('.', '')}.dll"
        shim_imports = imports(shim)
        check(expected in shim_imports, f"{shim.name} does not import {expected}")
        for dll in shim_imports:
            check(is_system(dll) or dll in (expected, "pybind11nonlimitedapi_stubs.dll"), f"{shim.name} imports {dll}")
        check(any("pybind11NLA_" in e for e in exports(shim)), f"{shim.name} exports no pybind11NLA_ functions")

    if errors:
        print("Windows module check FAILED:")
        for e in errors:
            print("  " + e)
        sys.exit(1)
    print(f"Windows module check passed ({pyd.name}, stubs, {len(versions)} shims)")


if __name__ == "__main__":
    main()
