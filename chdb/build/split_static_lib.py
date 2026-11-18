#!/usr/bin/env python3
"""Build the split Linux static library: libchdb.a as a few relocatable objects of a bounded
size instead of the one object prelink_static_lib.sh makes (chdb-io/chdb-core#342).

chdb-go ships the static library inside Go modules, as .syso files, and Go caps a module at
500 MiB. The prelinked archive is one object of over a gigabyte, which no grouping of archive
members can fit, and its members cannot be shipped as they are either: that is the archive
whose hidden definitions bound a consumer's own references to chDB's libc++, libunwind, zstd
and OpenSSL (chdb-io/chdb-rust#53, chdb-io/chdb-core#261). So the same isolation is applied to
shards rather than to the whole:

  1. The members are cut, in archive order, into contiguous runs of at most --shard-mb of
     input, and `ld -r` merges each run into one shard. Archive order is link order, so the
     shards, linked in order, put the static constructors in the order the single object had.
  2. A symbol that one shard defines and nothing outside it defines or references becomes
     local, as in the single object, and keeps its name for backtraces and profilers.
  3. A symbol that more than one shard defines or references has to stay global for the
     shards to reach each other. It gets the .chdb suffix in every shard - definitions and
     references alike - and hidden visibility, so a consumer's linker resolves chDB's own
     references between the shards, keeps the symbol out of any .dynsym, and has no name
     the consumer's own code could ask for. The macOS archive uses the same suffix
     (rename_static_lib_symbols_macos.py). Weak definitions in several shards stay weak:
     the consumer's linker then picks what `ld -r` would have picked, a strong definition
     over a weak default.
  4. COMDAT groups are kept, not dissolved, so the consumer's linker deduplicates the inline
     functions and template instantiations that several shards share. Their signatures are
     global and get the suffix too: the hazard --force-group-allocation removes from the single
     object, a group discarded in favour of the host's same-named group, needs a name the
     host can have.
  5. The optional hooks get the .chdb suffix as in prelink_static_lib.sh: weak references
     that nothing in the archive defines and that are not meant for the C library.
  6. Every shard references an anchor symbol of the next one. A consumer that links the
     archive with a plain -lchdb pulls the shard with the C API function it calls, and that
     pulls in the rest: a shard only some static constructors live in would otherwise be
     left out, and its registrations with it. Go links every .syso anyway.

The rest is what prelink_static_lib.sh does, per shard: -z noexecstack, `ld.bfd -r -d` for
the shard with a COMMON symbol (OpenSSL's hidden OPENSSL_ia32cap_P on x86_64; every member that
has one is kept in the same shard, so it is allocated once), lld otherwise, .llvm_addrsig
dropped, and on aarch64 the code sections renamed into runs of at most 64 MB, with names of
their own per shard (split_static_lib_text.py).

The archive's members are padded so that each object starts at an 8-byte aligned offset
(gold, see prelink_static_lib.sh). A JSON manifest records the link order, the sizes and
checksums, and what the shards need from the system. check_static_lib_hermetic.sh checks the
result.

Usage: split_static_lib.py <input libchdb.a> <output libchdb.a> [--manifest out.json]
                           [--shard-mb N] [--version V]

The input is the minimised, stripped archive build_static_lib.sh hands to prelink_static_lib.sh;
it is not modified.
"""

import argparse
import concurrent.futures
import glob
import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile

SUFFIX = ".chdb"
MY_DIR = os.path.dirname(os.path.abspath(__file__))
PROJ_DIR = os.path.dirname(os.path.dirname(MY_DIR))
MB = 1 << 20
# Go's limit for a module is 500 MiB; leave room for two shards per module plus the rest of it.
DEFAULT_SHARD_MB = 192
ANCHOR = "chdb_split_shard_{}" + SUFFIX
MANIFEST_FORMAT = 1


def log(message):
    print(message, flush=True)


def run(cmd, **kwargs):
    try:
        return subprocess.run(cmd, check=True, **kwargs)
    except subprocess.CalledProcessError as e:
        out = (e.stderr or b"")
        if isinstance(out, bytes):
            out = out.decode(errors="replace")
        sys.exit(f"failed ({e.returncode}): {' '.join(cmd[:4])} ...\n{out[-4000:]}")


def newest(pattern):
    found = []
    for directory in ("/usr/bin", "/usr/local/bin"):
        found += glob.glob(os.path.join(directory, pattern))

    def version(path):
        return [int(x) for x in re.findall(r"\d+", os.path.basename(path))]
    return max(found, key=version) if found else None


def find_tools():
    tools = {}
    tools["lld"] = os.environ.get("SPLIT_LLD") or newest("ld.lld-*") or shutil.which("ld.lld")
    tools["bfd"] = os.environ.get("SPLIT_BFD") or shutil.which("ld.bfd") or shutil.which("ld")
    tools["objcopy"] = os.environ.get("SPLIT_OBJCOPY") or newest("llvm-objcopy*") or shutil.which("llvm-objcopy")
    tools["nm"] = os.environ.get("SPLIT_NM") or newest("llvm-nm*") or shutil.which("llvm-nm")
    tools["readelf"] = shutil.which("readelf")
    tools["as"] = os.environ.get("AS") or shutil.which("as")
    tools["ar"] = shutil.which("ar")
    for name, path in tools.items():
        if not path:
            sys.exit(f"split_static_lib: no {name} found")
    for name, marker in (("lld", "LLD"), ("bfd", "GNU ld"), ("objcopy", "LLVM"), ("nm", "LLVM")):
        version = subprocess.run([tools[name], "--version"], capture_output=True, text=True).stdout
        if marker not in version:
            sys.exit(f"split_static_lib: {tools[name]} is not {marker}")
        tools[name + "_version"] = version.splitlines()[0] if version else "?"
    return tools


def archive_members(path):
    """(name, offset of the data, size) of every member, in order, the symbol and name tables
    left out."""
    members, long_names = [], b""
    with open(path, "rb") as f:
        if f.read(8) != b"!<arch>\n":
            sys.exit(f"{path}: not an ar archive")
        offset = 8
        while True:
            f.seek(offset)
            header = f.read(60)
            if len(header) < 60:
                break
            name, size = header[:16].decode().rstrip(), int(header[48:58].decode())
            if name == "//":
                long_names = f.read(size)
            elif name not in ("/", "/SYM64/"):
                if name.startswith("/"):
                    start = int(name[1:])
                    name = long_names[start:long_names.index(b"\n", start)].decode()
                members.append((name.rstrip("/"), offset + 60, size))
            offset += 60 + size + (size & 1)
    return members


def partition(members, sizes, limit, together):
    """Contiguous runs of members of at most `limit` bytes each (a larger member gets a run
    of its own). Members in the same set of `together` go into the run of the first of them."""
    shards, current, current_size = [], [], 0
    for name in members:
        if current and current_size + sizes[name] > limit:
            shards.append(current)
            current, current_size = [], 0
        current.append(name)
        current_size += sizes[name]
    if current:
        shards.append(current)
    for group in together:
        home = next(i for i, shard in enumerate(shards) if group & set(shard))
        for i, shard in enumerate(shards):
            if i != home:
                moved = [m for m in shard if m in group]
                if moved:
                    shards[i] = [m for m in shard if m not in group]
                    shards[home] += moved
    return [shard for shard in shards if shard]


def write_anchor(tools, work, index, count):
    """An object that defines the anchor of shard `index` and references that of the next."""
    source = os.path.join(work, f"anchor_{index}.s")
    obj = os.path.join(work, f"anchor_{index}.o")
    with open(source, "w") as f:
        f.write(f"""\
    .section .note.GNU-stack,"",%progbits
    .section .data.rel.ro.chdb_split_anchor,"aw",%progbits
    .p2align 3
    .globl "{ANCHOR.format(index)}"
    .hidden "{ANCHOR.format(index)}"
"{ANCHOR.format(index)}":
    .8byte "{ANCHOR.format((index + 1) % count)}"
""")
    run([tools["as"], source, "-o", obj], capture_output=True)
    return obj


def symbols(tools, obj):
    """(global or weak definitions, undefined references, weak undefined references) of an
    object, as name sets. --defined-only -g rather than the type letters: `i` (an ifunc) does
    not say whether the symbol is global."""
    out = run([tools["nm"], "--no-sort", "-g", "--defined-only", obj], capture_output=True, text=True).stdout
    defined = set()
    for line in out.splitlines():
        fields = line.split()
        if len(fields) >= 2:
            if fields[-2] == "C":
                sys.exit(f"{obj}: COMMON symbol {fields[-1]} was not allocated")
            defined.add(fields[-1])
    out = run([tools["nm"], "--no-sort", "-u", obj], capture_output=True, text=True).stdout
    undefined, weak_undefined = set(), set()
    for line in out.splitlines():
        fields = line.split()
        if len(fields) == 2:
            undefined.add(fields[1])
            if fields[0] in ("w", "v"):
                weak_undefined.add(fields[1])
    return defined, undefined, weak_undefined


def group_signatures(tools, obj):
    out = run([tools["readelf"], "-gW", obj], capture_output=True, text=True).stdout
    return set(re.findall(r"^COMDAT group section \[\s*\d+\] `[^']*' \[([^\]]+)\]", out, flags=re.M))


def c_library_exports():
    out = run(["bash", os.path.join(MY_DIR, "c_library_exports.sh")], capture_output=True, text=True).stdout
    names = set(out.split())
    if not names:
        sys.exit("split_static_lib: the C library exports nothing")
    return names


def contract():
    out = run([sys.executable, os.path.join(MY_DIR, "check_export_contract.py")],
              capture_output=True, text=True).stdout
    names = set(out.split())
    if not names:
        sys.exit("split_static_lib: the C API contract is empty")
    return names


SYMBOL = struct.Struct("<IBBHQQ")
SECTION_HEADER = struct.Struct("<IIQQQQIIQQ")


def hide_private_definitions(path):
    """Gives every global or weak definition with the .chdb suffix hidden visibility, in place.
    llvm-objcopy only learnt --set-symbols-visibility in LLVM 20."""
    with open(path, "r+b") as f:
        data = bytearray(f.read())
        if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
            sys.exit(f"{path}: not an ELF64 little-endian object")
        (shoff,) = struct.unpack_from("<Q", data, 0x28)
        shentsize, shnum = struct.unpack_from("<HH", data, 0x3A)
        if shnum == 0:
            shnum = SECTION_HEADER.unpack_from(data, shoff)[5]
        sections = [SECTION_HEADER.unpack_from(data, shoff + i * shentsize) for i in range(shnum)]
        symtab = [s for s in sections if s[1] == 2]  # SHT_SYMTAB
        if len(symtab) != 1:
            sys.exit(f"{path}: {len(symtab)} symbol tables")
        _, _, _, _, offset, size, link, first_global, _, entsize = symtab[0]
        if entsize != SYMBOL.size:
            sys.exit(f"{path}: unexpected symbol size {entsize}")
        strtab = sections[link]
        names = bytes(data[strtab[4]:strtab[4] + strtab[5]])
        suffix = SUFFIX.encode()
        start = offset + first_global * entsize
        hidden = 0
        for index, (st_name, st_info, st_other, st_shndx, _, _) in enumerate(
                SYMBOL.iter_unpack(data[start:offset + size])):
            if st_shndx == 0 or (st_info >> 4) not in (1, 2):  # SHN_UNDEF; STB_GLOBAL, STB_WEAK
                continue
            end = names.index(b"\0", st_name)
            if names[end - len(suffix):end] != suffix:
                continue
            data[start + index * entsize + 5] = (st_other & ~3) | 2  # STV_HIDDEN
            hidden += 1
        f.seek(0)
        f.write(data)
    return hidden


def ar_header(name, size):
    header = f"{name:<16}{0:<12}{0:<6}{0:<6}{100644:<8}{size:<10}`\n".encode()
    assert len(header) == 60, name
    return header


def write_archive(objects, definitions, path):
    """A GNU ar archive of `objects`, in order, with the symbol table a linker resolves -lchdb
    through (`definitions`: the global definitions of each object), every object at an
    8-byte aligned offset, and nothing that changes from one build to the next: no
    timestamps, owners, or names other than the objects' own.

    Written here rather than with `ar` for the alignment (gold, see prelink_static_lib.sh). A
    member's data follows a 60-byte header and odd sizes are padded to even, so an object of
    a size that is 4 more than a multiple of 8 keeps the next one aligned too: each object is
    padded with zeros to such a size (a reader goes by the ELF header, not the file size).
    The first one is placed by NUL bytes after the last name in the symbol table, which
    readers do not look at: they read as many names as the table has offsets."""
    if sum(os.path.getsize(obj) for obj in objects) >= 1 << 32:
        sys.exit(f"{path}: over 4 GiB; the 32-bit symbol table cannot address it")
    for obj in objects:
        size = os.path.getsize(obj)
        with open(obj, "ab") as f:
            f.write(b"\0" * ((4 - size) % 8))
    names = [os.path.basename(obj) for obj in objects]
    name_table = b"".join(name.encode() + b"/\n" for name in names)
    name_offsets, at = [], 0
    for name in names:
        name_offsets.append(at)
        at += len(name) + 2
    name_table += b"\n" * (len(name_table) & 1)
    symbols_ = [(name, i) for i, defined in enumerate(definitions) for name in sorted(defined)]
    strings = b"".join(name.encode() + b"\0" for name, _ in symbols_)
    table_size = 4 + 4 * len(symbols_) + len(strings)
    # Where the first object's data starts with `padding` NUL bytes after the strings.
    def first_offset(padding):
        size = table_size + padding
        return 8 + 60 + size + (size & 1) + 60 + len(name_table) + 60
    padding = next(p for p in range(8) if first_offset(p) % 8 == 0)
    member_offsets, at = [], first_offset(padding) - 60
    table_size += padding
    for obj in objects:
        member_offsets.append(at)
        size = os.path.getsize(obj)
        at += 60 + size + (size & 1)
    with open(path, "wb") as out:
        out.write(b"!<arch>\n")
        out.write(ar_header("/", table_size))
        out.write(struct.pack(">I", len(symbols_)))
        out.write(b"".join(struct.pack(">I", member_offsets[i]) for _, i in symbols_))
        out.write(strings + b"\0" * padding + b"\n" * (table_size & 1))
        out.write(ar_header("//", len(name_table)))
        out.write(name_table)
        for obj, name_offset, member_offset in zip(objects, name_offsets, member_offsets):
            if out.tell() != member_offset:
                sys.exit(f"{path}: {obj} landed at {out.tell()}, not {member_offset}")
            size = os.path.getsize(obj)
            out.write(ar_header(f"/{name_offset}", size))
            with open(obj, "rb") as f:
                shutil.copyfileobj(f, out, 16 * MB)
            out.write(b"\n" * (size & 1))
    members = archive_members(path)
    if [m[0] for m in members] != names or not all(offset % 8 == 0 for _, offset, _ in members):
        sys.exit(f"{path}: unexpected layout {members}")
    return members


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(MB), b""):
            digest.update(block)
    return digest.hexdigest()


def git_commit():
    result = subprocess.run(["git", "-C", PROJ_DIR, "rev-parse", "HEAD"], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", help="the minimised, stripped multi-member archive")
    parser.add_argument("output", help="the split archive to write")
    parser.add_argument("--manifest", help="where to write the JSON manifest (default: <output>.json)")
    parser.add_argument("--shard-mb", type=float,
                        default=float(os.environ.get("CHDB_SPLIT_SHARD_MB", DEFAULT_SHARD_MB)),
                        help=f"the most input a shard takes, in MiB (default: $CHDB_SPLIT_SHARD_MB or {DEFAULT_SHARD_MB})")
    parser.add_argument("--version", default=os.environ.get("CHDB_VERSION"), help="chDB version for the manifest")
    parser.add_argument("--jobs", type=int, default=min(os.cpu_count() or 1, 8))
    args = parser.parse_args()

    if not sys.platform.startswith("linux"):
        sys.exit("split_static_lib: Linux only; the macOS archive is already made of ordinary members")
    tools = find_tools()
    keep = contract()
    c_library = c_library_exports()

    out_dir = os.path.dirname(os.path.abspath(args.output))
    # Next to the output rather than in /tmp: the members alone are over a gigabyte.
    work = tempfile.mkdtemp(prefix="split.", dir=out_dir)
    try:
        members = archive_members(args.input)
        names = [name for name, _, _ in members]
        if len(set(names)) != len(names):
            sys.exit(f"{args.input}: duplicate member names; `ar x` would lose some")
        sizes = {name: size for name, _, size in members}
        log(f"Splitting {args.input}: {len(names)} members, {sum(sizes.values()) / MB:.0f} MiB, "
            f"shards of at most {args.shard_mb:g} MiB of input")
        log(f"  lld:     {tools['lld']} ({tools['lld_version']})")
        log(f"  bfd:     {tools['bfd']} ({tools['bfd_version']})")
        log(f"  objcopy: {tools['objcopy']}")

        member_dir = os.path.join(work, "members")
        os.makedirs(member_dir)
        run([tools["ar"], "x", os.path.abspath(args.input)], cwd=member_dir)

        # Every member that has a given COMMON symbol goes into one shard, where `ld.bfd -r -d`
        # allocates it once; the other shards reach it by its private name.
        nm_all = run([tools["nm"], "-A", "--no-sort", os.path.abspath(args.input)],
                     capture_output=True, text=True).stdout
        commons = {}
        for line in nm_all.splitlines():
            fields = line.split()
            # "<archive>:<member>: <value> C <name>"
            if len(fields) == 4 and fields[2] == "C":
                member = fields[0].rsplit(":", 2)[-2]
                commons.setdefault(fields[3], set()).add(member)
        shards = partition(names, sizes, args.shard_mb * MB, list(commons.values()))
        count = len(shards)
        common_members = set().union(*commons.values()) if commons else set()
        log(f"  COMMON symbols: {' '.join(sorted(commons)) or 'none'}")

        def link(index):
            shard = shards[index]
            has_commons = bool(common_members & set(shard))
            response = os.path.join(work, f"shard_{index}.rsp")
            with open(response, "w") as f:
                for member in shard:
                    f.write(os.path.join(member_dir, member) + "\n")
                f.write(write_anchor(tools, work, index, count) + "\n")
            linked = os.path.join(work, f"linked_{index}.o")
            linker = tools["bfd"] if has_commons else tools["lld"]
            cmd = [linker, "-r", "-z", "noexecstack", "-o", linked, "@" + response]
            if has_commons:
                cmd.insert(2, "-d")
            run(cmd, capture_output=True)
            return linked, os.path.basename(linker)

        with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
            linked = list(pool.map(link, range(count)))
            tables = list(pool.map(lambda item: symbols(tools, item[0]), linked))
            signatures = list(pool.map(lambda item: group_signatures(tools, item[0]), linked))
        shutil.rmtree(member_dir)

        defined_in, seen_in = {}, {}
        weak_undefined = set()
        for index, (defined, undefined, weak) in enumerate(tables):
            for name in defined:
                defined_in.setdefault(name, []).append(index)
                seen_in.setdefault(name, set()).add(index)
            for name in undefined:
                seen_in.setdefault(name, set()).add(index)
            weak_undefined |= weak
        missing = keep - defined_in.keys()
        if missing:
            sys.exit(f"{len(missing)} C API symbols are not defined, e.g. {sorted(missing)[:5]}")
        already = [n for n in defined_in if n.endswith(SUFFIX) and not re.fullmatch(r"chdb_split_shard_\d+\.chdb", n)]
        if already:
            sys.exit(f"{args.input} already has {SUFFIX} definitions, e.g. {already[:5]}; it was renamed before")

        all_signatures = set().union(*signatures)
        # Shared between shards, or a COMDAT signature: global under a private name.
        shared = {name for name in defined_in
                  if name not in keep and (len(seen_in[name]) > 1 or name in all_signatures)}
        hooks = {name for name in weak_undefined if name not in defined_in and name not in c_library}
        rename = {name: name + SUFFIX for name in (shared | hooks) if not name.endswith(SUFFIX)}
        log(f"  {count} shards; {len(defined_in)} global definitions, {len(shared)} shared between shards "
            f"or COMDAT signatures, {len(hooks)} optional hooks: {' '.join(sorted(hooks))}")

        rename_file = os.path.join(work, "rename.txt")
        with open(rename_file, "w") as f:
            for old, new in sorted(rename.items()):
                f.write(f"{old} {new}\n")

        is_aarch64 = "AArch64" in run([tools["readelf"], "-h", linked[0][0]], capture_output=True, text=True).stdout

        def finish(index):
            source = linked[index][0]
            keep_file = os.path.join(work, f"keep_{index}.txt")
            with open(keep_file, "w") as f:
                for name in sorted((keep | shared) & tables[index][0]):
                    f.write(name + "\n")
            options = []
            if is_aarch64:
                split = run([sys.executable, os.path.join(MY_DIR, "split_static_lib_text.py"), source,
                             "--prefix", f".chdb.text.{index}."], capture_output=True, text=True)
                sys.stderr.write(split.stderr)
                options_file = os.path.join(work, f"text_{index}.txt")
                with open(options_file, "w") as f:
                    f.write(split.stdout)
                options.append("@" + options_file)
            out = os.path.join(work, "out", f"libchdb_part_{index:03d}.o")
            os.makedirs(os.path.dirname(out), exist_ok=True)
            # Binding is decided on the old names, then the names change (llvm-objcopy's order).
            run([tools["objcopy"], f"--keep-global-symbols={keep_file}", f"--redefine-syms={rename_file}",
                 "--remove-section=.llvm_addrsig"] + options + [source, out], capture_output=True)
            os.remove(source)
            hidden = hide_private_definitions(out)
            return out, hidden

        with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
            finished = list(pool.map(finish, range(count)))
            final_tables = list(pool.map(lambda item: symbols(tools, item[0]), finished))
            final_signatures = list(pool.map(lambda item: group_signatures(tools, item[0]), finished))

        # What check_static_lib_hermetic.sh checks again on the archive, here against the names
        # the input had.
        private_defined = set()
        for defined, _, _ in final_tables:
            extra = {n for n in defined if n not in keep and not n.endswith(SUFFIX)}
            if extra:
                sys.exit(f"{len(extra)} definitions kept a global name outside the C API, e.g. {sorted(extra)[:5]}")
            private_defined |= {n for n in defined if n.endswith(SUFFIX)}
        for index, (_, undefined, _) in enumerate(final_tables):
            stale = {n for n in undefined if n in defined_in and n not in keep and not n.endswith(SUFFIX)}
            if stale:
                sys.exit(f"shard {index} still references {len(stale)} symbols of the archive by their old "
                         f"name, e.g. {sorted(stale)[:5]}")
            dangling = {n for n in undefined if n.endswith(SUFFIX) and n not in private_defined
                        and n[:-len(SUFFIX)] not in hooks}
            if dangling:
                sys.exit(f"shard {index} references {len(dangling)} private names nothing defines, e.g. {sorted(dangling)[:5]}")
        for index, names_ in enumerate(final_signatures):
            public = {n for n in names_ if not n.endswith(SUFFIX)}
            if public:
                sys.exit(f"shard {index} has COMDAT groups under public names, e.g. {sorted(public)[:5]}")

        objects = [out for out, _ in finished]
        archive = write_archive(objects, [table[0] for table in final_tables], os.path.join(work, "libchdb.a"))
        os.replace(os.path.join(work, "libchdb.a"), args.output)

        parts = []
        for index, (obj, (member, offset, size)) in enumerate(zip(objects, archive)):
            parts.append({
                "index": index,
                "member": member,
                "size": size,
                "sha256": sha256(obj),
                "input_members": len(shards[index]),
                "input_size": sum(sizes[m] for m in shards[index]),
                "first_input_member": shards[index][0],
                "last_input_member": shards[index][-1],
                "linker": linked[index][1],
                "hidden_private_definitions": finished[index][1],
            })
        machine = "aarch64" if is_aarch64 else "x86_64"
        manifest = {
            "format": MANIFEST_FORMAT,
            "artifact": "libchdb split static library",
            "platform": f"linux-{machine}",
            "version": args.version,
            "source_commit": git_commit(),
            "shard_limit_mib": args.shard_mb,
            # The name it ships under in the release tarball.
            "archive": "libchdb.a",
            "archive_size": os.path.getsize(args.output),
            "archive_sha256": sha256(args.output),
            "link_order": [p["member"] for p in parts],
            "link": {
                "notes": [
                    "Link every member, in link_order. With -lchdb a linker pulls all of them: each "
                    "member references an anchor symbol of the next.",
                    "Every symbol besides the C API is local or a hidden global with a .chdb suffix; "
                    "do not rename, localize or strip symbols of the members.",
                    "Do not pass --allow-multiple-definition: no member defines a name the system or "
                    "a host copy of zstd, zlib, OpenSSL or the C++ runtime defines.",
                ],
                "system_libraries": ["-lpthread", "-ldl", "-lm", "-lrt"],
            },
            "c_api_symbols": len(keep),
            "shared_private_symbols": len(shared),
            "optional_hooks": sorted(hooks),
            "parts": parts,
        }
        manifest_path = args.manifest or os.path.splitext(args.output)[0] + ".json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
            f.write("\n")
        for p in parts:
            log(f"  {p['member']}: {p['size'] / MB:.1f} MiB from {p['input_members']} members "
                f"({p['input_size'] / MB:.1f} MiB), {p['linker']}")
        log(f"Split: {args.output}, {os.path.getsize(args.output) / MB:.0f} MiB in {count} members; "
            f"manifest {manifest_path}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
