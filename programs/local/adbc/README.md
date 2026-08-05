# Vendored ADBC header

> **Note.** The chDB ADBC driver (implemented in `../chdb-adbc.cpp`) is an
> experimental/preview feature; its behavior and packaging may change before
> it is declared stable.

`adbc.h` is vendored verbatim from Apache Arrow ADBC:

- Source: https://github.com/apache/arrow-adbc/blob/apache-arrow-adbc-23/c/include/arrow-adbc/adbc.h
- Tag: `apache-arrow-adbc-23`
- License: Apache-2.0 (header retained in the file)

It is the single-file, dependency-free C definition of the ADBC API
(struct AdbcDriver, status codes, option constants) implemented by
`../chdb-adbc.cpp`. To upgrade, replace the file with a newer tag's copy
and update this note.
