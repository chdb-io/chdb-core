/* chdb_spec: libarchive's external-program filters, disabled on Windows.
 *
 * A format libarchive has no built-in decoder or encoder for (lz4, lzop, lrzip, grzip, ...) falls
 * back to running the matching command-line tool through __archive_create_child. On Windows that is
 * CreateProcess with a bare program name, which searches the application's directory and the current
 * directory before PATH, so a planted lz4.exe next to the data would run. The Windows port does not
 * run external programs at all (see src/Common/ShellCommand.cpp), so this replaces
 * filter_fork_windows.c with what libarchive itself builds for Windows Store apps: launching a
 * program always fails, and the filter reports "unable to run program". */
#include "archive_platform.h"

#if defined(_WIN32) && !defined(__CYGWIN__)

#include "archive.h"
#include "filter_fork.h"

int
__archive_create_child(const char *cmd, int *child_stdin, int *child_stdout, HANDLE *out_child)
{
	(void)cmd;
	(void)child_stdin;
	(void)child_stdout;
	(void)out_child;
	return ARCHIVE_FAILED;
}

void
__archive_check_child(int in, int out)
{
	(void)in;
	(void)out;
}

#endif
