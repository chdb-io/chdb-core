#pragma once

#include <string>
#include <string_view>

namespace CHDB
{

/// Text that is about to become a Python str: the engine's error messages and the what() of C++
/// exceptions. Python decodes it as strict UTF-8, so a message that is not valid UTF-8 would turn
/// the error being reported into a UnicodeDecodeError. ClickHouse's own messages are UTF-8, but on
/// Windows text from the operating system - FormatMessageA, which std::system_category() uses for
/// every std::filesystem_error - is in the ANSI code page (GBK on a Chinese system, for one).
/// Valid UTF-8 is returned unchanged. Otherwise, on Windows, the text is converted from the ANSI code
/// page; elsewhere, or if that fails, each invalid byte becomes U+FFFD.
std::string messageForPython(std::string_view message);

}
