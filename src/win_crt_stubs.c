/* tree-sitter parser.c calls _fdopen as a UCRT dllimport (__imp__fdopen).
   Recent UCRT headers inline it; the object still wants the IAT slot.
   legacy_stdio_definitions.lib provides a plain _fdopen. */
#ifdef _WIN32
typedef struct _iobuf FILE;
FILE *__cdecl _fdopen(int fd, char const *mode);
FILE *(__cdecl *__imp__fdopen)(int fd, char const *mode) = _fdopen;
#endif
