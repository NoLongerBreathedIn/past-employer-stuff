# Copyright statement
While many things here were originally written as works for hire,
I kept none of the source code.
Everything here has been rewritten from memory.

# Guide to contents
They're split out by language.

## cs/mini-web-server
This is a miniature web server I wrote. It doesn't support very much.
Systemd-based, so Linux-only.

## pas/PipeService
This is intended to be a replacement for DelphiPipes/Pipes.pas.
Windows-only.

It has the following improvements:

- It only uses the thread pool, instead of persistent threads.
- A `TPipeService` or `TPipeClient` is no longer thread-bound when stopped.
- `TPipeService` has had its arrays swapped out for dictionaries,
so everything is now O(1) except for broadcast, which is O(n).
- A concept of "subscribed" messages has been added.
- Pipes are no longer created with null DACLs.
- I've removed `FreeObjectInstance` and `MakeObjectInstance`;
they allocate RWX memory.
- I've stripped out the bare-buffers; the currency used is `TBytes`.
This also fixes a bug in `AllocPipeWriteWithPrefix`
where passing N as the prefix count copies the main buffer
after an N-byte gap (if `PChar` is UTF-16),
and therefore writes N bytes out of bounds.
- You get to choose whether "new pipe created" and "pipe broken" notifications
occur on the owning thread (the thread that started the service).
- Instead of using `procedure of object`,
everything uses `TProc`,
allowing you to pass closures.
- A predefined message format is provided,
and the `TPipeService` will automatically route messages to a specified handler.

I stripped out `TPipeConsole`. It really didn't have much to do with the rest.

## ts/i18n
A miniature internationalization framework,
based loosely on Factorio's [localization parameters](https://wiki.factorio.com/Tutorial:Localisation).
Technically not pure TypeScript;
the preprocessor is written in Perl
with dependencies on `Parse::Lex` and `JSON`.

Formatting options are much like Factorio's pluralization,
except that `__plural_for_parameter__n__{` is shortened to `__plural_for_n_{`.
Also, `__ifnull_n_{this|that}__` is defined (null case goes first).

Both input and output of the preprocessor are JSON,
but the output has had all its string values munged
so that the `localize` function can quickly put everything together.

# Not everything there?
This is at least partially plans for what to add.
