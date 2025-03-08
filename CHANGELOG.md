# walqueue changelog

- [FEATURE] Add support for 'zstd' compression option in filequeue, in addition to the default 'snappy' compression. (@mattdurham)
- [ENHANCEMENT] Instead of file queue pushing data, the network writers request it. (@mattdurham)
- [BUG] Fix issue with memory growth with slicing vs copying file bytes. (@mattdurham)
- [FEATURE] Add support for headers in outgoing connections. (@mattdurham)
- [FEATURE] Add support for proxies. (@mattdurham)
