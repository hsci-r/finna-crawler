
# Finna-crawler

Crawler meant for downloading records and their metadata from [OAI-PMH API](https://www.kiwi.fi/display/Finna/OAI-PMH+Harvesting+Interface+for+Finna%27s+Index) of the Finnish cultural heritage aggregator [Finna](https://finna.fi/?lng=en-gb).

## Installation

Install the script (and Python module) using `pip install finna-crawler`. After this, the script should be usable from the command line, and the functionality importable from Python. Or, if you have [pipx](https://pypa.github.io/pipx/) and just want the command line script, use `pipx install finna-crawler` instead.

## Usage

```
Usage: finna-crawler [OPTIONS]

  Download metadata and records from Finna from the desired metadata prefix

Options:
  -p, --metadata-prefix TEXT      metadata prefix to query
  -s, --set TEXT                  set to query
  -sf, --status-file TEXT         status file for recovering an aborted crawl
                                  [required]
  -sx, --strip-xml / -nsx, --no-strip-xml
                                  whether to strip XML namespaces from XML
                                  output (default is to strip)
  -fr, --full-record / -nfr, --no-full-record
                                  whether to output the record in full or only
                                  the main content of it without the OAI/PMH
                                  metadata (default is to output only the main
                                  content)
  -mo, --metadata-output TEXT     output TSV (gz/bz2/xz/zst) file in which to
                                  write metadata
  -ro, --record-output TEXT       output (gz/bz2/xz/zst) file in which to
                                  write records
  --help                          Show this message and exit.
```

For information on what the different available metadata sets and versions mean and contain, please consult the [Finna OAI-PMH API documentation](https://www.kiwi.fi/display/Finna/OAI-PMH+Harvesting+Interface+for+Finna%27s+Index).