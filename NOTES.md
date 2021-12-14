# NOTES for `develop-viridian-C900000008-325` branch

At present `make_sample_data` looks for e.g. `analysis/report/illumina/ {sp3_sample_name}_report.json` (or the nanopore equivalent).

## Current state
* If both are there it logs a warning and returns `None`.
* If neither are there it then looks for `tsv` files, and if it still can't find any, it logs an error and returns `None`
* Iff there is only does it proceed.

## Implications

* because only analysis output files are aggregated into the `report.json`, this means that no fields from `viridian` are passed ever
* if a sample fails i.e. a consensus genome cannot be created due to e.g. amplicon dropout, then no data is given to APEX

## Design

* change so first the `viridian` log `json` file is *always* parsed if available, with key fields retained (or simply throw the whole JSON at APEX since it is stored in a CLOB).
* iff the consensus genome was successfully built, parse the `report.json` and throw that at APEX

Hence if a sample fails, APEX will still receive the viridian JSON which may contain a lot of null fields (or may be shorter TBD).

For now, assume that "failed" == "not enough good reads to assemble a genome"

-> Do we pass the entire viridian JSON to APEX or just a subset?

For now, assume we just want to keep a subset!




## APEX design considerations

They currently store the entire JSON object in CLOB with only some fields pulled out and put in conventional DB fields with dtypes etc.
