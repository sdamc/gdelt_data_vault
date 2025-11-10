# Data Directory (Ignored)

This directory holds raw, bronze, and silver layer files downloaded or produced during the pipeline run. They are large, reproducible, and therefore excluded from version control.

Layers:
- raw/    : Original GDELT GKG CSV drops
- bronze/ : Initial landed ingestion set (optional retention)
- silver/ : Database / transformed intermediate storage (Postgres mounted volume)

To add a small sample for tests or documentation, create a `sample/` subfolder and remove the leading `#` from the `!workspace/data/sample/**` exception rule in the root `.gitignore`.

Never place secrets here. Acquisition is documented in pipeline scripts (`download_gkg_files.py`).
