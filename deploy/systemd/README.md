# systemd examples

Copy units to `/etc/systemd/system/` and create `/etc/datafilter/datafilter.env` from `.env.example`.
Run the API with `INPROCESS_WORKERS_ENABLED=false`; start one worker per step type, e.g. `datafilter-worker@fetch_raw`, `datafilter-worker@unzip_package`, `datafilter-worker@analyze_phrase_stats`, `datafilter-worker@summarize_result`.
Keep exactly one scheduler/notification owner, normally the API service. SQLite mode assumes a single host/local volume.
