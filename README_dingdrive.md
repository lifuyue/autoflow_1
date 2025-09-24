# DingTalk Drive Integration Module

## Overview
The `autoflow.services.dingdrive` package adds a production-ready connector to DingTalk Drive (DingPan). It exposes a unified provider interface, a high-level `DingDriveClient`, and Typer-driven CLI commands that cover CRUD, upload/download, and preview flows.

## Setup
1. **Dependencies**: The module relies on the standard library and `requests`. Optional retries use the built-in lightweight policy—no extra installs are required.
2. **Configuration**:
   - Copy or extend `autoflow/config/profiles.yaml` with a `dingdrive` section.
   - Reference secrets via environment variables; do not hardcode credentials.
   - Example:
     ```yaml
     dingdrive:
       default:
         app_key: "${DING_APP_KEY}"
         app_secret: "${DING_APP_SECRET}"
         space_id: "YOUR_SPACE_ID"
         timeout_sec: 10
         retries:
           max_attempts: 3
           backoff_ms: 200
           max_backoff_ms: 1500
         verify_tls: true
         trust_env: false
     ```
3. **Environment**: Export `DING_APP_KEY` and `DING_APP_SECRET`, then run CLI commands from the repo root.

## Python Usage
```python
from autoflow.services.dingdrive import DingDriveClient

client = DingDriveClient.from_profile("default")
folder_id = client.create_folder("root", "Reports")
file_id = client.upload_file(folder_id, "out/latest.xlsx")
client.rename(file_id, "Reports_2025-01.xlsx")
client.download_file(file_id, "downloads/")
client.delete(file_id)
client.close()
```

## CLI Usage
```
python -m autoflow.cli dingdrive ls --parent root
python -m autoflow.cli dingdrive mkdir --parent root --name "2025发票"
python -m autoflow.cli dingdrive upload --parent <folder_id> --file out/invoice.xlsx
python -m autoflow.cli dingdrive download --id <file_id> --out downloads/
python -m autoflow.cli dingdrive rename --id <file_id> --name renamed.xlsx
python -m autoflow.cli dingdrive mv --id <file_id> --parent <folder_id>
python -m autoflow.cli dingdrive rm --id <item_id>
python -m autoflow.cli dingdrive preview --id <file_id>
```
All commands respect the `--profile` option (default `dingdrive`). Failures exit with non-zero status codes and emit ISO-8601 logs to `autoflow/work/logs/app.log`.

## Permissions & Security Notes
- Ensure the DingTalk mini-app has Drive read/write scopes and the target space is shared with the app.
- Tokens are cached in-memory only; long-running processes refresh automatically on 401 responses.
- TLS verification stays enabled by default; proxies are disabled unless explicitly configured.
- Large uploads follow the official `get upload info → upload → confirm` flow with bounded retries.

## Packaging Notes
- The module stays PyInstaller-friendly (no dynamic imports). The CLI integrates into the existing Typer entry point.
- When generating `auto_flow.exe`, include `autoflow/config/profiles.yaml` and ensure environment variables are injected at runtime.
- The shared requests session respects `verify_tls` and `trust_env`; bundle corporate root certificates when packaging if DingTalk endpoints require them.

For troubleshooting, enable verbose logs with `--log-level DEBUG` on the CLI and inspect `autoflow/work/logs/app.log` for HTTP traces.
