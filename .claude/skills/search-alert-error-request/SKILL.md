---
name: search-alert-error-request
description: Investigate a Lambda error alert from Feishu (PA 生产网关告警 / Lambda Log 告警). Parses the alert text to extract function/region/timestamp, then pulls (1) all [ERROR] events around the alert time and (2) the FULL request logs for every request id that errored — giving you the error context, not just the error line. Use when the user pastes a Feishu alert and asks "what happened" / "拉一下这个告警的完整日志" / "帮我看这条 Lambda 告警".
---

# search-alert-error-request

Run `CloudWatch/LambdaRequestLog/SearchAlertErrorRequest.py` to investigate a Feishu Lambda error alert and produce two CSV files: `*_ERROR.csv` (error lines only) and `*_FULL.csv` (every log line of every errored request, ordered by request then time).

## When this skill applies

The user pastes a Feishu Lambda alert (or refers to one) and wants to investigate what happened. Typical phrasings:
- "帮我看一下这条飞书告警"
- "拉一下这个告警的完整请求日志"
- "Lambda 出错了，看看是什么原因"
- "把告警相关的请求日志导出来"

The alert text looks like this (the script needs at least 区域/函数/告警时间):

```
Lambda Log 告警
区域: cn-northwest-1
函数: PartyAnimals--209820-LoginFunction
告警内容: ...
告警时间: 2026-04-18T12:34:56.789+0800
错误数量：5
首行错误：...
查看详情
```

Do NOT use this skill for:
- General log pattern search without an alert (use `search-cloudwatch-logs`)
- Lambda timeout request investigation (use the timeout-specific skill when it exists)

## How to invoke

The script is a CLI. Run from project root with the project venv:

```bash
.venv_win/Scripts/python.exe CloudWatch/LambdaRequestLog/SearchAlertErrorRequest.py \
  --alert-file <path-to-alert.txt> \
  [--window-before <minutes>] [--window-after <minutes>] \
  [--start "YYYY-MM-DD HH:MM:SS+0800"] [--end "YYYY-MM-DD HH:MM:SS+0800"] \
  [--output-dir <path>] \
  --print-result-json
```

Always pass `--print-result-json` so the last stdout line is parseable JSON:

```json
{"error_csv": "...", "error_cnt": 12, "full_csv": "...", "full_cnt": 88}
```

Run `--help` first if any flag is unclear.

## Step-by-step

1. **Get the alert text.** If the user pastes it inline, write it verbatim to a temp file (e.g. `CloudWatch/LambdaRequestLog/_alert_<short-id>.txt`). Don't use `--alert-stdin` from inside Claude Code — it's harder to debug. Confirm the file contains `区域:`, `函数:`, `告警时间:` lines before running; if any are missing, ask the user.
2. **Decide the time window.**
   - Default (no flags) = `[告警时间 - 5min, 告警时间]`. Good for: "what happened right before the alert fired".
   - `--window-before 30 --window-after 10` widens to `[-30min, +10min]`. Good for: errors that started earlier and rippled, or whose recovery you also want to see.
   - `--start ... --end ...` overrides the window-anchored defaults. Use when the user gives an explicit window. **Mutually exclusive with `--window-before` / `--window-after`** at the argparse level — don't pass both.
   - If the user just says "around the alert" without specifying, **default is fine** — don't widen unless asked.
3. **Run the command** with `--print-result-json`. Echo the actual command to the user before running.
4. **Parse the JSON line** at the end of stdout. Extract `error_csv`, `error_cnt`, `full_csv`, `full_cnt`.
5. **Read the CSVs to summarize** (this is the actual value-add):
   - Read `error_csv` first. Each row is `DateTime, Msg, Url`. The `Msg` column starts with the request id (first space-separated token).
   - Group errors by error type / first line of stack trace. Tell the user: "X distinct error types across N requests", with one example URL per type.
   - If `full_cnt > 0`, mention `full_csv` so the user can dig deeper. Don't dump the whole CSV into the response — link to the file.
6. **Hand the user actionable output**: the error breakdown + CSV paths. Not the raw rows.

## Argument rules

- **`--alert-file` / `-f`** vs `--alert-stdin`: required, mutually exclusive. Always prefer `--alert-file` when invoking from Claude Code.
- **`--window-before`** (minutes, default 5) and **`--window-after`** (minutes, default 0) are anchored to the alert time. Both are ints. Mutually exclusive with `--start` / `--end` respectively.
- **`--start` / `-s`** and **`--end` / `-e`**: explicit absolute time, MUST include a timezone offset (e.g. `"2026-04-18 12:00:00+0800"` or `"+0000"`). Naive datetime is rejected. The script also rejects `start >= end`.
- **`--output-dir` / `-o`**: defaults to the script directory (or a per-PC override baked into the script). Override when the user wants the CSVs in a specific place. The directory is auto-created.
- **`--print-result-json`**: always pass when invoking from a skill. The JSON line is the contract — the rest of stdout is human-readable progress.

## Common pitfalls

- **告警时间格式**: the parser expects `YYYY-MM-DDTHH:MM:SS.sss+ZZZZ` (e.g. `2026-04-18T12:34:56.789+0800`). If the alert text uses a different format, the script will throw `ValueError: time data '...' does not match format`. Tell the user and ask them to double-check the pasted content.
- **Function name must include the env prefix**: e.g. `PartyAnimals--209820-LoginFunction`, not just `LoginFunction`. The env prefix (before the `--`) is parsed via `AllEnvs.get_env_by_name`; a missing or wrong prefix throws `Unknown environment=...`.
- **Cross-region credentials**: the script uses the AWS profile derived from the env. If you see `ExpiredToken` / `AccessDenied`, tell the user to refresh credentials (likely via the MFA tool in this repo) before retrying.
- **`error_cnt == 0`**: the search window may have missed the actual errors. Suggest widening with `--window-before 30` (or larger).
- **`full_cnt == 0` but `error_cnt > 0`**: either the request-id extraction failed (first whitespace-separated token of the message) or AWS returned no events for the id-filtered second pass. Show the `error_csv` path; the user may need to inspect manually.

## Output

- Two CSV files in the output directory, named `<func>_<region>_<start>_<end>_ERROR.csv` and `..._FULL.csv`.
- Each CSV: `DateTime` (ISO 8601 UTC), `Msg`, `Url` (deep link to the CloudWatch console for that event).
- Final stdout line (with `--print-result-json`): one-line JSON with both csv paths and counts.
