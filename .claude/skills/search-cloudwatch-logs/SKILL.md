---
name: search-cloudwatch-logs
description: Search AWS CloudWatch Lambda log groups by filter pattern across one or more regions. Use when the user wants to find log events matching a pattern in specific Lambda log groups (e.g. "search NX logs for ERROR in LoginFunction", "find recent timeout messages across all regions"). Supports time windows, ascending/descending order, and "find first match then stop" mode for locating the most recent occurrence.
---

# search-cloudwatch-logs

Run `CloudWatch/SearchCloudWatchLogs.py` to search Lambda log groups by CloudWatch filter pattern.

## When this skill applies

The user wants to search CloudWatch Lambda logs by content. Typical phrasings:
- "搜一下 NX 的 LoginFunction 最近有没有 [ERROR]"
- "查 PartyAnimals StoreFunction 在 4 月 18-22 号 UTC 有没有 'bad publisher key'"
- "find the most recent timeout in /aws/lambda/Foo across all regions"

Do NOT use this skill for:
- Downloading whole log groups (use `LogGroupDownloader.py` directly)
- Finding Lambda timeout request IDs specifically (that's `SearchCloudWatchTimeoutRequest.py`)
- Finding latest ID-generation logs per ID type (that's `SearchCloudWatchIdGenLogs.py`)

## How to invoke

The script is a CLI. Run it from the project root with the project venv:

```bash
.venv_win/Scripts/python.exe CloudWatch/SearchCloudWatchLogs.py \
  --log-groups <name> [<name> ...] \
  --pattern '<filter-pattern>' \
  --regions <NX|JP|EU|US|...> [<region> ...] \
  [--start "YYYY-MM-DD HH:MM:SS+0800"] \
  [--end   "YYYY-MM-DD HH:MM:SS+0800"] \
  [--ascending | --descending] \
  [--find-first] \
  [--segment-duration <minutes>] \
  [--sequential]
```

Run `--help` first if any flag is unclear.

## Argument rules

- **`--log-groups` / `-lg`** (required, 1+ values). Lambda log group names. The `/aws/lambda/` prefix is optional — both `LoginFunction` and `/aws/lambda/PartyAnimals--209820-LoginFunction` work, but the **environment prefix is part of the name** (e.g. `PartyAnimals--209820-LoginFunction`, `NemoDev-trunk--47607-LoginFunction`). Never invent a log group name; ask the user if unsure which environment/build number to use.
- **`--pattern` / `-p`** (required). CloudWatch Filter Pattern syntax — NOT regex. Use `%...%` for substring match, e.g. `'%[ERROR]%'`, `'%bad publisher key%'`. Always single-quote the pattern in shell.
- **`--regions` / `-rgn`** (required, 1+ values). Accepts both abbreviations and full names:
  - `BJ` = `cn-north-1`
  - `NX` = `cn-northwest-1`
  - `AP` / `JP` = `ap-northeast-1`
  - `EU` = `eu-central-1`
  - `US` = `us-east-1`
- **`--start` / `-s`** and **`--end` / `-e`**. Format MUST include a timezone offset, e.g. `"2026-04-18 08:00:00+0800"` or `"2026-04-18 00:00:00+0000"`. Both are accepted — internally compared as tz-aware datetimes. A naive datetime (no offset) is rejected. Both are optional but strongly recommended — without them the search hits the entire log group and is very slow. The script also rejects `start >= end`.
- **Sort**: default is `--descending` (newest first). Pass `--ascending` only when the user explicitly wants chronological order from a known start time.
- **`--find-first` / `-f1`**. When set, each `(log_group, region)` worker stops after the first matching batch. Combine with descending (the default) to find "the most recent occurrence" — this is the common case for "is there any X in the last few days?". Without `--find-first`, the script pulls ALL matching events in the time window, which can be expensive.
- **`--segment-duration` / `-seg`**. In minutes. Only takes effect with descending + find-first: the time range is sliced into segments from latest to earliest, each fetched separately. Default 60. Use a longer segment (e.g. 1440 = 24h) when matches are rare; shorter when matches are dense.
- **`--sequential`**. Skip multiprocessing (one log_group × region at a time). Default is parallel — usually keep parallel; only switch to sequential when debugging a specific worker.

## Step-by-step

1. **Confirm what's missing.** The required pieces are: log group name(s), pattern, region(s). If any are missing or ambiguous (especially the env-prefixed log group name), ask before running.
2. **Confirm the time window.** Time range is technically optional but practically required. If the user says "recently" or "the past few days", propose a concrete UTC window and confirm.
3. **Decide the search mode.**
   - "Is there any X?" / "When was the last X?" → **descending + `--find-first`** (default and recommended).
   - "List all X in this window." → **descending without `--find-first`**, or `--ascending` if the user wants chronological order.
4. **Run the command.** Quote the pattern with single quotes. Echo the actual command back to the user so they can re-run it themselves.
5. **Read the output.** Each match prints as `<utc_ts>\t<message>\t<console_url>`. The console URL is a direct deep-link into the CloudWatch console for that event. Summarize hits/per region in the final reply, but keep the raw output so the user can click through.

## Common pitfalls

- **Filter pattern is NOT regex.** `[ERROR]` in CloudWatch literally means "tokens ERROR appear in this group of words"; you usually want `'%[ERROR]%'` for substring match. If the user gives a regex, translate it to filter-pattern semantics or warn them.
- **Time without timezone is rejected.** The script parses with `%Y-%m-%d %H:%M:%S%z`; `"2026-04-18 00:00:00"` (no tz) will throw. Always include the offset (`+0800` for the Feishu-alert local tz, `+0000` for UTC).
- **Log group naming.** `LoginFunction` alone is ambiguous — the same Lambda exists in many envs. Always include the env+build prefix.
- **Cross-region permissions.** Some envs/regions need a specific AWS profile. The script uses `utils.aws_client_helper.get_aws_profile` based on the env parsed from the log group name. If you see `ExpiredToken` or `AccessDenied`, tell the user to refresh credentials (likely via the MFA tool in this repo) before retrying.

## Output

The script prints to stdout. There is no file output — pipe to `tee` or redirect if the user wants to save it.
