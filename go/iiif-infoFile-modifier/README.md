# iiif-infoFile-modifier

Finds IIIF Image API `info.json` tile-metadata files in an S3 collection,
backs each one up in place, then rewrites it to a corrected format.

## What it does

For a configured bucket and collection prefix, the tool:

1. Lists every `info.json` object at the **default per-item tile layout**:

   ```
   <collection_prefix>/<tiles_dir_name>/<item_identifier>-<index>/info.json
   ```

   Only objects exactly one directory level below `<collection_prefix>/<tiles_dir_name>/`
   are considered. Other `info.json` files required elsewhere by the IIIF
   spec (e.g. presentation manifests) live outside that path, are formatted
   differently, and are never touched.

2. **Backs up** every matched `info.json` first, as a server-side S3 copy
   named `backup_info.json` (configurable) at the same key location. If any
   backup fails, the run aborts before modifying anything.

3. **Rewrites** each `info.json` in place (same bucket, same key) to the
   corrected format — see [Transform](#transform) below.

Run with `-dry-run` (or `dry_run: true` in the config) to see exactly what
would be backed up and rewritten without touching S3.

## Transform

Given an original `info.json` like [`incorrect_info.json`](incorrect_info.json):

```json
{
  "@context": "http://iiif.io/api/image/2/context.json",
  "@id": "https://<cloudfront-distribution-id>.cloudfront.net/federated/glink/tiles/glink002121-1",
  "protocol": "http://iiif.io/api/image",
  "width": 4309,
  "height": 2702,
  "sizes": [ ... ],
  "profile": [
    "http://iiif.io/api/image/2/level0.json",
    { "supports": ["cors", "sizeByWhListed", "baseUriRedirect"] }
  ],
  "tiles": [ { "width": 512, "scaleFactors": [1, 2, 4, 8] } ]
}
```

the tool writes back [`corrected_info.json`](corrected_info.json):

```json
{
  "@context": "http://iiif.io/api/image/2/context.json",
  "@id": "https://<cloudfront-distribution-id>.cloudfront.net/federated/glink/tiles/glink002121-1",
  "profile": [
    "http://iiif.io/api/image/2/level0.json",
    {
      "formats": ["jpg"],
      "qualities": ["default"],
      "supports": ["cors", "baseUriRedirect"]
    }
  ],
  "protocol": "http://iiif.io/api/image",
  "tiles": [ { "scaleFactors": [1, 2, 4, 8], "width": 512 } ],
  "width": 4309,
  "height": 2702
}
```

Specifically (see `transformInfoJSON` in [`main.go`](main.go)):

| Field | Behavior |
|---|---|
| `@id`, `width`, `height` | Preserved from the input. |
| `tiles[]` | Preserved from the input, but each entry's fields are reordered to `scaleFactors` then `width`. |
| `sizes` | Dropped entirely. |
| `@context` | Hardcoded to `http://iiif.io/api/image/2/context.json`. |
| `protocol` | Hardcoded to `http://iiif.io/api/image`. |
| `profile` | Hardcoded to level0 + `{"formats":["jpg"],"qualities":["default"],"supports":["cors","baseUriRedirect"]}`, regardless of what the input had. |

`@context`, `protocol`, and `profile` are hardcoded (not derived from the
input) because every file this tool processes comes from the same tiler,
which always produces this exact shape. Re-running the transform on an
already-corrected file is a no-op (see `TestTransformInfoJSON_Idempotent` in
[`main_test.go`](main_test.go)) — it's safe to run more than once.

If you need to change the target values (e.g. a different tiler that emits
PNG, or additional `supports` features), edit the `requiredContext`,
`requiredProtocol`, and `requiredProfile` values at the top of `main.go`.

## Configuration

Config is a YAML file, passed with `-config` (defaults to `config.yaml` in
the working directory). See [`config.yaml`](config.yaml) for a full example
with comments.

| Key | Required | Default | Description |
|---|---|---|---|
| `region` | yes | — | AWS region the bucket lives in. |
| `bucket` | yes | — | S3 bucket containing the info.json files. |
| `collection_prefix` | yes | — | Key prefix for the collection (trailing slash optional; added automatically). |
| `tiles_dir_name` | no | `tiles` | Directory directly under `collection_prefix` holding one subdirectory per item's tiles. |
| `info_file_name` | no | `info.json` | Filename to look for inside each item's tile directory. |
| `backup_file_name` | no | `backup_info.json` | Filename for the pre-modification backup, written alongside each `info.json`. |
| `dry_run` | no | `false` | If true, scan and log planned actions but write nothing to S3. |

## Usage

```sh
go build -o iiif-infoFile-modifier .

# Dry run first — always do this before a live run:
./iiif-infoFile-modifier -config config.yaml -dry-run

# Live run:
./iiif-infoFile-modifier -config config.yaml
```

Flags:

- `-config <path>` — path to the YAML config file (default `config.yaml`).
- `-dry-run` — force dry-run mode even if `dry_run: false` in the config.
  (`dry_run: true` in the config cannot be overridden back to live via
  flags — edit the file instead.)

The process logs a summary line at the end:

```
done: found=42 backed_up=42 modified=42 failed=0
```

and exits non-zero if any object failed to back up, download, transform, or
upload.

## AWS credentials & permissions

The tool uses the AWS SDK's default credential chain (environment
variables, shared config/credentials file, SSO, instance/task role, etc.) —
there is no credentials configuration in `config.yaml`. Whatever identity
runs the tool needs, at minimum, on the target bucket/prefix:

- `s3:ListBucket`
- `s3:GetObject`
- `s3:PutObject`
- `s3:CopyObject` (used for the backup step)

## Development

```sh
go build ./...
go vet ./...
gofmt -l .        # should print nothing
go test ./...     # runs the transform against incorrect_info.json / corrected_info.json
```

`main_test.go` verifies the transform against the two fixture files in this
directory and checks idempotency; there's no S3 interaction in the test
suite, so no AWS credentials are needed to run it.
