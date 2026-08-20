# x3d-batch-rotate

Batch-processes a directory of X3D models: for each `.x3d` file, writes a
rotated copy and — unless rendering is disabled — renders PNG snapshots of
the model in its original and rotated orientations, using headless Chrome
to run the same X3DOM viewer the DLP archive site uses. Optionally, it can
first populate that directory itself by downloading models for a list of
archive identifiers looked up in DynamoDB.

This automates, for a whole batch at once, the manual workflow of opening
a model in a browser, dragging to rotate it, and screenshotting the
result.

## What it does

**Fetch phase (optional).** If `identifiers_file` is set, see
[Fetching models by identifier](#fetching-models-by-identifier) below —
this populates `input_dir` before the rest of the tool runs.

**Rotate/render phase.** For each `<name>.x3d` file directly inside
`input_dir` (subdirectories are not scanned, and files already carrying
the `rotated_` prefix are skipped so re-running the tool over its own
output is safe):

1. **Rotates.** Wraps everything between `<Scene>` and `</Scene>` in one
   new `<Transform rotation="<axis> <angle>">` node and writes the result
   to `output_dir/rotated_<name>.x3d`. This is structure-agnostic — it
   doesn't parse or care how many `Shape`/`Group` nodes the model has, or
   how large its geometry arrays are, so it works the same way on a
   hand-written test model and a 7MB photogrammetry scan.

2. **Renders**, unless `render: false`:
   - `output_dir/<name-without-extension>_initial.PNG` — the original
     model as an X3D viewer would show it on first load.
   - `output_dir/<name-without-extension>_rotated.PNG` — the rotated
     model, same way.

   Any relative `<ImageTexture url="...">` reference a model needs is
   copied into `output_dir` alongside it if it isn't already there, so
   `output_dir` can safely differ from `input_dir`.

Run with `-dry-run` to see exactly what would be written/rendered without
touching disk or starting a browser.

## Choosing a rotation

`degrees` and `axis` describe a single-axis X3D rotation applied to the
whole model. The default (`axis: "1 0 0"`, `degrees: -75`) reproduces
dragging **up** on the model in an examine-mode X3D viewer by 75°, tipping
the far/dorsal side toward the camera — this is what was used to go from
an edge-on insect specimen view to a top-down view of the wings. Flip the
sign of `degrees` for the opposite direction; change `axis` for a
different rotation axis (e.g. `"0 1 0"` to spin around the vertical axis
instead).

## Fetching models by identifier

Instead of (or in addition to — a partial fetch failure just leaves fewer
`.x3d` files for the rotate/render phase to find) manually placing `.x3d`
files in `input_dir`, point `identifiers_file` at a JSON file containing a
flat array of identifier strings:

```json
["368a8114", "abcd1234"]
```

For each identifier, the tool:

1. Looks the item up in DynamoDB via the **AWS SDK for Go v2**
   (`dynamodb.Client.GetItem` or `.Scan`, per `lookup_mode` — see the
   table below; `scan` paginates through the whole table via
   `LastEvaluatedKey` until it finds a match or runs out of pages). This
   uses the SDK's default credential chain — environment variables,
   `~/.aws/credentials`, SSO, an instance/task role, etc. — whatever `aws
   sts get-caller-identity` would need to succeed with the AWS CLI, but
   the CLI itself is not required; there is no credential configuration
   in `config.yaml`.
2. Reads the item's `archiveOptions` attribute — a DynamoDB String whose
   *value* is itself JSON text (this is how an AppSync AWSJSON field is
   stored) — and parses out `assets.x3d_config`, the model's download URL.
3. Downloads that URL into `input_dir`.
4. Parses the downloaded model's own `<ImageTexture url="...">`
   reference and downloads that too (resolved relative to the model's
   URL), so the texture filename is never guessed or hardcoded — it's
   whatever the model actually references.

A failure for one identifier (not found, malformed `archiveOptions`,
download error, ...) is logged and does **not** stop the batch; the
rotate/render phase afterward just runs over whatever `.x3d` files
actually made it into `input_dir`. An identifier whose model was already
downloaded in a previous run is left alone (existing files are never
overwritten), so re-running the tool with the same `identifiers_file` is
cheap and safe.

Override `identifiers_file` per-invocation with `-identifiers <path>`
without editing the config file.

### AWS credentials & permissions

The tool uses the AWS SDK's default credential chain (environment
variables, shared config/credentials file, SSO, instance/task role, etc.)
— there is no credentials configuration in `config.yaml`. Whatever
identity runs the tool needs, at minimum, on `table_name`:

- `dynamodb:GetItem`, if `lookup_mode: get_item`
- `dynamodb:Scan`, if `lookup_mode: scan`

## Configuration

Config is a YAML file, passed with `-config` (defaults to `config.yaml` in
the working directory, which is not tracked in git). See
[`config_example.yaml`](config_example.yaml) for a full example with
comments — copy it to `config.yaml` (or any path) and edit.

| Key | Required | Default | Description |
|---|---|---|---|
| `input_dir` | yes | — | Directory containing the `.x3d` files to process (and, if `identifiers_file` is set, where fetched models are downloaded to). |
| `output_dir` | no | `input_dir` | Where rotated `.x3d` files and rendered PNGs go. |
| `identifiers_file` | no | — | Path to a JSON array of identifier strings; enables the fetch phase. |
| `region` | if `identifiers_file` set | — | AWS region for the DynamoDB client. |
| `table_name` | if `identifiers_file` set | — | DynamoDB table name. |
| `lookup_mode` | no | `get_item` | `get_item` (identifier is the table's partition key) or `scan` (identifier is some other attribute's value; reads the whole table per lookup). |
| `partition_key_attr` | no | `id` | DynamoDB attribute used as the `get_item` key. |
| `identifier_attr` | no | `identifier` | DynamoDB attribute matched against in `scan` mode. |
| `identifier_prefix` | no | — | Prepended to each identifier before lookup (e.g. `"ark:/53696/"`). |
| `archive_options_field` | no | `archiveOptions` | DynamoDB attribute holding the model config as JSON text. |
| `degrees` | no | `0` | Rotation angle in degrees. Sign controls direction. |
| `axis` | no | `1 0 0` | X3D `SFVec3f` rotation axis. |
| `rotated_prefix` | no | `rotated_` | Filename prefix for the rotated `.x3d` output. |
| `render` | no | `false` | Whether to render PNGs at all. |
| `skip_initial_render` | no | `false` | If true, only render the `_rotated` PNG, not `_initial`. |
| `image_size` | no | `1000` | Width/height in pixels of the rendered square PNG. |
| `render_wait_seconds` | no | `4` | Extra wait after the model's geometry loads, for its texture to decode. |
| `render_timeout_seconds` | no | `60` | Max seconds to wait for a single model to load/render before failing it. |
| `x3dom_js_url` | no | VT CDN build | X3DOM runtime script URL. |
| `x3dom_css_url` | no | VT CDN build | X3DOM runtime stylesheet URL. |
| `initial_suffix` | no | `_initial.PNG` | Filename suffix for the initial-view render. |
| `rotated_suffix` | no | `_rotated.PNG` | Filename suffix for the rotated-view render. |
| `dry_run` | no | `false` | If true, log planned actions but write/render nothing. |

## Usage

```sh
go build -o x3d-batch-rotate .
cp config_example.yaml config.yaml   # then edit config.yaml

# Dry run first — always do this before a live run:
./x3d-batch-rotate -config config.yaml -dry-run

# Live run:
./x3d-batch-rotate -config config.yaml
```

Flags (all optional, override the config file):

- `-config <path>` — path to the YAML config file (default `config.yaml`).
- `-dry-run` — force dry-run mode even if `dry_run: false` in the config.
- `-no-render` — skip rendering; only write rotated `.x3d` files.
- `-input <dir>` — override `input_dir`.
- `-output <dir>` — override `output_dir`.
- `-degrees <n>` — override `degrees` (a literal `0` is treated as "use
  the config value", since 0° is a no-op rotation anyway).
- `-identifiers <path>` — override `identifiers_file`; enables the fetch
  phase even if unset in the config.

The process logs a summary line at the end:

```
done: fetched=2 fetch_failed=0 found=12 rotated=12 rendered_initial=12 rendered_rotated=12 failed=0 elapsed=2m14s
```

(`fetched`/`fetch_failed` are always `0` when `identifiers_file` isn't
set.) It exits non-zero if any identifier failed to fetch or any model
failed to rotate and/or render (each failure is also logged individually
as it happens).

## How rendering works

Rendering needs a real browser because X3DOM parses and draws the model
with WebGL; there is no pure-Go X3D renderer. This tool:

1. Starts a local HTTP file server rooted at `output_dir` (after staging
   any files the model needs there — see above), so relative texture
   references resolve exactly as a real hosted model would.
2. Starts headless Chrome via [chromedp](https://github.com/chromedp/chromedp),
   with software WebGL (`--use-gl=swiftshader`) enabled — headless Chrome
   has no GPU, and X3DOM needs *some* WebGL implementation to draw at all.
3. For each render, opens a **fresh browser tab**, navigates it to a small
   generated HTML page embedding an `<x3d>` viewer pointed at the model,
   polls the X3DOM runtime until the model's geometry is actually linked
   into the renderable scene graph (not just "loaded"), waits a few more
   seconds for the texture to decode, and screenshots the full viewport
   (sized to exactly match the `<x3d>` element, so this is pixel-identical
   to cropping the canvas).

A fresh tab per render is deliberate, not incidental: reusing one tab
context across sequential renders — even wrapping each `chromedp.Run` call
in its own `context.WithTimeout` derived from a shared tab context, which
looks like the obvious pattern — was observed to make every render after
the first fail instantly with `context canceled`. Opening a new tab from
the shared browser allocator for each render avoids that entirely and
also isolates X3DOM's global `window` state between models.

Similarly, a node-targeted `chromedp.Screenshot("canvas", ...)` was
observed to hang indefinitely against X3DOM's WebGL canvas even though
navigation and readiness-polling both completed in about a second; a
full-viewport `chromedp.CaptureScreenshot` does not have this problem and,
given the page's layout, produces the same pixels.

If a model's HTML used self-closing custom tags (`<viewpoint ... />`)
instead of explicit closing tags, HTML parsing would **not** treat them as
void elements — the element would silently absorb every following sibling
as its own child, corrupting the scene graph while `load="true"` still
reports success and no error is thrown. The render template avoids this by
always closing X3DOM tags explicitly.

## Development

```sh
go build ./...
go vet ./...
gofmt -l .        # should print nothing
go test ./...     # runs the rotation/text-processing logic against testdata/sample.x3d
```

`main_test.go` covers `wrapSceneInTransform`, `extractTextureURLs`,
`isRelativeAssetURL`, `findModels`, and `copyFile` against small fixtures.
`fetch_test.go` covers the DynamoDB-item and `archiveOptions` JSON parsing,
URL resolution, and download logic in `fetch.go`; `ddbAPI` (the subset of
`*dynamodb.Client` this tool calls — `GetItem` and `Scan`) is a small
interface specifically so tests can substitute a fake implementation, and
downloads are tested against a real `httptest.Server` rather than mocked.
None of this needs network access or AWS credentials to run. The rendering
path (`render.go`) is exercised by manual end-to-end runs rather than the
test suite, since it requires a real Chrome install and downloads the
X3DOM runtime from the VT CDN.
