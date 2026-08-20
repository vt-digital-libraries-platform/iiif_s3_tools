# x3d-batch-rotate

Batch-renders a square PNG thumbnail of each X3D model in a directory,
rotated by a configured angle, using headless Chrome to run the same
X3DOM viewer the DLP archive site uses. Optionally, it can first populate
that directory itself by downloading models for a list of archive
identifiers looked up in DynamoDB.

This automates, for a whole batch at once, the manual workflow of opening
a model in a browser, dragging to rotate it, and screenshotting the
result — but produces only the one thumbnail per model, not the model
itself or an unrotated preview.

## What it does

**Fetch phase (optional).** If `identifiers_file` is set, see
[Fetching models by identifier](#fetching-models-by-identifier) below —
this downloads models into `input_dir` before the render phase runs, and
each identifier is used to name its own thumbnail.

**Render phase.** For each model, in memory only (nothing described in
this step is ever written to disk):

1. **Rotates.** Wraps everything between `<Scene>` and `</Scene>` in one
   new `<Transform rotation="<axis> <angle>">` node. This is
   structure-agnostic — it doesn't parse or care how many `Shape`/`Group`
   nodes the model has, or how large its geometry arrays are, so it works
   the same way on a hand-written test model and a 7MB photogrammetry
   scan.
2. **Renders** the rotated model with X3DOM and screenshots it.

The **only** file this phase writes is the thumbnail:
`output_dir/<identifier>_thumbnail.png`, a `thumbnail_size` x
`thumbnail_size` (default 250x250) square PNG. No rotated `.x3d` file and
no unrotated ("initial") render are ever written anywhere. If the
rendered canvas isn't already exactly square (it always should be — this
is a safety net), it's first centered on a white square background sized
to its longer side before being scaled down, so the model is never
cropped or stretched; see [`squareThumbnail`](thumbnail.go).

Where `<identifier>` comes from depends on how the model got there: for a
model fetched via `identifiers_file`, it's that identifier's own value
(e.g. `368a8114`); for a model already sitting in `input_dir` without a
fetch phase, it's that file's basename without its `.x3d` extension.

Run with `-dry-run` to see exactly what would be rendered without
touching disk or starting a browser. If `render: false`, the render phase
does nothing at all — useful to run only the fetch phase.

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
models for the render phase to find) manually placing `.x3d` files in
`input_dir`, point `identifiers_file` at a JSON file containing a flat
array of identifier strings:

```json
["368a8114", "abcd1234"]
```

For each identifier, the tool:

1. Looks the item up in DynamoDB via the **AWS SDK for Go v2**
   (`dynamodb.Client.GetItem` or `.Scan`, per `lookup_mode` — see the
   table below; `scan` paginates through the whole table via
   `LastEvaluatedKey` until it finds a match or runs out of pages). This
   uses the SDK's default credential chain — environment variables,
   `~/.aws/credentials`, SSO, an instance/task role, etc.; there is no
   credential configuration in `config.yaml`.
2. Reads the item's `archiveOptions` attribute — a DynamoDB String whose
   *value* is itself JSON text (this is how an AppSync AWSJSON field is
   stored) — and parses out `assets.x3d_config`, the model's download URL.
3. Downloads that URL into `input_dir`.
4. Parses the downloaded model's own `<ImageTexture url="...">`
   reference and downloads that too (resolved relative to the model's
   URL), so the texture filename is never guessed or hardcoded — it's
   whatever the model actually references.

`input_dir` is working/scratch space for this — the model and texture
files stay there, they are not moved or copied to `output_dir`, and only
the identifier (not the downloaded filename) is used to name the
resulting thumbnail.

A failure for one identifier (not found, malformed `archiveOptions`,
download error, ...) is logged and does **not** stop the batch; the
render phase afterward just runs over whatever models were actually
fetched. An identifier whose model was already downloaded in a previous
run is left alone (existing files are never overwritten), so re-running
the tool with the same `identifiers_file` is cheap and safe.

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
| `input_dir` | yes | — | Scratch directory for `.x3d`/texture files (populated by the fetch phase, if used). |
| `output_dir` | no | `input_dir` | Where thumbnail PNGs go — the only thing this tool writes there. |
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
| `render` | no | `false` | Whether to rotate/render thumbnails at all. |
| `thumbnail_size` | no | `250` | Width and height, in pixels, of the square thumbnail PNG. |
| `render_wait_seconds` | no | `4` | Extra wait after the model's geometry loads, for its texture to decode. |
| `render_timeout_seconds` | no | `60` | Max seconds to wait for a single model to load/render before failing it. |
| `x3dom_js_url` | no | VT CDN build | X3DOM runtime script URL. |
| `x3dom_css_url` | no | VT CDN build | X3DOM runtime stylesheet URL. |
| `thumbnail_suffix` | no | `_thumbnail.png` | Filename suffix appended to each model's identifier. |
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
- `-no-render` — skip rotating/rendering; only run the fetch phase, if any.
- `-input <dir>` — override `input_dir`.
- `-output <dir>` — override `output_dir`.
- `-degrees <n>` — override `degrees` (a literal `0` is treated as "use
  the config value", since 0° is a no-op rotation anyway).
- `-identifiers <path>` — override `identifiers_file`; enables the fetch
  phase even if unset in the config.

The process logs a summary line at the end:

```
done: fetched=12 fetch_failed=0 found=12 rendered=12 failed=0 elapsed=2m14s
```

(`fetched`/`fetch_failed` are always `0` when `identifiers_file` isn't
set; `found` is the number of models the render phase had to work with,
whether from the fetch phase or already sitting in `input_dir`.) It exits
non-zero if any identifier failed to fetch or any model failed to render
(each failure is also logged individually as it happens).

## How rendering works

Rendering needs a real browser because X3DOM parses and draws the model
with WebGL; there is no pure-Go X3D renderer. This tool:

1. Starts a local HTTP file server rooted at `input_dir`, so relative
   texture references resolve exactly as a real hosted model would.
   Rotating a model writes its rotated X3D content to a scratch file
   inside `input_dir` (prefixed `.tmp_rotated_`) just long enough for the
   browser to load it, then deletes that file immediately after
   rendering — it's never written to `output_dir`.
2. Starts headless Chrome via [chromedp](https://github.com/chromedp/chromedp),
   with software WebGL (`--use-gl=swiftshader`) enabled — headless Chrome
   has no GPU, and X3DOM needs *some* WebGL implementation to draw at all.
3. For each render, opens a **fresh browser tab**, navigates it to a small
   generated HTML page embedding an `<x3d>` viewer (sized to
   `thumbnail_size`) pointed at the scratch model file, polls the X3DOM
   runtime until the model's geometry is actually linked into the
   renderable scene graph (not just "loaded"), waits a few more seconds
   for the texture to decode, and screenshots the full viewport — sized
   to exactly match the `<x3d>` element, so this is pixel-identical to
   cropping the canvas.
4. Pads the screenshot to square with white (if it isn't already exactly
   `thumbnail_size` x `thumbnail_size` — see [`squareThumbnail`](thumbnail.go))
   and writes the result to `output_dir/<identifier>_thumbnail.png`.

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
go test ./...     # runs the rotation/text-processing/thumbnail logic against fixtures
```

`main_test.go` covers `wrapSceneInTransform`, `extractTextureURLs`,
`isRelativeAssetURL`, `findModels`, and `localJobs` against small
fixtures. `thumbnail_test.go` covers `squareThumbnail`'s padding/resize
behavior against synthetic in-memory images. `fetch_test.go` covers the
DynamoDB-item and `archiveOptions` JSON parsing, URL resolution, and
download logic in `fetch.go`; `ddbAPI` (the subset of `*dynamodb.Client`
this tool calls — `GetItem` and `Scan`) is a small interface specifically
so tests can substitute a fake implementation, and downloads are tested
against a real `httptest.Server` rather than mocked. None of this needs
network access or AWS credentials to run. The rendering path (`render.go`)
is exercised by manual end-to-end runs rather than the test suite, since
it requires a real Chrome install and downloads the X3DOM runtime from
the VT CDN.
