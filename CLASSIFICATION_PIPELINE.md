# Post-scrape classification pipeline

The scraper now has a registry-driven classification stage for models that run
after the broad conflict, nation, and item-type decisions. Model-specific scope
is declared in JSON; the scraper core does not contain `if WWII German helmet`
branches.

## Runtime flow

1. Scrape and insert the product.
2. Upload its full image gallery.
3. Run the existing broad conflict, nation, and item-type classifiers.
4. Send the product to the persistent classification worker.
5. The worker evaluates enabled manifests in order and only runs stages whose
   scope matches the current effective labels.
6. Human-confirmed target values are always preserved.
7. Every run, skip, failure, prediction, confidence, and evidence image is
   written to a daily JSONL audit file.

The initial registered chain is:

`WW2 + Germany + helmets_accessories -> helmet subtype v9 -> component N/A or service/organization v7`

The branch model uses every usable gallery image. The subtype model uses the
first usable primary image plus `text-embedding-3-large`, with its validated
text-only artifact as a fallback.

## Safety modes

The current checked-in defaults, enabled after explicit approval, are:

```python
"classificationPipelineMode": "apply"
"classificationAllowRemoteEmbeddings": True
```

Shadow mode runs eligible local stages and records proposed values without
writing them to product classification fields. Remote embeddings remain off,
so listing text is not newly sent to the embeddings endpoint by this pipeline.

To pause database writes and remote text embeddings while retaining local
audit behavior, use:

```python
"classificationPipelineMode": "shadow"
"classificationAllowRemoteEmbeddings": False
```

Classifier failures never abort scraping.

## Targeted backfills

`classification_pipeline.backfill` runs the same registry against recent
database records. It is dry-run by default and requires separate explicit
flags for database writes and remote embeddings. For example:

```powershell
& ".\venv\Scripts\python.exe" -B -m classification_pipeline.backfill `
  --db-credentials "C:\Users\keena\Desktop\Milivault\credentials\pgadmin_credentials.json" `
  --registry ".\classification_models\registry.json" `
  --worker-python "C:\Users\keena\Desktop\Milivault\Milivault ML Classifier\.venv\Scripts\python.exe" `
  --openai-credentials "C:\Users\keena\Desktop\Milivault\credentials\chatgpt_api_key.json" `
  --limit 5 --apply --allow-remote-embeddings
```

Use `--conflict`, `--nation`, and `--item-type` when running a different
registered model scope. Use `--ids` for an exact comma-separated product set.

## Adding another model

1. Train from human-confirmed labels and retain a holdout report.
2. Export a self-contained trusted `joblib` bundle.
3. Add a manifest under `classification_models/manifests/` with a stable model
   ID/version, ordered dependencies, exact scope, target and human guard fields,
   runtime inputs, checksums, and acceptance/review thresholds. Add declarative
   text exclusions when an upstream broad label has known, explicit out-of-scope
   markers such as `WW1` or `WO1`.
4. Add the manifest path to `classification_models/registry.json`.
5. Provision the binary in `classification_models/artifacts/`. Binaries are
   ignored by Git; manifests and checksums are tracked.
6. Run registry validation and tests before changing from shadow to apply.

Provision artifacts atomically with:

```powershell
python -B -m classification_pipeline.install_artifact --source <trained.joblib> --destination <artifacts/model.joblib> --expected-sha256 <manifest hash>
```

Copy `classification_models/MANIFEST_TEMPLATE.json` into the `manifests`
directory as the starting point for each new stage.

Supported runtimes are constant scoped values, OpenAI text-embedding models,
CLIP plus OpenAI combined models, and full-gallery CLIP plus local TF-IDF late
fusion. A genuinely unusual model can add a versioned plugin without changing
pipeline ordering, auditing, safety, or persistence code.

## Validation

From the scraper directory:

```powershell
& "C:\Users\keena\Desktop\Milivault\Milivault ML Classifier\.venv\Scripts\python.exe" -B -m classification_pipeline --registry classification_models\registry.json --verify-checksums
& "C:\Users\keena\Desktop\Milivault\Milivault ML Classifier\.venv\Scripts\python.exe" -B -m unittest discover -s tests -p "test_classification_pipeline.py" -v
```

For EC2, create `/home/ec2-user/milivault/classification-venv`, install
`requirements-classification.txt`, provision the same checksum-verified
artifacts, and leave the scraper environment unchanged.
