import os

os.environ.setdefault("HF_HUB_ENABLE_HF_TRANSFER", "1")

from huggingface_hub import snapshot_download

REPO_ID = "predict-quant/poly-btc-orderbook"
SUBDIR = "5m/2026/04/19"
LOCAL_DIR = os.path.join(os.path.dirname(__file__), "data")

path = snapshot_download(
    repo_id=REPO_ID,
    repo_type="dataset",
    allow_patterns=[f"{SUBDIR}/*"],
    local_dir=LOCAL_DIR,
    max_workers=8,
)

print(f"Downloaded to: {os.path.join(path, SUBDIR)}")
