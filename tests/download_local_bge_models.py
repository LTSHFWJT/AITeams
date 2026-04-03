from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_EMBED_MODEL = "BAAI/bge-m3"
DEFAULT_RERANK_MODEL = "BAAI/bge-reranker-v2-m3"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download local BGE embedding/rerank models into the repo models directory.",
    )
    parser.add_argument(
        "--models-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "models",
        help="Target directory for downloaded models. Defaults to <repo>/models.",
    )
    parser.add_argument(
        "--embed-model",
        default=DEFAULT_EMBED_MODEL,
        help="Embedding model repo id to download.",
    )
    parser.add_argument(
        "--rerank-model",
        default=DEFAULT_RERANK_MODEL,
        help="Rerank model repo id to download.",
    )
    parser.add_argument(
        "--skip-embed",
        action="store_true",
        help="Skip embedding model download.",
    )
    parser.add_argument(
        "--skip-rerank",
        action="store_true",
        help="Skip rerank model download.",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Optional Hugging Face token. If omitted, huggingface_hub will use local login/env.",
    )
    return parser.parse_args()


def safe_dir_name(repo_id: str) -> str:
    return repo_id.strip().replace("\\", "_").replace("/", "__")


def download_model(repo_id: str, models_dir: Path, *, token: str | None) -> dict[str, str]:
    try:
        from huggingface_hub import snapshot_download
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise SystemExit(
            "Missing dependency `huggingface_hub`. Install it with `pip install huggingface_hub`."
        ) from exc

    target_dir = models_dir / safe_dir_name(repo_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    resolved_dir = Path(
        snapshot_download(
            repo_id=repo_id,
            local_dir=str(target_dir),
            token=token,
        )
    ).resolve()
    return {
        "repo_id": repo_id,
        "local_dir": str(resolved_dir),
    }


def main() -> None:
    args = parse_args()
    models_dir = args.models_dir.expanduser().resolve()
    models_dir.mkdir(parents=True, exist_ok=True)

    plan: list[str] = []
    if not args.skip_embed:
        plan.append(args.embed_model)
    if not args.skip_rerank:
        plan.append(args.rerank_model)
    if not plan:
        raise SystemExit("Nothing to download. Remove `--skip-embed` or `--skip-rerank`.")

    downloaded: list[dict[str, str]] = []
    for repo_id in plan:
        result = download_model(repo_id, models_dir, token=args.token)
        downloaded.append(result)
        print(f"Downloaded {repo_id} -> {result['local_dir']}")

    manifest_path = models_dir / "model-manifest.json"
    manifest = {
        "models_dir": str(models_dir),
        "downloaded": downloaded,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote manifest -> {manifest_path}")


if __name__ == "__main__":
    main()
