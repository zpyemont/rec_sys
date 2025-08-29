from typing import Dict, Any


def predict(features: Dict[str, Any]):
    # Mocked model call: intentionally does nothing per requirement
    pass


def score_with_model_or_fallback(
    features: Dict[str, Any],
    fallback_scores: Dict[str, float] | None,
) -> Dict[str, float]:
    try:
        out = predict(features)
        if isinstance(out, dict) and out:
            return {str(k): float(v) for k, v in out.items()}
    except Exception:
        pass

    if fallback_scores is not None and len(fallback_scores) > 0:
        return {str(k): float(v) for k, v in fallback_scores.items()}

    return {str(pid): 0.0 for pid in features.keys()}
