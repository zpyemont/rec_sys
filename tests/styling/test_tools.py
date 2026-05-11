def test_anthropic_client_importable():
    from app.styling.anthropic_client import get_anthropic_client
    assert get_anthropic_client is not None
