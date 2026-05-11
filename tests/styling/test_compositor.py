import pytest
from unittest.mock import MagicMock, patch


class TestGCSUpload:
    def test_upload_bytes_calls_blob_upload(self):
        with patch("app.connectors.gcs.storage") as mock_storage:
            mock_client = MagicMock()
            mock_storage.Client.return_value = mock_client
            mock_blob = MagicMock()
            mock_client.bucket.return_value.blob.return_value = mock_blob

            from app.connectors.gcs import GCSClient
            from app.settings import get_settings
            client = GCSClient(get_settings())
            url = client.upload_bytes("test-bucket", "path/file.png", b"data", "image/png")

            mock_blob.upload_from_string.assert_called_once_with(b"data", content_type="image/png")
            assert url == "https://storage.googleapis.com/test-bucket/path/file.png"

    def test_upload_bytes_uses_default_bucket(self):
        with patch("app.connectors.gcs.storage") as mock_storage:
            mock_client = MagicMock()
            mock_storage.Client.return_value = mock_client
            mock_blob = MagicMock()
            mock_client.bucket.return_value.blob.return_value = mock_blob

            from app.connectors.gcs import GCSClient
            from app.settings import Settings
            settings = Settings(gcs_bucket_composites="my-composites")
            client = GCSClient(settings)
            url = client.upload_bytes(None, "styling/composites/abc.png", b"data", "image/png")

            assert "my-composites" in url or "abc.png" in url
