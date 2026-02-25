import json
import os
from unittest.mock import MagicMock, patch

import responses

from src.fetchers.tsv_fetcher import COUNTRIES_TSV_URL


FIXTURES_DIR = os.path.join(
    os.path.dirname(__file__), "..", "fixtures"
)


def _read_fixture(filename: str) -> str:
    path = os.path.join(FIXTURES_DIR, filename)
    with open(path, "r") as f:
        return f.read()


class TestLambdaHandler:
    @responses.activate
    @patch("src.handler.get_database")
    @patch.dict(
        os.environ,
        {"MONGODB_URI": "mongodb://localhost:27017/test"},
    )
    def test_successful_run(self, mock_get_db):
        tsv_text = _read_fixture("sample_countries.tsv")
        responses.add(
            responses.GET,
            COUNTRIES_TSV_URL,
            body=tsv_text,
            status=200,
        )

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        mock_result = MagicMock()
        mock_result.upserted_count = 4
        mock_result.modified_count = 0
        mock_db.__getitem__.return_value.bulk_write.return_value = (
            mock_result
        )

        from src.handler import lambda_handler

        result = lambda_handler({}, None)
        body = json.loads(result["body"])

        assert result["statusCode"] == 200
        assert body["status"] == "success"
        assert body["source_used"] == "tsv"
        assert body["saved"] == 4

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_config(self):
        from src.handler import lambda_handler

        result = lambda_handler({}, None)
        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "error" in body

    @patch("src.fetchers.orchestrator.fetch_all_countries")
    @patch("src.fetchers.orchestrator.fetch_latest_week")
    @patch("src.handler.get_database")
    @patch.dict(
        os.environ,
        {"MONGODB_URI": "mongodb://localhost:27017/test"},
    )
    def test_tsv_failure_falls_back(
        self, mock_get_db, mock_tsv, mock_html
    ):
        mock_tsv.side_effect = Exception("TSV download failed")
        mock_html.return_value = ()

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db

        from src.handler import lambda_handler

        result = lambda_handler({}, None)
        body = json.loads(result["body"])

        assert body["source_used"] in ("html_fallback", "none")
