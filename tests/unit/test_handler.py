import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "fetch_stock_data"))
import requests
import pytest
from app import apply_threshold
from app import fetch_stock_data

# significant move sample
@pytest.fixture()
def sample_stock_data():
    return {
        "Meta Data": {
            "2. Symbol": "IBM"
        },
        "Time Series (Daily)": {
            "2026-07-10": {
                "1. open": "100.00",
                "2. high": "106.00",
                "3. low": "99.00",
                "4. close": "105.00",
                "5. volume": "1000000"
            },
            "2026-07-09": {
                "1. open": "98.00",
                "2. high": "99.00",
                "3. low": "97.50",
                "4. close": "98.50",
                "5. volume": "900000"
            }
        }
    }

# no significant move sample
@pytest.fixture()
def sample_stock_data2():
    return {
        "Meta Data": {
            "2. Symbol": "IBM"
        },
        "Time Series (Daily)": {
            "2026-07-10": {
                "1. open": "100.00",
                "2. high": "101.50",
                "3. low": "99.50",
                "4. close": "101.00",
                "5. volume": "1000000"
            },
            "2026-07-09": {
                "1. open": "99.00",
                "2. high": "100.00",
                "3. low": "98.50",
                "4. close": "99.50",
                "5. volume": "900000"
            }
        }
    }

# fake api response
class FakeResponse:
    def __init__(self, json_data):
        self._json_data = json_data

    def raise_for_status(self):
        pass
    
    def json(self):
        return self._json_data

def test_fetch_stock_data(monkeypatch):
    fake_response_data = {"Information": "Thank you for using Alpha Vantage!..."}
    def fake_get(url, params):
        return FakeResponse(fake_response_data)
    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(requests.exceptions.RequestException):
        fetch_stock_data("AAPL")


def test_apply_threshold_significant_move(sample_stock_data):
    data, most_recent_date = apply_threshold(sample_stock_data)
    assert most_recent_date == "2026-07-10"

    tagged_day = data["Time Series (Daily)"][most_recent_date]
   
    assert tagged_day["significant_move"]
    assert tagged_day["pct_change"] == pytest.approx(5.0)

def test_apply_threshold_nonsignificant_move(sample_stock_data2):
    data, most_recent_date = apply_threshold(sample_stock_data2)
    assert most_recent_date == "2026-07-10"

    tagged_day = data["Time Series (Daily)"][most_recent_date]
   
    assert tagged_day["significant_move"] == False
    assert tagged_day["pct_change"] == pytest.approx(1.0)