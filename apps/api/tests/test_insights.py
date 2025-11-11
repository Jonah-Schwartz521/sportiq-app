def test_insights_nba_smoke(client):
    r = client.get("/insights/nba/7")
    assert r.status_code == 200
    body = r.json()

    assert body["event_id"] == 7
    assert body["sport"] == "nba"
    assert body["model_key"].startswith("nba-winprob-")
    assert isinstance(body["insights"], list)
    assert len(body["insights"]) >= 1
    first = body["insights"][0]
    assert {"type", "label", "detail"} <= set(first.keys())


def test_insights_ufc_smoke(client):
    r = client.get("/insights/ufc/10")
    assert r.status_code == 200
    body = r.json()
    assert body["sport"] == "ufc"
    assert len(body["insights"]) >= 1


def test_insights_invalid_event(client):
    r = client.get("/insights/nba/0")
    assert r.status_code == 400