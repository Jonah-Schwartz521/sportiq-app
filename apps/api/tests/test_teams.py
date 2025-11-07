def test_teams_list(client):
    r = client.get("/teams?limit=5")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body["items"], list)
    assert body["limit"] == 5

def test_team_by_id(client):
    # uses seeded IDs from your migrations (e.g., 7 exists)
    r = client.get("/teams/7")
    assert r.status_code == 200
    body = r.json()
    assert body["team_id"] == 7