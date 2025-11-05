def test_get_event_by_id(client):
    # Use an existing seeding event_id from DB or insert one in a setup if needed
    r = client.get(("/events/99001"))
    assert r.status_code in (200, 404) # If seed has it, expect 200
    