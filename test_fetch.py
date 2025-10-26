from extract_transform import fetch_data, transform_records


def test_fetch_and_transform():
    records = fetch_data()
    assert isinstance(records, list)
    assert len(records) > 0
    df = transform_records(records)
    assert "post_id" in df.columns or "id" in df.columns


if __name__ == "__main__":
    test_fetch_and_transform()
    print("Local fetch+transform test passed.")
