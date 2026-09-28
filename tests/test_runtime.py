from pathlib import Path

from src import dq, runtime


class FakeWriter:
    def __init__(self, value: str):
        self.value = value

    def mode(self, mode: str):
        assert mode == "overwrite"
        return self

    def parquet(self, path: str):
        destination = Path(path)
        destination.mkdir(parents=True)
        (destination / "part-test").write_text(self.value, encoding="utf-8")


class FakeFrame:
    def __init__(self, value: str):
        self.write = FakeWriter(value)


class FailingWriter(FakeWriter):
    def parquet(self, path: str):
        destination = Path(path)
        destination.mkdir(parents=True)
        (destination / "partial").write_text(self.value, encoding="utf-8")
        raise OSError("simulated write failure")


class FailingFrame:
    def __init__(self):
        self.write = FailingWriter("partial")


def test_local_publish_replaces_snapshot_atomically(tmp_path, monkeypatch):
    monkeypatch.setenv("BUILDABIDA_DATA_ROOT", str(tmp_path))
    for value in ("first", "second"):
        runtime.publish_table(FakeFrame(value), "02-silver", "sample")
    target = tmp_path / "tables" / "02-silver" / "sample" / "part-test"
    assert target.read_text(encoding="utf-8") == "second"
    assert not list((tmp_path / "tables" / "02-silver").glob(".*.backup-*"))
    assert not list((tmp_path / "tables" / "02-silver").glob(".*.staging-*"))


def test_failed_local_publish_keeps_previous_snapshot(tmp_path, monkeypatch):
    monkeypatch.setenv("BUILDABIDA_DATA_ROOT", str(tmp_path))
    runtime.publish_table(FakeFrame("complete"), "02-silver", "sample")
    try:
        runtime.publish_table(FailingFrame(), "02-silver", "sample")
    except OSError as error:
        assert "simulated" in str(error)
    else:
        raise AssertionError("Expected the simulated write to fail")

    target = tmp_path / "tables" / "02-silver" / "sample" / "part-test"
    assert target.read_text(encoding="utf-8") == "complete"
    assert not list((tmp_path / "tables" / "02-silver").glob(".*.staging-*"))


def test_stop_checks_raise_only_for_failures():
    dq.require_no_failures([{"status": "PASS", "check_name": "key"}])
    try:
        dq.require_no_failures([{"status": "FAIL", "check_name": "key"}])
    except RuntimeError as error:
        assert "key" in str(error)
    else:
        raise AssertionError("Expected a failed stop check to raise")
