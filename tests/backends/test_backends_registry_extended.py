import pytest
from stream_checkpoint.backends import list_backends, get_backend


INFLUXDB_BACKEND = "influxdb"
SCYLLADB_BACKEND = "scylladb"


class TestRegistryInfluxDB:
    def test_influxdb_in_list(self):
        backends = list_backends()
        assert INFLUXDB_BACKEND in backends

    def test_get_influxdb_backend_returns_class(self):
        cls = get_backend(INFLUXDB_BACKEND)
        from stream_checkpoint.backends.influxdb_store import InfluxDBCheckpointStore
        assert cls is InfluxDBCheckpointStore


class TestRegistryScyllaDB:
    def test_scylladb_in_list(self):
        backends = list_backends()
        assert SCYLLADB_BACKEND in backends

    def test_get_scylladb_backend_returns_class(self):
        cls = get_backend(SCYLLADB_BACKEND)
        from stream_checkpoint.backends.scylladb_store import ScyllaDBCheckpointStore
        assert cls is ScyllaDBCheckpointStore


class TestRegistryGeneral:
    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")

    def test_all_backends_importable(self):
        for name in list_backends():
            cls = get_backend(name)
            assert cls is not None
