import pytest
from controller import NetworkDeviceController

def test_get_blackbox_targets_json(client, controller: NetworkDeviceController):
    """Test GET /targets/blackbox returns Prometheus HTTP SD JSON for blackbox targets."""
    controller.blackbox_targets_cache = {
        "namespace/name": {
            "address": "192.168.1.1",
            "hostname": "host1",
            "namespace": "namespace",
            "kind": "host",
            "commonLabels": {
                "env": "test",
            },
            "ssh_port": 1234,
        }
    }
    response = client.get("/targets/blackbox")
    assert response.status_code == 200
    data = response.get_json()
    assert data == [
        {
            "targets": ["192.168.1.1"],
            "labels": {
                "hostname": "host1",
                "__meta_nht_address": "192.168.1.1",
                "__meta_nht_namespace": "namespace",
                "__meta_nht_ssh_port": "1234",
                "env": "test",
                "kind": "host",
            },
        }
    ]


def test_get_blackbox_targets_json_filter_by_kind(client, controller: NetworkDeviceController):
    """Test GET /targets/blackbox?kind=host filters by kind."""
    controller.blackbox_targets_cache = {
        "ns/device1": {
            "address": "10.0.0.1",
            "hostname": "dev1",
            "namespace": "ns",
            "kind": "device",
            "commonLabels": {},
        },
        "ns/host1": {
            "address": "10.0.0.2",
            "hostname": "host1",
            "namespace": "ns",
            "kind": "host",
            "commonLabels": {},
            "ssh_port": 22,
        },
    }
    response = client.get("/targets/blackbox?kind=device")
    assert response.status_code == 200
    data = response.get_json()
    assert len(data) == 1
    assert data[0]["targets"] == ["10.0.0.1"]
    assert data[0]["labels"]["hostname"] == "dev1"
    assert "__meta_ndt_address" in data[0]["labels"]
    assert data[0]["labels"]["__meta_ndt_namespace"] == "ns"
    assert data[0]["labels"]["kind"] == "device"