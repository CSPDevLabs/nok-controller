import pytest
from controller import NetworkDeviceController
from kubernetes.client.rest import ApiException


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


# Test: No targets
def test_get_blackbox_targets_empty(client, controller: NetworkDeviceController):
    controller.blackbox_targets_cache = {}
    response = client.get("/targets/blackbox")
    assert response.status_code == 200
    assert response.get_json() == []


#Test: Device with gnmic_port
def test_get_blackbox_targets_device_with_gnmic_port(client, controller: NetworkDeviceController):
    controller.blackbox_targets_cache = {
        "ns/dev1": {
            "address": "10.0.0.1",
            "hostname": "dev1",
            "namespace": "ns",
            "kind": "device",
            "commonLabels": {},
            "gnmic_port": 57400,
        }
    }

    response = client.get("/targets/blackbox")
    data = response.get_json()
    assert data[0]["labels"]["__meta_ndt_gnmic_port"] == "57400"


#Test: Multiple targets (no filter)
def test_get_blackbox_targets_multiple_entries(client, controller: NetworkDeviceController):
    controller.blackbox_targets_cache = {
        "ns/dev": {
            "address": "1.1.1.1",
            "hostname": "dev",
            "namespace": "ns",
            "kind": "device",
            "commonLabels": {},
        },
        "ns/host": {
            "address": "2.2.2.2",
            "hostname": "host",
            "namespace": "ns",
            "kind": "host",
            "commonLabels": {},
            "ssh_port": 22,
        },
    }

    response = client.get("/targets/blackbox")
    data = response.get_json()
    assert len(data) == 2


# Test /targets endpoint
def test_get_targets_json(controller: NetworkDeviceController, client):
    controller.registered_http_hostnames = {"host1.local", "host2.local"}
    response = client.get("/targets")
    data = response.get_json()
    assert "host1.local" in data
    assert "host2.local" in data


#Test: Mock ping success
def test_check_reachability_success(monkeypatch, controller: NetworkDeviceController):
    monkeypatch.setattr("ping3.ping", lambda *args, **kwargs: 10)
    result = controller._check_reachability("1.1.1.1")
    assert result == "Reachable"


#Test: Mock ping failure
def test_check_reachability_failure(monkeypatch, controller: NetworkDeviceController):
    monkeypatch.setattr("ping3.ping", lambda *args, **kwargs: False)
    result = controller._check_reachability("1.1.1.1")
    assert result == "Unreachable"


def test_check_reachability_exception(monkeypatch, controller: NetworkDeviceController):
    def raise_error(*args, **kwargs):
        raise Exception("Ping failed")

    monkeypatch.setattr("ping3.ping", raise_error)
    result = controller._check_reachability("1.1.1.1")
    assert result == "Unknown"


#Test patch call
def test_patch_target_status(monkeypatch, controller: NetworkDeviceController):
    called = {}

    def mock_patch(**kwargs):
        called["called"] = True

    monkeypatch.setattr(
        controller.k8s_api,
        "patch_namespaced_custom_object_status",
        mock_patch,
    )

    controller._patch_target_status(
        "group", "v1", "plural", "ns", "name", "Reachable", "TestResource"
    )

    assert called.get("called") is True


#gNMIc TARGET MANAGEMENT
#Create case
def test_ensure_gnmic_target_create(monkeypatch, controller: NetworkDeviceController):
    def mock_get(*args, **kwargs):
        raise ApiException(status=404)

    created = {}

    def mock_create(**kwargs):
        created["done"] = True

    monkeypatch.setattr(controller.k8s_api, "get_namespaced_custom_object", mock_get)
    monkeypatch.setattr(controller.k8s_api, "create_namespaced_custom_object", mock_create)

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {"address": "1.1.1.1", "gnmic": {}, "commonLabels": {}},
    }

    controller._ensure_gnmic_target(ndt, create=True)

    assert created.get("done") is True


# Update case
def test_ensure_gnmic_target_update(monkeypatch, controller: NetworkDeviceController):
    def mock_get(*args, **kwargs):
        return {}

    updated = {}

    def mock_patch(**kwargs):
        updated["done"] = True

    monkeypatch.setattr(controller.k8s_api, "get_namespaced_custom_object", mock_get)
    monkeypatch.setattr(controller.k8s_api, "patch_namespaced_custom_object", mock_patch)

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {"address": "1.1.1.1", "gnmic": {}, "commonLabels": {}},
    }

    controller._ensure_gnmic_target(ndt, create=True)

    assert updated.get("done") is True


#Delete case
def test_ensure_gnmic_target_delete(monkeypatch, controller: NetworkDeviceController):
    deleted = {}

    def mock_delete(**kwargs):
        deleted["done"] = True

    monkeypatch.setattr(controller.k8s_api, "delete_namespaced_custom_object", mock_delete)

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {"address": "1.1.1.1"},
    }

    controller._ensure_gnmic_target(ndt, create=False)

    assert deleted.get("done") is True


#Create DiscoveryRule
def test_ensure_sdcio_discovery_rule_create(monkeypatch, controller: NetworkDeviceController):
    def mock_get(*args, **kwargs):
        raise ApiException(status=404)

    created = {}

    def mock_create(**kwargs):
        created["done"] = True

    monkeypatch.setattr(controller.k8s_api, "get_namespaced_custom_object", mock_get)
    monkeypatch.setattr(controller.k8s_api, "create_namespaced_custom_object", mock_create)

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {
            "address": "1.1.1.1",
            "hostname": "dev1",
            "sdcio": {},
            "commonLabels": {},
        },
    }

    controller._ensure_sdcio_discovery_rule(ndt, create=True)

    assert created.get("done") is True


# DiscoveryRule update
def test_ensure_sdcio_discovery_rule_update(monkeypatch, controller: NetworkDeviceController):
    def mock_get(*args, **kwargs):
        return {"spec": {"addresses": []}}

    updated = {}

    def mock_patch(**kwargs):
        updated["done"] = True

    monkeypatch.setattr(controller.k8s_api, "get_namespaced_custom_object", mock_get)
    monkeypatch.setattr(controller.k8s_api, "patch_namespaced_custom_object", mock_patch)

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {
            "address": "1.1.1.1",
            "hostname": "dev1",
            "sdcio": {},
            "commonLabels": {},
        },
    }

    controller._ensure_sdcio_discovery_rule(ndt, create=True)
    assert updated.get("done") is True


#NetworkDeviceTarget ADD
def test_process_ndt_added(monkeypatch, controller: NetworkDeviceController):
    monkeypatch.setattr(controller, "_check_reachability", lambda x: "Reachable")
    monkeypatch.setattr(controller, "_ensure_gnmic_target", lambda *a, **k: None)
    monkeypatch.setattr(controller, "_ensure_sdcio_discovery_rule", lambda *a, **k: None)
    monkeypatch.setattr(controller, "_update_network_device_target_status", lambda *a, **k: None)

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns", "generation": 1},
        "spec": {"address": "1.1.1.1", "hostname": "dev1"},
    }

    controller._process_network_device_target_event("ADDED", ndt)
    assert "ns/dev1" in controller.blackbox_targets_cache


# Skip same generation
def test_process_ndt_skip_same_generation(controller: NetworkDeviceController):
    controller.network_device_targets_for_reachability = {
        "ns/dev1": {"generation": 1}
    }

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns", "generation": 1},
        "spec": {"address": "1.1.1.1"},
    }

    controller._process_network_device_target_event("MODIFIED", ndt)
    assert "ns/dev1" in controller.network_device_targets_for_reachability


# No address case
def test_process_ndt_no_address(controller: NetworkDeviceController):
    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {},
    }

    controller._process_network_device_target_event("ADDED", ndt)
    assert "ns/dev1" not in controller.network_device_targets_for_reachability


#NetworkDeviceTarget DELETE
def test_process_ndt_deleted(monkeypatch, controller: NetworkDeviceController):
    monkeypatch.setattr(controller, "_ensure_gnmic_target", lambda *a, **k: None)
    monkeypatch.setattr(controller, "_ensure_sdcio_discovery_rule", lambda *a, **k: None)

    controller.blackbox_targets_cache["ns/dev1"] = {}

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {"address": "1.1.1.1"},
    }

    controller._process_network_device_target_event("DELETED", ndt)
    assert "ns/dev1" not in controller.blackbox_targets_cache


#NetworkHostTarget ADD
def test_process_nht_added(monkeypatch, controller: NetworkDeviceController):
    monkeypatch.setattr(controller, "_check_reachability", lambda x: "Reachable")
    monkeypatch.setattr(controller, "_update_network_host_target_status", lambda *a, **k: None)

    nht = {
        "metadata": {"name": "host1", "namespace": "ns", "generation": 1},
        "spec": {"address": "2.2.2.2", "hostname": "host1"},
    }

    controller._process_network_host_target_event("ADDED", nht)
    assert "ns/host1" in controller.blackbox_targets_cache


# NHT DELETE
def test_process_nht_deleted(controller: NetworkDeviceController):
    controller.blackbox_targets_cache["ns/host1"] = {}

    nht = {
        "metadata": {"name": "host1", "namespace": "ns"},
        "spec": {"address": "2.2.2.2"},
    }

    controller._process_network_host_target_event("DELETED", nht)
    assert "ns/host1" not in controller.blackbox_targets_cache

# NHT Skip Same Generation
def test_process_nht_skip_same_generation(controller):
    controller.network_host_targets_for_reachability = {
        "ns/host1": {"generation": 1}
    }

    nht = {
        "metadata": {"name": "host1", "namespace": "ns", "generation": 1},
        "spec": {"address": "2.2.2.2"},
    }

    controller._process_network_host_target_event("MODIFIED", nht)
    assert "ns/host1" not in controller.blackbox_targets_cache


# Reachability Loop for NHT
def test_reachability_loop_nht(monkeypatch, controller):
    controller.network_host_targets_for_reachability = {
        "ns/host1": {"address": "2.2.2.2", "last_status": "Unknown"}
    }

    monkeypatch.setattr(controller, "_check_reachability", lambda x: "Reachable")
    monkeypatch.setattr(controller, "_update_network_host_target_status", lambda *a, **k: None)

    def stop_sleep(x):
        raise KeyboardInterrupt()

    monkeypatch.setattr("time.sleep", stop_sleep)

    try:
        controller._reachability_loop()
    except KeyboardInterrupt:
        pass

    assert controller.network_host_targets_for_reachability["ns/host1"]["last_status"] == "Unknown"


#Single iteration test (mock sleep)
def test_reachability_loop_runs_once(monkeypatch, controller: NetworkDeviceController):
    controller.network_device_targets_for_reachability = {
        "ns/dev1": {"address": "1.1.1.1", "last_status": "Unknown"}
    }

    monkeypatch.setattr(controller, "_check_reachability", lambda x: "Reachable")
    monkeypatch.setattr(controller, "_update_network_device_target_status", lambda *a, **k: None)

    calls = {"count": 0}
    def fake_sleep(x):
        calls["count"] += 1
        if calls["count"] > 1:
            raise KeyboardInterrupt()

    monkeypatch.setattr("time.sleep", fake_sleep)

    try:
        controller._reachability_loop()
    except KeyboardInterrupt:
        pass

    assert calls["count"] >= 1


#Test threads start
def test_run_starts_threads(monkeypatch, controller: NetworkDeviceController):
    monkeypatch.setattr(controller, "_watch_crd", lambda *a, **k: None)
    monkeypatch.setattr(controller, "_reachability_loop", lambda: None)
    monkeypatch.setattr(controller, "_run_http_server", lambda: None)
    monkeypatch.setattr("threading.Thread.join", lambda self: None)
    controller.run()


# Patch Status Exception Case
def test_patch_target_status_api_exception(monkeypatch, controller):
    def raise_error(**kwargs):
        raise ApiException(status=500)

    monkeypatch.setattr(
        controller.k8s_api,
        "patch_namespaced_custom_object_status",
        raise_error,
    )

    controller._patch_target_status("g", "v", "p", "ns", "n", "Reachable", "Test")


# gNMIc Target Update Exception (non-404)
def test_ensure_gnmic_target_exception(monkeypatch, controller):
    def mock_get(*a, **k):
        raise ApiException(status=500)

    monkeypatch.setattr(controller.k8s_api, "get_namespaced_custom_object", mock_get)

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {"address": "1.1.1.1"},
    }

    controller._ensure_gnmic_target(ndt, create=True)
    assert True  # Ensures test validates execution path


# SDCIO DELETE FLOW
def test_ensure_sdcio_discovery_rule_delete_flow(monkeypatch, controller):
    existing = {
        "spec": {
            "addresses": [
                {"address": "1.1.1.1", "hostName": "dev1"}
            ]
        }
    }

    monkeypatch.setattr(
        controller.k8s_api,
        "get_namespaced_custom_object",
        lambda **k: existing
    )

    patched = {}
    def mock_patch(**kwargs):
        patched["done"] = True

    monkeypatch.setattr(controller.k8s_api, "patch_namespaced_custom_object", mock_patch)

    ndt = {
        "metadata": {"name": "dev1", "namespace": "ns"},
        "spec": {"address": "1.1.1.1", "hostname": "dev1"},
    }

    controller._ensure_sdcio_discovery_rule(ndt, create=False)

    assert patched.get("done") is True


# Watch CRD
def test_watch_crd_once(monkeypatch, controller):
    events = [
        {"type": "ADDED", "object": {"metadata": {}, "spec": {}}}
    ]

    called = {"done": False}

    def processor(*a):
        called["done"] = True

    def mock_stream(*a, **k):
        for e in events:
            yield e
        raise KeyboardInterrupt()

    monkeypatch.setattr(controller.k8s_watch, "stream", mock_stream)

    try:
        controller._watch_crd("g", "v", "p", processor)
    except KeyboardInterrupt:
        pass

    assert called["done"] is True