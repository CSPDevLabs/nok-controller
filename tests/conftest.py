from controller import NetworkDeviceController
import pytest


@pytest.fixture
def controller(monkeypatch):
    """Controller instance without real Kubernetes config loading."""

    # Prevent real Kubernetes config loading in CI
    monkeypatch.setattr(
        "kubernetes.config.load_incluster_config",
        lambda: None,
    )

    monkeypatch.setattr(
        "kubernetes.config.load_kube_config",
        lambda: None,
    )

    ctrl = NetworkDeviceController()

    yield ctrl


@pytest.fixture
def client(controller: NetworkDeviceController):
    return controller.app.test_client()