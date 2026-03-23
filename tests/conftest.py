from controller import NetworkDeviceController
import pytest


@pytest.fixture
def controller():
    """Controller instance without run() — run() blocks on Kubernetes watch thread join()."""
    ctrl = NetworkDeviceController()
    # Do not call ctrl.run(): it starts watch threads and blocks on join().
   
    yield ctrl

@pytest.fixture
def client(controller: NetworkDeviceController):
    return controller.app.test_client()
    
