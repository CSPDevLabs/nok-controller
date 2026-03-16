# NOK Controller

A Kubernetes controller that manages network device and host targets for the NetOpsKube ecosystem.

## Overview

The NOK Controller watches custom resources (`NetworkDeviceTarget` and `NetworkHostTarget`) and automatically:

- **Creates/updates gNMIc Targets** (`operator.gnmic.dev/v1alpha1`) for gNMI telemetry collection.
- **Manages SDCIO DiscoveryRules** (`inv.sdcio.dev/v1alpha1`) for automated device discovery.
- **Performs ICMP reachability checks** and updates CRD status with connectivity state.
- **Exposes HTTP endpoints** for Prometheus service discovery (targets and blackbox used by Blackbox Monitoring).

## HTTP Endpoints

| Endpoint | Description |
|----------|-------------|
| `/targets` | Returns registered target hostnames |
| `/targets/blackbox` | Prometheus HTTP SD JSON for blackbox exporter (supports `?kind=device\|host` filter) |

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `HTTP_SERVER_PORT` | `8080` | HTTP server port |
| `HTTP_SERVER_HOST` | `0.0.0.0` | HTTP server bind address |
| `REACHABILITY_CHECK_PERIOD_SECONDS` | `60` | Interval for periodic reachability checks |

## CRDs

**Input (watched):**

- `networkdevicetargets.nok.dev/v1alpha1` - Network device definitions
- `networkhosttargets.nok.dev/v1alpha1` - Network host definitions

**Output (managed):**

- `targets.operator.gnmic.dev/v1alpha1` - gNMIc telemetry targets
- `discoveryrules.inv.sdcio.dev/v1alpha1` - SDCIO discovery rules
