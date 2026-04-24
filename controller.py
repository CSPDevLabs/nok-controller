import json
import os
import logging
import time
import threading
from datetime import datetime, timezone
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from flask import Flask, jsonify, request
import ping3  # For soft ping

# --- Configuration ---
# Kubernetes CRD details for NetworkDeviceTarget (nok.dev) - Primary input for this controller
NETWORK_DEVICE_TARGET_CRD_GROUP = "nok.dev"
NETWORK_DEVICE_TARGET_CRD_VERSION = "v1alpha1"
NETWORK_DEVICE_TARGET_CRD_PLURAL = "networkdevicetargets"

# Kubernetes CRD details for NetworkHostTarget (nok.dev)
NETWORK_HOST_TARGET_CRD_GROUP = "nok.dev"
NETWORK_HOST_TARGET_CRD_VERSION = "v1alpha1"
NETWORK_HOST_TARGET_CRD_PLURAL = "networkhosttargets"

# Kubernetes CRD details for operator.gnmic.dev/v1alpha1 Target - Managed by this controller
GNMIC_TARGET_CRD_GROUP = "operator.gnmic.dev"
GNMIC_TARGET_CRD_VERSION = "v1alpha1"
GNMIC_TARGET_CRD_PLURAL = "targets"

# Kubernetes CRD details for inv.sdcio.dev/v1alpha1 DiscoveryRule - Managed by this controller
SDCIO_DISCOVERY_RULE_CRD_GROUP = "inv.sdcio.dev"
SDCIO_DISCOVERY_RULE_CRD_VERSION = "v1alpha1"
SDCIO_DISCOVERY_RULE_CRD_PLURAL = "discoveryrules"
# Name of the DiscoveryRule to manage (can be configured or derived)
DEFAULT_DISCOVERY_RULE_NAME = "dr-static"
DEFAULT_DISCOVERY_RULE_PERIOD = "1m"  # Default period for DiscoveryRule

# HTTP Server Configuration
HTTP_SERVER_PORT = int(os.getenv("HTTP_SERVER_PORT", "8080"))
HTTP_SERVER_HOST = os.getenv("HTTP_SERVER_HOST", "0.0.0.0")

# Reachability Check Configuration
REACHABILITY_CHECK_PERIOD_SECONDS = int(
    os.getenv("REACHABILITY_CHECK_PERIOD_SECONDS", "60")
)  # 1 minute

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class NetworkDeviceController:
    def __init__(self):
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration.")
        except config.ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded kubeconfig from file system for local development.")
            except config.ConfigException:
                logger.error("Could not configure Kubernetes client. Exiting.")
                raise

        self.k8s_api = client.CustomObjectsApi()
        self.k8s_core_api = client.CoreV1Api()  # For secret checks if needed
        self.k8s_watch = watch.Watch()

        # Initialize Flask app
        self.app = Flask(__name__)
        self.app.add_url_rule("/targets", "get_targets", self._get_targets_json)

        # Data structure to hold registered Target hostnames for the HTTP service
        # This will now be populated by NetworkDeviceTargets
        self.registered_http_hostnames = set()
        self.http_hostnames_lock = threading.Lock()

        # Data structure to hold NetworkDeviceTarget info for reachability checks
        # { "namespace/name": { "address": "ip", "last_status": "Reachable/Unreachable/Unknown" } }
        self.network_device_targets_for_reachability = {}
        self.network_device_targets_lock = threading.Lock()

        # Data structure to hold NetworkHostTarget info for reachability checks
        self.network_host_targets_for_reachability = {}
        self.network_host_targets_lock = threading.Lock()

        # Unified blackbox targets cache for Prometheus HTTP SD
        # { "namespace/name": { address, hostname, namespace, kind, commonLabels, gnmic_port|ssh_port } }
        self.blackbox_targets_cache = {}
        self.blackbox_targets_lock = threading.Lock()
        self.app.add_url_rule(
            "/targets/blackbox", "get_blackbox_targets", self._get_blackbox_targets_json
        )

    def _get_targets_json(self):
        """HTTP endpoint to return registered Target hostnames."""
        with self.http_hostnames_lock:
            # Convert set to a dictionary with empty strings as values
            targets_dict = {hostname: "" for hostname in self.registered_http_hostnames}
        return jsonify(targets_dict)

    def _get_blackbox_targets_json(self):
        """HTTP endpoint returning Prometheus HTTP SD JSON for blackbox targets."""
        kind_filter = request.args.get("kind")

        # Gets the entries from the blackbox targets cache.
        entries = []
        with self.blackbox_targets_lock:
            entries = list(self.blackbox_targets_cache.values())

        # Filters the entries based on the kind filter.
        result = []
        for entry in entries:
            if kind_filter and entry["kind"] != kind_filter:
                continue

            # skip = False
            # for param_key, param_val in request.args.items():
            #     if param_key == "kind":
            #         continue
            #     if param_key == "namespace":
            #         if entry["namespace"] != param_val:
            #             skip = True
            #             break
            #     else:
            #         if entry.get("commonLabels", {}).get(param_key) != param_val:
            #             skip = True
            #             break
            # if skip:
            #     continue

            meta_prefix = "__meta_ndt" if entry["kind"] == "device" else "__meta_nht"
            labels = {
                "hostname": entry["hostname"],
                "kind": entry["kind"],
                f"{meta_prefix}_address": entry["address"],
                f"{meta_prefix}_namespace": entry["namespace"],
            }
            labels.update(entry.get("commonLabels", {}))

            if entry["kind"] == "device" and "gnmic_port" in entry:
                labels["__meta_ndt_gnmic_port"] = str(entry["gnmic_port"])
            elif entry["kind"] == "host" and "ssh_port" in entry:
                labels["__meta_nht_ssh_port"] = str(entry["ssh_port"])

            result.append(
                {
                    "targets": [entry["address"]],
                    "labels": labels,
                }
            )

        return jsonify(result)

    def _check_reachability(self, address: str) -> str:
        """Performs a soft ping to the given address and returns 'Reachable' or 'Unreachable'."""
        try:
            # ping3.ping returns latency in seconds, or False if unreachable
            delay = ping3.ping(address, timeout=1, unit="ms")
            if delay is not False:
                logger.debug(f"Ping to {address} successful, latency: {delay:.2f} ms")
                return "Reachable"
            else:
                logger.debug(f"Ping to {address} failed.")
                return "Unreachable"
        except Exception as e:
            logger.error(f"Error during reachability check for {address}: {e}")
            return "Unknown"

    def _patch_target_status(
        self,
        group: str,
        version: str,
        plural: str,
        namespace: str,
        name: str,
        status: str,
        resource_label: str,
    ):
        """Patches the status subresource of a custom object (CRD) with the given status and last probe time.

        Args:
            group (str): The group of the custom object (CRD).
            version (str): The version of the custom object (CRD).
            plural (str): The plural of the custom object (CRD).
            namespace (str): The namespace of the custom object (CRD).
            name (str): The name of the custom object (CRD).
            status (str): The status of the custom object (CRD).
            resource_label (str): The label of the custom object (CRD).
            
        Returns:
            None
        """
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        status_body = {"status": {"reachability": status, "lastProbeTime": now}}
        try:
            self.k8s_api.patch_namespaced_custom_object_status(
                group=group,
                version=version,
                name=name,
                namespace=namespace,
                plural=plural,
                body=status_body,
            )
            logger.info(f"Updated {resource_label} {namespace}/{name} status to: {status}")
        except ApiException as e:
            logger.error(f"Error updating status for {resource_label} {namespace}/{name}: {e}")
        except Exception as e:
            logger.error(
                f"Unexpected error updating status for {resource_label} {namespace}/{name}: {e}"
            )

    def _update_network_device_target_status(self, ndt_namespace: str, ndt_name: str, status: str):
        """Updates the status subresource of a NetworkDeviceTarget CRD."""
        self._patch_target_status(
            NETWORK_DEVICE_TARGET_CRD_GROUP,
            NETWORK_DEVICE_TARGET_CRD_VERSION,
            NETWORK_DEVICE_TARGET_CRD_PLURAL,
            ndt_namespace,
            ndt_name,
            status,
            "NetworkDeviceTarget",
        )

    def _update_network_host_target_status(self, nht_namespace: str, nht_name: str, status: str):
        """Updates the status subresource of a NetworkHostTarget CRD."""
        self._patch_target_status(
            NETWORK_HOST_TARGET_CRD_GROUP,
            NETWORK_HOST_TARGET_CRD_VERSION,
            NETWORK_HOST_TARGET_CRD_PLURAL,
            nht_namespace,
            nht_name,
            status,
            "NetworkHostTarget",
        )

    def _ensure_gnmic_target(self, ndt_obj: dict, create: bool):
        """
        Creates, updates, or deletes an operator.gnmic.dev/v1alpha1 Target resource
        based on the NetworkDeviceTarget.
        """
        ndt_name = ndt_obj["metadata"]["name"]
        ndt_namespace = ndt_obj["metadata"]["namespace"]
        gnmic_spec = ndt_obj["spec"].get("gnmic", {})
        common_labels = ndt_obj["spec"].get("commonLabels", {})

        target_name = ndt_name  # gNMIc Target name matches NDT name
        target_namespace = ndt_namespace

        if not gnmic_spec.get("enabled", True) or not create:
            # Delete the gNMIc Target if disabled or NDT is being deleted
            try:
                self.k8s_api.delete_namespaced_custom_object(
                    group=GNMIC_TARGET_CRD_GROUP,
                    version=GNMIC_TARGET_CRD_VERSION,
                    name=target_name,
                    namespace=target_namespace,
                    plural=GNMIC_TARGET_CRD_PLURAL,
                    body=client.V1DeleteOptions(),
                )
                logger.info(
                    f"Deleted gNMIc Target {target_namespace}/{target_name} for NetworkDeviceTarget {ndt_namespace}/{ndt_name}."
                )
            except ApiException as e:
                if e.status == 404:
                    logger.debug(
                        f"gNMIc Target {target_namespace}/{target_name} not found, no need to delete."
                    )
                else:
                    logger.error(
                        f"Error deleting gNMIc Target {target_namespace}/{target_name}: {e}"
                    )
            return

        # Construct gNMIc Target spec
        gnmic_target_address = (
            f"{ndt_obj['spec']['address']}:{gnmic_spec.get('port', '57400')}"  # Default gNMI port
        )

        profile = gnmic_spec.get("targetProfileRef", "default")

        # cleanup
        if isinstance(profile, str):
            profile = profile.strip()
        
        # reject empty or invalid format
        if not profile:
            raise ValueError("targetProfileRef cannot be empty")
            
        # format validation
        if " " in profile:
            raise ValueError(f"Invalid profile '{profile}' (contains spaces)")

        gnmic_target_profile = profile


        #gnmic_target_profile = gnmic_spec.get("targetProfileRef", "default")  # Default profile

        # Combine labels
        gnmic_labels = common_labels.copy()
        gnmic_labels.update(gnmic_spec.get("labels", {}))
        gnmic_labels["networkdevicetarget.nok.dev/name"] = ndt_name  # Link back to NDT

        gnmic_target_body = {
            "apiVersion": f"{GNMIC_TARGET_CRD_GROUP}/{GNMIC_TARGET_CRD_VERSION}",
            "kind": "Target",
            "metadata": {
                "name": target_name,
                "namespace": target_namespace,
                "labels": gnmic_labels,
            },
            "spec": {
                "address": gnmic_target_address,
                "profile": gnmic_target_profile,
                "credentialsSecretRef": gnmic_spec.get("credentialsSecretRef"),
            },
        }

        try:
            # Try to get the existing gNMIc Target
            existing_target = self.k8s_api.get_namespaced_custom_object(
                group=GNMIC_TARGET_CRD_GROUP,
                version=GNMIC_TARGET_CRD_VERSION,
                name=target_name,
                namespace=target_namespace,
                plural=GNMIC_TARGET_CRD_PLURAL,
            )
            # Update existing
            self.k8s_api.patch_namespaced_custom_object(
                group=GNMIC_TARGET_CRD_GROUP,
                version=GNMIC_TARGET_CRD_VERSION,
                name=target_name,
                namespace=target_namespace,
                plural=GNMIC_TARGET_CRD_PLURAL,
                body=gnmic_target_body,
            )
            logger.info(
                f"Updated gNMIc Target {target_namespace}/{target_name} for NetworkDeviceTarget {ndt_namespace}/{ndt_name}."
            )
        except ApiException as e:
            if e.status == 404:
                # Create new if not found
                self.k8s_api.create_namespaced_custom_object(
                    group=GNMIC_TARGET_CRD_GROUP,
                    version=GNMIC_TARGET_CRD_VERSION,
                    namespace=target_namespace,
                    plural=GNMIC_TARGET_CRD_PLURAL,
                    body=gnmic_target_body,
                )
                logger.info(
                    f"Created gNMIc Target {target_namespace}/{target_name} for NetworkDeviceTarget {ndt_namespace}/{ndt_name}."
                )
            else:
                logger.error(f"Error ensuring gNMIc Target {target_namespace}/{target_name}: {e}")

    def _ensure_sdcio_discovery_rule(self, ndt_obj: dict, create: bool):
        """
        Ensures the inv.sdcio.dev/v1alpha1 DiscoveryRule exists and contains the
        NetworkDeviceTarget's information.
        """
        ndt_name = ndt_obj["metadata"]["name"]
        ndt_namespace = ndt_obj["metadata"]["namespace"]
        sdcio_spec = ndt_obj["spec"].get("sdcio", {})
        common_labels = ndt_obj["spec"].get("commonLabels", {})

        discovery_rule_name = DEFAULT_DISCOVERY_RULE_NAME  # Use a fixed name for now
        discovery_rule_namespace = ndt_namespace  # DiscoveryRule in the same namespace as NDT

        # Define the target entry for the DiscoveryRule
        target_entry = {
            "address": ndt_obj["spec"]["address"],
            "hostName": ndt_obj["spec"]["hostname"],
            "labels": common_labels.copy(),  # Labels for this specific address entry
        }
        target_entry["labels"].update(sdcio_spec.get("labels", {}))
        target_entry["labels"]["networkdevicetarget.nok.dev/name"] = ndt_name  # Link back to NDT

        # When not creating (create=False) or SD-CIO disabled (spec.sdcio.enabled == false)
        if not sdcio_spec.get("enabled", True) or not create:
            # Remove the target from the DiscoveryRule if disabled or NDT is being deleted
            try:
                
                # Fetches the existing DiscoveryRule (fixed name dr-static, same namespace as NDT).
                existing_dr = self.k8s_api.get_namespaced_custom_object(
                    group=SDCIO_DISCOVERY_RULE_CRD_GROUP,
                    version=SDCIO_DISCOVERY_RULE_CRD_VERSION,
                    name=discovery_rule_name,
                    namespace=discovery_rule_namespace,
                    plural=SDCIO_DISCOVERY_RULE_CRD_PLURAL,
                )
                
                #Removes the target entry that matches this NDT’s address/hostName from spec.addresses.
                addresses = existing_dr["spec"].get("addresses", []) # type: ignore[reportGeneralTypeIssues]
                updated_addresses = [
                    addr
                    for addr in addresses
                    if not (
                        addr.get("address") == target_entry["address"]
                        and addr.get("hostName") == target_entry["hostName"]
                    )
                ]
        
                # Patches the DiscoveryRule (or does nothing if rule missing or entry not found).
                if len(addresses) != len(updated_addresses):
                    existing_dr["spec"]["addresses"] = updated_addresses # type: ignore[reportGeneralTypeIssues]
                    self.k8s_api.patch_namespaced_custom_object(
                        group=SDCIO_DISCOVERY_RULE_CRD_GROUP,
                        version=SDCIO_DISCOVERY_RULE_CRD_VERSION,
                        name=discovery_rule_name,
                        namespace=discovery_rule_namespace,
                        plural=SDCIO_DISCOVERY_RULE_CRD_PLURAL,
                        body=existing_dr,
                    )
                    logger.info(
                        f"Removed NetworkDeviceTarget {ndt_namespace}/{ndt_name} from DiscoveryRule {discovery_rule_namespace}/{discovery_rule_name}."
                    )
                else:
                    logger.debug(
                        f"NetworkDeviceTarget {ndt_namespace}/{ndt_name} not found in DiscoveryRule {discovery_rule_namespace}/{discovery_rule_name}, no action needed."
                    )
            except ApiException as e:
                if e.status == 404:
                    logger.debug(
                        f"DiscoveryRule {discovery_rule_namespace}/{discovery_rule_name} not found, no need to update."
                    )
                else:
                    logger.error(
                        f"Error removing NetworkDeviceTarget {ndt_namespace}/{ndt_name} from DiscoveryRule {discovery_rule_namespace}/{discovery_rule_name}: {e}"
                    )
            return
        
        # When creating (create=True) and SD-CIO enabled (spec.sdcio.enabled == true)
        # Construct SDCIO DiscoveryRule body
        sdcio_dr_body = {
            "apiVersion": f"{SDCIO_DISCOVERY_RULE_CRD_GROUP}/{SDCIO_DISCOVERY_RULE_CRD_VERSION}",
            "kind": "DiscoveryRule",
            "metadata": {
                "name": discovery_rule_name,
                "namespace": discovery_rule_namespace,
                "labels": {
                    "managed-by": "nokiagpt-controller",
                    "networkdevicetarget.nok.dev/managed": "true",
                },
            },
            "spec": {
                "period": DEFAULT_DISCOVERY_RULE_PERIOD,
                "concurrentScans": 1,  # Example value
                "defaultSchema": sdcio_spec.get("schema", {}),
                "targetConnectionProfiles": [
                    {
                        "credentials": sdcio_spec.get("credentialsSecretRef"),
                        "connectionProfile": sdcio_spec.get("connectionProfileRef"),
                        "syncProfile": sdcio_spec.get("syncProfileRef"),
                    }
                ],
                "targetTemplate": {
                    "labels": common_labels.copy()  # Labels for all targets created by this rule
                },
                "addresses": [],  # This will be populated dynamically
            },
        }
        sdcio_dr_body["spec"]["targetTemplate"]["labels"].update(sdcio_spec.get("labels", {}))

        try:
            existing_dr = self.k8s_api.get_namespaced_custom_object(
                group=SDCIO_DISCOVERY_RULE_CRD_GROUP,
                version=SDCIO_DISCOVERY_RULE_CRD_VERSION,
                name=discovery_rule_name,
                namespace=discovery_rule_namespace,
                plural=SDCIO_DISCOVERY_RULE_CRD_PLURAL,
            )
            # Update existing DiscoveryRule
            addresses = existing_dr["spec"].get("addresses", []) # type: ignore[reportGeneralTypeIssues]
            # Check if target_entry already exists to avoid duplicates
            found = False
            for i, addr in enumerate(addresses):
                if (
                    addr.get("address") == target_entry["address"]
                    and addr.get("hostName") == target_entry["hostName"]
                ):
                    addresses[i] = target_entry  # Update existing entry
                    found = True
                    break
            if not found:
                addresses.append(target_entry)
            existing_dr["spec"]["addresses"] = addresses # type: ignore[reportGeneralTypeIssues]

            # Patch other fields if necessary, or replace the whole spec
            # For simplicity, we'll just update addresses and ensure defaultSchema/profiles are present
            existing_dr["spec"]["defaultSchema"] = sdcio_dr_body["spec"]["defaultSchema"] # type: ignore[reportGeneralTypeIssues]
            existing_dr["spec"]["targetConnectionProfiles"] = sdcio_dr_body["spec"][ # type: ignore[reportGeneralTypeIssues]
                "targetConnectionProfiles"
            ]
            existing_dr["spec"]["targetTemplate"] = sdcio_dr_body["spec"]["targetTemplate"] # type: ignore[reportGeneralTypeIssues]
            existing_dr["spec"]["period"] = sdcio_dr_body["spec"]["period"] # type: ignore[reportGeneralTypeIssues]
            existing_dr["spec"]["concurrentScans"] = sdcio_dr_body["spec"]["concurrentScans"] # type: ignore[reportGeneralTypeIssues]

            self.k8s_api.patch_namespaced_custom_object(
                group=SDCIO_DISCOVERY_RULE_CRD_GROUP,
                version=SDCIO_DISCOVERY_RULE_CRD_VERSION,
                name=discovery_rule_name,
                namespace=discovery_rule_namespace,
                plural=SDCIO_DISCOVERY_RULE_CRD_PLURAL,
                body=existing_dr,
            )
            logger.info(
                f"Updated DiscoveryRule {discovery_rule_namespace}/{discovery_rule_name} with NetworkDeviceTarget {ndt_namespace}/{ndt_name}."
            )
        except ApiException as e:
            if e.status == 404:
                # Create new DiscoveryRule
                sdcio_dr_body["spec"]["addresses"] = [target_entry]
                self.k8s_api.create_namespaced_custom_object(
                    group=SDCIO_DISCOVERY_RULE_CRD_GROUP,
                    version=SDCIO_DISCOVERY_RULE_CRD_VERSION,
                    namespace=discovery_rule_namespace,
                    plural=SDCIO_DISCOVERY_RULE_CRD_PLURAL,
                    body=sdcio_dr_body,
                )
                logger.info(
                    f"Created DiscoveryRule {discovery_rule_namespace}/{discovery_rule_name} for NetworkDeviceTarget {ndt_namespace}/{ndt_name}."
                )
            else:
                logger.error(
                    f"Error ensuring DiscoveryRule {discovery_rule_namespace}/{discovery_rule_name}: {e}"
                )

    def _process_network_device_target_event(self, event_type, ndt_obj: dict):
        """Processes events for NetworkDeviceTarget CRDs."""
        ndt_name = ndt_obj["metadata"]["name"]
        ndt_namespace = ndt_obj["metadata"]["namespace"]
        ndt_address = ndt_obj["spec"].get("address")
        ndt_key = f"{ndt_namespace}/{ndt_name}"
        ndt_generation = ndt_obj["metadata"].get("generation", 0)

        if event_type == "MODIFIED":
            with self.network_device_targets_lock:
                last_gen = self.network_device_targets_for_reachability.get(ndt_key, {}).get(
                    "generation"
                )
            if last_gen is not None and last_gen == ndt_generation:
                logger.debug(
                    f"Skipping status-only MODIFIED for NetworkDeviceTarget {ndt_key} (generation={ndt_generation})"
                )
                return

        logger.info(f"Processing event: {event_type} for NetworkDeviceTarget {ndt_key}")

        # Update the HTTP service's registered hostnames
        http_service_hostname = (
            f"{ndt_name}.network-device.local"  # Generic hostname for HTTP service
        )
        with self.http_hostnames_lock:
            if event_type == "ADDED" or event_type == "MODIFIED":
                self.registered_http_hostnames.add(http_service_hostname)
            elif event_type == "DELETED":
                self.registered_http_hostnames.discard(http_service_hostname)

        if not ndt_address:
            logger.warning(
                f"NetworkDeviceTarget {ndt_key} has no 'spec.address'. Skipping processing."
            )
            with self.network_device_targets_lock:
                self.network_device_targets_for_reachability.pop(ndt_key, None)
            return

        with self.network_device_targets_lock:
            self.network_device_targets_for_reachability[ndt_key] = {
                "address": ndt_address,
                "last_status": ndt_obj.get("status", {}).get("reachability", "Unknown"),
                "generation": ndt_generation,
            }

        if event_type == "ADDED" or event_type == "MODIFIED":
            # Ensure gNMIc Target
            self._ensure_gnmic_target(ndt_obj, create=True)
            # Ensure SDCIO DiscoveryRule entry
            self._ensure_sdcio_discovery_rule(ndt_obj, create=True)
            # Perform initial reachability check and update status
            current_reachability = self._check_reachability(ndt_address)
            self._update_network_device_target_status(ndt_namespace, ndt_name, current_reachability)
            with self.network_device_targets_lock:
                if ndt_key in self.network_device_targets_for_reachability:
                    self.network_device_targets_for_reachability[ndt_key][
                        "last_status"
                    ] = current_reachability
                    
            with self.blackbox_targets_lock:
                self.blackbox_targets_cache[ndt_key] = {
                    "address": ndt_address,
                    "hostname": ndt_obj["spec"].get("hostname", ndt_name),
                    "namespace": ndt_namespace,
                    "kind": "device",
                    "commonLabels": ndt_obj["spec"].get("commonLabels", {}),
                    "gnmic_port": ndt_obj["spec"].get("gnmic", {}).get("port", 57400),
                }


        elif event_type == "DELETED":
            # Delete gNMIc Target
            self._ensure_gnmic_target(ndt_obj, create=False)
            # Remove from SDCIO DiscoveryRule, not discovery rule!
            self._ensure_sdcio_discovery_rule(ndt_obj, create=False)
            with self.blackbox_targets_lock:
                self.blackbox_targets_cache.pop(ndt_key, None)
            # Remove from reachability tracking
            with self.network_device_targets_lock:
                self.network_device_targets_for_reachability.pop(ndt_key, None)
            logger.info(f"Removed NetworkDeviceTarget {ndt_key} from tracking.")
        else:
            logger.warning(f"Unknown event type: {event_type} for NetworkDeviceTarget {ndt_key}")

    def _process_network_host_target_event(self, event_type, nht_obj: dict):
        """Processes events for NetworkHostTarget CRDs."""
        nht_name = nht_obj["metadata"]["name"]
        nht_namespace = nht_obj["metadata"]["namespace"]
        nht_address = nht_obj["spec"].get("address")
        nht_key = f"{nht_namespace}/{nht_name}"
        nht_generation = nht_obj["metadata"].get("generation", 0)

        if event_type == "MODIFIED":
            with self.network_host_targets_lock:
                last_gen = self.network_host_targets_for_reachability.get(nht_key, {}).get(
                    "generation"
                )
            if last_gen is not None and last_gen == nht_generation:
                logger.debug(
                    f"Skipping status-only MODIFIED for NetworkHostTarget {nht_key} (generation={nht_generation})"
                )
                return

        logger.info(f"Processing event: {event_type} for NetworkHostTarget {nht_key}")

        http_service_hostname = f"{nht_name}.network-host.local"
        with self.http_hostnames_lock:
            if event_type in ("ADDED", "MODIFIED"):
                self.registered_http_hostnames.add(http_service_hostname)
            elif event_type == "DELETED":
                self.registered_http_hostnames.discard(http_service_hostname)

        if not nht_address:
            logger.warning(
                f"NetworkHostTarget {nht_key} has no 'spec.address'. Skipping processing."
            )
            with self.network_host_targets_lock:
                self.network_host_targets_for_reachability.pop(nht_key, None)
            with self.blackbox_targets_lock:
                self.blackbox_targets_cache.pop(nht_key, None)
            return

        if event_type in ("ADDED", "MODIFIED"):
            ssh_port = nht_obj["spec"].get("ssh", {}).get("port", 22) # if not specified, default to 22

            with self.blackbox_targets_lock:
                self.blackbox_targets_cache[nht_key] = {
                    "address": nht_address,
                    "hostname": nht_obj["spec"].get("hostname", nht_name),
                    "namespace": nht_namespace,
                    "kind": "host",
                    "commonLabels": nht_obj["spec"].get("commonLabels", {}),
                    "ssh_port": ssh_port,
                }

            with self.network_host_targets_lock:
                self.network_host_targets_for_reachability[nht_key] = {
                    "address": nht_address,
                    "last_status": nht_obj.get("status", {}).get("reachability", "Unknown"),
                    "generation": nht_generation,
                }

            # Performing the reachability check
            current_reachability = self._check_reachability(nht_address)
            # Updating the status subresource of the NetworkHostTarget CRD
            self._update_network_host_target_status(nht_namespace, nht_name, current_reachability)
            # Updating the reachability status in the cache
            with self.network_host_targets_lock:
                if nht_key in self.network_host_targets_for_reachability:
                    self.network_host_targets_for_reachability[nht_key][
                        "last_status"
                    ] = current_reachability

        elif event_type == "DELETED":
            # Removing the target from the blackbox targets cache
            with self.blackbox_targets_lock:
                self.blackbox_targets_cache.pop(nht_key, None)
            # Removing the target from the network host targets cache
            with self.network_host_targets_lock:
                self.network_host_targets_for_reachability.pop(nht_key, None)
            logger.info(f"Removed NetworkHostTarget {nht_key} from tracking.")
        else:
            logger.warning(f"Unknown event type: {event_type} for NetworkHostTarget {nht_key}")

    def _watch_crd(self, crd_group, crd_version, crd_plural, processor_func):
        """Watches for events of a specific CRD type and calls the appropriate processor."""
        logger.info(f"Starting watch for CRD: {crd_group}/{crd_version}/{crd_plural}")
        while True:
            try:
                for event in self.k8s_watch.stream(
                    self.k8s_api.list_cluster_custom_object,
                    group=crd_group,
                    version=crd_version,
                    plural=crd_plural,
                    _preload_content=False,
                ):
                    event_type = event["type"] # type: ignore[reportGeneralTypeIssues]
                    logger.info(
                        f"Event type: {event_type}"
                    )
                    obj = event["object"] # type: ignore[reportGeneralTypeIssues]
                    logger.info(
                        "Object: %s",
                        json.dumps(obj, indent=2, default=str),
                    )
                    processor_func(event_type, obj)
            except client.ApiException as e:
                logger.error(
                    f"Kubernetes API error for {crd_plural} CRD: {e}. Retrying in 5 seconds."
                )
                time.sleep(5)
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred for {crd_plural} CRD: {e}. Retrying in 5 seconds."
                )
                time.sleep(5)

    def _reachability_loop(self):
        """Periodically checks reachability for all tracked targets."""
        logger.info(
            f"Starting reachability check loop with period: {REACHABILITY_CHECK_PERIOD_SECONDS} seconds."
        )
        while True:
            time.sleep(REACHABILITY_CHECK_PERIOD_SECONDS)
            logger.debug("Performing periodic reachability checks...")
            targets_to_check = {}
            with self.network_device_targets_lock:
                targets_to_check = self.network_device_targets_for_reachability.copy()

            for ndt_key, info in targets_to_check.items():
                ndt_namespace, ndt_name = ndt_key.split("/", 1)
                address = info["address"]
                current_status = self._check_reachability(address)

                if current_status != info["last_status"]:
                    logger.info(
                        f"Reachability status changed for {ndt_key}: {info['last_status']} -> {current_status}"
                    )
                    self._update_network_device_target_status(
                        ndt_namespace, ndt_name, current_status
                    )
                    with self.network_device_targets_lock:
                        if ndt_key in self.network_device_targets_for_reachability:
                            self.network_device_targets_for_reachability[ndt_key][
                                "last_status"
                            ] = current_status
                else:
                    logger.debug(f"Reachability status for {ndt_key} remains {current_status}.")
                    # Still update lastProbeTime even if status hasn't changed
                    self._update_network_device_target_status(
                        ndt_namespace, ndt_name, current_status
                    )

            # Check NetworkHostTargets
            nht_targets_to_check = {}
            with self.network_host_targets_lock:
                nht_targets_to_check = self.network_host_targets_for_reachability.copy()

            for nht_key, info in nht_targets_to_check.items():
                nht_namespace, nht_name = nht_key.split("/", 1)
                address = info["address"]
                current_status = self._check_reachability(address)

                if current_status != info["last_status"]:
                    logger.info(
                        f"Reachability status changed for {nht_key}: {info['last_status']} -> {current_status}"
                    )
                    self._update_network_host_target_status(nht_namespace, nht_name, current_status)
                    with self.network_host_targets_lock:
                        if nht_key in self.network_host_targets_for_reachability:
                            self.network_host_targets_for_reachability[nht_key][
                                "last_status"
                            ] = current_status
                else:
                    logger.debug(f"Reachability status for {nht_key} remains {current_status}.")
                    self._update_network_host_target_status(nht_namespace, nht_name, current_status)

    def _run_http_server(self):
        """Runs the Flask HTTP server."""
        logger.info(f"Starting HTTP server on {HTTP_SERVER_HOST}:{HTTP_SERVER_PORT}")
        self.app.run(host=HTTP_SERVER_HOST, port=HTTP_SERVER_PORT, debug=False)

    def run(self):
        """Starts watching for CRD events concurrently and runs the HTTP server."""
        # Only watch for NetworkDeviceTarget CRD events
        network_device_target_thread = threading.Thread(
            target=self._watch_crd,
            args=(
                NETWORK_DEVICE_TARGET_CRD_GROUP,
                NETWORK_DEVICE_TARGET_CRD_VERSION,
                NETWORK_DEVICE_TARGET_CRD_PLURAL,
                self._process_network_device_target_event,
            ),
            daemon=True,
        )

        # Only watch for NetworkHostTarget CRD events
        network_host_target_thread = threading.Thread(
            target=self._watch_crd,
            args=(
                NETWORK_HOST_TARGET_CRD_GROUP,
                NETWORK_HOST_TARGET_CRD_VERSION,
                NETWORK_HOST_TARGET_CRD_PLURAL,
                self._process_network_host_target_event,
            ),
            daemon=True,
        )

        reachability_thread = threading.Thread(target=self._reachability_loop, daemon=True)

        http_server_thread = threading.Thread(target=self._run_http_server, daemon=True)

        # network_device_target_thread.start()  # Start the NDT watch
        network_host_target_thread.start()  # Start the NHT watch
        reachability_thread.start()  # Start the reachability loop
        http_server_thread.start()

        # Join a CRD watching thread to keep the main thread alive
        network_device_target_thread.start()
        network_device_target_thread.join()
        # The http_server_thread and reachability_thread are daemon threads, so they will exit when the main program exits.


if __name__ == "__main__":
    controller = NetworkDeviceController()
    controller.run()
