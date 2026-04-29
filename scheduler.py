# central_scheduler.py

from collections import deque
import json
import os
import threading
import time
from typing import Deque, Dict, Optional

from env_loader import load_repo_env


load_repo_env()

import paho.mqtt.client as mqtt


def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except ValueError:
        return default


MQTT_HOST = env_str("MQTT_HOST", "127.0.0.1")
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_QOS = env_int("MQTT_QOS", 0)

RAW_TOPIC = "uwb/raw/+"
CMD_TOPIC_BASE = "uwb/cmd"
TARGET_SELECT_TOPIC = env_str("AUTOCAM_TARGET_SELECT_TOPIC", "autocam/target/select")
TARGET_REQUEST_TOPIC = env_str("AUTOCAM_TARGET_REQUEST_TOPIC", "autocam/target/request")
PUBLISH_TARGET_SELECT = bool(env_int("SCHEDULER_PUBLISH_TARGET_SELECT", 0))
RETAIN_TARGET_SELECT = bool(env_int("SCHEDULER_RETAIN_TARGET_SELECT", 1))
SCHEDULER_MODE = env_str("SCHEDULER_MODE", "round_robin").strip().lower()
if SCHEDULER_MODE not in {"round_robin", "audio_scene"}:
    SCHEDULER_MODE = "round_robin"

NODES = [
    node_id.strip()
    for node_id in env_str("UWB_NODES", "rpi1,rpi3").split(",")
    if node_id.strip()
]
SINGLE_NODE_MODE = len(NODES) == 1
SLOT_TIMEOUT_S = env_float("SLOT_TIMEOUT_S", 6.0)
INTER_SLOT_GAP_S = env_float("INTER_SLOT_GAP_S", 0.30)
TIMEOUT_SKIP_THRESHOLD = env_int("TIMEOUT_SKIP_THRESHOLD", 2)
TIMEOUT_COOLDOWN_S = env_float("TIMEOUT_COOLDOWN_S", 5.0)
TOKEN_RESPONSE_LIMIT = env_int("TOKEN_RESPONSE_LIMIT", 256)
BURST_ACTIVE_S = env_float("BURST_ACTIVE_S", 0.50)
BURST_RESPONSE_TIMEOUT_S = env_float("BURST_RESPONSE_TIMEOUT_S", 0.75)
BURST_POST_PAUSE_GRACE_S = env_float("BURST_POST_PAUSE_GRACE_S", 0.05)
TARGET_REQUEST_MAX_AGE_S = env_float("TARGET_REQUEST_MAX_AGE_S", 10.0)
CENTER_NODE_ID = "__center__"


class RoundRobinScheduler:
    def __init__(self) -> None:
        self.cv = threading.Condition()
        self.responses: Dict[str, dict] = {}
        self.token_responses: Dict[str, dict] = {}
        self.token_order = deque()
        self.running = True
        self.token_counter = 0
        self.consecutive_timeouts: Dict[str, int] = {node_id: 0 for node_id in NODES}
        self.cooldown_until: Dict[str, float] = {node_id: 0.0 for node_id in NODES}
        self.target_requests: Deque[dict] = deque()
        self.current_audio_node: Optional[str] = None
        self.current_audio_token: Optional[str] = None

        try:
            self.client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id="central_scheduler",
            )
        except AttributeError:
            self.client = mqtt.Client(client_id="central_scheduler")

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        print(f"scheduler mqtt connected rc={reason_code}", flush=True)
        client.subscribe(RAW_TOPIC, qos=MQTT_QOS)
        print(f"scheduler subscribed topic={RAW_TOPIC}", flush=True)
        if SCHEDULER_MODE == "audio_scene":
            client.subscribe(TARGET_REQUEST_TOPIC, qos=MQTT_QOS)
            print(f"scheduler subscribed topic={TARGET_REQUEST_TOPIC}", flush=True)

    def on_disconnect(self, client, userdata, disconnect_flags=None, reason_code=None, properties=None):
        print(f"scheduler mqtt disconnected rc={reason_code}", flush=True)

    def on_message(self, client, userdata, msg):
        topic = getattr(msg, "topic", "")
        if topic == TARGET_REQUEST_TOPIC:
            self.on_target_request(msg)
            return

        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            node_id = str(payload["node_id"])
            slot_token = payload.get("slot_token")
            with self.cv:
                self.responses[node_id] = payload
                if isinstance(slot_token, str) and slot_token:
                    if slot_token not in self.token_responses:
                        self.token_order.append(slot_token)
                    self.token_responses[slot_token] = payload
                    while len(self.token_order) > TOKEN_RESPONSE_LIMIT:
                        expired = self.token_order.popleft()
                        self.token_responses.pop(expired, None)
                self.cv.notify_all()
        except Exception as exc:
            print(f"scheduler message parse error: {exc}", flush=True)

    def on_target_request(self, msg) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception as exc:
            print(f"target request parse error: {exc}", flush=True)
            return

        node_id = str(payload.get("node_id", "")).strip()
        actor = str(payload.get("actor", "")).strip()
        source = str(payload.get("source", "")).strip() or "unknown"
        home = bool(payload.get("home", False)) or node_id == CENTER_NODE_ID
        if not node_id:
            print(
                f"target request ignored reason=missing_node actor={actor or '-'} source={source}",
                flush=True,
            )
            return
        if not home and node_id not in NODES:
            print(
                f"target request ignored reason=unknown_node node={node_id} actor={actor or '-'} source={source}",
                flush=True,
            )
            return
        ts = payload.get("ts")
        if isinstance(ts, (int, float)) and TARGET_REQUEST_MAX_AGE_S > 0.0:
            age_s = time.time() - float(ts)
            if age_s > TARGET_REQUEST_MAX_AGE_S:
                print(
                    f"target request ignored reason=stale node={node_id} actor={actor or '-'} "
                    f"age={age_s:.2f}s max_age={TARGET_REQUEST_MAX_AGE_S:.2f}s",
                    flush=True,
                )
                return

        request = {
            "node_id": node_id,
            "actor": actor,
            "source": source,
            "home": home,
            "ts": ts if isinstance(ts, (int, float)) else time.time(),
        }
        with self.cv:
            self.target_requests.append(request)
            while len(self.target_requests) > TOKEN_RESPONSE_LIMIT:
                self.target_requests.popleft()
            self.cv.notify_all()
        print(
            f"target request queued node={node_id} actor={actor or '-'} home={int(home)} source={source}",
            flush=True,
        )

    def start(self) -> None:
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
        self.client.loop_start()

    def stop(self) -> None:
        self.running = False
        with self.cv:
            self.cv.notify_all()
        try:
            self.client.loop_stop()
        finally:
            self.client.disconnect()

    def next_token(self, node_id: str) -> str:
        self.token_counter += 1
        return f"{node_id}-{self.token_counter}-{int(time.time() * 1000)}"

    def publish_activate(
        self,
        node_id: str,
        token: str,
        *,
        continuous: bool,
        publish_select: Optional[bool] = None,
    ) -> None:
        topic = f"{CMD_TOPIC_BASE}/{node_id}"
        payload = {
            "cmd": "activate",
            "token": token,
            "ts": time.time(),
            "continuous": continuous,
        }
        self.client.publish(
            topic,
            json.dumps(payload, separators=(",", ":"), allow_nan=False),
            qos=MQTT_QOS,
            retain=False,
        )
        if PUBLISH_TARGET_SELECT if publish_select is None else publish_select:
            self.publish_target_select(node_id, token)

    def publish_target_select(
        self,
        node_id: str,
        token: str,
        *,
        actor: Optional[str] = None,
        source: str = "scheduler",
    ) -> None:
        payload = {
            "node_id": node_id,
            "source": source,
            "slot_token": token,
            "ts": time.time(),
        }
        if actor:
            payload["actor"] = actor
        self.client.publish(
            TARGET_SELECT_TOPIC,
            json.dumps(payload, separators=(",", ":"), allow_nan=False),
            qos=MQTT_QOS,
            retain=RETAIN_TARGET_SELECT,
        )

    def publish_home_select(self, *, actor: Optional[str], source: str) -> None:
        token = self.next_token(CENTER_NODE_ID)
        payload = {
            "node_id": CENTER_NODE_ID,
            "actor": actor or "Center",
            "source": source,
            "slot_token": token,
            "home": True,
            "ts": time.time(),
        }
        self.client.publish(
            TARGET_SELECT_TOPIC,
            json.dumps(payload, separators=(",", ":"), allow_nan=False),
            qos=MQTT_QOS,
            retain=RETAIN_TARGET_SELECT,
        )

    def publish_pause(self, node_id: str) -> None:
        topic = f"{CMD_TOPIC_BASE}/{node_id}"
        payload = {"cmd": "pause", "ts": time.time()}
        self.client.publish(
            topic,
            json.dumps(payload, separators=(",", ":"), allow_nan=False),
            qos=MQTT_QOS,
            retain=False,
        )

    def wait_for_slot(self, node_id: str, token: str, timeout_s: float) -> Optional[dict]:
        deadline = time.monotonic() + timeout_s
        with self.cv:
            while self.running:
                matched = self.take_slot_response_locked(node_id, token)
                if matched is not None:
                    return matched

                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return None
                self.cv.wait(timeout=remaining)
        return None

    def take_slot_response(self, node_id: str, token: str) -> Optional[dict]:
        with self.cv:
            return self.take_slot_response_locked(node_id, token)

    def take_slot_response_locked(self, node_id: str, token: str) -> Optional[dict]:
        matched = self.token_responses.pop(token, None)
        if matched is not None:
            return matched

        latest = self.responses.get(node_id)
        if latest is not None and latest.get("slot_token") == token:
            return latest

        return None

    def pause_inactive_nodes(self, target_node_id: str) -> None:
        for node_id in NODES:
            if node_id == target_node_id:
                continue
            self.publish_pause(node_id)
            print(
                f"audio_scene pause_inactive node={node_id} target_node={target_node_id}",
                flush=True,
            )

    def wait_for_target_request(self) -> Optional[dict]:
        with self.cv:
            while self.running:
                if self.target_requests:
                    return self.target_requests.popleft()
                self.cv.wait(timeout=0.5)
        return None

    def run_audio_scene(self) -> None:
        print(
            f"audio_scene waiting target_request_topic={TARGET_REQUEST_TOPIC}",
            flush=True,
        )
        while self.running:
            request = self.wait_for_target_request()
            if request is None:
                continue

            node_id = request["node_id"]
            actor = request.get("actor") or ""
            if bool(request.get("home", False)) or node_id == CENTER_NODE_ID:
                for pause_node_id in NODES:
                    self.publish_pause(pause_node_id)
                    print(
                        f"audio_scene pause_for_home node={pause_node_id} actor={actor or 'Center'}",
                        flush=True,
                    )
                self.current_audio_node = None
                self.current_audio_token = None
                self.publish_home_select(actor=actor, source="scheduler_audio_scene")
                print(f"audio_scene home actor={actor or 'Center'}", flush=True)
                continue
            if node_id == self.current_audio_node:
                self.pause_inactive_nodes(node_id)
                token = self.current_audio_token or self.next_token(node_id)
                self.current_audio_token = token
                self.publish_target_select(
                    node_id,
                    token,
                    actor=actor,
                    source="scheduler_audio_scene",
                )
                print(
                    f"audio_scene keep node={node_id} actor={actor or '-'} token={token}",
                    flush=True,
                )
                continue

            if self.current_audio_node is not None:
                self.publish_pause(self.current_audio_node)
                print(
                    f"audio_scene pause node={self.current_audio_node} "
                    f"next_node={node_id} actor={actor or '-'}",
                    flush=True,
                )

            self.pause_inactive_nodes(node_id)
            token = self.next_token(node_id)
            self.current_audio_node = node_id
            self.current_audio_token = token
            self.publish_activate(
                node_id,
                token,
                continuous=True,
                publish_select=False,
            )
            self.publish_target_select(
                node_id,
                token,
                actor=actor,
                source="scheduler_audio_scene",
            )
            print(
                f"audio_scene activate node={node_id} actor={actor or '-'} token={token}",
                flush=True,
            )

    def run(self) -> None:
        if SCHEDULER_MODE == "audio_scene":
            self.run_audio_scene()
            return

        if SINGLE_NODE_MODE:
            node_id = NODES[0]
            token = self.next_token(node_id)
            self.publish_activate(node_id, token, continuous=True)
            print(f"single-node activate node={node_id} token={token}", flush=True)

            payload = self.wait_for_slot(node_id, token, SLOT_TIMEOUT_S)
            if payload is None:
                print(f"single-node activate_timeout node={node_id} token={token}", flush=True)
            else:
                seq = payload.get("sequence_number")
                print(f"single-node running node={node_id} seq={seq} token={token}", flush=True)

            while self.running:
                time.sleep(max(INTER_SLOT_GAP_S, 0.25))
            return

        while self.running:
            now = time.monotonic()
            ready_nodes = [
                node_id
                for node_id in NODES
                if now >= self.cooldown_until.get(node_id, 0.0)
            ]
            if not ready_nodes:
                next_ready = min(self.cooldown_until.values(), default=now)
                sleep_for = max(0.05, next_ready - now)
                print(
                    f"scheduler waiting cooldown sleep={sleep_for:.2f}s",
                    flush=True,
                )
                time.sleep(sleep_for)
                continue

            for node_id in ready_nodes:
                if not self.running:
                    return

                token = self.next_token(node_id)
                burst_started_monotonic = time.monotonic()
                burst_deadline_monotonic = burst_started_monotonic + BURST_ACTIVE_S
                self.publish_activate(node_id, token, continuous=True)
                print(
                    f"slot activate node={node_id} token={token} burst={BURST_ACTIVE_S:.2f}s",
                    flush=True,
                )

                initial_wait_s = min(
                    SLOT_TIMEOUT_S,
                    BURST_RESPONSE_TIMEOUT_S,
                    max(0.0, burst_deadline_monotonic - time.monotonic()),
                )
                payload = self.wait_for_slot(node_id, token, initial_wait_s)

                remaining_burst_s = burst_deadline_monotonic - time.monotonic()
                if remaining_burst_s > 0.0:
                    time.sleep(remaining_burst_s)

                if payload is None:
                    payload = self.take_slot_response(node_id, token)

                self.publish_pause(node_id)
                print(f"slot pause node={node_id} token={token}", flush=True)
                if payload is None and BURST_POST_PAUSE_GRACE_S > 0.0:
                    payload = self.wait_for_slot(
                        node_id,
                        token,
                        BURST_POST_PAUSE_GRACE_S,
                    )

                if payload is None:
                    self.consecutive_timeouts[node_id] = (
                        self.consecutive_timeouts.get(node_id, 0) + 1
                    )
                    print(
                        f"slot timeout node={node_id} token={token} "
                        f"burst={BURST_ACTIVE_S:.2f}s",
                        flush=True,
                    )
                    if (not SINGLE_NODE_MODE) and self.consecutive_timeouts[node_id] >= TIMEOUT_SKIP_THRESHOLD:
                        cooldown_deadline = time.monotonic() + TIMEOUT_COOLDOWN_S
                        self.cooldown_until[node_id] = cooldown_deadline
                        print(
                            f"node cooldown node={node_id} "
                            f"timeouts={self.consecutive_timeouts[node_id]} "
                            f"cooldown={TIMEOUT_COOLDOWN_S:.2f}s",
                            flush=True,
                        )
                else:
                    self.consecutive_timeouts[node_id] = 0
                    self.cooldown_until[node_id] = 0.0
                    seq = payload.get("sequence_number")
                    print(
                        f"slot done node={node_id} seq={seq} token={token} "
                        f"burst={BURST_ACTIVE_S:.2f}s",
                        flush=True,
                    )

                if INTER_SLOT_GAP_S > 0.0:
                    time.sleep(INTER_SLOT_GAP_S)


def main() -> None:
    scheduler = RoundRobinScheduler()
    print(
        f"scheduler start host={MQTT_HOST} port={MQTT_PORT} "
        f"mode={SCHEDULER_MODE} "
        f"nodes={','.join(NODES)} slot_timeout={SLOT_TIMEOUT_S:.2f}s "
        f"gap={INTER_SLOT_GAP_S:.2f}s "
        f"burst={BURST_ACTIVE_S:.2f}s "
        f"burst_timeout={BURST_RESPONSE_TIMEOUT_S:.2f}s "
        f"burst_grace={BURST_POST_PAUSE_GRACE_S:.2f}s "
        f"single_node={SINGLE_NODE_MODE} "
        f"publish_target_select={PUBLISH_TARGET_SELECT} "
        f"retain_target_select={RETAIN_TARGET_SELECT} "
        f"timeout_skip={TIMEOUT_SKIP_THRESHOLD} "
        f"cooldown={TIMEOUT_COOLDOWN_S:.2f}s",
        flush=True,
    )
    scheduler.start()
    try:
        scheduler.run()
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.stop()


if __name__ == "__main__":
    main()
