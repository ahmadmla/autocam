import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import zmq
from flask import Flask
from flask_socketio import SocketIO
from difflib import SequenceMatcher
from metaphone import doublemetaphone

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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


MODULE_DIR = Path(__file__).resolve().parent
SCENE_PATH = Path(env_str("AUDIO_SCENE_PATH", str(MODULE_DIR / "scene.json")))
if not SCENE_PATH.is_absolute():
    SCENE_PATH = REPO_ROOT / SCENE_PATH
MQTT_HOST = env_str("MQTT_HOST", "127.0.0.1")
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_QOS = env_int("MQTT_QOS", 0)
TARGET_REQUEST_TOPIC = env_str("AUTOCAM_TARGET_REQUEST_TOPIC", "autocam/target/request")
AUDIO_EVENT_TOPIC = env_str("AUTOCAM_AUDIO_EVENT_TOPIC", "autocam/audio/event")
AUDIO_DASHBOARD_PORT = env_int("AUDIO_DASHBOARD_PORT", 5000)
AUDIO_STATE_PATH = Path(env_str("AUDIO_STATE_PATH", str(REPO_ROOT / "audio_state.json")))
if not AUDIO_STATE_PATH.is_absolute():
    AUDIO_STATE_PATH = REPO_ROOT / AUDIO_STATE_PATH

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

with SCENE_PATH.open(encoding="utf-8") as f:
    scene_doc = json.load(f)
scene = scene_doc["scene"]
actor_nodes = {
    actor: str(config.get("node_id", "")).strip()
    for actor, config in scene_doc.get("actors", {}).items()
    if isinstance(config, dict)
}
actor_home_flags = {
    actor: bool(config.get("home", False))
    for actor, config in scene_doc.get("actors", {}).items()
    if isinstance(config, dict)
}

# Collect all unique actor names from the scene
all_actors = list(dict.fromkeys(
    [cue["actor"] for cue in scene] +
    [cue["next_actor"] for cue in scene if cue["next_actor"]]
))

current_cue_index = 0
cue_fired = False
state_lock = threading.RLock()
mqtt_connected = False
latest_transcripts = {}
last_trigger = None
last_event_source = "startup"
initial_target_published = False

# Actor color map — add more actors here as needed
ACTOR_COLORS = {
    "Nora":     "#ff6b9d",
    "Torvald":  "#4a9eff",
    "Krogstad": "#f4a261",
    "Kristine": "#2ec4b6",
}


def current_actor_for_index(index: int) -> Optional[str]:
    if 0 <= index < len(scene):
        return scene[index]["actor"]
    return None


def next_actor_for_index(index: int) -> Optional[str]:
    if 0 <= index < len(scene):
        return scene[index].get("next_actor")
    return None


def write_audio_state() -> None:
    with state_lock:
        index = current_cue_index
        current_actor = current_actor_for_index(index)
        next_actor = next_actor_for_index(index)
        payload = {
            "timestamp": time.time(),
            "mqtt_connected": mqtt_connected,
            "cue_index": index,
            "cue_display_index": min(index, len(scene)),
            "cue_total": len(scene),
            "current_actor": current_actor,
            "current_node_id": actor_node_id(current_actor),
            "next_actor": next_actor,
            "next_node_id": actor_node_id(next_actor),
            "last_trigger": last_trigger,
            "last_event_source": last_event_source,
            "latest_transcripts": latest_transcripts,
            "actors": actor_nodes,
            "home_actors": actor_home_flags,
        }
    tmp_path = AUDIO_STATE_PATH.with_suffix(AUDIO_STATE_PATH.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    tmp_path.replace(AUDIO_STATE_PATH)


try:
    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="autocam_audio_dashboard",
    )
except AttributeError:
    mqtt_client = mqtt.Client(client_id="autocam_audio_dashboard")


def actor_node_id(actor: Optional[str]) -> Optional[str]:
    if not actor:
        return None
    node_id = actor_nodes.get(actor)
    if not node_id:
        print(f"[AUDIO] Missing node mapping for actor={actor}", flush=True)
        return None
    return node_id


def publish_target_request(actor: Optional[str], source: str) -> None:
    node_id = actor_node_id(actor)
    if not actor or not node_id:
        return
    payload = {
        "actor": actor,
        "node_id": node_id,
        "source": source,
        "ts": time.time(),
    }
    if actor_home_flags.get(actor, False) or node_id == "__center__":
        payload["home"] = True
    mqtt_client.publish(
        TARGET_REQUEST_TOPIC,
        json.dumps(payload, separators=(",", ":"), allow_nan=False),
        qos=MQTT_QOS,
        retain=False,
    )
    print(f"[TARGET] request actor={actor} node={node_id} source={source}", flush=True)
    write_audio_state()


def publish_initial_target_request(source: str = "audio_cue_0") -> None:
    global current_cue_index, initial_target_published, last_event_source
    if not scene:
        return
    with state_lock:
        current_actor = scene[0]["actor"]
        current_cue_index = 0
        initial_target_published = True
        last_event_source = source
    print(f"[CUE/0] initial actor={current_actor}", flush=True)
    publish_target_request(current_actor, source=source)


def on_mqtt_connect(client, userdata, flags, reason_code, properties=None):
    global mqtt_connected
    mqtt_connected = True
    print(f"[MQTT] connected host={MQTT_HOST} port={MQTT_PORT} rc={reason_code}", flush=True)
    client.subscribe(AUDIO_EVENT_TOPIC, qos=MQTT_QOS)
    print(f"[MQTT] subscribed topic={AUDIO_EVENT_TOPIC}", flush=True)
    write_audio_state()
    publish_initial_target_request()


def on_mqtt_disconnect(client, userdata, disconnect_flags=None, reason_code=None, properties=None):
    global mqtt_connected
    mqtt_connected = False
    print(f"[MQTT] disconnected rc={reason_code}", flush=True)
    write_audio_state()


def on_mqtt_message(client, userdata, msg):
    if msg.topic != AUDIO_EVENT_TOPIC:
        return
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as exc:
        print(f"[AUDIO] event parse error: {exc}", flush=True)
        return
    if payload.get("event") != "actor_done":
        print(f"[AUDIO] ignored event={payload.get('event')}", flush=True)
        return
    actor = str(payload.get("actor", "")).strip()
    if not actor:
        print("[AUDIO] actor_done ignored reason=missing_actor", flush=True)
        return
    handle_actor_done(actor, source=str(payload.get("source", "audio_node")))


mqtt_client.on_connect = on_mqtt_connect
mqtt_client.on_disconnect = on_mqtt_disconnect
mqtt_client.on_message = on_mqtt_message
mqtt_client.reconnect_delay_set(min_delay=1, max_delay=10)


def fuzzy_match(phrase, text, threshold=0.80):
    words = text.split()
    phrase_words = phrase.split()
    n = len(phrase_words)
    if n == 0:
        return False
    for i in range(max(1, len(words) - n + 1)):
        chunk = " ".join(words[i:i+n])
        ratio = SequenceMatcher(None, phrase, chunk).ratio()
        if ratio >= threshold:
            return True
    return False


def phonetic_match(phrase, text, threshold=0.75):
    phrase_words = phrase.lower().split()
    text_words = text.lower().split()
    phrase_codes = [doublemetaphone(w)[0] for w in phrase_words]
    text_codes = [doublemetaphone(w)[0] for w in text_words]
    n = len(phrase_codes)
    if n == 0:
        return False
    for i in range(max(1, len(text_codes) - n + 1)):
        window = text_codes[i:i+n]
        matches = sum(
            1 for p, t in zip(phrase_codes, window) if p and t and p == t
        )
        if matches / n >= threshold:
            return True
    return False


def fire_cue(cue_index, source="AUTO"):
    """Fire a cue by index and advance the system state."""
    global current_cue_index, cue_fired, last_trigger, last_event_source
    with state_lock:
        cue = scene[cue_index]
        next_index = cue_index + 1
        next_actor = cue["next_actor"]
        print(f"[CUE/{source}] '{cue['trigger']}' -> next: {next_actor}", flush=True)
        cue_fired = False
        current_cue_index = next_index
        last_trigger = cue["trigger"]
        last_event_source = source
        socketio.emit("cue_fired", {
            "fired_index": cue_index,
            "next_index": next_index,
            "next_actor": next_actor,
            "trigger": cue["trigger"]
        })
    write_audio_state()
    if next_actor:
        publish_target_request(next_actor, source=f"audio_{source.lower()}")


def handle_actor_done(actor: str, source="audio_node"):
    global cue_fired
    with state_lock:
        if current_cue_index >= len(scene):
            print(f"[AUDIO] actor_done ignored actor={actor} reason=end_of_scene", flush=True)
            return
        cue = scene[current_cue_index]
        if actor != cue["actor"]:
            print(
                f"[AUDIO] actor_done ignored actor={actor} expected={cue['actor']} source={source}",
                flush=True,
            )
            return
        cue_index = current_cue_index
        cue_fired = True
    print(f"[AUDIO] actor_done accepted actor={actor} source={source}", flush=True)
    fire_cue(cue_index, source="AUDIO_DONE")


def check_trigger(text, actor, is_final):
    global cue_fired
    with state_lock:
        if cue_fired:
            return
        if current_cue_index >= len(scene):
            return

        cue = scene[current_cue_index]

        # Only process messages from the currently active actor
        if actor != cue["actor"]:
            return

    trigger = cue["trigger"].lower()
    fuzzy_hit = fuzzy_match(trigger, text.lower())
    phonetic_hit = phonetic_match(trigger, text.lower())

    if fuzzy_hit or phonetic_hit:
        with state_lock:
            cue_fired = True
            cue_index = current_cue_index
        method = "fuzzy" if fuzzy_hit else "phonetic"
        source = f"{'FINAL' if is_final else 'PARTIAL'}/{method}"
        fire_cue(cue_index, source=source)


@socketio.on('manual_override')
def handle_manual_override():
    global cue_fired
    with state_lock:
        if current_cue_index >= len(scene):
            print("[OVERRIDE] Already at end of scene.", flush=True)
            return
        cue_fired = False
        cue_index = current_cue_index
    print(f"[OVERRIDE] Director manually advanced cue {cue_index}", flush=True)
    fire_cue(cue_index, source="OVERRIDE")


HTML = """
<!DOCTYPE html>
<html>
<head>
<title>Camera Cue System</title>
<script src="/static/socket.io.min.js"></script>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body {
    font-family: -apple-system, sans-serif;
    background: #0d0d0d;
    color: #fff;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    padding: 32px;
    gap: 24px;
}
.header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
}
.header-left { display: flex; flex-direction: column; gap: 4px; }
.header-right { display: flex; align-items: center; gap: 12px; }
.camera-label {
    font-size: 13px;
    color: #555;
    text-transform: uppercase;
    letter-spacing: 2px;
}
.actor-display {
    font-size: 64px;
    font-weight: 700;
    transition: all 0.5s ease;
}
.next-badge {
    font-size: 13px;
    color: #555;
    background: #111;
    border: 1px solid #1e1e1e;
    border-radius: 6px;
    padding: 6px 14px;
}
.override-btn {
    background: #1a1a1a;
    color: #ff6b6b;
    border: 1px solid #ff6b6b;
    border-radius: 8px;
    padding: 10px 20px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s ease;
    letter-spacing: 0.5px;
}
.override-btn:hover {
    background: #ff6b6b;
    color: #000;
}
.override-btn:active { transform: scale(0.97); }
.override-btn:disabled {
    opacity: 0.3;
    cursor: not-allowed;
    border-color: #555;
    color: #555;
}
.override-btn:disabled:hover {
    background: #1a1a1a;
    color: #555;
}
.divider { border: none; border-top: 1px solid #1e1e1e; }
.script-box {
    background: #111;
    border: 1px solid #1e1e1e;
    border-radius: 12px;
    padding: 24px;
    display: flex;
    flex-direction: column;
    gap: 16px;
    flex: 1;
    overflow-y: auto;
    max-height: 50vh;
}
.speech-block {
    padding: 16px;
    border-radius: 8px;
    border-left: 3px solid transparent;
    transition: all 0.4s ease;
    opacity: 0.3;
}
.speech-block.done { opacity: 0.2; }
.speech-block.active { opacity: 1; background: #181818; }
.speech-block.upcoming { opacity: 0.4; }
.actor-name {
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 6px;
}
.speech-text { font-size: 15px; line-height: 1.7; color: #aaa; }
.speech-block.active .speech-text { color: #fff; }
.trigger-badge {
    display: inline-block;
    margin-top: 8px;
    font-size: 11px;
    color: #444;
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-radius: 4px;
    padding: 2px 8px;
}
.actor-feeds {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
}
.actor-feed {
    flex: 1;
    min-width: 180px;
    background: #111;
    border: 1px solid #1e1e1e;
    border-radius: 8px;
    padding: 12px 16px;
}
.feed-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 6px;
}
.feed-text { color: #555; font-size: 13px; min-height: 18px; }
.status-bar {
    font-size: 12px;
    color: #333;
    text-align: center;
}
.status-bar span { color: #555; }
</style>
</head>
<body>

<div class="header">
    <div class="header-left">
        <div class="camera-label">Camera on</div>
        <div class="actor-display" id="actor-display">ACTOR_PLACEHOLDER</div>
    </div>
    <div class="header-right">
        <div class="next-badge" id="next-badge">Next: NEXT_PLACEHOLDER</div>
        <button class="override-btn" id="override-btn" onclick="manualOverride()">
            ⚡ Override
        </button>
    </div>
</div>

<hr class="divider">

<div class="script-box" id="script-box"></div>

<div class="actor-feeds" id="actor-feeds"></div>

<div class="status-bar">
    Cue <span id="cue-counter">1</span> of <span id="cue-total">TOTAL_PLACEHOLDER</span>
    &nbsp;·&nbsp;
    Last trigger: <span id="last-trigger">—</span>
</div>

<script>
const scene = SCENE_PLACEHOLDER;
const actorColors = COLORS_PLACEHOLDER;
const allActors = ACTORS_PLACEHOLDER;
let currentIndex = 0;

function getColor(actor) {
    return actorColors[actor] || '#aaaaaa';
}

// Build mic feed panels
const feedsEl = document.getElementById('actor-feeds');
allActors.forEach(function(actor) {
    const div = document.createElement('div');
    div.className = 'actor-feed';
    div.style.borderLeft = '3px solid ' + getColor(actor);
    div.innerHTML =
        '<div class="feed-label" style="color:' + getColor(actor) + '">' + actor + ' mic</div>' +
        '<div class="feed-text" id="feed-' + actor + '">listening...</div>';
    feedsEl.appendChild(div);
});

function renderScript() {
    const box = document.getElementById('script-box');
    box.innerHTML = '';
    scene.forEach(function(cue, i) {
        const color = getColor(cue.actor);
        const div = document.createElement('div');
        div.className = 'speech-block';
        div.id = 'block-' + i;
        div.style.borderLeftColor = i === currentIndex ? color : 'transparent';
        if (i < currentIndex) div.classList.add('done');
        else if (i === currentIndex) div.classList.add('active');
        else div.classList.add('upcoming');
        div.innerHTML =
            '<div class="actor-name" style="color:' + color + '">' + cue.actor + '</div>' +
            '<div class="speech-text">' + cue.lines + '</div>' +
            '<div class="trigger-badge">trigger: "' + cue.trigger + '"</div>';
        box.appendChild(div);
    });

    // Auto-scroll active block into view
    const active = document.getElementById('block-' + currentIndex);
    if (active) active.scrollIntoView({ behavior: 'smooth', block: 'center' });

    // Update cue counter
    document.getElementById('cue-counter').textContent = currentIndex + 1;

    // Disable override button at end of scene
    const btn = document.getElementById('override-btn');
    btn.disabled = currentIndex >= scene.length;
}

// Set initial display color
const firstActor = scene[0].actor;
document.getElementById('actor-display').style.color = getColor(firstActor);
document.getElementById('cue-total').textContent = scene.length;
renderScript();

const socket = io();

socket.on('transcript', function(data) {
    const el = document.getElementById('feed-' + data.actor);
    if (el) el.textContent = data.text;
});

socket.on('cue_fired', function(data) {
    currentIndex = data.next_index;
    const display = document.getElementById('actor-display');
    const nextBadge = document.getElementById('next-badge');

    if (data.next_actor) {
        display.textContent = data.next_actor;
        display.style.color = getColor(data.next_actor);
        const afterNext = scene[data.next_index + 1];
        nextBadge.textContent = afterNext ? 'Next: ' + afterNext.actor : 'Final scene';
    } else {
        display.textContent = 'Scene end';
        display.style.color = '#555';
        nextBadge.textContent = '';
    }

    if (data.trigger) {
        document.getElementById('last-trigger').textContent = '"' + data.trigger + '"';
    }

    renderScript();
});

function manualOverride() {
    socket.emit('manual_override');
}
</script>
</body>
</html>
"""


@app.route("/")
def index():
    first_actor = scene[0]["actor"]
    second_actor = scene[1]["actor"] if len(scene) > 1 else ""
    html = HTML
    html = html.replace("SCENE_PLACEHOLDER", json.dumps(scene))
    html = html.replace("COLORS_PLACEHOLDER", json.dumps(ACTOR_COLORS))
    html = html.replace("ACTORS_PLACEHOLDER", json.dumps(all_actors))
    html = html.replace("ACTOR_PLACEHOLDER", first_actor)
    html = html.replace("NEXT_PLACEHOLDER", second_actor)
    html = html.replace("TOTAL_PLACEHOLDER", str(len(scene)))
    return html


def zmq_listener():
    context = zmq.Context()
    sock = context.socket(zmq.PULL)
    sock.bind("tcp://*:5555")
    print("Central cue engine listening for actor nodes...", flush=True)

    while True:
        msg = sock.recv_json()
        actor = msg["actor"]
        text = msg["text"]
        is_final = msg["type"] == "final"

        if is_final:
            print(f"[{actor}] FINAL: {text}", flush=True)

        with state_lock:
            latest_transcripts[actor] = {
                "text": text,
                "type": msg.get("type"),
                "timestamp": time.time(),
            }
        write_audio_state()
        socketio.emit("transcript", {"actor": actor, "text": text})
        check_trigger(text, actor, is_final)


if __name__ == "__main__":
    write_audio_state()
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    mqtt_client.loop_start()
    zmq_thread = threading.Thread(target=zmq_listener, daemon=True)
    zmq_thread.start()
    print(f"Open http://<central-pi-ip>:{AUDIO_DASHBOARD_PORT} in your browser", flush=True)
    try:
        socketio.run(
            app,
            host="0.0.0.0",
            port=AUDIO_DASHBOARD_PORT,
            debug=False,
            allow_unsafe_werkzeug=True,
        )
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
