import json
import threading
import zmq
from flask import Flask, render_template_string
from flask_socketio import SocketIO
from difflib import SequenceMatcher
from metaphone import doublemetaphone

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

with open("scene.json") as f:
    scene = json.load(f)["scene"]

# Collect all unique actor names from the scene
all_actors = list(dict.fromkeys(
    [cue["actor"] for cue in scene] +
    [cue["next_actor"] for cue in scene if cue["next_actor"]]
))

current_cue_index = 0
cue_fired = False

# Actor color map — add more actors here as needed
ACTOR_COLORS = {
    "Nora":     "#ff6b9d",
    "Torvald":  "#4a9eff",
    "Krogstad": "#f4a261",
    "Kristine": "#2ec4b6",
}


def send_motor_command(actor):
    """
    Send pivot command to motor controller.
    Replace this with your actual motor controller integration —
    either an HTTP request or a ZeroMQ message to your teammate's system.
    """
    print(f"[MOTOR] Pivot to {actor}")
    # Example HTTP implementation (uncomment and configure when ready):
    # import requests
    # try:
    #     requests.post("http://motor-controller-ip/pivot", json={"actor": actor})
    # except Exception as e:
    #     print(f"[MOTOR] Failed: {e}")


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
    global current_cue_index, cue_fired
    cue = scene[cue_index]
    next_index = cue_index + 1
    next_actor = cue["next_actor"]
    print(f"[CUE/{source}] '{cue['trigger']}' -> next: {next_actor}")
    cue_fired = False
    current_cue_index = next_index
    socketio.emit("cue_fired", {
        "fired_index": cue_index,
        "next_index": next_index,
        "next_actor": next_actor,
        "trigger": cue["trigger"]
    })
    if next_actor:
        send_motor_command(next_actor)


def check_trigger(text, actor, is_final):
    global cue_fired
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
        cue_fired = True
        method = "fuzzy" if fuzzy_hit else "phonetic"
        source = f"{'FINAL' if is_final else 'PARTIAL'}/{method}"
        fire_cue(current_cue_index, source=source)


@socketio.on('manual_override')
def handle_manual_override():
    global cue_fired
    if current_cue_index >= len(scene):
        print("[OVERRIDE] Already at end of scene.")
        return
    cue_fired = False
    print(f"[OVERRIDE] Director manually advanced cue {current_cue_index}")
    fire_cue(current_cue_index, source="OVERRIDE")


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
    print("Central cue engine listening for actor nodes...")

    while True:
        msg = sock.recv_json()
        actor = msg["actor"]
        text = msg["text"]
        is_final = msg["type"] == "final"

        if is_final:
            print(f"[{actor}] FINAL: {text}")

        socketio.emit("transcript", {"actor": actor, "text": text})
        check_trigger(text, actor, is_final)


if __name__ == "__main__":
    zmq_thread = threading.Thread(target=zmq_listener, daemon=True)
    zmq_thread.start()
    print("Open http://<central-pi-ip>:5000 in your browser")
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)