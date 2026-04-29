import json
import queue
import numpy as np
import sounddevice as sd
import zmq
from vosk import Model, KaldiRecognizer

# ── CONFIGURE THESE TWO LINES PER PI ──────────────────────────────────────────
ACTOR_NAME = "Nora"          # Change to "Torvald" on the other Pi
CENTRAL_IP = "172.26.160.17"  # Replace with your central Pi's IP address
# ──────────────────────────────────────────────────────────────────────────────

SAMPLE_RATE = 48000
VOSK_RATE = 16000
RESAMPLE_RATIO = SAMPLE_RATE // VOSK_RATE

print(f"Loading Vosk model for {ACTOR_NAME}...")
model = Model("model")
recognizer = KaldiRecognizer(model, VOSK_RATE)

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect(f"tcp://{CENTRAL_IP}:5555")

audio_queue = queue.Queue()


def audio_callback(indata, frames, time, status):
    resampled = indata[::RESAMPLE_RATIO, 0]
    audio_queue.put(bytes(resampled.astype(np.int16)))


# Check mic device
import sounddevice as sd
devices = sd.query_devices()
print("Available audio devices:")
print(devices)
print(f"\n{ACTOR_NAME} node is live, sending to {CENTRAL_IP}...")

with sd.InputStream(
    samplerate=SAMPLE_RATE,
    blocksize=12000,
    device=0,
    dtype="int16",
    channels=1,
    callback=audio_callback
):
    try:
        while True:
            data = audio_queue.get()
            if recognizer.AcceptWaveform(data):
                result = json.loads(recognizer.Result())
                text = result.get("text", "")
                if text:
                    print(f"[FINAL] {text}")
                    socket.send_json({
                        "actor": ACTOR_NAME,
                        "text": text,
                        "type": "final"
                    })
            else:
                partial = json.loads(recognizer.PartialResult())
                text = partial.get("partial", "")
                if text:
                    socket.send_json({
                        "actor": ACTOR_NAME,
                        "text": text,
                        "type": "partial"
                    })
    except KeyboardInterrupt:
        print(f"\n{ACTOR_NAME} node stopped.")