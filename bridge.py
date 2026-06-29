# type: ignore
import os
import sys

# Configure DLL search paths for PJSIP on Windows before importing pjsua2
if sys.platform.startswith('win'):
    pjsip_root = os.getenv("PJSIP_ROOT", r"D:\Maya Ai\pjproject")
    pjsua2_python_path = os.getenv(
        "PJSUA2_PYTHON_PATH",
        r"D:\Maya Ai\venv314\Lib\site-packages"
    )
    if os.path.isdir(pjsua2_python_path) and pjsua2_python_path not in sys.path:
        # Append so this project's packages remain preferred over the old venv.
        sys.path.append(pjsua2_python_path)

    mingw_paths = [
        r"C:\Program Files\Git\mingw64\bin",
        r"C:\msys64\mingw64\bin",
        r"D:\msys64\mingw64\bin",
        os.path.dirname(sys.executable),
    ]
    workspace_dir = os.path.dirname(os.path.abspath(__file__))
    local_pjsip_root = os.path.join(workspace_dir, "pjproject")
    dll_roots = [local_pjsip_root, pjsip_root]
    local_dll_paths = [
        os.path.join(root, component, "lib")
        for root in dll_roots
        for component in ("pjlib", "pjlib-util", "pjmedia", "pjnath", "pjsip", "third_party")
    ]
    _dll_directory_handles = []
    for p in mingw_paths + local_dll_paths:
        if os.path.exists(p):
            try:
                # Keep handles alive; closing them removes the DLL search path.
                _dll_directory_handles.append(os.add_dll_directory(p))
            except Exception:
                pass

import pjsua2 as pj  # type: ignore[import]
import queue
import numpy as np
from scipy import signal as sp_signal
import threading
import asyncio
import json
import websockets
import wave
import time
import io
import base64
import zlib
import signal

# Tune Windows system timer resolution to 1ms for high-precision real-time audio
if sys.platform.startswith('win'):
    try:
        import ctypes

        winmm = ctypes.WinDLL('winmm')
        winmm.timeBeginPeriod(1)
    except Exception:
        pass
from concurrent.futures import ThreadPoolExecutor
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict
from datetime import datetime
import pytz
import uuid
import firebase_admin
from firebase_admin import credentials, firestore, storage


def load_local_env(path: str = ".env") -> None:
    """Load KEY=VALUE pairs without requiring python-dotenv."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass


load_local_env()


# ==========================================
# 1. LOGGING & UTILS
# ==========================================

def fix_encoding():
    try:
        if sys.platform.startswith('win'):
            reconfigure_out = getattr(sys.stdout, 'reconfigure', None)
            if reconfigure_out:
                reconfigure_out(encoding='utf-8')
            reconfigure_err = getattr(sys.stderr, 'reconfigure', None)
            if reconfigure_err:
                reconfigure_err(encoding='utf-8')
    except Exception:
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')  # type: ignore
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')  # type: ignore
        except Exception:
            pass


fix_encoding()


def setup_logging(name: str = 'SIPBridge', level: int = logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        file_handler = logging.FileHandler(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "bridge.log"),
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


logger = setup_logging()

# ==========================================
# 1.5 FIREBASE
# ==========================================
try:
    if not firebase_admin._apps:
        cred = credentials.Certificate(os.getenv("FIREBASE_CREDENTIALS", "mesService.json"))
        firebase_admin.initialize_app(
            cred,
            {'storageBucket': os.getenv("FIREBASE_STORAGE_BUCKET", 'mes-maya.firebasestorage.app')}
        )
    db = firestore.client(database_id='default')
    logger.info("✅ Firebase initialized in bridge.py")
except Exception as e:
    logger.error(f"❌ Failed to init Firebase: {e}")

# --- Shared Thread Pool for Non-Blocking Firestore/Storage I/O ---
_fs_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="firestore_bridge_io")


# ==========================================
# 2. CONFIGURATION
# ==========================================

@dataclass
class BridgeConfig:
    """Configuration for the SIP-WebSocket bridge"""
    # SIP Configuration
    SIP_USER: str = os.getenv("SIP_USER", "UdVVCKNM")
    SIP_PASSWORD: str = os.getenv("SIP_PASSWORD", "Admin@123")
    SIP_DOMAIN: str = os.getenv("SIP_DOMAIN", "pbx.voxbaysolutions.com")
    SIP_PORT: int = int(os.getenv("SIP_PORT", "5260"))
    SIP_TRANSPORT_PORT: int = int(os.getenv("SIP_TRANSPORT_PORT", "0"))
    # WebSocket Configuration
    WS_URI: str = os.getenv("MAYA_WS_URI", "ws://localhost:8081")
    WS_PING_INTERVAL: int = 30
    WS_PING_TIMEOUT: int = 10
    WS_SEND_TIMEOUT_SEC: float = float(os.getenv("MAYA_WS_SEND_TIMEOUT_SEC", "1.25"))

    # Audio Configuration
    SIP_SAMPLE_RATE: int = 8000
    AI_INPUT_RATE: int = 16000
    AI_OUTPUT_RATE: int = 24000
    SAMPLES_PER_FRAME: int = 160  # 20ms at 8kHz
    BITS_PER_SAMPLE: int = 16
    CHANNELS: int = 1
    MAX_QUEUE_FRAMES: int = int(os.getenv("MAX_QUEUE_FRAMES", "180"))
    SEND_QUEUE_MAX_BATCHES: int = int(os.getenv("SEND_QUEUE_MAX_BATCHES", "320"))
    RECEIVE_QUEUE_MAX_PACKETS: int = int(os.getenv("RECEIVE_QUEUE_MAX_PACKETS", "80"))
    CAPTURE_BATCH_FRAMES: int = int(os.getenv("CAPTURE_BATCH_FRAMES", "2"))
    CAPTURE_BATCH_MAX_LATENCY_SEC: float = float(os.getenv("CAPTURE_BATCH_MAX_LATENCY_SEC", "0.045"))
    # Retain 300 ms immediately before VAD opens. Without preroll, the first
    # consonant of short replies such as "athe"/"yes" is clipped and Gemini
    # may complete an empty turn even though speech was detected.
    CAPTURE_PREROLL_FRAMES: int = int(os.getenv("CAPTURE_PREROLL_FRAMES", "36"))
    CAPTURE_SPEECH_START_RMS: float = float(os.getenv("CAPTURE_SPEECH_START_RMS", "105"))
    CAPTURE_SPEECH_START_FRAMES: int = int(os.getenv("CAPTURE_SPEECH_START_FRAMES", "2"))
    CAPTURE_SPEECH_CONTINUE_RMS: float = float(os.getenv("CAPTURE_SPEECH_CONTINUE_RMS", "75"))
    CAPTURE_SPEECH_TAIL_SEC: float = float(os.getenv("CAPTURE_SPEECH_TAIL_SEC", "0.95"))
    # A wake/check word such as "Maya" can be shorter than 300 ms on a phone
    # line. Keep rejecting clicks, but do not throw away valid short speech.
    CAPTURE_MIN_TURN_SEC: float = float(os.getenv("CAPTURE_MIN_TURN_SEC", "0.22"))
    CAPTURE_MIN_SPEECH_END_INTERVAL_SEC: float = float(os.getenv("CAPTURE_MIN_SPEECH_END_INTERVAL_SEC", "0.60"))
    ECHO_TAIL_GUARD_SEC: float = 0.10
    AI_PLAYBACK_EMPTY_GRACE_SEC: float = 0.15
    POST_INTERRUPT_DROP_SEC: float = float(os.getenv("POST_INTERRUPT_DROP_SEC", "0.80"))
    # Phone-line echo is often louder than ordinary speech, so RMS alone cannot
    # safely distinguish barge-in from Maya's own voice. Keep capture closed
    # during playback by default; this prevents false caller turns and wrong
    # Firebase queries. Install/configure acoustic echo cancellation before
    # enabling this option.
    ALLOW_BARGE_IN: bool = os.getenv("ALLOW_BARGE_IN", "false").casefold() == "true"
    BARGE_IN_RMS: float = float(os.getenv("BARGE_IN_RMS", "900"))
    BARGE_IN_VOICE_RMS: float = float(os.getenv("BARGE_IN_VOICE_RMS", "340"))
    BARGE_IN_START_FRAMES: int = int(os.getenv("BARGE_IN_START_FRAMES", "5"))
    PLAYBACK_PEAK_LIMIT: int = int(os.getenv("PLAYBACK_PEAK_LIMIT", "21000"))
    AI_PLAYBACK_GAIN: float = float(os.getenv("AI_PLAYBACK_GAIN", "0.64"))
    AI_PLAYBACK_SOFT_PEAK: int = int(os.getenv("AI_PLAYBACK_SOFT_PEAK", "11800"))
    AI_PLAYBACK_SMOOTHING: float = float(os.getenv("AI_PLAYBACK_SMOOTHING", "0.38"))
    ASR_TARGET_RMS: float = float(os.getenv("ASR_TARGET_RMS", "2200"))
    ASR_MAX_GAIN: float = float(os.getenv("ASR_MAX_GAIN", "2.8"))
    ASR_PEAK_LIMIT: int = int(os.getenv("ASR_PEAK_LIMIT", "22000"))
    # Hold a steady cushion before starting/restarting playback. This jitter buffer
    # prevents momentary WebSocket/resampling delays from breaking Maya's voice.
    PLAYBACK_PREROLL_FRAMES: int = int(os.getenv("PLAYBACK_PREROLL_FRAMES", "14"))
    PLAYBACK_CONCEAL_FRAMES: int = int(os.getenv("PLAYBACK_CONCEAL_FRAMES", "40"))

    # Call Configuration
    AUTO_ANSWER: bool = True
    MAX_CALL_DURATION: int = 0
    _welcome_value = os.getenv("WELCOME_AUDIO_FILE", "maya_welcome.wav")
    WELCOME_AUDIO_FILE: str = (
        _welcome_value if os.path.isabs(_welcome_value)
        else os.path.join(os.path.dirname(os.path.abspath(__file__)), _welcome_value)
    )
    TRANSFER_AUDIO_FILE: str = "hold_music.wav"

HEALTH_FILE = "health_state.json"
HEARTBEAT_INTERVAL = 5


# ---------------------------------------------------------------------------
# Shared executor for CPU-bound audio resampling.
# Each concurrent call has its own asyncio loop in a dedicated thread; without
# this, scipy.resample_poly blocks that loop and stalls ALL audio for the call.
# ---------------------------------------------------------------------------
_audio_executor = ThreadPoolExecutor(max_workers=32, thread_name_prefix="audio_resample")


def drain_thread_queue(q: queue.Queue, max_items: Optional[int] = None) -> int:
    drained = 0
    while max_items is None or drained < max_items:
        try:
            q.get_nowait()
            drained += 1
        except queue.Empty:
            break
    return drained


def limit_pcm16(audio_data: bytes, peak_limit: int = BridgeConfig.PLAYBACK_PEAK_LIMIT) -> bytes:
    if not audio_data:
        return audio_data
    try:
        if len(audio_data) % 2 != 0:
            audio_data = audio_data[:-1]
        audio = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32)
        if audio.size == 0:
            return audio_data
        peak = float(np.max(np.abs(audio)))
        if peak > max(1, peak_limit):
            audio *= float(peak_limit) / peak
        return np.clip(audio, -32768, 32767).astype(np.int16).tobytes()
    except Exception:
        return audio_data


def soften_ai_playback_pcm16(
        audio_data: bytes,
        previous_output: float = 0.0,
        gain: float = BridgeConfig.AI_PLAYBACK_GAIN,
        peak_limit: int = BridgeConfig.AI_PLAYBACK_SOFT_PEAK,
        smoothing: float = BridgeConfig.AI_PLAYBACK_SMOOTHING,
) -> tuple[bytes, float]:
    """Make generated phone playback less sharp without changing the spoken content."""
    if not audio_data:
        return audio_data, previous_output
    try:
        if len(audio_data) % 2 != 0:
            audio_data = audio_data[:-1]
        audio = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32)
        if audio.size == 0:
            return audio_data, previous_output

        alpha = max(0.0, min(0.45, float(smoothing)))
        gain = max(0.5, min(1.1, float(gain)))
        peak_limit = max(12000, min(30000, int(peak_limit)))

        softened = np.empty_like(audio)
        y = float(previous_output)
        for i, sample in enumerate(audio):
            y = (alpha * y) + ((1.0 - alpha) * float(sample))
            softened[i] = y

        softened *= gain
        peak = float(np.max(np.abs(softened)))
        if peak > peak_limit:
            softened *= float(peak_limit) / peak

        return np.clip(softened, -32768, 32767).astype(np.int16).tobytes(), float(y)
    except Exception:
        return audio_data, previous_output


def normalize_caller_pcm16(
        audio_data: bytes,
        target_rms: float = 2200.0,
        max_gain: float = 3.5,
        peak_limit: int = 22000,
) -> bytes:
    """Lift soft caller speech before ASR without changing silence/noise too much."""
    if not audio_data:
        return audio_data
    try:
        if len(audio_data) % 2 != 0:
            audio_data = audio_data[:-1]
        audio = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32)
        if audio.size == 0:
            return audio_data
        rms = float(np.sqrt(np.mean(np.square(audio))))
        if rms < 120.0:
            return audio_data
        gain = min(max_gain, max(1.0, target_rms / rms))
        peak_limit = max(12000, min(30000, int(peak_limit)))
        return np.clip(audio * gain, -peak_limit, peak_limit).astype(np.int16).tobytes()
    except Exception:
        return audio_data


def fade_pcm16_tail(audio_data: bytes, fade_samples: int = 24) -> bytes:
    if not audio_data:
        return audio_data
    try:
        if len(audio_data) % 2 != 0:
            audio_data = audio_data[:-1]
        audio = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32)
        if audio.size == 0:
            return audio_data
        n = min(max(1, fade_samples), audio.size)
        audio[-n:] *= np.linspace(1.0, 0.0, n, dtype=np.float32)
        return np.clip(audio, -32768, 32767).astype(np.int16).tobytes()
    except Exception:
        return audio_data


# ==========================================
# 3. AUDIO PROCESSING
# ==========================================

class AudioResampler:
    @staticmethod
    def resample(audio_data: bytes, from_rate: int, to_rate: int) -> bytes:
        """Sync resampler — only call from sync (non-async) code paths."""
        return AudioResampler._resample_sync(audio_data, from_rate, to_rate)

    @staticmethod
    def _resample_sync(audio_data: bytes, from_rate: int, to_rate: int) -> bytes:
        try:
            if not audio_data or from_rate == to_rate:
                return audio_data

            if len(audio_data) % 2 != 0:
                audio_data = audio_data[:-1]

            audio_array = np.frombuffer(audio_data, dtype=np.int16)

            if from_rate == 24000 and to_rate == 8000:
                remainder = len(audio_array) % 3
                if remainder != 0:
                    audio_array = audio_array[:-remainder]
                resampled = sp_signal.resample_poly(
                    audio_array.astype(np.float32), 1, 3
                )
            elif from_rate == 8000 and to_rate == 16000:
                # Polyphase interpolation avoids the spectral images produced
                # by repeating each sample, improving telephone ASR clarity.
                resampled = sp_signal.resample_poly(
                    audio_array.astype(np.float32), 2, 1
                )
            else:
                resampled = sp_signal.resample_poly(audio_array, to_rate, from_rate)

            resampled_bytes = np.clip(resampled, -32768, 32767).astype(np.int16).tobytes()
            return limit_pcm16(resampled_bytes)

        except Exception as e:
            logger.error(f"Resampling error: {e}")
            return audio_data

    @staticmethod
    async def resample_async(audio_data: bytes, from_rate: int, to_rate: int) -> bytes:
        """Async wrapper — offloads CPU resampling to the thread pool."""
        if not audio_data or from_rate == to_rate:
            return audio_data
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _audio_executor, AudioResampler._resample_sync,
            audio_data, from_rate, to_rate
        )


class AudioAnalyzer:
    _voice_sos_cache = {}

    @staticmethod
    def calculate_rms(audio_data: bytes) -> float:
        try:
            if len(audio_data) % 2 != 0:
                audio_data = audio_data[:-1]
            if len(audio_data) == 0:
                return 0.0
            audio_array = np.frombuffer(audio_data, dtype=np.int16)
            rms = np.sqrt(np.mean(np.square(audio_array.astype(np.float32))))
            return float(rms)
        except:
            return 0.0

    @staticmethod
    def calculate_voice_rms(audio_data: bytes, sample_rate: int = BridgeConfig.SIP_SAMPLE_RATE) -> float:
        """RMS focused on telephone speech frequencies, ignoring rumble/clicks."""
        try:
            if len(audio_data) % 2 != 0:
                audio_data = audio_data[:-1]
            if len(audio_data) == 0:
                return 0.0
            audio = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32)
            if audio.size == 0:
                return 0.0
            audio = audio - float(np.mean(audio))
            sos = AudioAnalyzer._voice_sos_cache.get(sample_rate)
            if sos is None:
                high = min(3400, (sample_rate // 2) - 100)
                sos = sp_signal.butter(4, [250, high], btype="bandpass", fs=sample_rate, output="sos")
                AudioAnalyzer._voice_sos_cache[sample_rate] = sos
            voice = sp_signal.sosfilt(sos, audio)
            return float(np.sqrt(np.mean(np.square(voice))))
        except Exception:
            return 0.0


# ==========================================
# 4. PJSIP AUDIO PORT (THE BRIDGE)
# ==========================================

class AudioBridge(pj.AudioMediaPort):
    def __init__(self, config: BridgeConfig, max_playback_frames: Optional[int] = None, max_capture_frames: int = 200):
        pj.AudioMediaPort.__init__(self)
        self.config = config
        if max_playback_frames is None:
            max_playback_frames = config.MAX_QUEUE_FRAMES
        max_playback_frames = max(max_playback_frames, 500)
        self.playback_queue = queue.Queue(maxsize=max_playback_frames)
        self.capture_queue = queue.Queue(maxsize=max_capture_frames)
        self.active = False
        self._prerolling = True
        self._is_playing_false_since = 0.0
        self._last_played_frame = bytes(config.SAMPLES_PER_FRAME * 2)
        self._conceal_frames_left = 0
        self.stats = {
            'frames_captured': 0,
            'frames_played': 0,
            'playback_underruns': 0,
            'capture_overruns': 0,
            'playback_queue_full_events': 0,
            'drain_thread_queue_calls': 0,
            'audio_chunks_dropped': 0,
            'max_playback_queue_depth': 0
        }

        fmt = pj.MediaFormatAudio()
        fmt.type = pj.PJMEDIA_TYPE_AUDIO
        fmt.clockRate = config.SIP_SAMPLE_RATE
        fmt.channelCount = config.CHANNELS
        fmt.bitsPerSample = config.BITS_PER_SAMPLE
        fmt.frameTimeUsec = (config.SAMPLES_PER_FRAME * 1000000) // config.SIP_SAMPLE_RATE
        logger.info(f"🎵 Audio format: {config.SIP_SAMPLE_RATE}Hz, {config.CHANNELS}ch, {config.BITS_PER_SAMPLE}bit")
        self.createPort("ai_audio_bridge", fmt)
        logger.info(f"🎵 Audio bridge created: {config.SIP_SAMPLE_RATE}Hz, {config.SAMPLES_PER_FRAME} samples/frame")

    def _fill_silence_frame(self, frame, size):
        frame.size = size
        frame.type = pj.PJMEDIA_FRAME_TYPE_AUDIO
        frame.buf = pj.ByteVector()
        for _ in range(size): frame.buf.append(0)

    def _fill_audio_frame(self, frame, audio_data: bytes, size: int):
        if len(audio_data) < size:
            audio_data = audio_data + bytes(size - len(audio_data))
        elif len(audio_data) > size:
            audio_data = audio_data[:size]
        frame.size = size
        frame.type = pj.PJMEDIA_FRAME_TYPE_AUDIO
        frame.buf = pj.ByteVector()
        for byte in audio_data:
            frame.buf.append(byte)

    def _fill_concealment_frame(self, frame, size: int) -> bool:
        if self._conceal_frames_left <= 0 or not self._last_played_frame:
            return False
        try:
            audio = np.frombuffer(self._last_played_frame[:size], dtype=np.int16).astype(np.float32)
            fade = max(0.04, self._conceal_frames_left / max(1, self.config.PLAYBACK_CONCEAL_FRAMES)) * 0.12
            concealed = np.clip(audio * fade, -32768, 32767).astype(np.int16).tobytes()
            self._conceal_frames_left -= 1
            self._fill_audio_frame(frame, concealed, size)
            return True
        except Exception:
            return False

    def _playback_preroll_frames(self) -> int:
        return max(12, self.config.PLAYBACK_PREROLL_FRAMES)

    def onFrameRequested(self, frame):
        """Called when PJSIP needs audio to send to the phone - RUNS IN PJSIP THREAD"""
        if self.stats['frames_played'] == 0:
            logger.info("🎵 First onFrameRequested call!")
        self.stats['frames_played'] += 1
        try:
            expected_size = self.config.SAMPLES_PER_FRAME * 2

            # Track max queue depth
            q_depth = self.playback_queue.qsize()
            if q_depth > self.stats.get('max_playback_queue_depth', 0):
                self.stats['max_playback_queue_depth'] = q_depth

            # Check if AI or welcome audio is supposed to be playing
            is_playing = False
            call_obj = getattr(self, 'call_obj', None)
            if call_obj:
                is_playing = getattr(call_obj, '_ai_playing', False) or getattr(call_obj, '_welcome_playing', False)

            # Log queue depth for Phase 1 instrumentation
            recv_q_depth = 0
            if call_obj and call_obj.ai_client:
                recv_q_depth = call_obj.ai_client.audio_receive_queue.qsize()
            if self.stats['frames_played'] % 50 == 0:
                logger.info(
                    f"TIMING_METRIC: [QUEUE_DEPTH] receive_queue={recv_q_depth} playback_queue={q_depth} is_playing={is_playing} at {time.time()}")

            if is_playing:
                self._is_playing_false_since = 0.0
            else:
                if self._is_playing_false_since == 0.0:
                    self._is_playing_false_since = time.monotonic()
                elif time.monotonic() - self._is_playing_false_since >= 0.20:
                    self._prerolling = True

            # If prerolling, check if we have accumulated enough frames
            if self._prerolling:
                if q_depth >= self._playback_preroll_frames():
                    self._prerolling = False
                    logger.info(f"Buffered {q_depth} frames, starting playback")
                else:
                    self._fill_silence_frame(frame, expected_size)
                    return

            # If not prerolling (we have cushion or are draining)
            if self.active and not self._prerolling:
                try:
                    ai_audio = self.playback_queue.get_nowait()
                    self._fill_audio_frame(frame, ai_audio, expected_size)
                    self._last_played_frame = bytes(ai_audio[:expected_size]).ljust(expected_size, b"\0")
                    self._conceal_frames_left = self.config.PLAYBACK_CONCEAL_FRAMES
                except queue.Empty:
                    if is_playing:
                        self.stats['playback_underruns'] += 1
                        logger.warning(f"TIMING_METRIC: [PLAYBACK_UNDERRUN] type=starvation at {time.time()}")
                        if self._fill_concealment_frame(frame, expected_size):
                            return
                        # Jitter Buffer Fix: If we starved, wait until we accumulate some frames again!
                        self._prerolling = True
                    self._fill_silence_frame(frame, expected_size)
            else:
                self._fill_silence_frame(frame, expected_size)
        except Exception as e:
            logger.error(f"❌ Error in onFrameRequested: {e}")
            self._fill_silence_frame(frame, expected_size)

    def onFrameReceived(self, frame):
        if not self.active: return
        try:
            if frame.size > 0:
                audio_data = None
                try:
                    if hasattr(frame, 'buf') and frame.buf is not None:
                        audio_data = bytes(frame.buf[:frame.size])
                except:
                    pass

                if audio_data is None:
                    try:
                        audio_data = bytes([frame.buf[i] for i in range(frame.size)])
                    except Exception:
                        return

                if audio_data:
                    try:
                        self.capture_queue.put_nowait(audio_data)
                        self.stats['frames_captured'] += 1
                    except queue.Full:
                        try:
                            self.capture_queue.get_nowait()
                            self.capture_queue.put_nowait(audio_data)
                            self.stats['capture_overruns'] += 1
                        except:
                            pass
        except Exception:
            pass

    def start(self):
        self.active = True
        self._prerolling = True
        logger.info("✅ Audio bridge started")

    def stop(self):
        self.active = False
        capture_cleared = drain_thread_queue(self.capture_queue)
        playback_cleared = drain_thread_queue(self.playback_queue)

        # Track drain call and dropped chunks at stop/cleanup
        self.stats['drain_thread_queue_calls'] += 1
        self.stats['audio_chunks_dropped'] += playback_cleared

        if capture_cleared or playback_cleared:
            logger.info(f"Cleared audio bridge queues: capture={capture_cleared}, playback={playback_cleared}")

        logger.info("=" * 50)
        logger.info("📊 BRIDGE AUDIO STATISTICS SUMMARY:")
        logger.info(f"   Playback queue full events: {self.stats.get('playback_queue_full_events', 0)}")
        logger.info(f"   drain_thread_queue calls: {self.stats.get('drain_thread_queue_calls', 0)}")
        logger.info(f"   Total audio chunks dropped: {self.stats.get('audio_chunks_dropped', 0)}")
        logger.info(f"   Playback underruns (expected empty reads omitted): {self.stats.get('playback_underruns', 0)}")
        logger.info(f"   Maximum playback queue depth reached: {self.stats.get('max_playback_queue_depth', 0)}")
        logger.info(f"   Frames captured: {self.stats.get('frames_captured', 0)}")
        logger.info(f"   Frames played: {self.stats.get('frames_played', 0)}")
        logger.info("=" * 50)
        logger.info(f"🛑 Audio bridge stopped")


# ==========================================
# 5. WEBSOCKET CLIENT (AI CONNECTION)
# ==========================================

class WebSocketState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    READY = "ready"
    ERROR = "error"


class AIWebSocketClient:
    def __init__(self, call_object=None):
        # Get config from call_object if available, otherwise create default instance
        if call_object and hasattr(call_object, 'config'):
            self.config = call_object.config
        else:
            self.config = BridgeConfig()
        self.call_object = call_object
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.state = WebSocketState.DISCONNECTED
        self.audio_send_queue = asyncio.Queue(maxsize=self.config.SEND_QUEUE_MAX_BATCHES)
        self.audio_receive_queue = asyncio.Queue(maxsize=self.config.RECEIVE_QUEUE_MAX_PACKETS)
        self.call_id: Optional[str] = None
        self.caller_id: Optional[str] = None
        self.stats = {
            'packets_sent': 0,
            'packets_received': 0,
            'bytes_sent': 0,
            'bytes_received': 0,
            'outbound_audio_drops': 0,
        }
        # Timestamp used to ignore incoming AI audio immediately after an interrupt
        self.last_interrupt_ts: float = 0.0
        self._ai_playback_soften_state: float = 0.0

    def _is_websocket_open(self) -> bool:
        if not self.websocket: return False
        try:
            if hasattr(self.websocket, 'open'):
                return self.websocket.open
            elif hasattr(self.websocket, 'closed'):
                return not self.websocket.closed
            else:
                return True
        except Exception:
            return False

    # PHASE 2 FIX: _drop_oldest_async_queue_item removed to prevent dropping packets

    def _drain_async_queue(self, q: asyncio.Queue, label: str) -> int:
        cleared = 0
        while True:
            try:
                q.get_nowait()
                cleared += 1
                try:
                    q.task_done()
                except ValueError:
                    pass
            except asyncio.QueueEmpty:
                break
        if cleared:
            logger.info(f"Cleared {cleared} {label} audio packets")
        return cleared

    async def connect(self, call_id: str, caller_id: str) -> bool:
        self.call_id = call_id
        self.caller_id = caller_id
        self.state = WebSocketState.CONNECTING
        try:
            logger.info(f"🔌 Connecting to AI agent at {self.config.WS_URI}")
            self.websocket = await websockets.connect(
                self.config.WS_URI,
                ping_interval=self.config.WS_PING_INTERVAL,
                ping_timeout=self.config.WS_PING_TIMEOUT,
                max_size=10 * 1024 * 1024
            )
            self.state = WebSocketState.CONNECTED
            logger.info("✅ WebSocket connected successfully")
            await self._send_handshake()
            asyncio.create_task(self._handle_messages())
            asyncio.create_task(self._send_audio_loop())
            return True
        except Exception as e:
            logger.error(f"❌ WebSocket connection failed: {e}")
            self.state = WebSocketState.ERROR
            return False

    async def _send_handshake(self):
        try:
            start_msg = {
                "event": "start",
                "call_id": self.call_id,
                "caller_id": self.caller_id,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
            }
            await self.websocket.send(json.dumps(start_msg))
            media_msg = {
                "event": "media",
                "data": {
                    "sample_rate": self.config.AI_INPUT_RATE,
                    "codec": "L16",
                    "channels": self.config.CHANNELS
                }
            }
            await self.websocket.send(json.dumps(media_msg))
            self.state = WebSocketState.READY
            logger.info("✅ Handshake complete")
        except Exception as e:
            logger.error(f"❌ Handshake failed: {e}")
            self.state = WebSocketState.ERROR

    async def send_audio(self, audio_data: bytes):
        if self.state != WebSocketState.READY:
            return
        try:
            # Resample 8kHz → 16kHz before sending (AI agent expects 16kHz)
            # Offloaded to thread pool to avoid blocking the event loop
            audio_16k = await AudioResampler.resample_async(audio_data, self.config.SIP_SAMPLE_RATE,
                                                            self.config.AI_INPUT_RATE)
            audio_16k = normalize_caller_pcm16(
                audio_16k,
                target_rms=self.config.ASR_TARGET_RMS,
                max_gain=self.config.ASR_MAX_GAIN,
                peak_limit=self.config.ASR_PEAK_LIMIT,
            )
            if len(audio_16k) > 0:
                try:
                    self.audio_send_queue.put_nowait(audio_16k)
                except asyncio.QueueFull:
                    try:
                        self.audio_send_queue.get_nowait()
                        self.audio_send_queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    self.audio_send_queue.put_nowait(audio_16k)
                    self.stats['outbound_audio_drops'] += 1
                    if self.stats['outbound_audio_drops'] % 25 == 1:
                        logger.warning(
                            f"Dropped stale outbound caller audio packet(s): {self.stats['outbound_audio_drops']}")
        except Exception as e:
            logger.error(f"❌ Error queueing audio: {e}")

    async def send_control_event(self, event: str):
        if self.state != WebSocketState.READY or not self.websocket:
            return
        try:
            if event == "speech_end":
                await self.flush_outbound_audio()
            await asyncio.wait_for(
                self.websocket.send(json.dumps({"event": event})),
                timeout=self.config.WS_SEND_TIMEOUT_SEC,
            )
            logger.info(f"Sent control event to AI server: {event}")
        except Exception as e:
            logger.error(f"Error sending control event {event}: {e}")

    async def flush_outbound_audio(self):
        if self.state != WebSocketState.READY or not self.websocket:
            return
        flushed = 0
        while True:
            try:
                audio_data = self.audio_send_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await asyncio.wait_for(self.websocket.send(audio_data), timeout=self.config.WS_SEND_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                self.stats['outbound_audio_drops'] += 1
                logger.warning("Timed out flushing caller audio packet; dropping stale packet")
                continue
            self.stats['packets_sent'] += 1
            self.stats['bytes_sent'] += len(audio_data)
            flushed += 1
        if flushed:
            logger.info(f"Flushed {flushed} caller audio packet(s) before speech_end")

    async def _send_audio_loop(self):
        logger.info("🎤 Starting audio transmission loop")
        packet_id = 0
        try:
            # FIX: Must check for both CONNECTED and READY - state becomes READY after handshake
            while self.state in [WebSocketState.CONNECTED, WebSocketState.READY]:
                try:
                    audio_data = await asyncio.wait_for(self.audio_send_queue.get(), timeout=0.02)

                    if self.state == WebSocketState.READY:
                        # Send RAW BYTES
                        try:
                            await asyncio.wait_for(
                                self.websocket.send(audio_data),
                                timeout=self.config.WS_SEND_TIMEOUT_SEC,
                            )
                        except asyncio.TimeoutError:
                            self.stats['outbound_audio_drops'] += 1
                            logger.warning("Timed out sending caller audio packet; dropping stale packet")
                            continue

                        self.stats['packets_sent'] += 1
                        self.stats['bytes_sent'] += len(audio_data)

                        # Debug logging
                        if packet_id % 100 == 0:
                            logger.info(f"📤 Sent packet #{packet_id}: {len(audio_data)} bytes to server")
                        packet_id += 1
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"❌ Error in send loop: {e}")
                    await asyncio.sleep(0.005)
        except Exception as e:
            logger.error(f"❌ Outer error in send loop: {e}")

    async def _handle_messages(self):
        logger.info("📥 Starting message reception loop")
        websocket = self.websocket
        if websocket is None:
            logger.error("❌ WebSocket is not connected in _handle_messages")
            return
        try:
            async for message in websocket:  # type: ignore[union-attr]
                if isinstance(message, bytes):
                    await self._handle_audio_response(message)
                else:
                    await self._handle_control_message(message)
        except websockets.exceptions.ConnectionClosed:
            logger.info("📵 WebSocket connection closed")
        except Exception as e:
            logger.error(f"❌ Error in message handler: {e}")
        finally:
            self.state = WebSocketState.DISCONNECTED

    async def _handle_audio_response(self, audio_data: bytes):
        try:
            self.stats['packets_received'] += 1
            now = time.time()
            if not hasattr(self, '_last_ws_recv_time') or self._last_ws_recv_time == 0.0:
                interval = 0.0
            else:
                interval = (now - self._last_ws_recv_time) * 1000.0
            self._last_ws_recv_time = now
            logger.info(f"TIMING_METRIC: [WS_RECV] interval={interval:.1f}ms size={len(audio_data)} at {now}")

            # If we recently processed an interrupt, drop any incoming audio for a short window
            if time.time() - getattr(self, 'last_interrupt_ts', 0.0) < self.config.POST_INTERRUPT_DROP_SEC:
                logger.info("🛑 Dropping AI audio received immediately after interrupt")
                self._playback_resample_remainder = bytearray()
                self._ai_playback_soften_state = 0.0
                return

            if not hasattr(self, '_playback_resample_remainder'):
                self._playback_resample_remainder = bytearray()

            full_data = self._playback_resample_remainder + audio_data
            # For 24kHz to 8kHz, we decimate by 3. A 16-bit sample is 2 bytes. 3 samples = 6 bytes.
            # We must pass exact multiples of 6 bytes to avoid AudioResampler throwing away the remainder!
            remainder_len = len(full_data) % 6
            if remainder_len != 0:
                self._playback_resample_remainder = bytearray(full_data[-remainder_len:])
                full_data = full_data[:-remainder_len]
            else:
                self._playback_resample_remainder = bytearray()

            if not full_data:
                return

            # Server sends Gemini's native output rate — resample once to 8kHz for PJSIP playback.
            # Offloaded to thread pool to avoid blocking the event loop
            t0 = time.perf_counter()
            if self.config.AI_OUTPUT_RATE == 24000 and self.config.SIP_SAMPLE_RATE == 8000:
                audio_8k = AudioResampler.resample(bytes(full_data), 24000, 8000)
            else:
                audio_8k = await AudioResampler.resample_async(
                    bytes(full_data),
                    from_rate=self.config.AI_OUTPUT_RATE,
                    to_rate=self.config.SIP_SAMPLE_RATE
                )
            t1 = time.perf_counter()
            resample_dur = (t1 - t0) * 1000.0
            logger.info(
                f"TIMING_METRIC: [RESAMPLE] time={resample_dur:.2f}ms size_in={len(audio_data)} size_out={len(audio_8k)}")

            audio_8k = limit_pcm16(audio_8k, self.config.PLAYBACK_PEAK_LIMIT)
            audio_8k, self._ai_playback_soften_state = soften_ai_playback_pcm16(
                audio_8k,
                previous_output=getattr(self, "_ai_playback_soften_state", 0.0),
                gain=self.config.AI_PLAYBACK_GAIN,
                peak_limit=self.config.AI_PLAYBACK_SOFT_PEAK,
                smoothing=self.config.AI_PLAYBACK_SMOOTHING,
            )
            if self.stats['packets_received'] % 50 == 0:
                rms = AudioAnalyzer.calculate_rms(audio_8k)
                logger.info(
                    f"🔊 AI response #{self.stats['packets_received']}: {len(audio_data)}→{len(audio_8k)} bytes, RMS={rms:.1f}")

            # Phase 2 FIX: Await without dropping to apply TCP backpressure
            await self.audio_receive_queue.put(audio_8k)
        except Exception as e:
            logger.error(f"Error handling audio response: {e}")

    async def _handle_control_message(self, message: str):
        try:
            data = json.loads(message)
            event = data.get('event', 'unknown')

            # --- HANDLE AUDIO EVENT (JSON encoded) ---
            if event == 'media':
                media = data.get("media", {})
                payload = media.get("payload", "")
                if payload:
                    try:
                        # Decode base64 audio
                        audio_data = base64.b64decode(payload)
                        await self._handle_audio_response(audio_data)
                    except Exception as e:
                        logger.error(f"❌ Failed to decode audio payload: {e}")
                return

            logger.info(f"📨 Received event: {event}")

            # --- HANDLE CLEAR EVENT (User Interruption / Barge-In) ---
            if event == 'clear':
                reason = data.get('reason', 'unknown')
                logger.info(f"🛑 [BARGE-IN] Clear event received - Reason: {reason}")
                await self._handle_interrupt(reason)
                return

            if event == 'ai_speaking':
                speaking = data.get('speaking', False)
                logger.info(f"🔊 AI speaking state updated from server: {speaking}")
                if self.call_object:
                    if speaking:
                        self._ai_playback_soften_state = 0.0
                        self.call_object._ai_playing = True
                        self.call_object._server_done_speaking = False
                        self.call_object._server_done_speaking_at = 0.0
                    else:
                        self.call_object._server_done_speaking = True
                        self.call_object._server_done_speaking_at = time.time()
                        asyncio.create_task(self._release_ai_speaking_after_tail())
                return

            if event == 'transfer':
                target_ext = data.get('destination') or data.get('extension')
                logger.info(f"📞 Transfer requested to: {target_ext}")

                # Capture extracted name from AI
                caller_name = data.get('caller_name')
                if caller_name and self.call_object:
                    logger.info(f"📝 Updating caller name from AI: {caller_name}")
                    self.call_object.caller_name_str = caller_name

                if self.call_object and target_ext:
                    self.call_object.transfer_to_extension(str(target_ext))
            elif event == 'hangup':
                if self.call_object and self.call_object.is_transferring:
                    logger.warning("🛡️ PREVENTED AI HANGUP: Call is in transfer mode.")
                    return
                reason = data.get('reason', 'unknown')
                logger.info("=" * 60)
                logger.info(f"📴 [HANGUP EVENT RECEIVED]")
                logger.info(f"   Reason: {reason}")
                logger.info(f"   Call ID: {data.get('call_id')}")
                # Hang up the SIP call
                if self.call_object:
                    call_prm = pj.CallOpParam()
                    call_prm.statusCode = 200  # Normal call clearing
                    self.call_object.hangup(call_prm)
                    logger.info("✅ SIP call hangup command sent successfully.")
        except json.JSONDecodeError:
            logger.error(f"❌ Invalid JSON: {message[:100]}")
        except Exception as e:
            logger.error(f"❌ Error handling control message: {e}")

    async def _clear_playback_queue_bridge(self):
        """Clear the playback queue on the bridge side for barge-in."""
        if self.call_object and self.call_object.audio_bridge:
            try:
                cleared_count = drain_thread_queue(self.call_object.audio_bridge.playback_queue)
                self.call_object.audio_bridge.stats['drain_thread_queue_calls'] += 1
                self.call_object.audio_bridge.stats['audio_chunks_dropped'] += cleared_count
                logger.info(f"✅ [BRIDGE] Playback queue cleared ({cleared_count} packets due to interrupt/barge-in)")
            except Exception as e:
                logger.error(f"Error clearing playback queue: {e}")

    async def _release_ai_speaking_after_tail(self):
        """Guarantee listening reopens after Gemini finishes a turn.

        The PJSIP audio port starts playback only after PLAYBACK_PREROLL_FRAMES
        are queued. If Gemini's final flush leaves 1-2 frames, those frames can
        sit forever and keep _ai_playing=True, so the caller's next question is
        treated as echo instead of a new turn.
        """
        call_obj = self.call_object
        if not call_obj:
            return
        done_marker = getattr(call_obj, '_server_done_speaking_at', 0.0)
        for _ in range(30):  # up to ~1.5s for bridge playback to drain naturally
            await asyncio.sleep(0.05)
            if not call_obj.call_active or getattr(call_obj, '_server_done_speaking_at', 0.0) != done_marker:
                return
            audio_bridge = getattr(call_obj, 'audio_bridge', None)
            if not audio_bridge:
                return
            qsize = audio_bridge.playback_queue.qsize()
            preroll_frames = (
                audio_bridge._playback_preroll_frames()
                if hasattr(audio_bridge, "_playback_preroll_frames")
                else max(12, call_obj.config.PLAYBACK_PREROLL_FRAMES)
            )
            if qsize == 0 or qsize < preroll_frames:
                if qsize > 0:
                    audio_bridge._prerolling = False
                    logger.info(
                        f"AI speaking release allowing {qsize} final queued frame(s) to play below preroll threshold")
                    continue
                call_obj._ai_playing = False
                call_obj._server_done_speaking = False
                call_obj._server_done_speaking_at = 0.0
                call_obj._ai_queue_empty_since = 0.0
                call_obj._ai_play_ended_at = time.time()
                call_obj._reset_capture_batch()
                call_obj._reset_caller_speech_gate()
                logger.info(
                    f"AI speaking released after server_done; qsize_was={qsize}")
                return

    async def _handle_interrupt(self, reason: str = 'unknown'):
        """Handle a user interruption / barge-in more robustly."""
        try:
            self.last_interrupt_ts = time.time()

            # Best-effort: notify remote AI server about the interrupt
            try:
                if self.websocket and self._is_websocket_open():
                    await self.websocket.send(json.dumps({"event": "interrupt", "reason": reason}))
            except Exception:
                logger.debug("⚠️ Failed to notify AI server about interrupt (non-fatal)")

            self._drain_async_queue(self.audio_receive_queue, "inbound AI")
            # PHASE 3 FIX: DO NOT drain audio_send_queue or capture_queue.
            # Draining them destroys the very user speech that triggered the interrupt!

            # Clear playback queue on bridge side
            await self._clear_playback_queue_bridge()
            if self.call_object:
                self.call_object._ai_playing = False
                self.call_object._ai_queue_empty_since = 0.0
                self.call_object._last_ai_audio_queued_at = 0.0
                self.call_object._ai_play_ended_at = time.time()

            logger.info("✅ Interrupt processed: cleared receive/playback queues")
        except Exception as e:
            logger.error(f"❌ Error while processing interrupt: {e}")

    async def disconnect(self):
        if self.websocket:
            try:
                if self._is_websocket_open():
                    hangup_msg = {"event": "hangup", "call_id": self.call_id, "reason": "normal"}
                    await asyncio.wait_for(self.websocket.send(json.dumps(hangup_msg)), timeout=0.5)
                await self.websocket.close()
            except Exception:
                logger.debug("WebSocket shutdown completed with a non-fatal transport error")
        self._drain_async_queue(self.audio_send_queue, "outbound caller")
        self._drain_async_queue(self.audio_receive_queue, "inbound AI")
        self.state = WebSocketState.DISCONNECTED
        logger.info("✅ Disconnected from AI agent")


# ==========================================
# 6. MEMORY RECORDER & TRANSFER
# ==========================================

class MemoryAudioRecorder(pj.AudioMediaPort):
    def __init__(self):
        pj.AudioMediaPort.__init__(self)
        fmt = pj.MediaFormatAudio()
        fmt.type = pj.PJMEDIA_TYPE_AUDIO
        fmt.clockRate = BridgeConfig.SIP_SAMPLE_RATE
        fmt.channelCount = BridgeConfig.CHANNELS
        fmt.bitsPerSample = BridgeConfig.BITS_PER_SAMPLE
        fmt.frameTimeUsec = (BridgeConfig.SAMPLES_PER_FRAME * 1000000) // BridgeConfig.SIP_SAMPLE_RATE

        self.createPort("mem_recorder", fmt)
        self.buffer = io.BytesIO()
        self.active = False

    def onFrameReceived(self, frame):
        if not self.active: return
        try:
            if frame.size > 0:
                try:
                    self.buffer.write(bytes(frame.buf[:frame.size]))
                except:
                    pass
        except Exception:
            pass

    def get_wav_bytes(self) -> bytes:
        raw_data = self.buffer.getvalue()
        wav_buffer = io.BytesIO()
        with wave.open(wav_buffer, 'wb') as wf:
            wf.setnchannels(BridgeConfig.CHANNELS)
            wf.setsampwidth(BridgeConfig.BITS_PER_SAMPLE // 8)
            wf.setframerate(BridgeConfig.SIP_SAMPLE_RATE)
            wf.writeframes(raw_data)
        return wav_buffer.getvalue()

    def start_record(self):
        self.active = True

    def stop_record(self):
        self.active = False


# ==========================================
# 7. TRANSFER CALL
# ==========================================

class TransferCall(pj.Call):
    def __init__(self, account, parent_call, dest_uri, config):
        pj.Call.__init__(self, account)
        self.parent_call = parent_call
        self.config = config
        self.connected = False

    def onCallState(self, prm):
        ci = self.getInfo()
        logger.info(f"⏩ Transfer Leg State: {ci.stateText} ({ci.state})")

        if ci.state == pj.PJSIP_INV_STATE_CONFIRMED:
            if self.parent_call:
                self.parent_call.stop_transfer_music()
            self.connected = True
            logger.info("✅ Pharmacy/Extension Answered! Conversation active.")
            # Update Firestore status (non-blocking)
            if self.parent_call:
                _call_id = self.parent_call.custom_call_id

                def _update_completed():
                    try:
                        calls_ref = db.collection('tenants').document('mes_hosp').collection('calls')
                        query = calls_ref.where('callId', '==', _call_id).limit(1).get()
                        for doc in query:
                            doc.reference.update({"status": "Completed"})
                            logger.info("✅ Updated Firestore status to Completed (Transfer Leg Answered)")
                    except Exception as e:
                        logger.error(f"Failed to update Success status: {e}")

                _fs_executor.submit(_update_completed)

        elif ci.state == pj.PJSIP_INV_STATE_DISCONNECTED:
            if self.parent_call:
                self.parent_call.stop_transfer_music()

            logger.info("❌ Transfer Leg Disconnected")
            was_connected = self.connected
            self.connected = False

            if not was_connected and self.parent_call:
                logger.warning("⚠️ Transfer failed (never connected). Returning to AI.")
                self.parent_call.is_transferring = False
                if self.parent_call.audio_bridge:
                    self.parent_call.audio_bridge.active = True
                # Update Firestore status to Failed (non-blocking)
                _call_id_f = self.parent_call.custom_call_id

                def _update_failed():
                    try:
                        calls_ref = db.collection('tenants').document('mes_hosp').collection('calls')
                        query = calls_ref.where('callId', '==', _call_id_f).limit(1).get()
                        for doc in query:
                            doc.reference.update({"status": "Failed"})
                            logger.info("✅ Updated Firestore status to Failed (Transfer Leg Rejected)")
                    except Exception as e:
                        logger.error(f"Failed to update Failed status: {e}")

                _fs_executor.submit(_update_failed)

            if was_connected:
                if self.parent_call and self.parent_call.call_active:
                    logger.info("🔌 Extension hung up -> Disconnecting Caller.")
                    try:
                        self.parent_call.hangup(pj.CallOpParam())
                    except:
                        pass

    def onCallMediaState(self, prm):
        """Bridge the Audio: Caller <---> Pharmacy"""
        try:
            ci = self.getInfo()
            for mi in ci.media:
                if mi.type == pj.PJMEDIA_TYPE_AUDIO and mi.status == pj.PJSUA_CALL_MEDIA_ACTIVE:
                    transfer_media = self.getAudioMedia(mi.index)

                    parent_ci = self.parent_call.getInfo()
                    for p_mi in parent_ci.media:
                        if p_mi.type == pj.PJMEDIA_TYPE_AUDIO and p_mi.status == pj.PJSUA_CALL_MEDIA_ACTIVE:
                            parent_media = self.parent_call.getAudioMedia(p_mi.index)

                            logger.info("🔗 BRIDGING AUDIO: Caller <--> Extension")
                            parent_media.startTransmit(transfer_media)
                            transfer_media.startTransmit(parent_media)
                            return
        except Exception as e:
            logger.error(f"❌ Audio Bridge Error: {e}")


class AICall(pj.Call):
    custom_call_id: str
    audio_bridge: Optional[AudioBridge]
    ai_client: Optional[AIWebSocketClient]
    call_active: bool
    transfer_leg: Optional[TransferCall]
    is_transferring: bool
    call_start_time: Optional[float]
    async_thread: Optional[threading.Thread]
    async_loop: Optional[asyncio.AbstractEventLoop]
    first_ai_audio_received: bool
    stop_welcome_audio_event: threading.Event
    rec_filename: Optional[str]
    rec_token: str
    last_playback_audio_crc: Optional[int]
    last_playback_audio_len: int
    last_playback_audio_ts: float
    _ai_playing: bool
    _ai_play_ended_at: float
    _ECHO_TAIL_GUARD: float
    _AI_PLAYBACK_EMPTY_GRACE: float
    _ai_queue_empty_since: float
    _welcome_playing: bool
    _capture_batch: bytearray
    _capture_batch_started_at: float
    _last_ai_audio_queued_at: float

    def __init__(self, account, config: BridgeConfig, call_id=pj.PJSUA_INVALID_ID):
        pj.Call.__init__(self, account, call_id)
        self.account = account
        self.config = config
        self.call_id = call_id
        self.custom_call_id = str(uuid.uuid4())
        self.audio_bridge: Optional[AudioBridge] = None
        self.ai_client: Optional[AIWebSocketClient] = None
        self.call_active = False
        self.transfer_leg: Optional[TransferCall] = None
        self.is_transferring = False
        self.call_start_time: Optional[float] = None
        self.async_thread: Optional[threading.Thread] = None
        self.async_loop: Optional[asyncio.AbstractEventLoop] = None
        self.first_ai_audio_received = False
        self.stop_welcome_audio_event = threading.Event()
        self.recorder: Optional[MemoryAudioRecorder] = None
        self.rec_filename: Optional[str] = None

        # Metadata for recording
        self.caller_id_str: str = "Unknown"
        self.caller_name_str: str = "Unknown"
        self.extension_transferred_to: Optional[str] = None
        timestamp = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y%m%d_%H%M%S")
        clean_id = str(self.call_id).replace(":", "")
        self.rec_filename = f"call_{timestamp}_{clean_id}.wav"
        self.rec_token = str(uuid.uuid4())
        self.last_playback_audio_crc = None
        self.last_playback_audio_len = 0
        self.last_playback_audio_ts = 0.0
        # Echo suppression: track when AI audio is being played to the phone
        self._ai_playing = False
        self._ai_play_ended_at = 0.0
        self._ECHO_TAIL_GUARD = self.config.ECHO_TAIL_GUARD_SEC
        self._AI_PLAYBACK_EMPTY_GRACE = self.config.AI_PLAYBACK_EMPTY_GRACE_SEC
        self._ai_queue_empty_since = 0.0  # Timestamp when playback queue first went empty
        self._server_done_speaking = False
        self._server_done_speaking_at = 0.0
        self._welcome_playing = False  # True while welcome WAV is being played to phone
        self._capture_batch = bytearray()
        self._capture_batch_started_at = 0.0
        self._capture_preroll = bytearray()
        self._last_ai_audio_queued_at = 0.0
        self._caller_speech_active = False
        self._caller_speech_candidate_frames = 0
        self._barge_in_candidate_frames = 0
        self._caller_voice_noise_rms = 35.0
        self._caller_speech_last_loud_at = 0.0
        self._caller_speech_started_at = 0.0
        self._last_speech_end_sent_at = 0.0
        self._welcome_echo_block_until = 0.0

    def onCallState(self, prm):
        try:
            ci = self.getInfo()
            logger.info(f"📞 Call state: {ci.stateText} ({ci.state})")
            if ci.state == pj.PJSIP_INV_STATE_CONFIRMED:
                logger.info("✅ CALL CONNECTED - Starting AI integration")
                self.call_active = True
                self.call_start_time = time.time()
                self._start_ai_integration(ci)
                # Set call duration timer
                if self.config.MAX_CALL_DURATION > 0:
                    timer = threading.Timer(self.config.MAX_CALL_DURATION, self._timeout_call)
                    timer.daemon = True
                    timer.start()
                    logger.info(f"⏰ Call will timeout in {self.config.MAX_CALL_DURATION} seconds")
            elif ci.state == pj.PJSIP_INV_STATE_DISCONNECTED:
                self.call_active = False
                logger.info(f"📵 Parent Call Ended")

                if hasattr(self, 'recorder') and self.recorder:
                    try:
                        self.recorder.stop_record()
                        wav_bytes = self.recorder.get_wav_bytes()
                        rec_name = getattr(self, 'rec_filename', "recording.wav")
                        if wav_bytes:
                            self._save_recording(wav_bytes, rec_name)
                        self.recorder = None
                    except Exception as e:
                        logger.error(f"Recording stop error: {e}")

                self._stop_ai_integration()

                # If Caller hangs up, we must hang up the Transfer Leg
                if self.transfer_leg:
                    try:
                        self.transfer_leg.hangup(pj.CallOpParam())
                    except:
                        pass

                # Remove from active calls to free resources
                if hasattr(self, 'account') and hasattr(self.account, 'active_calls'):
                    call_id_val = ci.id if hasattr(ci, 'id') else None
                    if call_id_val is not None and call_id_val in self.account.active_calls:
                        del self.account.active_calls[call_id_val]
                        logger.info(f"🧹 Removed call {call_id_val} from active_calls")

                # Force immediate re-registration to be ready for next call
                try:
                    self.account.setRegistration(True)
                    logger.info("🔄 Triggered immediate SIP re-registration")
                except Exception as e:
                    logger.warning(f"⚠️ Re-registration trigger failed: {e}")
        except Exception as e:
            logger.error(f"❌ Error in onCallState: {e}")

    def onCallMediaState(self, prm):
        try:
            ci = self.getInfo()
            for mi in ci.media:
                if mi.type == pj.PJMEDIA_TYPE_AUDIO and mi.status == pj.PJSUA_CALL_MEDIA_ACTIVE:

                    # --- ADD THIS CHECK ---
                    if self.audio_bridge is not None:
                        logger.info("🎵 Audio media updated, but bridge is already active. Skipping re-init.")
                        return
                    # ----------------------

                    logger.info("🎵 Setting up audio bridge")
                    aud_media = self.getAudioMedia(mi.index)

                    # 1. Start AI Bridge
                    self.audio_bridge = AudioBridge(self.config)
                    self.audio_bridge.call_obj = self  # Link the call object to inspect playing state
                    aud_media.startTransmit(self.audio_bridge)
                    self.audio_bridge.startTransmit(aud_media)
                    self.audio_bridge.active = True
                    self.audio_bridge.start()

                    # 2. Start Recording
                    try:
                        self.recorder = MemoryAudioRecorder()
                        timestamp = time.strftime("%Y%m%d_%H%M%S")
                        call_id_clean = "".join(char for char in self.custom_call_id if char.isalnum())
                        self.rec_filename = f"call_{timestamp}_{call_id_clean}.wav"
                        self.recorder.start_record()
                        aud_media.startTransmit(self.recorder)
                        self.audio_bridge.startTransmit(self.recorder)
                    except Exception as rec_err:
                        logger.error(f"❌ Failed to start recorder: {rec_err}")

                    # 3. Play Welcome Audio — delayed until async consumer is ready
                    if self.config.WELCOME_AUDIO_FILE and os.path.exists(self.config.WELCOME_AUDIO_FILE):
                        self.stop_welcome_audio_event = threading.Event()
                        self.first_ai_audio_received = False
                        welcome_thread = threading.Thread(
                            target=self._play_wav_delayed,
                            args=(self.config.WELCOME_AUDIO_FILE, self.stop_welcome_audio_event),
                            daemon=True
                        )
                        welcome_thread.start()
                        logger.info(f"🎵 Welcome audio thread started")
                    else:
                        logger.info(
                            "🎵 No welcome audio file found; scheduling immediate welcome_completed notification")

                        def notify_welcome_skipped():
                            # Poll until WebSocket connection is ready
                            wait_count = 0
                            while wait_count < 40:
                                if self.ai_client and self.ai_client.state == WebSocketState.READY:
                                    asyncio.run_coroutine_threadsafe(
                                        self.ai_client.websocket.send(json.dumps({"event": "welcome_completed"})),
                                        self.async_loop
                                    )
                                    logger.info("📡 Sent welcome_completed event (skipped)")
                                    break
                                time.sleep(0.1)
                                wait_count += 1

                        threading.Thread(target=notify_welcome_skipped, daemon=True).start()

                    logger.info("✅ Audio bridge connected and started")

        except Exception as e:
            logger.error(f"❌ Error in onCallMediaState: {e}")

    def _save_recording(self, wav_bytes, filename):
        """Helper to upload recording to Firebase Storage and update Firestore (non-blocking)."""
        custom_id = self.custom_call_id

        def _upload():
            try:
                bucket = storage.bucket()
                blob = bucket.blob(f"recordings/{filename}")
                blob.upload_from_string(wav_bytes, content_type='audio/wav')
                blob.make_public()
                rec_url = blob.public_url
                logger.info(f"☁️ Uploaded to Firebase Storage: {rec_url}")
                try:
                    calls_ref = db.collection('tenants').document('mes_hosp').collection('calls')
                    query = calls_ref.where('callId', '==', custom_id).limit(1).get()
                    for doc in query:
                        doc.reference.update({"recordingUrl": rec_url})
                        logger.info("✅ Updated Firestore with recordingUrl")
                except Exception as e:
                    logger.error(f"Failed to update Firestore with recordingUrl: {e}")
            except Exception as e:
                logger.error(f"Failed to upload recording: {e}")

        _fs_executor.submit(_upload)

    def _reset_capture_batch(self):
        self._capture_batch = bytearray()
        self._capture_batch_started_at = 0.0

    def _reset_caller_speech_gate(self):
        self._caller_speech_active = False
        self._caller_speech_candidate_frames = 0
        self._barge_in_candidate_frames = 0
        self._caller_speech_last_loud_at = 0.0
        self._caller_speech_started_at = 0.0
        self._capture_preroll = bytearray()

    def _remember_capture_preroll(self, audio_data: bytes):
        """Keep a bounded slice of audio immediately before speech starts."""
        frame_bytes = self.config.SAMPLES_PER_FRAME * 2
        max_bytes = max(frame_bytes, self.config.CAPTURE_PREROLL_FRAMES * frame_bytes)
        self._capture_preroll.extend(audio_data)
        if len(self._capture_preroll) > max_bytes:
            del self._capture_preroll[:-max_bytes]

    def _seed_capture_from_preroll(self):
        if self._capture_preroll:
            self._capture_batch.extend(self._capture_preroll)
            self._capture_preroll.clear()
            if not self._capture_batch_started_at:
                self._capture_batch_started_at = time.time()

    def _update_caller_noise_floor(self, speech_rms: float):
        if self._caller_speech_active:
            return
        current = getattr(self, "_caller_voice_noise_rms", 35.0)
        if speech_rms > max(350.0, current * 4.0):
            return
        alpha = 0.02 if speech_rms > current else 0.08
        self._caller_voice_noise_rms = max(20.0, min(220.0, current + ((speech_rms - current) * alpha)))

    def _speech_start_rms(self) -> float:
        noise_floor = getattr(self, "_caller_voice_noise_rms", 35.0)
        return max(130.0, self.config.CAPTURE_SPEECH_START_RMS, noise_floor * 3.0)

    def _speech_start_frames(self) -> int:
        return max(2, self.config.CAPTURE_SPEECH_START_FRAMES)

    def _speech_continue_rms(self) -> float:
        noise_floor = getattr(self, "_caller_voice_noise_rms", 35.0)
        return max(70.0, self.config.CAPTURE_SPEECH_CONTINUE_RMS, noise_floor * 1.8)

    def _speech_tail_sec(self) -> float:
        return max(0.80, self.config.CAPTURE_SPEECH_TAIL_SEC)

    def _min_turn_sec(self) -> float:
        return max(0.18, self.config.CAPTURE_MIN_TURN_SEC)

    async def _mark_caller_speech_start(self):
        if self._caller_speech_active:
            return
        self._caller_speech_active = True
        self._caller_speech_started_at = time.time()
        if self.ai_client:
            await self.ai_client.send_control_event("speech_start")

    async def _mark_caller_speech_end(self):
        now = time.time()
        since_last_end = now - self._last_speech_end_sent_at
        if since_last_end < self.config.CAPTURE_MIN_SPEECH_END_INTERVAL_SEC:
            await asyncio.sleep(self.config.CAPTURE_MIN_SPEECH_END_INTERVAL_SEC - since_last_end)
            now = time.time()
        if self._caller_speech_started_at and now - self._caller_speech_started_at < self._min_turn_sec():
            self._reset_capture_batch()
            self._reset_caller_speech_gate()
            return
        if self.ai_client:
            await self._flush_capture_batch()
            await self.ai_client.send_control_event("speech_end")
            self._last_speech_end_sent_at = now
        self._reset_caller_speech_gate()

    def _drain_capture_queue(self, reason: str) -> int:
        if not self.audio_bridge:
            return 0
        drained = drain_thread_queue(self.audio_bridge.capture_queue)
        if drained:
            logger.info(f"Drained {drained} captured audio frames after {reason}")
        return drained

    async def _flush_capture_batch(self):
        if not self._capture_batch or not self.ai_client:
            return
        batch = bytes(self._capture_batch)
        self._reset_capture_batch()
        await self.ai_client.send_audio(batch)

    def transfer_to_extension(self, extension: str):
        logger.info(f"🔄 INITIATING TRANSFER to {extension}")
        self.is_transferring = True
        self.extension_transferred_to = extension

        # 1. Clear any remaining AI audio from playback queue
        if self.audio_bridge:
            cleared = drain_thread_queue(self.audio_bridge.playback_queue)
            self.audio_bridge.stats['drain_thread_queue_calls'] += 1
            self.audio_bridge.stats['audio_chunks_dropped'] += cleared
            if cleared:
                logger.info(f"🧹 Cleared {cleared} AI audio packets before transfer music")

        # 2. Play transfer music
        self.play_transfer_music()

        # 3. Create the second leg
        if "@" in extension:
            dest_uri = f"sip:{extension}"
        else:
            dest_uri = f"sip:{extension}@{self.config.SIP_DOMAIN}:{self.config.SIP_PORT};transport=udp"

        try:
            self.transfer_leg = TransferCall(self.account, self, dest_uri, self.config)

            call_prm = pj.CallOpParam()
            call_prm.opt.audioCount = 1
            call_prm.opt.videoCount = 0

            self.transfer_leg.makeCall(dest_uri, call_prm)
            logger.info(f"🚀 Outbound INVITE sent to {dest_uri}")

        except Exception as e:
            logger.error(f"❌ Failed to initiate transfer: {e}")
            self.stop_transfer_music()
            self.is_transferring = False
            if self.audio_bridge:
                self.audio_bridge.active = True

    def play_transfer_music(self):
        self.stop_transfer_music_event = threading.Event()
        if self.config.TRANSFER_AUDIO_FILE and os.path.exists(self.config.TRANSFER_AUDIO_FILE):
            logger.info(f"🎶 Playing transfer music: {self.config.TRANSFER_AUDIO_FILE}")
            t = threading.Thread(
                target=self._play_wav_thread,
                args=(self.config.TRANSFER_AUDIO_FILE, self.stop_transfer_music_event),
                daemon=True
            )
            t.start()
        else:
            logger.info("🎵 No transfer WAV found — generating ringback tone")
            t = threading.Thread(
                target=self._play_ringback_tone_thread,
                args=(self.stop_transfer_music_event,),
                daemon=True
            )
            t.start()

    def _play_ringback_tone_thread(self, stop_event: threading.Event):
        """Generate and play a standard telephony ringback tone (440+480 Hz)."""
        try:
            sample_rate = self.config.SIP_SAMPLE_RATE
            chunk_samples = self.config.SAMPLES_PER_FRAME
            chunk_bytes = chunk_samples * 2

            tone_duration = 2.0
            silence_duration = 4.0

            tone_samples = int(sample_rate * tone_duration)
            t = np.arange(tone_samples) / sample_rate
            tone = (np.sin(2 * np.pi * 440 * t) + np.sin(2 * np.pi * 480 * t)) * 0.25
            tone_pcm = (tone * 32767).clip(-32768, 32767).astype(np.int16)

            silence_samples = int(sample_rate * silence_duration)
            silence_pcm = np.zeros(silence_samples, dtype=np.int16)

            cycle_pcm = np.concatenate([tone_pcm, silence_pcm])
            cycle_bytes = cycle_pcm.tobytes()

            logger.info(f"🎵 Ringback tone ready: {len(cycle_pcm) / sample_rate:.1f}s cycle, "
                        f"{tone_duration}s tone + {silence_duration}s silence")

            while self.call_active and not stop_event.is_set():
                offset = 0
                while offset < len(cycle_bytes) and self.call_active and not stop_event.is_set():
                    chunk = cycle_bytes[offset:offset + chunk_bytes]
                    if len(chunk) < chunk_bytes:
                        chunk = chunk + bytes(chunk_bytes - len(chunk))

                    try:
                        if self.audio_bridge:
                            self.audio_bridge.playback_queue.put(chunk, timeout=0.1)
                            # Track max queue depth
                            q_depth = self.audio_bridge.playback_queue.qsize()
                            if q_depth > self.audio_bridge.stats.get('max_playback_queue_depth', 0):
                                self.audio_bridge.stats['max_playback_queue_depth'] = q_depth
                        else:
                            return
                    except queue.Full:
                        if stop_event.is_set():
                            return

                    offset += chunk_bytes
                    time.sleep(0.018)

            logger.info("🎵 Ringback tone stopped")
        except Exception as e:
            logger.error(f"❌ Ringback tone error: {e}")

    def stop_transfer_music(self):
        if hasattr(self, 'stop_transfer_music_event') and self.stop_transfer_music_event:
            self.stop_transfer_music_event.set()

    def _start_ai_integration(self, call_info):
        def run_async_loop():
            try:
                call_id = str(call_info.id) if call_info else "unknown"
                caller_id = "unknown"
                pj.Endpoint.instance().libRegisterThread(f"aicall_{call_id}")
                if call_info:
                    remote_uri = call_info.remoteUri
                    if "@" in remote_uri and ":" in remote_uri:
                        caller_id = remote_uri.split("@")[0].split(":")[-1].strip("<>\"")

                # Use custom_call_id (UUID) so Firestore documents match between app and bridge
                ai_call_id = self.custom_call_id

                logger.info(f"🤖 Starting AI integration for call {call_id} (AI ID: {ai_call_id}) from {caller_id}")
                self.async_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.async_loop)

                try:
                    self.async_loop.run_until_complete(self._run_ai_client(ai_call_id, caller_id))
                except asyncio.CancelledError:
                    logger.info("✅ AI Task Cancelled")

            except Exception as e:
                logger.error(f"❌ AI integration error: {e}")

        self.async_thread = threading.Thread(target=run_async_loop, daemon=True)
        self.async_thread.start()

    async def _shutdown_tasks(self):
        tasks = [t for t in asyncio.all_tasks(self.async_loop) if t is not asyncio.current_task()]
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.async_loop.stop()

    def _stop_ai_integration(self):
        logger.info("🛑 Stopping AI integration")
        self.call_active = False
        self.first_ai_audio_received = False  # Reset for next call
        self.is_transferring = False  # Reset transfer state
        self._ai_playing = False
        self._welcome_playing = False
        self._ai_queue_empty_since = 0.0
        self._last_ai_audio_queued_at = 0.0
        self._reset_capture_batch()

        if self.stop_welcome_audio_event:
            self.stop_welcome_audio_event.set()

        if self.audio_bridge:
            self.audio_bridge.stop()

        # --- DO NOT BLOCK PJSIP ---
        def cleanup_async_loop():
            if self.async_loop and self.async_loop.is_running():
                try:
                    # Fire and forget the shutdown tasks
                    asyncio.run_coroutine_threadsafe(self._shutdown_tasks(), self.async_loop)
                except Exception as e:
                    logger.warning(f"⚠️ Shutdown cleanup warning: {e}")

        # Run cleanup in a separate daemon thread
        threading.Thread(target=cleanup_async_loop, daemon=True).start()
        # --------------------------

    async def _run_ai_client(self, call_id: str, caller_id: str):
        try:
            max_wait = 10
            wait_count = 0
            while not self.audio_bridge or not self.audio_bridge.active:
                if wait_count >= max_wait:
                    logger.error("❌ Audio bridge failed to initialize in time")
                    return
                logger.info(f"⏳ Waiting for audio bridge... ({wait_count + 1}/{max_wait})")
                await asyncio.sleep(1)
                wait_count += 1

            logger.info("✅ Audio bridge is ready")

            self.ai_client = AIWebSocketClient(call_object=self)
            self._reset_capture_batch()
            self._drain_capture_queue("AI client startup")
            if not await self.ai_client.connect(call_id, caller_id): return

            tasks = [
                asyncio.create_task(self._bridge_captured_audio()),
                asyncio.create_task(self._bridge_playback_audio()),
                asyncio.create_task(self._monitor_call_state())
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"❌ AI client error: {e}")
        finally:
            if self.ai_client:
                await self.ai_client.disconnect()

    async def _bridge_captured_audio(self):
        logger.info("🎤 Starting capture bridge")
        packet_count = 0

        while self.call_active:
            try:
                if self.is_transferring:
                    self._reset_capture_batch()
                    await asyncio.sleep(0.02)
                    continue

                if getattr(self, 'audio_bridge', None):
                    try:
                        audio_data = self.audio_bridge.capture_queue.get_nowait()

                        # Never let Maya's own phone-line echo become a caller turn.
                        # RMS-only barge-in is unsafe: real traces show playback echo
                        # above 3000 RMS, easily exceeding ordinary caller speech.
                        rms = AudioAnalyzer.calculate_rms(audio_data)
                        speech_rms = AudioAnalyzer.calculate_voice_rms(audio_data, self.config.SIP_SAMPLE_RATE)
                        BARGE_IN_THRESHOLD = self.config.BARGE_IN_RMS
                        BARGE_IN_VOICE_THRESHOLD = self.config.BARGE_IN_VOICE_RMS
                        BARGE_IN_START_FRAMES = self.config.BARGE_IN_START_FRAMES

                        # The prerecorded welcome is not interruptible. Its phone-line
                        # echo can be louder than the barge-in threshold and previously
                        # opened a fake caller turn before the user spoke.
                        if self._welcome_playing:
                            self._reset_capture_batch()
                            self._reset_caller_speech_gate()
                            self._drain_capture_queue("welcome playback active (echo suppressed)")
                            continue

                        if time.time() < getattr(self, "_welcome_echo_block_until", 0.0):
                            self._reset_capture_batch()
                            self._reset_caller_speech_gate()
                            self._drain_capture_queue("welcome tail active (echo suppressed)")
                            continue

                        if self._ai_playing:
                            if not self.config.ALLOW_BARGE_IN:
                                self._reset_capture_batch()
                                self._reset_caller_speech_gate()
                                self._drain_capture_queue("AI playback active (barge-in disabled)")
                                continue

                            if speech_rms < BARGE_IN_VOICE_THRESHOLD or rms < BARGE_IN_THRESHOLD:
                                self._reset_capture_batch()
                                self._reset_caller_speech_gate()
                                self._drain_capture_queue("AI playback active (echo suppressed)")
                                continue

                            self._barge_in_candidate_frames += 1
                            self._remember_capture_preroll(audio_data)
                            if self._barge_in_candidate_frames < BARGE_IN_START_FRAMES:
                                continue
                            self._caller_speech_candidate_frames = max(
                                self._caller_speech_candidate_frames,
                                self._speech_start_frames()
                            )

                        if self._ai_play_ended_at > 0:
                            echo_age = time.time() - self._ai_play_ended_at
                            if echo_age < self._ECHO_TAIL_GUARD:
                                if rms < BARGE_IN_THRESHOLD or speech_rms < BARGE_IN_VOICE_THRESHOLD:
                                    self._reset_capture_batch()
                                    self._reset_caller_speech_gate()
                                    continue

                        if not self._caller_speech_active and not self._ai_playing:
                            self._update_caller_noise_floor(speech_rms)

                        now = time.time()
                        if speech_rms >= self._speech_start_rms():
                            if not self._caller_speech_active:
                                self._caller_speech_candidate_frames += 1
                                self._remember_capture_preroll(audio_data)
                                if self._caller_speech_candidate_frames < self._speech_start_frames():
                                    continue
                                self._seed_capture_from_preroll()
                                await self._mark_caller_speech_start()
                            self._caller_speech_last_loud_at = now
                        elif self._caller_speech_active and speech_rms >= self._speech_continue_rms():
                            self._caller_speech_last_loud_at = now
                        elif self._caller_speech_active:
                            if now - self._caller_speech_last_loud_at > self._speech_tail_sec():
                                await self._mark_caller_speech_end()
                                continue
                        else:
                            # Do not feed Gemini endless phone-line silence/noise between
                            # turns. Real-time Live VAD is much more reliable when each
                            # caller utterance arrives as a clean speech burst.
                            if not self._caller_speech_active:
                                self._caller_speech_candidate_frames = 0
                            self._remember_capture_preroll(audio_data)
                            self._reset_capture_batch()
                            continue

                        if packet_count % 50 == 0:
                            logger.info(
                                f"🎤 Bridging audio packet #{packet_count}: RMS = {rms:.1f}, Size={len(audio_data)}")

                        if self.ai_client:
                            if not self._capture_batch:
                                self._capture_batch_started_at = time.time()
                            self._capture_batch.extend(audio_data)
                            batch_bytes = self.config.SAMPLES_PER_FRAME * 2 * self.config.CAPTURE_BATCH_FRAMES
                            batch_age = time.time() - self._capture_batch_started_at
                            if len(self._capture_batch) >= batch_bytes or batch_age >= self.config.CAPTURE_BATCH_MAX_LATENCY_SEC:
                                await self._flush_capture_batch()
                            packet_count += 1
                    except queue.Empty:
                        if (
                                self._capture_batch
                                and self._capture_batch_started_at > 0
                                and (
                                time.time() - self._capture_batch_started_at) >= self.config.CAPTURE_BATCH_MAX_LATENCY_SEC
                        ):
                            await self._flush_capture_batch()
                        await asyncio.sleep(0.002)
                    except AttributeError:
                        # Call was destroyed in another thread
                        break
                else:
                    await asyncio.sleep(0.02)
            except Exception:
                await asyncio.sleep(0.02)

    async def _bridge_playback_audio(self):
        async def queue_playback_frame(frame: bytes) -> bool:
            """Queue one 20ms playback frame without adding async thread jitter."""
            while self.call_active and not self.is_transferring:
                audio_bridge = getattr(self, 'audio_bridge', None)
                if not audio_bridge:
                    return False
                try:
                    audio_bridge.playback_queue.put_nowait(frame)
                except queue.Full:
                    audio_bridge.stats['playback_queue_full_events'] += 1
                    try:
                        await asyncio.to_thread(
                            audio_bridge.playback_queue.put,
                            frame, True, 0.5,
                        )
                    except queue.Full:
                        continue
                    except AttributeError:
                        return False
                except AttributeError:
                    return False

                q_depth = audio_bridge.playback_queue.qsize()
                if q_depth > audio_bridge.stats.get('max_playback_queue_depth', 0):
                    audio_bridge.stats['max_playback_queue_depth'] = q_depth
                return True
            return False

        logger.info("🔊 Starting playback bridge")
        packet_count = 0
        while self.call_active:
            try:
                if self.is_transferring:
                    if self.ai_client:
                        self.ai_client._drain_async_queue(self.ai_client.audio_receive_queue,
                                                          "inbound AI during transfer")
                    await asyncio.sleep(0.02)
                    continue

                if self.ai_client and self.audio_bridge:
                    try:
                        audio_data = await asyncio.wait_for(
                            self.ai_client.audio_receive_queue.get(),
                            timeout=0.02
                        )

                        if packet_count % 50 == 0:
                            rms = AudioAnalyzer.calculate_rms(audio_data)
                            logger.info(f"🔊 AI audio #{packet_count}: RMS={rms:.1f}, Size={len(audio_data)}")
                        packet_count += 1

                        # Keep duplicate playback PCM. Identical chunks can be
                        # valid speech audio; dropping them creates audible gaps.
                        now = time.time()
                        crc = zlib.crc32(audio_data)
                        is_silent = False
                        if len(audio_data) > 0:
                            is_silent = (AudioAnalyzer.calculate_rms(audio_data) < 10)

                        if (
                                False
                                and not is_silent
                                and self.last_playback_audio_crc == crc
                                and self.last_playback_audio_len == len(audio_data)
                                and (now - self.last_playback_audio_ts) < 1.2
                        ):
                            logger.info("🧹 Dropped duplicate inbound AI playback packet")
                            continue
                        self.last_playback_audio_crc = crc
                        self.last_playback_audio_len = len(audio_data)
                        self.last_playback_audio_ts = now

                        # Stop welcome audio on first AI audio. Only drain queued
                        # frames if welcome playback is actually active; otherwise
                        # the first AI utterance can lose its leading audio.
                        if not self.first_ai_audio_received:
                            self.first_ai_audio_received = True
                            if self.stop_welcome_audio_event:
                                self.stop_welcome_audio_event.set()
                            if self._welcome_playing:
                                cleared = drain_thread_queue(self.audio_bridge.playback_queue)
                                self.audio_bridge.stats['drain_thread_queue_calls'] += 1
                                self.audio_bridge.stats['audio_chunks_dropped'] += cleared
                                if cleared:
                                    logger.info(f"Cleared {cleared} queued welcome frames before AI playback")

                        # Mark AI as actively playing — suppresses capture echo gate
                        self._ai_playing = True
                        self._last_ai_audio_queued_at = time.time()

                        if not hasattr(self, '_playback_chunk_remainder'):
                            self._playback_chunk_remainder = bytearray()

                        full_data = self._playback_chunk_remainder + audio_data
                        chunk_size = self.config.SAMPLES_PER_FRAME * 2  # 320 bytes at 8kHz

                        # Only process complete chunks
                        valid_len = len(full_data) - (len(full_data) % chunk_size)
                        for i in range(0, valid_len, chunk_size):
                            chunk = full_data[i:i + chunk_size]
                            if not await queue_playback_frame(chunk):
                                break

                        remainder_len = len(full_data) % chunk_size
                        if remainder_len != 0:
                            self._playback_chunk_remainder = bytearray(full_data[-remainder_len:])
                        else:
                            self._playback_chunk_remainder = bytearray()

                    except asyncio.TimeoutError:
                        pass

                # Update echo gate: if playback queue is empty, AI *might* have finished speaking.
                # But the queue can be momentarily empty between audio chunks. We require
                # the queue to stay empty briefly before declaring AI speech truly ended,
                # preventing echo from leaking through during brief inter-chunk gaps.
                qsize = self.audio_bridge.playback_queue.qsize() if self.audio_bridge else 0
                is_server_done = getattr(self, '_server_done_speaking', False)
                server_done_at = getattr(self, '_server_done_speaking_at', 0.0)

                if (
                        self.audio_bridge
                        and self._ai_playing
                        and is_server_done
                        and getattr(self, '_playback_chunk_remainder', None)
                        and server_done_at > 0.0
                        and (time.time() - server_done_at) >= 0.02
                ):
                    chunk_size = self.config.SAMPLES_PER_FRAME * 2
                    remainder = bytes(self._playback_chunk_remainder)
                    self._playback_chunk_remainder = bytearray()
                    padded = fade_pcm16_tail(remainder + bytes(chunk_size - len(remainder)))
                    if not await queue_playback_frame(padded):
                        # Keep the final syllable pending; the next loop pass
                        # retries once the phone consumes another frame.
                        self._playback_chunk_remainder = bytearray(remainder)
                        await asyncio.sleep(0.01)
                        continue
                    qsize = self.audio_bridge.playback_queue.qsize()
                    logger.info(f"Padded and queued final AI audio frame: remainder={len(remainder)} bytes")

                if self.audio_bridge and qsize == 0:
                    if self._ai_playing:
                        now = time.time()
                        if self._ai_queue_empty_since == 0.0:
                            self._ai_queue_empty_since = now
                        else:
                            # Prefer the server's explicit end marker. A short
                            # queue gap is normal while Gemini is still producing
                            # audio and must not reopen the microphone to echo.
                            elapsed = now - self._ai_queue_empty_since
                            if is_server_done or elapsed >= 2.0:
                                self._ai_playing = False
                                self._server_done_speaking = False
                                self._server_done_speaking_at = 0.0
                                self._ai_play_ended_at = now
                                self._ai_queue_empty_since = 0.0
                                self._reset_capture_batch()
                                self._reset_caller_speech_gate()
                                logger.info(
                                    f"🔊 AI speaking ended: queue empty, server_done={is_server_done}, elapsed={elapsed:.2f}s")
                elif self.audio_bridge:
                    # Queue has audio — reset the empty timer
                    self._ai_queue_empty_since = 0.0

                await asyncio.sleep(0.001)
            except Exception:
                await asyncio.sleep(0.02)

    async def _monitor_call_state(self):
        while self.call_active:
            await asyncio.sleep(1)

    async def _clear_playback_queue(self):
        if not self.audio_bridge: return
        while not self.audio_bridge.playback_queue.empty():
            try:
                await asyncio.to_thread(self.audio_bridge.playback_queue.get_nowait)
            except queue.Empty:
                break

    def _play_wav_thread(self, wav_file_path: str, stop_event: threading.Event):
        try:
            time.sleep(0.3)  # Wait for async consumer to be ready
            fname = os.path.basename(wav_file_path)

            with wave.open(wav_file_path, 'rb') as wf:
                if wf.getframerate() != self.config.SIP_SAMPLE_RATE:
                    logger.error(
                        f"❌ WAV mismatch: {fname} is {wf.getframerate()}Hz, need {self.config.SIP_SAMPLE_RATE}Hz")
                    stop_event.set()
                    return

                chunk_size = self.config.SAMPLES_PER_FRAME
                logger.info(f"🎵 Playing: {fname}")
                prebuffered_frames = 0
                if fname == os.path.basename(self.config.WELCOME_AUDIO_FILE):
                    while self.call_active and not stop_event.is_set():
                        audio_data = wf.readframes(chunk_size)
                        if not audio_data:
                            break
                        if not self.audio_bridge:
                            return
                        if len(audio_data) < chunk_size * 2:
                            audio_data = fade_pcm16_tail(audio_data + bytes(chunk_size * 2 - len(audio_data)))
                        self.audio_bridge.playback_queue.put(
                            limit_pcm16(audio_data, self.config.PLAYBACK_PEAK_LIMIT),
                            timeout=0.5,
                        )
                        prebuffered_frames += 1
                    logger.info(f"Prebuffered welcome audio frames: {prebuffered_frames}")
                    while (
                        self.call_active
                        and not stop_event.is_set()
                        and self.audio_bridge
                        and self.audio_bridge.playback_queue.qsize() > 0
                    ):
                        time.sleep(0.02)
                    logger.info(f"🎵 WAV finished: {fname}")
                    return

                audio_data = wf.readframes(chunk_size)

                while audio_data and self.call_active and not stop_event.is_set():
                    if not self.audio_bridge:
                        break
                    try:
                        self.audio_bridge.playback_queue.put(limit_pcm16(audio_data, self.config.PLAYBACK_PEAK_LIMIT),
                                                             timeout=0.5)
                        # Track max queue depth
                        q_depth = self.audio_bridge.playback_queue.qsize()
                        if q_depth > self.audio_bridge.stats.get('max_playback_queue_depth', 0):
                            self.audio_bridge.stats['max_playback_queue_depth'] = q_depth
                        audio_data = wf.readframes(chunk_size)  # Only advance on successful put
                        # Feed slightly faster than real time so the welcome also
                        # maintains a cushion instead of underrunning on Windows.
                        time.sleep(0.018)
                    except queue.Full:
                        if stop_event.is_set():
                            break
                        time.sleep(0.02)
                        continue  # Retry the SAME chunk

            logger.info(f"🎵 WAV finished: {fname}")
        except Exception as e:
            logger.error(f"❌ WAV playback error: {e}")
        finally:
            stop_event.set()

    def _play_wav_delayed(self, path: str, stop_event: threading.Event):
        """Wait for the async AI client to be fully connected before playing welcome audio."""
        wait_count = 0
        max_wait = 40  # 4 seconds max
        while wait_count < max_wait:
            if stop_event.is_set():
                return
            if (self.ai_client and
                    self.ai_client.state == WebSocketState.READY and
                    self.audio_bridge and
                    self.audio_bridge.active):
                break
            time.sleep(0.1)
            wait_count += 1

        if wait_count >= max_wait:
            logger.warning("Welcome audio: timed out waiting for AI client, playing anyway")

        if not stop_event.is_set():
            # Mark welcome as playing so echo gate suppresses captured audio
            self._welcome_playing = True
            try:
                self._play_wav_thread(path, stop_event)
            finally:
                self._welcome_playing = False
                # Set echo tail guard so residual echo after welcome is also blocked
                self._ai_play_ended_at = time.time()
                self._welcome_echo_block_until = time.time() + 0.90
                self._reset_capture_batch()
                self._reset_caller_speech_gate()
                self._drain_capture_queue("welcome completed (tail echo suppressed)")
                # Notify new.py welcome audio completed
                if self.ai_client and self.ai_client._is_websocket_open():
                    asyncio.run_coroutine_threadsafe(
                        self.ai_client.websocket.send(json.dumps({"event": "welcome_completed"})),
                        self.async_loop
                    )
                    logger.info("📡 Sent welcome_completed event")


# ==========================================
# 8. ACCOUNT & MAIN APP STRUCTURE
# ==========================================

class AISipAccount(pj.Account):
    def __init__(self, config: BridgeConfig):
        pj.Account.__init__(self)
        self.config = config
        self.active_calls: Dict[int, AICall] = {}

    def onRegState(self, prm):
        ai = self.getInfo()
        logger.info(f"📞 SIP Registration: {ai.regStatusText}")

    def onIncomingCall(self, prm):
        logger.info("📞 INCOMING CALL")
        call = AICall(self, self.config, prm.callId)
        self.active_calls[prm.callId] = call
        if self.config.AUTO_ANSWER:
            call_prm = pj.CallOpParam()
            call_prm.statusCode = 200
            call.answer(call_prm)
        return call


def update_heartbeat(service_name):
    """Periodically updates this service's timestamp in the shared health file."""
    while True:
        try:
            data = {}
            if os.path.exists(HEALTH_FILE):
                try:
                    with open(HEALTH_FILE, "r") as f:
                        data = json.load(f)
                except (json.JSONDecodeError, IOError):
                    data = {}

            data[service_name] = time.time()

            temp_file = f"{HEALTH_FILE}.tmp"
            try:
                with open(temp_file, "w") as f:
                    json.dump(data, f)
                os.replace(temp_file, HEALTH_FILE)
            except IOError as e:
                logger.warning(f"Failed to write heartbeat ({service_name}): {e}")

        except Exception as e:
            logger.error(f"Health heartbeat error: {e}")

        time.sleep(HEARTBEAT_INTERVAL)


class SIPBridge:
    def __init__(self, config: Optional[BridgeConfig] = None):
        self.config = config if config else BridgeConfig()
        self.endpoint: Optional[pj.Endpoint] = None
        self.account: Optional[AISipAccount] = None
        self.running = False

    def initialize(self) -> bool:
        try:
            logger.info("🚀 Initializing SIP Bridge")
            self.endpoint = pj.Endpoint()
            self.endpoint.libCreate()
            ep_cfg = pj.EpConfig()
            ep_cfg.logConfig.level = 3
            ep_cfg.logConfig.consoleLevel = 3
            ep_cfg.uaConfig.stunEnabled = False
            ep_cfg.uaConfig.maxCalls = 64
            ep_cfg.medConfig.clockRate = self.config.SIP_SAMPLE_RATE
            ep_cfg.medConfig.sndClockRate = self.config.SIP_SAMPLE_RATE
            ep_cfg.medConfig.channelCount = self.config.CHANNELS
            ep_cfg.medConfig.audioFramePtime = 20
            ep_cfg.medConfig.sndRecLatency = 20
            ep_cfg.medConfig.sndPlayLatency = 100
            ep_cfg.medConfig.quality = 10
            self.endpoint.libInit(ep_cfg)
            self.endpoint.audDevManager().setNullDev()

            transport_cfg = pj.TransportConfig()
            transport_cfg.port = self.config.SIP_TRANSPORT_PORT
            try:
                self.endpoint.transportCreate(pj.PJSIP_TRANSPORT_UDP, transport_cfg)
                logger.info(f"✅ SIP transport created on port {transport_cfg.port}")
            except Exception as e:
                logger.warning(f"⚠️ Primary port {transport_cfg.port} failed ({e}), trying {transport_cfg.port + 1}...")
                transport_cfg.port = self.config.SIP_TRANSPORT_PORT + 1
                self.endpoint.transportCreate(pj.PJSIP_TRANSPORT_UDP, transport_cfg)
                logger.info(f"✅ SIP transport created on backup port {transport_cfg.port}")

            self.endpoint.libStart()
            self._create_account()
            return True
        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}")
            return False

    def _create_account(self):
        acc_cfg = pj.AccountConfig()
        acc_cfg.idUri = f"sip:{self.config.SIP_USER}@{self.config.SIP_DOMAIN}"
        acc_cfg.regConfig.registrarUri = f"sip:{self.config.SIP_DOMAIN}:{self.config.SIP_PORT}"

        # --- NEW FIX: Aggressive Registration Timeout ---
        # The default is 300s. Your network drops UDP after ~120-180s.
        # Forcing a full SIP REGISTER every 60 seconds keeps the port permanently open.
        acc_cfg.regConfig.timeoutSec = 60
        # ------------------------------------------------

        cred = pj.AuthCredInfo("digest", "*", self.config.SIP_USER, 0, self.config.SIP_PASSWORD)
        acc_cfg.sipConfig.authCreds.append(cred)
        acc_cfg.sipConfig.proxies.append(f"sip:{self.config.SIP_DOMAIN}:{self.config.SIP_PORT};transport=udp")

        acc_cfg.natConfig.sipOutboundUse = 1
        acc_cfg.natConfig.udpKaIntervalSec = 15  # Keep this, it acts as a secondary failsafe

        self.account = AISipAccount(self.config)
        self.account.create(acc_cfg)

    def run(self):
        # Register signal handlers for graceful shutdown
        def signal_handler(sig, frame):
            sig_name = signal.Signals(sig).name
            logger.info(f"⚠️ Received {sig_name}, initiating graceful shutdown...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            self.running = True
            logger.info("🤖 AI-ENHANCED SIP BRIDGE READY")
            while self.running:
                self.endpoint.libHandleEvents(10)
        except Exception as e:
            logger.error(f"❌ Runtime error: {e}")
        finally:
            self.shutdown()

    def shutdown(self):
        if not hasattr(self, '_shutting_down'):
            self._shutting_down = True
        else:
            return

        logger.info("🛑 Shutting down SIP Bridge")
        self.running = False
        try:
            if self.account:
                self.account.shutdown()
                del self.account
            if self.endpoint:
                self.endpoint.libDestroy()
                del self.endpoint
        except Exception:
            pass


if __name__ == "__main__":

    # start heartbeat
    threading.Thread(
        target=update_heartbeat,
        args=("dash_bridge",),
        daemon=True
    ).start()

    config = BridgeConfig()
    bridge = SIPBridge(config)

    if bridge.initialize():
        bridge.run()
