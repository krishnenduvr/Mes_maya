"""MES Maya: compact Firestore-grounded Gemini Live voice server."""

from __future__ import annotations

import asyncio
import errno
import importlib.util
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path

import firebase_admin
from fastapi import FastAPI
from firebase_admin import credentials, firestore
from google import genai
from google.oauth2 import service_account
from google.genai.types import (
    AudioTranscriptionConfig,
    FunctionDeclaration,
    LanguageHints,
    LiveConnectConfig,
    PrebuiltVoiceConfig,
    SpeechConfig,
    Tool,
    VoiceConfig,
    ThinkingConfig,
)
import uvicorn
import websockets


def load_local_env(path: str = ".env") -> None:
    """Load KEY=VALUE pairs without requiring python-dotenv in production."""
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_local_env()


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            "maya.log", maxBytes=10 * 1024 * 1024, backupCount=2, encoding="utf-8"
        ),
    ],
)
log = logging.getLogger("maya")


@dataclass(frozen=True)
class Settings:
    project: str = os.getenv("GOOGLE_CLOUD_PROJECT", "docbooking-9ec13")
    location: str = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    model: str = os.getenv("GEMINI_MODEL", "gemini-live-2.5-flash-native-audio")
    ai_studio_model: str = os.getenv("GEMINI_API_MODEL", "gemini-3.1-flash-live-preview")
    voice: str = os.getenv("MAYA_VOICE", "Aoede")
    tenant_collection: str = os.getenv("TENANT_COLLECTION", "tenants")
    tenant_id: str = os.getenv("TENANT_ID", "mes_hosp")
    firebase_credentials: str = os.getenv("FIREBASE_CREDENTIALS", "mesService.json")
    firebase_database: str = os.getenv("FIREBASE_DATABASE", "default")
    google_credentials: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.getenv("FIREBASE_CREDENTIALS", "mesService.json"),
    )
    input_language_hints: str = os.getenv("MAYA_INPUT_LANGUAGE_HINTS", "ml-IN")
    ws_host: str = os.getenv("WS_HOST", "0.0.0.0")
    ws_port: int = int(os.getenv("WS_PORT", "8081"))
    health_port: int = int(os.getenv("HEALTH_PORT", "9000"))
    input_silence_fallback_sec: float = float(os.getenv("MAYA_INPUT_SILENCE_FALLBACK_SEC", "0.75"))
    caller_audio_queue_packets: int = int(os.getenv("MAYA_CALLER_AUDIO_QUEUE_PACKETS", "400"))
    caller_audio_flush_timeout_sec: float = float(os.getenv("MAYA_CALLER_AUDIO_FLUSH_TIMEOUT_SEC", "1.20"))
    gemini_audio_send_timeout_sec: float = float(os.getenv("MAYA_GEMINI_AUDIO_SEND_TIMEOUT_SEC", "1.5"))
    use_firestore_prompt: bool = os.getenv("USE_FIRESTORE_PROMPT", "false").casefold() == "true"


CFG = Settings()
IST = timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")
STARTED = datetime.now(IST)


def init_firestore():
    if not firebase_admin._apps:
        key = Path(CFG.firebase_credentials)
        if not key.exists():
            raise FileNotFoundError(
                f"Firebase credential not found: {key}. Set FIREBASE_CREDENTIALS."
            )
        firebase_admin.initialize_app(credentials.Certificate(str(key)))
    return firestore.client(database_id=CFG.firebase_database)


db = init_firestore()
_cache_lock = threading.RLock()
_doctor_cache: list[dict] = []
_clinic_cache: list[dict] = []
_prompt_cache = Path("maya_prompt.txt").read_text(encoding="utf-8")
_cache_updated_at = 0.0
FIRESTORE_TIMEOUT_SEC = float(os.getenv("FIRESTORE_TIMEOUT_SEC", "6"))

MALAYALAM_REPEAT_TEXT = "ഒന്ന് വ്യക്തമായി പറയാമോ?"
MALAYALAM_ASR_PHRASES = [
    "ഡോക്ടർ",
    "ഡോക്ടറെ ബുക്ക് ചെയ്യണം",
    "അപ്പോയിന്റ്മെന്റ് ബുക്ക് ചെയ്യണം",
    "ബുക്കിംഗ് വേണം",
    "ഏത് ഡോക്ടർ",
    "ഏത് ഡിപ്പാർട്ട്മെന്റ്",
    "ഡോക്ടറുടെ പേര്",
    "ഡോക്ടർ ഉണ്ടോ",
    "ഡോക്ടർ ഇന്ന് ഉണ്ടോ",
    "ഡോക്ടർ നാളെ ഉണ്ടോ",
    "ഡോക്ടറുടെ സമയം",
    "കൺസൾട്ടേഷൻ സമയം",
    "ഫീസ് എത്രയാണ്",
    "പേഷ്യന്റിന്റെ പേര്",
    "OP നമ്പർ",
    "സർജറി ഡിപ്പാർട്ട്മെന്റ്",
    "ജനറൽ സർജറി",
    "ഓർത്തോ",
    "ഇ എൻ ടി",
    "ഗൈനക്കോളജി",
    "ന്യൂറോളജി",
    "കാർഡിയോളജി",
    "ഡെർമറ്റോളജി",
    "പീഡിയാട്രിക്",
    "ഡെന്റൽ",
    "കണ്ണ്",
    "തലവേദന",
    "പല്ലുവേദന",
    "വയറുവേദന",
    "നെഞ്ചുവേദന",
    "പനി",
    "ചുമ",
    "ശരീരവേദന",
    "ത്വക്ക് പ്രശ്നം",
    "ആശുപത്രി എവിടെയാണ്",
    "റിസപ്ഷൻ കണക്റ്റ് ചെയ്യൂ",
    "മലയാളത്തിൽ പറയാം",
    "വ്യക്തമായി പറയാം",
]

GROUNDING_RULES = """
[NON-NEGOTIABLE ACCURACY RULES - THESE OVERRIDE ALL EARLIER INSTRUCTIONS]
A prerecorded Malayalam greeting has already played. NEVER greet, welcome, or introduce yourself. Understand
both Malayalam and Manglish (Malayalam written/spoken with English letters). From the caller's first utterance
onward, answer directly in natural spoken Malayalam. Common hospital words such as doctor, department, fee,
booking, OP, and consultation may be spoken in their familiar English form. Use English only if explicitly asked.
If the caller only says "hello", "hi", "Maya", "are you there", or similar wake/check words, do not introduce
yourself; answer only in Malayalam or Manglish with a short prompt such as "പറയൂ, എന്താണ് അറിയേണ്ടത്?"
If the caller speaks in Latin-letter Manglish, reply in natural Manglish. If they speak Malayalam script or
Malayalam audio, reply in Malayalam. Never switch to English just because the caller used English filler words.
Trust only Malayalam, Malayalam-script text, and Latin-letter Manglish with familiar hospital English words.
Never treat Hindi, Devanagari, Tamil, Telugu, Kannada, Arabic, or any other non-Malayalam/non-Manglish
transcript as the caller's real question. If ASR produces unrelated languages/scripts, random mixed words, or
content that is not Malayalam/Manglish, do not answer it as fact and do not guess the intent; treat it as
unusable audio. Ask briefly in Malayalam: "ഒന്ന് വ്യക്തമായി പറയാമോ?"
If the caller's Malayalam/Manglish audio is transcribed as Hindi/Devanagari, ignore the Hindi-looking words.
Never use "ഒന്നുകൂടി പറഞ്ഞു തരുമോ?" or any repeat request as random filler. Ask for repetition only when
the caller's audio/transcript is genuinely unusable or a required doctor/department/name is missing.
For every doctor, department, clinic, fee, timing, schedule, availability, or day question, call the relevant
lookup tool in that same turn before speaking. The tool result is the only source of truth. Never answer such
questions from memory, earlier conversation, or schedule text in this prompt. Never merge details from two
doctors. If found is false, do not guess: state that the record was not found or ask the caller to confirm one
of the returned suggestions. Operation days are surgery-only and must never be offered for consultation.
If the caller asks this hospital's location, address, "evideya", "where is hospital", or "our hospital
location", answer directly in Malayalam: "MES Hospital Perinthalmannaയിലാണ്." Never ask or offer to connect
reception for location questions.
When a lookup returns found=false or count=0, never mention the model's guessed doctor/department name as a
fact. Ask the caller to repeat the doctor or department name in Malayalam/Manglish in one short sentence.
If consultation_time_over_today is true, explicitly say today's consultation has ended; never say the doctor
is available today.
Act like a friendly real-time Malayalam hospital telecaller: warm, polite, helpful, and natural. Make the
conversation feel interactive by taking one small step at a time.
Keep every reply short-short: one sentence, normally three to seven spoken words. If more information is
needed, ask only one simple next question. Do not give long explanations, lists, extra offers, repeated
confirmations, or filler such as "Would you like anything else?", "കൂടുതൽ അറിയണോ?", or "ഇനിയും സഹായം വേണോ?"
Never give long warning, disclaimer, or policy-style replies. Never say anything like:
"ഞാൻ മെഡിക്കൽ നിർദ്ദേശങ്ങളോ രോഗനിർണയമോ നൽകുന്നില്ല", "ഡോക്ടറെയോ ആരോഗ്യ വിദഗ്ധനെയോ കാണുക",
or "നിരാകരണം നൽകേണ്ടതുണ്ട്".
If the caller asks anything outside hospital-related information, reply only in Malayalam:
"എനിക്ക് ആശുപത്രിയുമായി ബന്ധപ്പെട്ട വിവരങ്ങൾ മാത്രമേ പറയാൻ കഴിയൂ."
Speak at a calm, natural telephone pace, slightly slower than normal conversation. Pronounce names, days,
times, and fees clearly, with brief pauses between phrases. Never rush, speed-read, or speak rapidly.
Doctor schedules, fees, availability, departments, and booking requests are administrative—not medical
advice. Never add a medical-advice, diagnosis, safety, or healthcare-professional disclaimer to any reply.
When a requested Firestore field is N/A or missing, state only that the specific information is unavailable;
do not direct the caller to reception or add another sentence.
If the caller asks for receptionist, reception, or front desk, reply only in Malayalam:
"ശരി, റിസപ്ഷനിലേക്ക് കണക്ട് ചെയ്യാം."
"രോഗി തന്റെ രോഗം പറഞ്ഞ് ഏത് ഡോക്ടറെ കാണണം എന്ന് ചോദിച്ചാൽ, മായാ നേരിട്ട് ശരിയായ ഡിപ്പാർട്ട്മെന്റ്/ഡോക്ടർ പറയണം."
```text
[BOOKING WORKFLOW]

When the caller wants to book an appointment:

1. If the doctor is not specified, first ask which doctor the caller wants to book with.

2. After a particular doctor is known, ask in Malayalam:
   "ഏത് ദിവസത്തേക്ക് ബുക്ക് ചെയ്യണം?"

3. Then ask in Malayalam:
   "ഇത് പുതിയ അപോയിന്റ്മെന്റാണോ?"

4. If the caller says **Yes (പുതിയത്)**:
   - Collect only the patient's name.

5. If the caller says **No (പഴയത്)**:
   - Ask in Malayalam:
     "താങ്കളുടെ OP നമ്പർ പറയാമോ?"
   - Collect the OP number.
   - Then collect the patient's name.

6. When the caller gives the patient's name, confirm it once in Malayalam:
   "പേഷ്യന്റിന്റെ പേര് {patient_name} അല്ലേ?"
   Wait for the caller's confirmation or correction before proceeding.

7. After the patient's name is confirmed, call `book_patient_appointment`.

8. If `book_patient_appointment` returns `success=true`, reply only:
   "ബുക്കിംഗ് കൺഫം ചെയ്തിട്ടുണ്ട്."

9. Immediately after confirming the booking, ask in Malayalam:
   "ഞാൻ റിസപ്ഷനിലേക്ക് കോളിൽ കണക്റ്റ് ചെയ്യട്ടെയോ?"

10. If the caller says **Yes**:
    - Connect the call to reception.
    - Do not ask any further booking questions.

11. If the caller says **No**:
    - Politely end the conversation.

Additional Rules:
- Do not ask for the phone number unless the caller provides it voluntarily.
- Do not skip the patient name confirmation before calling `book_patient_appointment`.
- Always ask the reception connection question after a successful booking.
- Never skip the reception connection step after booking.
```

""".strip()

# Override the legacy prompt text above, which may contain mojibake on Windows
# terminals, with clean Malayalam instructions used at runtime.
GROUNDING_RULES = """
[NON-NEGOTIABLE ACCURACY RULES - THESE OVERRIDE ALL EARLIER INSTRUCTIONS]
A prerecorded Malayalam greeting has already played. NEVER greet, welcome, or introduce yourself. Understand
both Malayalam and Manglish (Malayalam written/spoken with English letters). From the caller's first utterance
onward, answer directly in natural spoken Malayalam. Common hospital words such as doctor, department, fee,
booking, OP, and consultation may be spoken in their familiar English form. Use English only if explicitly asked.
If the caller only says "hello", "hi", "Maya", "are you there", or similar wake/check words, do not introduce
yourself; answer only in Malayalam or Manglish with a short prompt such as "പറയൂ, എന്താണ് അറിയേണ്ടത്?"
If the caller speaks in Latin-letter Manglish, reply in natural Manglish. If they speak Malayalam script or
Malayalam audio, reply in Malayalam. Never switch to English just because the caller used English filler words.
Trust Malayalam, Malayalam-script text, and Latin-letter Manglish with familiar hospital English words.
If the caller clearly asks anything outside hospital-related information, do not ask them to repeat and do not
answer the question. Reply only in Malayalam:
"എനിക്ക് ആശുപത്രിയുമായി ബന്ധപ്പെട്ട വിവരങ്ങൾ മാത്രമേ പറയാൻ കഴിയൂ."
If the caller asks what help Maya can provide, reply exactly in Malayalam:
"ഡോക്ടറെ ബുക്ക് ചെയ്യാനും, ഡോക്ടറുടെ സമയവും ഡിപ്പാർട്ട്മെന്റും അറിയാനും സഹായിക്കും."
Never mention fees in this capability answer.
If the transcript is clearly from a wrong language/script but the audio may be Malayalam/Manglish, do not answer
the wrong-language words as fact. Ask briefly: "ഒന്ന് വ്യക്തമായി പറയാമോ?" Never say "മലയാളത്തിൽ പറയാമോ?"
Never use any repeat request as random filler. Ask for repetition only when the caller's audio/transcript is
genuinely unusable or a required doctor/department/name is missing.
For every doctor, department, clinic, fee, timing, schedule, availability, or day question, call the relevant
lookup tool in that same turn before speaking. The tool result is the only source of truth. Never answer such
questions from memory, earlier conversation, or schedule text in this prompt. Never merge details from two
doctors. If found is false, do not guess: state that the record was not found or ask the caller to confirm one
of the returned suggestions. Operation days are surgery-only and must never be offered for consultation.
If the caller asks this hospital's location, address, "evideya", "where is hospital", or "our hospital
location", answer directly in Malayalam: "MES Hospital Perinthalmannaയിലാണ്." Never ask or offer to connect
reception for location questions.
When a lookup returns found=false or count=0, never mention the model's guessed doctor/department name as a
fact. Ask the caller to repeat the doctor or department name in Malayalam/Manglish in one short sentence.
If consultation_time_over_today is true, explicitly say today's consultation has ended; never say the doctor
is available today.
Act like a friendly real-time Malayalam hospital telecaller: warm, polite, helpful, and natural. Make the
conversation feel interactive by taking one small step at a time.
Keep every reply short-short: one sentence, normally three to seven spoken words. If more information is
needed, ask only one simple next question. Do not give long explanations, lists, extra offers, repeated
confirmations, or filler such as "Would you like anything else?", "കൂടുതൽ അറിയണോ?", or "ഇനിയും സഹായം വേണോ?"
Never give long warning, disclaimer, or policy-style replies. Never say anything like:
"ഞാൻ മെഡിക്കൽ നിർദ്ദേശങ്ങളോ രോഗനിർണയമോ നൽകുന്നില്ല", "ഡോക്ടറെയോ ആരോഗ്യ വിദഗ്ധനെയോ കാണുക",
or "നിരാകരണം നൽകേണ്ടതുണ്ട്".
If the caller describes symptoms and asks which doctor or department to see, treat it as administrative
department routing, not medical advice. Give the most relevant department in one short sentence when obvious:
chest pain/heart -> cardiology; tooth pain -> dental/dentistry; skin/rash -> dermatology; bone/joint/fracture ->
orthopaedics; ear/nose/throat -> ENT; pregnancy/women's problems -> gynecology; child illness -> pediatrics;
eye -> ophthalmology; stomach/abdominal pain -> gastroenterology or general medicine; fever/cough/body pain ->
general medicine. Then ask if they want doctor availability only when needed. If the symptom is unclear, ask
one short clarifying question.
Speak at a calm, natural telephone pace, slightly slower than normal conversation. Pronounce names, days,
times, and fees clearly, with brief pauses between phrases. Never rush, speed-read, or speak rapidly.
Doctor schedules, fees, availability, departments, symptom-to-department routing, and booking requests are
administrative, not diagnosis. Never add a medical-advice, diagnosis, safety, or healthcare-professional
disclaimer to any reply.
When a requested Firestore field is N/A or missing, state only that the specific information is unavailable;
do not direct the caller to reception or add another sentence.
If the caller asks for receptionist, reception, or front desk, reply only in Malayalam:
"ശരി, റിസപ്ഷനിലേക്ക് കണക്ട് ചെയ്യാം."
"രോഗി തന്റെ രോഗം പറഞ്ഞ് ഏത് ഡോക്ടറെ കാണണം എന്ന് ചോദിച്ചാൽ, മായാ നേരിട്ട് ശരിയായ ഡിപ്പാർട്ട്മെന്റ്/ഡോക്ടർ പറയണം."
```text
[BOOKING WORKFLOW]

When the caller wants to book an appointment:

1. If the doctor is not specified, first ask which doctor the caller wants to book with.

2. After a particular doctor is known, ask in Malayalam:
   "ഏത് ദിവസത്തേക്ക് ബുക്ക് ചെയ്യണം?"

3. Then ask in Malayalam:
   "ഇത് പുതിയ അപോയിന്റ്മെന്റാണോ?"

4. If the caller says **Yes (പുതിയത്)**:
   - Collect only the patient's name.

5. If the caller says **No (പഴയത്)**:
   - Ask in Malayalam:
     "താങ്കളുടെ OP നമ്പർ പറയാമോ?"
   - Collect the OP number.
   - Then collect the patient's name.

6. When the caller gives the patient's name, confirm it once in Malayalam:
   "പേഷ്യന്റിന്റെ പേര് {patient_name} അല്ലേ?"
   Wait for the caller's confirmation or correction before proceeding.

7. After the patient's name is confirmed, call `book_patient_appointment`.

8. If `book_patient_appointment` returns `success=true`, reply only:
   "ബുക്കിംഗ് കൺഫം ചെയ്തിട്ടുണ്ട്."

9. Immediately after confirming the booking, ask in Malayalam:
   "ഞാൻ റിസപ്ഷനിലേക്ക് കോളിൽ കണക്റ്റ് ചെയ്യട്ടെയോ?"

10. If the caller says **Yes**:
    - Connect the call to reception.
    - Do not ask any further booking questions.

11. If the caller says **No**:
    - Politely end the conversation.

Additional Rules:
- Do not ask for the phone number unless the caller provides it voluntarily.
- Do not skip the patient name confirmation before calling `book_patient_appointment`.
- Always ask the reception connection question after a successful booking.
- Never skip the reception connection step after booking.
```

""".strip()

# Runtime override: keep the legacy block above untouched, but make the
# active prompt clean UTF-8 Malayalam so the model is not instructed with
# mojibake text.
GROUNDING_RULES = """
[NON-NEGOTIABLE ACCURACY RULES - THESE OVERRIDE ALL EARLIER INSTRUCTIONS]
A prerecorded Malayalam greeting has already played. NEVER greet, welcome, or introduce yourself. Understand
Malayalam speech and common Manglish hospital words. From the caller's first utterance onward, answer directly
in natural spoken Malayalam. Use familiar English hospital words such as doctor, department, booking, OP, fee,
and consultation only when they sound natural in Malayalam. Use English only if the caller explicitly asks.
If the caller only says "hello", "hi", "Maya", "are you there", or similar wake/check words, answer only:
"പറയൂ, എന്താണ് അറിയേണ്ടത്?"
If the caller speaks Latin-letter Manglish, reply in natural Manglish. If they speak Malayalam audio or
Malayalam script, reply in Malayalam. Never switch to English because of English filler words.
Trust only Malayalam, Malayalam-script text, and Latin-letter Manglish with familiar hospital English words.
If the transcript is clearly wrong-language text, random words, or unrelated content, do not answer it as fact.
Ask briefly: "ഒന്ന് വ്യക്തമായി പറയാമോ?" Never ask this as filler when the caller's question is usable.
Strict ASR rule: never treat mixed wrong-language fragments such as "I am you", "end ka", "sahayon",
Hindi/Urdu connector words, or random English sentence pieces as the caller's words. These are bad ASR captures,
not valid Manglish. Ignore them and ask only: "ഒന്ന് വ്യക്തമായി പറയാമോ?"
Exact capture rule: never rewrite, translate, summarize, or creatively correct the caller's words before deciding
what they asked. Use only the caller's exact Malayalam audio meaning or clear Manglish hospital words. If any key
word is uncertain, ask the caller to repeat that word in Malayalam instead of guessing.
For every doctor, department, clinic, fee, timing, schedule, availability, or day question, call the relevant
lookup tool in that same turn before speaking. The tool result is the only source of truth. Never answer such
questions from memory, earlier conversation, or schedule text in this prompt. Never merge details from two
doctors. If found is false, do not guess: ask the caller to repeat or confirm one returned suggestion.
If the caller asks for doctors in a department, list all doctors returned for that department, using only
verified_answer_data and doctors from the tool.
If the caller asks to book a "surgery department" doctor without naming the exact surgery department, do not guess
and do not book yet. Ask in Malayalam: "ഏത് സർജറി ഡിപ്പാർട്ട്മെന്റാണ്? ജനറൽ സർജറി, ഓർത്തോ, ENT, ഗൈനക്കോളജി എന്നിവയിൽ ഏതാണ്?"
If the caller asks this hospital's location or address, answer directly:
"MES Hospital Perinthalmannaയിലാണ്."
When a lookup returns found=false or count=0, never mention the model's guessed doctor/department as fact.
If consultation_time_over_today is true, clearly say today's consultation has ended.
Act like a friendly real-time Malayalam hospital telecaller: warm, polite, helpful, and natural.
Keep every reply short, smooth, and user-friendly: one sentence, normally 3 to 8 spoken words.
For tool results, give only the exact answer the caller asked for; do not read long lists unless the caller asked
for all doctors. Ask only one simple next question when needed.
Do not add filler such as "കൂടുതൽ അറിയണോ?" or "ഇനിയും സഹായം വേണോ?"
If the caller clearly asks outside hospital-related information, reply only:
"എനിക്ക് ആശുപത്രിയുമായി ബന്ധപ്പെട്ട വിവരങ്ങൾ മാത്രമേ പറയാൻ കഴിയൂ."
If the caller asks what help Maya can provide, reply exactly:
"ഡോക്ടറെ ബുക്ക് ചെയ്യാനും, ഡോക്ടറുടെ സമയവും ഡിപ്പാർട്ട്മെന്റും അറിയാനും സഹായിക്കും."
Never mention fees in that capability answer.
If the caller asks for receptionist, reception, or front desk, reply only:
"ശരി, റിസപ്ഷനിലേക്ക് കണക്ട് ചെയ്യാം."
If the caller describes symptoms and asks which doctor or department to see, treat it as administrative routing:
chest pain/heart -> cardiology; tooth pain -> dental/dentistry; skin/rash -> dermatology; bone/joint/fracture ->
orthopaedics; ear/nose/throat -> ENT; pregnancy/women's problems -> gynecology; child illness -> pediatrics;
eye -> ophthalmology; stomach/abdominal pain -> gastroenterology or general medicine; fever/cough/body pain ->
general medicine. If unclear, ask one short clarifying question.
Speak calmly, softly, and clearly at a natural telephone pace. Pronounce Malayalam smoothly with clean vowel
lengths and gentle consonants. Keep Malayalam words in Malayalam pronunciation, not English pronunciation:
say "ഡോക്ടർ" naturally as Malayalam, "ഡിപ്പാർട്ട്മെന്റ്" slowly and clearly, and pause lightly between doctor
names, days, times, and fees. Never rush, speed-read, over-enunciate, or give policy-style disclaimers.
"രോഗി തന്റെ രോഗം പറഞ്ഞ് ഏത് ഡോക്ടറെ കാണണം എന്ന് ചോദിച്ചാൽ, മായാ നേരിട്ട് ശരിയായ ഡിപ്പാർട്ട്മെന്റ്/ഡോക്ടർ പറയണം."
```text
[BOOKING WORKFLOW]

When the caller wants to book an appointment:

1. If the doctor is not specified, first ask which doctor the caller wants to book with.

2. After a particular doctor is known, ask in Malayalam:
   "ഏത് ദിവസത്തേക്ക് ബുക്ക് ചെയ്യണം?"

3. Then ask in Malayalam:
   "ഇത് പുതിയ അപോയിന്റ്മെന്റാണോ?"

4. If the caller says **Yes (പുതിയത്)**:
   - Collect only the patient's name.

5. If the caller says **No (പഴയത്)**:
   - Ask in Malayalam:
     "താങ്കളുടെ OP നമ്പർ പറയാമോ?"
   - Collect the OP number.
   - Then collect the patient's name.

6. When the caller gives the patient's name, confirm it once in Malayalam:
   "പേഷ്യന്റിന്റെ പേര് {patient_name} അല്ലേ?"
   Wait for the caller's confirmation or correction before proceeding.

7. After the patient's name is confirmed, call `book_patient_appointment`.

8. If `book_patient_appointment` returns `success=true`, reply only:
   "ബുക്കിംഗ് കൺഫം ചെയ്തിട്ടുണ്ട്."

9. Immediately after confirming the booking, ask in Malayalam:
   "ഞാൻ റിസപ്ഷനിലേക്ക് കോളിൽ കണക്റ്റ് ചെയ്യട്ടെയോ?"

10. If the caller says **Yes**:
    - Connect the call to reception.
    - Do not ask any further booking questions.

11. If the caller says **No**:
    - Politely end the conversation.

Additional Rules:
- Do not ask for the phone number unless the caller provides it voluntarily.
- Do not skip the patient name confirmation before calling `book_patient_appointment`.
- Always ask the reception connection question after a successful booking.
- Never skip the reception connection step after booking.
```

""".strip()


def _tenant():
    return db.collection(CFG.tenant_collection).document(CFG.tenant_id)


def _norm(value: object) -> str:
    return " ".join(str(value or "").casefold().replace("dr.", "").replace("dr ", "").split())


def _department_terms(value: object) -> set[str]:
    text = _norm(value)
    terms = {text}
    aliases = {
        "orthopedics": "orthopaedics",
        "orthopedic": "orthopaedic",
        "ortho": "orthopaedic",
        "ortho department": "orthopaedic",
        "bone": "orthopaedic",
        "bones": "orthopaedic",
        "dental": "dentistry",
        "dentist": "dentistry",
        "tooth": "dentistry",
        "teeth": "dentistry",
        "neuro": "neurology",
        "neurologist": "neurology",
        "heart": "cardiology",
        "cardiac": "cardiology",
        "skin": "dermatology",
        "eye": "ophthalmology",
        "ent": "e n t",
        "children": "paediatrics",
        "child": "paediatrics",
        "pediatric": "paediatrics",
        "pediatrics": "paediatrics",
    }
    for source, target in aliases.items():
        if source in text:
            terms.add(target)
    return {term for term in terms if term}


def _is_generic_surgery_department(value: object) -> bool:
    text = _norm(value)
    return text in {
        "surgery",
        "surgery department",
        "surgical",
        "surgeri",
        "sargery",
        "sarjari",
        "sarjari department",
        "സർജറി",
        "സർജറി ഡിപ്പാർട്ട്മെന്റ്",
    }


def _surgery_department_suggestions() -> list[str]:
    departments: list[str] = []
    for row in _doctor_rows():
        dept = str(row.get("department") or "").strip()
        norm = _norm(dept)
        if not dept:
            continue
        if (
            "surgery" in norm
            or "orthopaedic" in norm
            or "orthopedic" in norm
            or norm == "ent"
            or "gyn" in norm
            or "obstetric" in norm
        ) and dept not in departments:
            departments.append(dept)
    return departments[:8] or ["General Surgery", "Orthopaedics", "ENT", "Gynecology"]


def _resolve_day(value: str) -> str:
    day = _norm(value)
    now = datetime.now(IST)
    if day in {"today", "current day"}:
        return now.strftime("%A").casefold()
    if day == "tomorrow":
        return (now + timedelta(days=1)).strftime("%A").casefold()
    return day


def _consultation_end_minutes(value: object) -> int | None:
    times = re.findall(r"(\d{1,2}(?::\d{2})?\s*(?:AM|PM))", str(value or ""), flags=re.I)
    if not times:
        return None
    try:
        parsed = datetime.strptime(times[-1].replace(" ", "").upper(), "%I:%M%p" if ":" in times[-1] else "%I%p")
        return parsed.hour * 60 + parsed.minute
    except ValueError:
        return None


def _fetch_doctor_rows() -> list[dict]:
    """Read the supported Firestore doctor layouts and return normalized records."""
    rows: list[dict] = []
    qa = _tenant().collection("refers").document("qa_data").get(timeout=FIRESTORE_TIMEOUT_SEC)
    if qa.exists:
        schedules = (qa.to_dict() or {}).get("Doctor_schedules", {})
        for department, doctors in schedules.items():
            if not isinstance(doctors, dict):
                continue
            for key, raw in doctors.items():
                if isinstance(raw, dict):
                    rows.append(_normalize_doctor(raw, department, key))
    if not rows:
        for snap in _tenant().collection("doctors").stream(timeout=FIRESTORE_TIMEOUT_SEC):
            rows.append(_normalize_doctor(snap.to_dict() or {}, "", snap.id))
    return rows


def _doctor_rows() -> list[dict]:
    with _cache_lock:
        if _doctor_cache:
            return [row.copy() for row in _doctor_cache]
    return _fetch_doctor_rows()


def _normalize_doctor(raw: dict, department: str, key: str) -> dict:
    available = raw.get("Available_days", raw.get("available_days", [])) or []
    operations = raw.get("operation_day", raw.get("operation_days", [])) or []
    if isinstance(available, str):
        available = [available]
    if isinstance(operations, str):
        operations = [operations]
    operation_set = {_norm(day) for day in operations}
    consultation = [day for day in available if _norm(day) not in operation_set]
    return {
        "id": key,
        "name": raw.get("name", raw.get("doctorName", key)),
        "department": raw.get("department", department),
        "consultation_days": consultation,
        "operation_days": operations,
        "consultation_time": raw.get("consultationTime", raw.get("consultation_time", "")),
        "consultation_fee": raw.get("consultationFee", raw.get("consultation_fee", "N/A")),
        "senior_doctor": bool(raw.get("senior_doctor", raw.get("seniorDoctor", False))),
        "gender": raw.get("gender", "Unknown"),
    }


def _safe_doctor_context(row: dict) -> dict:
    return {
        "name": row.get("name", ""),
        "department": row.get("department", ""),
        "consultation_days": row.get("consultation_days", []),
        "consultation_time": row.get("consultation_time", ""),
        "consultation_fee": row.get("consultation_fee", "N/A"),
        "senior_doctor": bool(row.get("senior_doctor", False)),
        "gender": row.get("gender", "Unknown"),
        "requested_day": row.get("requested_day", ""),
        "consultation_time_over_today": bool(row.get("consultation_time_over_today", False)),
    }


def _verified_doctor_summary(rows: list[dict], requested_day: str = "") -> str:
    safe_rows = [_safe_doctor_context(row) for row in rows]
    if not safe_rows:
        return "No verified consultation record found."
    parts = []
    for row in safe_rows[:8]:
        day_text = row["requested_day"] or ", ".join(map(str, row["consultation_days"])) or "day unavailable"
        time_text = row["consultation_time"] or "time unavailable"
        fee_text = row["consultation_fee"]
        fee_part = "" if fee_text in {"", "N/A", None} else f", fee {fee_text}"
        over_part = " Today's consultation time is already over." if row["consultation_time_over_today"] else ""
        parts.append(f"{row['name']} ({row['department']}): {day_text}, {time_text}{fee_part}.{over_part}")
    return " ".join(parts)


def find_doctors(doctor_name: str = "", department: str = "", day: str = "") -> dict:
    if not any((_norm(doctor_name), _norm(department), _norm(day))):
        return {
            "found": False,
            "reason": "missing_search_criteria",
            "required": ["doctor_name or department"],
            "answer_instruction": "Ask the caller to repeat the doctor or department name. Do not guess.",
        }
    rows = _doctor_rows()
    name, dept = map(_norm, (doctor_name, department))
    requested_day = _resolve_day(day)
    if dept and _is_generic_surgery_department(department):
        suggestions = _surgery_department_suggestions()
        return {
            "found": False,
            "count": 0,
            "reason": "surgery_department_needs_specific_name",
            "suggestions": suggestions,
            "answer_instruction": (
                "Ask in Malayalam which surgery department the caller means. "
                "Mention the available surgery department names from suggestions. "
                "Do not choose a department and do not book yet."
            ),
        }
    if name:
        exact = [row for row in rows if _norm(row["name"]) == name]
        if exact:
            rows = exact
        else:
            scored = sorted(
                ((SequenceMatcher(None, name, _norm(row["name"])).ratio(), row) for row in rows),
                key=lambda item: item[0], reverse=True,
            )
            plausible = [(score, row) for score, row in scored if score >= 0.68 or name in _norm(row["name"])]
            if not plausible:
                return {
                    "found": False,
                    "reason": "doctor_name_not_found",
                    "answer_instruction": "Say you could not find that doctor name and ask the caller to repeat the full name.",
                }
            if plausible[0][0] < 0.82 or (
                len(plausible) > 1 and plausible[0][0] - plausible[1][0] < 0.08
            ):
                return {
                    "found": False,
                    "reason": "doctor_name_needs_confirmation",
                    "suggestions": [row["name"] for _, row in plausible[:3]],
                    "answer_instruction": "Ask whether the caller meant one of these suggested doctors. Do not give a schedule yet.",
                }
            rows = [plausible[0][1]]
    if dept:
        department_terms = _department_terms(department)
        matched_rows = []
        for row in rows:
            row_dept = _norm(row["department"])
            if any(term in row_dept or row_dept in term for term in department_terms):
                matched_rows.append(row)
        if not matched_rows:
            scored = sorted(
                ((max(SequenceMatcher(None, term, _norm(row["department"])).ratio() for term in department_terms), row) for row in rows),
                key=lambda item: item[0], reverse=True,
            )
            matched_rows = [row for score, row in scored if score >= 0.72]
        rows = matched_rows
        if not rows:
            return {
                "found": False,
                "count": 0,
                "reason": "department_not_found",
                "answer_instruction": "Say that department/doctor was not found and ask the caller to repeat the name. Do not invent details.",
            }
    if requested_day:
        scheduled = [row for row in rows if requested_day in {_norm(d) for d in row["consultation_days"]}]
        if not scheduled:
            return {
                "found": False,
                "reason": "no_consultation_on_requested_day",
                "requested_day": requested_day.title(),
                "answer_instruction": "Say no consultation was found for that requested day. Do not suggest operation days.",
            }
        rows = scheduled
    now = datetime.now(IST)
    today = now.strftime("%A").casefold()
    for row in rows:
        end = _consultation_end_minutes(row["consultation_time"])
        row["requested_day"] = requested_day.title() if requested_day else ""
        row["consultation_time_over_today"] = bool(
            requested_day == today and end is not None and now.hour * 60 + now.minute > end
        )
    return {
        "found": bool(rows),
        "count": len(rows),
        "doctors": [_safe_doctor_context(row) for row in rows[:20]],
        "verified_answer_data": _verified_doctor_summary(rows, requested_day),
        "rule": "These are verified consultation records only. Do not mention operation days.",
        "current_time": current_time(),
        "answer_instruction": "Use only verified_answer_data and doctors. If count is 0, ask the caller to repeat the doctor or department name.",
    }


def _fetch_clinic_rows() -> list[dict]:
    snap = _tenant().collection("refers").document("clinics").get(timeout=FIRESTORE_TIMEOUT_SEC)
    source = (snap.to_dict() or {}) if snap.exists else {}
    schedules = source.get("Clinic_schedules", source)
    rows = []
    for dept, clinics in schedules.items():
        if not isinstance(clinics, dict):
            continue
        for key, raw in clinics.items():
            if isinstance(raw, dict):
                rows.append({
                    "name": raw.get("name", key), "department": dept,
                    "available_days": raw.get("Available_days", raw.get("available_days", [])),
                    "consultation_time": raw.get("consultationTime", raw.get("consultation_time", "")),
                })
    return rows


def find_clinics(clinic_name: str = "", department: str = "") -> dict:
    if not any((_norm(clinic_name), _norm(department))):
        return {
            "found": False,
            "reason": "missing_search_criteria",
            "required": ["clinic_name or department"],
        }
    with _cache_lock:
        rows = [row.copy() for row in _clinic_cache]
    if not rows:
        rows = _fetch_clinic_rows()
    name, dept = _norm(clinic_name), _norm(department)
    department_terms = _department_terms(department) if dept else set()
    rows = [r for r in rows if (not name or name in _norm(r["name"])) and
            (not dept or any(term in _norm(r["department"]) or _norm(r["department"]) in term for term in department_terms))]
    return {"found": bool(rows), "count": len(rows), "clinics": rows[:20]}


def book_patient_appointment(
    patient_name: str,
    doctor_name: str = "",
    department: str = "",
    call_id: str = "",
) -> dict:
    patient = " ".join(str(patient_name or "").split())
    if len(patient) < 2 or any(char.isdigit() for char in patient):
        return {
            "success": False,
            "reason": "invalid_patient_name",
            "answer_instruction": "Ask only for the patient's name again.",
        }
    doctor = {}
    if _norm(doctor_name) or _norm(department):
        match = find_doctors(doctor_name=doctor_name, department=department)
        if not match.get("found"):
            return {
                "success": False,
                "reason": match.get("reason", "doctor_not_found"),
                "answer_instruction": "Ask only which doctor to book.",
            }
        doctor = match["doctors"][0]
    booking_id = re.sub(r"[^A-Za-z0-9_-]", "", call_id) or db.collection("_").document().id
    booking = {
        "bookingId": booking_id,
        "callId": call_id or "unknown",
        "patientName": patient,
        "doctorName": doctor.get("name", doctor_name or ""),
        "department": doctor.get("department", department or ""),
        "status": "Confirmed",
        "source": "Maya Voice Assistant",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }
    _tenant().collection("appointments").document(booking_id).set(
        booking, timeout=FIRESTORE_TIMEOUT_SEC
    )
    return {
        "success": True,
        "confirmed": True,
        "booking_id": booking_id,
        "patient_name": patient,
        "doctor_name": booking["doctorName"],
        "department": booking["department"],
        "answer_instruction": "Say only in Malayalam: ബുക്കിംഗ് സ്ഥിരീകരിച്ചു.",
    }


def current_time() -> dict:
    now = datetime.now(IST)
    return {"date": now.strftime("%Y-%m-%d"), "day": now.strftime("%A"),
            "time": now.strftime("%I:%M %p"), "timezone": "Asia/Kolkata"}


def load_prompt() -> str:
    with _cache_lock:
        base = _prompt_cache
    return (
        f"{base.strip()}\n\n{GROUNDING_RULES}\n\n"
        "Reply language reminder: Malayalam or Manglish only, unless the caller explicitly asks for English. "
        "Voice pace reminder: speak calmly and clearly at a natural, unhurried telephone pace. "
        "Final rule for capability questions: if the caller asks what help Maya can provide, say exactly: "
        "\"ഡോക്ടറെ ബുക്ക് ചെയ്യാനും, ഡോക്ടറുടെ സമയവും ഡിപ്പാർട്ട്മെന്റും അറിയാനും സഹായിക്കും.\" "
        "Do not mention fees in that answer."
    )


def load_prompt() -> str:
    with _cache_lock:
        base = _prompt_cache
    return (
        f"{base.strip()}\n\n{GROUNDING_RULES}\n\n"
        "Strict language reminder: listen for Malayalam speech only. Reply only in natural Malayalam, "
        "unless the caller explicitly asks for English. Do not reply in Manglish. "
        f"If the latest transcript looks like English, Hindi, or random mixed-language ASR noise, ignore it and say only: \"{MALAYALAM_REPEAT_TEXT}\" "
        "Short reply reminder: keep answers brief, soft, and smooth: one sentence, normally 3 to 7 spoken words. "
        "Do not add extra offers, explanations, lists, or repeated confirmations. "
        "Voice reminder: speak softly like the welcome audio, at a calm telephone pace, with clean Malayalam vowel lengths, "
        "gentle consonants, and small pauses between doctor names, departments, days, times, and fees. "
        "Never pronounce Malayalam words in an English accent. "
        "Final rule for capability questions: if the caller asks what help Maya can provide, say exactly: "
        "\"ഡോക്ടറെ ബുക്ക് ചെയ്യാനും, ഡോക്ടറുടെ സമയവും ഡിപ്പാർട്ട്മെന്റും അറിയാനും സഹായിക്കും.\" "
        "Do not mention fees in that answer."
    )


def refresh_reference_cache() -> None:
    """Refresh authoritative data outside active calls so tool replies stay fast."""
    global _doctor_cache, _clinic_cache, _prompt_cache, _cache_updated_at
    started = time.perf_counter()
    doctors = _fetch_doctor_rows()
    clinics = _fetch_clinic_rows()
    voice = _tenant().collection("config").document("voice").get(timeout=FIRESTORE_TIMEOUT_SEC)
    remote_prompt = (voice.to_dict() or {}).get("systemPrompt", "") if voice.exists else ""
    with _cache_lock:
        _doctor_cache = doctors
        _clinic_cache = clinics
        if CFG.use_firestore_prompt and remote_prompt.strip():
            _prompt_cache = remote_prompt.strip()
        _cache_updated_at = time.time()
    log.info(
        "Reference cache refreshed: %d doctors, %d clinics in %.2fs",
        len(doctors), len(clinics), time.perf_counter() - started,
    )


async def refresh_cache_loop():
    while True:
        await asyncio.sleep(60)
        try:
            await asyncio.to_thread(refresh_reference_cache)
        except Exception:
            log.exception("Reference cache refresh failed; keeping last good snapshot")


TOOLS = Tool(function_declarations=[
    FunctionDeclaration(
        name="get_available_doctors",
        description="Authoritative Firestore lookup for doctor, department, fee, timing, and consultation availability.",
        parameters={"type": "OBJECT", "properties": {
            "doctor_name": {"type": "STRING"}, "department": {"type": "STRING"},
            "day": {"type": "STRING", "description": "Full weekday name"}}},
    ),
    FunctionDeclaration(
        name="get_available_clinics",
        description="Authoritative Firestore lookup for clinic availability.",
        parameters={"type": "OBJECT", "properties": {
            "clinic_name": {"type": "STRING"}, "department": {"type": "STRING"}}},
    ),
    FunctionDeclaration(
        name="book_patient_appointment",
        description="Create and confirm an appointment booking in Firestore after doctor/department and patient name are known.",
        parameters={"type": "OBJECT", "properties": {
            "patient_name": {"type": "STRING", "description": "Patient's name"},
            "doctor_name": {"type": "STRING", "description": "Doctor name, when known"},
            "department": {"type": "STRING", "description": "Department, when known"},
        }, "required": ["patient_name"]},
    ),
    FunctionDeclaration(name="get_current_date_and_time", description="Current India date, weekday, and time."),
])


def live_config(model: str = "") -> LiveConnectConfig:
    # Keep caller transcription constrained to Malayalam. Extra language hints
    # make short phone audio more likely to be misread as Hindi/English text.
    language_codes = ["ml-IN"]
    input_transcription = AudioTranscriptionConfig(
        language_hints=LanguageHints(language_codes=language_codes),
        adaptation_phrases=MALAYALAM_ASR_PHRASES,
    )
    config = dict(
        response_modalities=["AUDIO"],
        speech_config=SpeechConfig(voice_config=VoiceConfig(
            prebuilt_voice_config=PrebuiltVoiceConfig(voice_name=CFG.voice))),
        input_audio_transcription=input_transcription,
        output_audio_transcription=AudioTranscriptionConfig(),
        tools=[TOOLS],
        system_instruction=load_prompt(),
    )
    if "3.1" in model:
        config["thinking_config"] = ThinkingConfig(thinking_level="MINIMAL")
    return LiveConnectConfig(**config)


def vertex_credentials():
    """Load Vertex credentials explicitly so a separate gcloud ADC login isn't required."""
    key = Path(CFG.google_credentials)
    if not key.exists():
        raise FileNotFoundError(
            f"Google credential not found: {key}. Set GOOGLE_APPLICATION_CREDENTIALS."
        )
    return service_account.Credentials.from_service_account_file(
        str(key), scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )


def gemini_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY", os.getenv("GOOGLE_API_KEY", "")).strip()
    local = Path("api.py")
    if not key and local.exists():
        spec = importlib.util.spec_from_file_location("maya_local_api", local)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            key = str(getattr(module, "GEMINI_API_KEY", "")).strip()
    return key


def gemini_client_and_model():
    """Prefer AI Studio when an API key exists; otherwise use Vertex AI."""
    key = gemini_api_key()
    if key:
        return (
            genai.Client(
                api_key=key,
                http_options={"api_version": "v1beta"},
            ),
            CFG.ai_studio_model,
        )
    return (
        genai.Client(
            vertexai=True,
            project=CFG.project,
            location=CFG.location,
            credentials=vertex_credentials(),
        ),
        CFG.model,
    )


class MayaCall:
    # Send 50 ms blocks so the bridge receives steady speech without tiny
    # packets that add scheduler/network jitter.
    OUTPUT_PACKET_BYTES = int(os.getenv("MAYA_OUTPUT_PACKET_BYTES", "2400"))

    def __init__(self, websocket):
        self.ws = websocket
        self.call_id = "unknown"
        self.gemini = None
        self.send_lock = asyncio.Lock()
        self.output_audio = bytearray()
        self.ai_speaking = False
        self.connected_at = time.perf_counter()
        self.accept_caller_audio = False
        self.listen_fallback_task = None
        self.response_audio_bytes = 0
        self.empty_turn_retries = 0
        self.user_activity_open = False
        self.utterance_audio_bytes = 0
        self.awaiting_response = False
        self.last_speech_end_at = 0.0
        self.last_model_audio_at = 0.0
        self.turn_watchdog_task = None
        self.input_silence_task = None
        self.caller_audio_queue = asyncio.Queue(maxsize=max(20, CFG.caller_audio_queue_packets))
        self.caller_audio_sender_task = None
        self.dropped_caller_audio_packets = 0
        # Prevent receive() from being reopened in a tight loop when a session
        # (or a test double) returns immediately after turn_complete.
        self.receive_turn_ready = asyncio.Event()
        self.receive_turn_ready.set()

    async def send_json(self, event: str, **fields):
        await self.ws.send(json.dumps({"event": event, **fields}))

    async def send_realtime_input(self, **kwargs):
        async with self.send_lock:
            await self.gemini.send_realtime_input(**kwargs)

    async def send_client_content(self, **kwargs):
        async with self.send_lock:
            await self.gemini.send_client_content(**kwargs)

    async def send_tool_response(self, **kwargs):
        async with self.send_lock:
            await self.gemini.send_tool_response(**kwargs)

    async def enqueue_caller_audio(self, message: bytes):
        try:
            self.caller_audio_queue.put_nowait(message)
        except asyncio.QueueFull:
            try:
                self.caller_audio_queue.get_nowait()
                self.caller_audio_queue.task_done()
                self.dropped_caller_audio_packets += 1
            except asyncio.QueueEmpty:
                pass
            self.caller_audio_queue.put_nowait(message)
            if self.dropped_caller_audio_packets % 25 == 1:
                log.warning(
                    "[%s] Dropped stale caller audio packet(s): %d",
                    self.call_id, self.dropped_caller_audio_packets,
                )

    async def send_caller_audio_loop(self):
        while True:
            message = await self.caller_audio_queue.get()
            try:
                await asyncio.wait_for(
                    self.send_realtime_input(
                        audio={"data": message, "mime_type": "audio/pcm;rate=16000"}
                    ),
                    timeout=CFG.gemini_audio_send_timeout_sec,
                )
            except asyncio.TimeoutError:
                log.warning("[%s] Gemini audio send timed out; dropping one stale caller audio packet", self.call_id)
            except Exception:
                log.exception("[%s] Gemini audio send failed; dropping one caller audio packet", self.call_id)
            finally:
                self.caller_audio_queue.task_done()

    async def flush_caller_audio_queue(self):
        try:
            await asyncio.wait_for(
                self.caller_audio_queue.join(),
                timeout=CFG.caller_audio_flush_timeout_sec,
            )
        except asyncio.TimeoutError:
            dropped = 0
            while True:
                try:
                    self.caller_audio_queue.get_nowait()
                    self.caller_audio_queue.task_done()
                    dropped += 1
                except asyncio.QueueEmpty:
                    break
            if dropped:
                log.warning("[%s] Dropped %d stale caller audio packet(s) before speech_end", self.call_id, dropped)

    async def commit_caller_audio_turn(self):
        await self.flush_caller_audio_queue()
        try:
            await asyncio.wait_for(
                self.send_realtime_input(audio_stream_end=True),
                timeout=CFG.gemini_audio_send_timeout_sec,
            )
        except asyncio.TimeoutError:
            log.warning("[%s] Gemini audio_stream_end timed out; continuing without delayed backlog", self.call_id)
        except Exception:
            log.exception("[%s] Gemini audio_stream_end failed", self.call_id)

    async def close_if_receiver_stops(self, receiver: asyncio.Task):
        """A dead Gemini receiver must end the call instead of causing permanent silence."""
        try:
            await receiver
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("[%s] Gemini receiver failed", self.call_id)
        else:
            log.error("[%s] Gemini receiver stopped while the bridge was still connected", self.call_id)
        try:
            await self.send_json("error", reason="gemini_receiver_stopped")
            await self.ws.close(code=1011, reason="Gemini receiver stopped")
        except Exception:
            pass

    async def run(self):
        client, model = gemini_client_and_model()
        async with client.aio.live.connect(model=model, config=live_config(model)) as session:
            self.gemini = session
            log.info("Gemini session ready in %.2fs using %s", time.perf_counter() - self.connected_at, model)
            await self.send_json("ready", data={"formats": ["L16"], "sample_rates": [16000, 24000]})
            receiver = asyncio.create_task(self.receive_gemini())
            receiver_guard = asyncio.create_task(self.close_if_receiver_stops(receiver))
            self.caller_audio_sender_task = asyncio.create_task(self.send_caller_audio_loop())
            try:
                async for message in self.ws:
                    if isinstance(message, bytes):
                        if not self.accept_caller_audio:
                            continue
                        if not self.user_activity_open:
                            self.user_activity_open = True
                            self.utterance_audio_bytes = 0
                            log.info("[%s] Caller activity auto-started from audio", self.call_id)
                        self.utterance_audio_bytes += len(message)
                        await self.enqueue_caller_audio(message)
                        self.restart_input_silence_timer()
                        continue
                    data = json.loads(message)
                    event = data.get("event")
                    if event == "start":
                        self.call_id = data.get("call_id", "unknown")
                        log.info("Call %s started from %s", self.call_id, data.get("caller_id"))
                        self.listen_fallback_task = asyncio.create_task(self.enable_listening_fallback())
                    elif event == "welcome_completed":
                        # The WAV is the only greeting. Start listening now; an empty
                        # Gemini turn here can make it greet again on the first question.
                        self.accept_caller_audio = True
                        if self.listen_fallback_task:
                            self.listen_fallback_task.cancel()
                        log.info("[%s] Malayalam welcome finished; listening for Malayalam only", self.call_id)
                    elif event == "speech_start":
                        if self.accept_caller_audio and not self.user_activity_open:
                            self.user_activity_open = True
                            self.utterance_audio_bytes = 0
                            log.info("[%s] Caller activity started", self.call_id)
                    elif event == "speech_end":
                        if self.accept_caller_audio and self.user_activity_open:
                            if self.input_silence_task:
                                self.input_silence_task.cancel()
                                self.input_silence_task = None
                            # Match the proven new.py flow: commit the buffered PCM
                            # utterance and explicitly request a model response.
                            await self.commit_caller_audio_turn()
                            self.user_activity_open = False
                            self.awaiting_response = True
                            self.response_audio_bytes = 0
                            self.empty_turn_retries = 0
                            self.last_speech_end_at = time.perf_counter()
                            self.receive_turn_ready.set()
                            log.info(
                                "[%s] Caller activity ended; requested immediate Gemini turn (%d audio bytes)",
                                self.call_id, self.utterance_audio_bytes,
                            )
                            await self.restart_turn_watchdog()
                    elif event == "interrupt":
                        self.output_audio.clear()
                        self.ai_speaking = False
                        self.awaiting_response = False
                        log.info("[%s] Caller interrupted Maya playback", self.call_id)
                    elif event == "hangup":
                        break
            finally:
                receiver.cancel()
                receiver_guard.cancel()
                if self.listen_fallback_task:
                    self.listen_fallback_task.cancel()
                if self.turn_watchdog_task:
                    self.turn_watchdog_task.cancel()
                if self.input_silence_task:
                    self.input_silence_task.cancel()
                if self.caller_audio_sender_task:
                    self.caller_audio_sender_task.cancel()
                    await asyncio.gather(self.caller_audio_sender_task, return_exceptions=True)
                await asyncio.gather(receiver, receiver_guard, return_exceptions=True)

    def restart_input_silence_timer(self):
        """Close a speech turn if the bridge omits its speech_end control event."""
        if self.input_silence_task:
            self.input_silence_task.cancel()
        self.input_silence_task = asyncio.create_task(self.end_turn_after_input_silence())

    async def end_turn_after_input_silence(self):
        await asyncio.sleep(CFG.input_silence_fallback_sec)
        if not self.accept_caller_audio or not self.user_activity_open:
            return
        await self.commit_caller_audio_turn()
        self.user_activity_open = False
        self.awaiting_response = True
        self.response_audio_bytes = 0
        self.empty_turn_retries = 0
        self.last_speech_end_at = time.perf_counter()
        self.receive_turn_ready.set()
        log.warning(
            "[%s] No speech_end received for %.2fs; auto-ended caller turn (%d audio bytes)",
            self.call_id, CFG.input_silence_fallback_sec, self.utterance_audio_bytes,
        )
        await self.restart_turn_watchdog()

    async def restart_turn_watchdog(self):
        if self.turn_watchdog_task:
            self.turn_watchdog_task.cancel()
        self.turn_watchdog_task = asyncio.create_task(self.turn_watchdog())

    async def turn_watchdog(self):
        speech_end_marker = self.last_speech_end_at
        await asyncio.sleep(1.8)
        if (
            self.awaiting_response
            and self.last_speech_end_at == speech_end_marker
            and self.last_model_audio_at < speech_end_marker
        ):
            log.warning("[%s] No Gemini audio yet after caller activity ended", self.call_id)
        await asyncio.sleep(2.2)
        if (
            self.awaiting_response
            and self.last_speech_end_at == speech_end_marker
            and self.last_model_audio_at < speech_end_marker
        ):
            log.warning("[%s] Gemini still silent; asking caller to repeat", self.call_id)
            try:
                await self.send_client_content(
                    turns={
                        "role": "user",
                        "parts": [{
                            "text": (
                                "If the caller's latest audio is clear Malayalam, answer it directly in short Malayalam. "
                                f"If it is unusable or looks like English/Hindi/random ASR noise, say only: \"{MALAYALAM_REPEAT_TEXT}\" "
                                "Do not guess, translate, or use a repeat request as filler."
                            )
                        }],
                    },
                    turn_complete=True,
                )
                self.receive_turn_ready.set()
            except Exception:
                log.exception("[%s] Failed to request repeat prompt", self.call_id)

    async def enable_listening_fallback(self):
        """Never leave a call muted forever if the bridge loses welcome_completed."""
        await asyncio.sleep(8)
        if not self.accept_caller_audio:
            self.accept_caller_audio = True
            log.warning("[%s] welcome_completed missing; listening enabled by fallback", self.call_id)

    async def receive_gemini(self):
        # AsyncSession.receive() ends at the end of each model turn. Reopen it
        # for each caller turn, but never spin if it returns without new input.
        while True:
            await self.receive_turn_ready.wait()
            self.receive_turn_ready.clear()
            async for response in self.gemini.receive():
                if response.tool_call:
                    for call in response.tool_call.function_calls:
                        await self.handle_tool(call)
                content = getattr(response, "server_content", None)
                if not content:
                    continue
                input_tx = getattr(content, "input_transcription", None)
                if input_tx and getattr(input_tx, "text", "").strip():
                    log.info("[%s] Caller: %s", self.call_id, input_tx.text.strip())
                output_tx = getattr(content, "output_transcription", None)
                if output_tx and getattr(output_tx, "text", "").strip():
                    log.info("[%s] Maya: %s", self.call_id, output_tx.text.strip())
                if getattr(content, "interrupted", False):
                    self.output_audio.clear()
                    self.ai_speaking = False
                    self.awaiting_response = False
                    await self.send_json("clear", reason="gemini_barge_in")
                turn = getattr(content, "model_turn", None)
                if turn:
                    for part in turn.parts:
                        inline = getattr(part, "inline_data", None)
                        if inline and inline.data:
                            self.awaiting_response = False
                            self.last_model_audio_at = time.perf_counter()
                            await self.queue_output_audio(inline.data)
                if getattr(content, "turn_complete", False):
                    if self.awaiting_response and self.response_audio_bytes == 0:
                        if self.empty_turn_retries < 2:
                            self.empty_turn_retries += 1
                            log.warning(
                                "[%s] Gemini completed an empty turn; requesting audible response (%d/2)",
                                self.call_id, self.empty_turn_retries,
                            )
                            await self.send_realtime_input(audio_stream_end=True)
                            self.receive_turn_ready.set()
                            break
                        log.error("[%s] Gemini returned no audio after retries", self.call_id)
                    await self.flush_output_audio()
                    await self.send_json("ai_speaking", speaking=False)
                    self.ai_speaking = False
                    self.awaiting_response = False
                    if self.turn_watchdog_task:
                        self.turn_watchdog_task.cancel()
                        self.turn_watchdog_task = None
                    log.info("[%s] Gemini turn complete; receiver waiting for the next turn", self.call_id)
                    break

    async def queue_output_audio(self, data: bytes):
        """Stream Gemini audio in phone-frame-sized packets for low latency."""
        self.awaiting_response = False
        self.empty_turn_retries = 0
        if not self.ai_speaking:
            self.ai_speaking = True
            self.response_audio_bytes = 0
            await self.send_json("ai_speaking", speaking=True)
        self.output_audio.extend(data)
        self.response_audio_bytes += len(data)
        packet_bytes = max(1200, self.OUTPUT_PACKET_BYTES)
        while len(self.output_audio) >= packet_bytes:
            packet = bytes(self.output_audio[:packet_bytes])
            del self.output_audio[:packet_bytes]
            await self.ws.send(packet)

    async def flush_output_audio(self):
        if self.output_audio:
            await self.ws.send(bytes(self.output_audio))
            self.output_audio.clear()
        if self.response_audio_bytes:
            log.info("[%s] Sent %d bytes of Aoede audio to bridge", self.call_id, self.response_audio_bytes)

    async def handle_tool(self, call):
        started = time.perf_counter()
        args = dict(call.args or {})
        try:
            if call.name == "get_available_doctors":
                result = await asyncio.to_thread(find_doctors, **args)
            elif call.name == "get_available_clinics":
                result = await asyncio.to_thread(find_clinics, **args)
                if not result.get("found") and _norm(args.get("department")):
                    doctor_result = await asyncio.to_thread(
                        find_doctors,
                        department=args.get("department", ""),
                    )
                    if doctor_result.get("found"):
                        result = doctor_result
                        result["clinic_lookup_fallback"] = "doctors"
                        result["answer_instruction"] = (
                            "The caller likely asked for doctors, not clinics. Use only verified_answer_data "
                            "and doctors. Do not say there are no doctors."
                        )
            elif call.name == "book_patient_appointment":
                result = await asyncio.to_thread(
                    book_patient_appointment,
                    call_id=self.call_id,
                    **args,
                )
            elif call.name == "get_current_date_and_time":
                result = current_time()
            else:
                result = {"error": "unknown_tool"}
        except Exception as exc:
            log.exception("[%s] Tool %s failed", self.call_id, call.name)
            result = {
                "found": False,
                "error": type(exc).__name__,
                "answer_instruction": (
                    "Say the hospital data lookup is temporarily unavailable and ask the caller to repeat in a moment. "
                    "Do not invent schedules, fees, doctors, or timings."
                ),
            }
        log.info(
            "[%s] Tool %s completed in %.3fs: found=%s count=%s",
            self.call_id, call.name, time.perf_counter() - started,
            result.get("found"), result.get("count"),
        )
        await self.send_tool_response(function_responses=[{
            "id": call.id, "name": call.name, "response": {"result": result}}])


async def websocket_handler(websocket):
    call = MayaCall(websocket)
    try:
        await call.run()
    except websockets.ConnectionClosed:
        pass
    except Exception:
        log.exception("Call failed")


api = FastAPI(title="MES Maya")


@api.get("/health_check")
def health_check():
    _, model = gemini_client_and_model()
    return {"maya": True, "model": model, "voice": CFG.voice,
            "tenant": CFG.tenant_id, "started": STARTED.isoformat()}


async def main():
    try:
        await asyncio.to_thread(refresh_reference_cache)
    except Exception:
        log.exception("Initial Firestore cache refresh failed; using on-demand fallback")
    refresh_task = asyncio.create_task(refresh_cache_loop())
    health = uvicorn.Server(uvicorn.Config(api, host="0.0.0.0", port=CFG.health_port, log_level="warning"))
    try:
        try:
            async with websockets.serve(websocket_handler, CFG.ws_host, CFG.ws_port, max_size=10 * 1024 * 1024):
                log.info("Maya ready: ws://%s:%s (health :%s)", CFG.ws_host, CFG.ws_port, CFG.health_port)
                await health.serve()
        except OSError as exc:
            if exc.errno in {errno.EADDRINUSE, 10048}:
                log.error(
                    "Maya is already running or another program is using ws://%s:%s. "
                    "Stop the old Python process or set WS_PORT to a free port and update MAYA_WS_URI.",
                    CFG.ws_host, CFG.ws_port,
                )
                return
            raise
    finally:
        refresh_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
