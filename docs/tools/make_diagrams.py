"""Generate architecture diagrams (PNG) for the weather ETL case study.

Pure-Pillow renderer so it works without Graphviz/Mermaid/network access.

Usage:
    python docs/tools/make_diagrams.py
Outputs:
    docs/images/architecture.png
    docs/images/dag_flow.png
"""

from __future__ import annotations

import os

from PIL import Image, ImageDraw, ImageFont

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.normpath(os.path.join(HERE, "..", "images"))

FONT_DIR = "/usr/share/fonts/truetype/dejavu"
F_REG = os.path.join(FONT_DIR, "DejaVuSans.ttf")
F_BOLD = os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf")
F_MONO = os.path.join(FONT_DIR, "DejaVuSansMono.ttf")

# Palette
BG = "#ffffff"
INK = "#1f2933"
MUTED = "#6b7280"
EXTRACT = "#2563eb"
TRANSFORM = "#7c3aed"
LOAD = "#059669"
API = "#0891b2"
STORE = "#b45309"
BORDER = "#cbd5e1"
PANEL = "#f8fafc"
SCALE = 2  # supersampling for crisp text


def font(path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path, size * SCALE)


def s(v: int | float) -> int:
    return int(v * SCALE)


def rounded(d: ImageDraw.ImageDraw, box, radius, fill=None, outline=None, width=2):
    x0, y0, x1, y1 = [s(v) for v in box]
    d.rounded_rectangle([x0, y0, x1, y1], radius=s(radius), fill=fill,
                        outline=outline, width=s(width))


def center_text(d, box, text, fnt, fill=INK):
    x0, y0, x1, y1 = [s(v) for v in box]
    tb = d.textbbox((0, 0), text, font=fnt)
    tx = x0 + ((x1 - x0) - (tb[2] - tb[0])) / 2 - tb[0]
    ty = y0 + ((y1 - y0) - (tb[3] - tb[1])) / 2 - tb[1]
    d.text((tx, ty), text, font=fnt, fill=fill)


def text_at(d, xy, text, fnt, fill=INK, anchor=None):
    d.text((s(xy[0]), s(xy[1])), text, font=fnt, fill=fill, anchor=anchor)


def arrow(d, start, end, color=MUTED, width=2, head=9):
    import math
    x0, y0 = s(start[0]), s(start[1])
    x1, y1 = s(end[0]), s(end[1])
    d.line([x0, y0, x1, y1], fill=color, width=s(width))
    ang = math.atan2(y1 - y0, x1 - x0)
    h = s(head)
    for sign in (1, -1):
        a = ang + sign * math.radians(155)
        d.line([x1, y1, x1 + h * math.cos(a), y1 + h * math.sin(a)],
               fill=color, width=s(width))


def new_canvas(w, h):
    img = Image.new("RGB", (s(w), s(h)), BG)
    return img, ImageDraw.Draw(img)


def save(img, name):
    os.makedirs(OUT_DIR, exist_ok=True)
    img = img.resize((img.width // SCALE, img.height // SCALE), Image.LANCZOS)
    path = os.path.join(OUT_DIR, name)
    img.save(path, "PNG")
    print(f"wrote {path}")


def diagram_architecture():
    W, H = 980, 470
    img, d = new_canvas(W, H)

    f_title = font(F_BOLD, 19)
    f_sub = font(F_REG, 11)
    f_box = font(F_BOLD, 13)
    f_small = font(F_REG, 10)
    f_mono = font(F_MONO, 9)

    text_at(d, (30, 22), "Weather ETL Pipeline — System Architecture", f_title)
    text_at(d, (30, 47), "Astro Runtime 12.1.1 (Airflow 2.10.2) orchestrating a daily Open-Meteo -> PostgreSQL ETL",
            f_sub, MUTED)

    # Source panel
    rounded(d, (30, 90, 210, 210), 10, fill="#ecfeff", outline=API, width=2)
    center_text(d, (30, 100, 210, 124), "Open-Meteo API", f_box, API)
    center_text(d, (30, 122, 210, 144), "public REST, no key", f_small, MUTED)
    center_text(d, (30, 146, 210, 166), "/v1/forecast", f_mono, INK)
    center_text(d, (30, 164, 210, 184), "current_weather=true", f_mono, INK)
    center_text(d, (30, 184, 210, 204), "JSON over HTTPS", f_small, MUTED)

    # Airflow panel
    rounded(d, (280, 78, 700, 300), 12, fill=PANEL, outline=BORDER, width=2)
    text_at(d, (298, 90), "Apache Airflow  (Astro CLI, local Docker)", f_box, INK)
    text_at(d, (298, 110), "DAG: weather_etl_pipeline   ·   schedule @daily   ·   catchup False",
            f_small, MUTED)

    # Task boxes
    rounded(d, (300, 138, 420, 196), 8, fill="#eff6ff", outline=EXTRACT, width=2)
    center_text(d, (300, 144, 420, 164), "extract", f_box, EXTRACT)
    center_text(d, (300, 160, 420, 178), "HttpHook", f_mono, INK)
    center_text(d, (300, 174, 420, 192), "GET forecast", f_small, MUTED)

    rounded(d, (450, 138, 570, 196), 8, fill="#f5f3ff", outline=TRANSFORM, width=2)
    center_text(d, (450, 144, 570, 164), "transform", f_box, TRANSFORM)
    center_text(d, (450, 160, 570, 178), "pure Python", f_mono, INK)
    center_text(d, (450, 174, 570, 192), "flatten JSON", f_small, MUTED)

    rounded(d, (600, 138, 690, 196), 8, fill="#ecfdf5", outline=LOAD, width=2)
    center_text(d, (600, 144, 690, 164), "load", f_box, LOAD)
    center_text(d, (600, 160, 690, 178), "PostgresHook", f_mono, INK)
    center_text(d, (600, 174, 690, 192), "DDL + INSERT", f_small, MUTED)

    arrow(d, (422, 167), (448, 167), EXTRACT)
    arrow(d, (572, 167), (598, 167), TRANSFORM)

    # XCom note
    rounded(d, (300, 218, 690, 282), 8, fill="#ffffff", outline=BORDER, width=1)
    text_at(d, (314, 226), "XCom (Airflow metadata DB) carries data between tasks", f_small, INK)
    text_at(d, (314, 244), "extract -> dict(full JSON)  ->  transform -> dict(6 fields)  -> load", f_mono, MUTED)
    text_at(d, (314, 262), "Each task is a separate process: nothing is shared in memory.", f_small, MUTED)

    # Warehouse
    rounded(d, (770, 90, 950, 210), 10, fill="#fffbeb", outline=STORE, width=2)
    center_text(d, (770, 100, 950, 124), "PostgreSQL", f_box, STORE)
    center_text(d, (770, 120, 950, 140), "docker-compose.yml", f_mono, MUTED)
    center_text(d, (770, 142, 950, 162), "table: weather_data", f_mono, INK)
    center_text(d, (770, 162, 950, 182), "append-only rows", f_small, MUTED)
    center_text(d, (770, 182, 950, 202), "port 5432", f_small, MUTED)

    arrow(d, (212, 150), (298, 150), API)
    text_at(d, (222, 128), "HTTPS", f_small, MUTED)
    arrow(d, (702, 167), (768, 150), STORE)
    text_at(d, (706, 178), "psycopg2", f_small, MUTED)

    # Metadata DB
    rounded(d, (280, 320, 700, 386), 10, fill="#f1f5f9", outline=BORDER, width=2)
    text_at(d, (298, 332), "Airflow metadata DB (separate Postgres container)", f_box, INK)
    text_at(d, (298, 354), "DAG runs · task state · XCom payloads · Connections (encrypted)", f_small, MUTED)
    arrow(d, (490, 302), (490, 318), BORDER)

    # Connections legend
    rounded(d, (770, 240, 950, 386), 10, fill=PANEL, outline=BORDER, width=2)
    text_at(d, (784, 250), "Airflow Connections", f_box, INK)
    text_at(d, (784, 274), "open_meteo_api", f_mono, API)
    text_at(d, (784, 290), "  HTTP · api.open-meteo.com", f_small, MUTED)
    text_at(d, (784, 314), "postgres_default", f_mono, STORE)
    text_at(d, (784, 330), "  Postgres · target warehouse", f_small, MUTED)
    text_at(d, (784, 352), "Credentials live in Airflow,", f_small, MUTED)
    text_at(d, (784, 366), "never in DAG source.", f_small, MUTED)

    text_at(d, (30, 424), "Data flow:  API (JSON)  ->  extract  ->  XCom  ->  transform  ->  XCom  ->  load  ->  weather_data",
            f_small, MUTED)
    text_at(d, (30, 442), "Every task boundary is a retry boundary and a serialization boundary.", f_small, MUTED)

    save(img, "architecture.png")


def diagram_dag_flow():
    W, H = 980, 560
    img, d = new_canvas(W, H)

    f_title = font(F_BOLD, 19)
    f_sub = font(F_REG, 11)
    f_stage = font(F_BOLD, 13)
    f_small = font(F_REG, 10)
    f_mono = font(F_MONO, 9)
    f_mono_b = font(F_MONO, 9)

    text_at(d, (30, 22), "weather_etl_pipeline — Task Flow & Data Shape", f_title)
    text_at(d, (30, 47), "TaskFlow API: return values are pushed to XCom and injected into the next task",
            f_sub, MUTED)

    lanes = [
        ("1. extract_weather_data", EXTRACT, "#eff6ff", 40,
         ["HttpHook(", "  http_conn_id='open_meteo_api',", "  method='GET')", "",
          "endpoint =", "  /v1/forecast?latitude=..", "    &current_weather=true", "",
          "returns response.json()"]),
        ("2. transform_weather_data", TRANSFORM, "#f5f3ff", 360,
         ["reads", "  weather_data['current_weather']", "",
          "picks the 4 measures and", "re-attaches the module-level",
          "LATITUDE / LONGITUDE constants", "", "returns a flat dict", "of 6 keys"]),
        ("3. load_weather_data", LOAD, "#ecfdf5", 680,
         ["PostgresHook(", "  postgres_conn_id=", "    'postgres_default')", "",
          "CREATE TABLE IF NOT EXISTS", "INSERT INTO weather_data ...", "conn.commit()",
          "", "no return value (chain ends)"]),
    ]

    for title, color, fill, x, lines in lanes:
        rounded(d, (x, 82, x + 280, 300), 10, fill=fill, outline=color, width=2)
        text_at(d, (x + 16, 96), title, f_stage, color)
        yy = 124
        for ln in lines:
            text_at(d, (x + 16, yy), ln, f_mono if ln else f_small, INK if ln.strip() else MUTED)
            yy += 15

    arrow(d, (324, 190), (356, 190), MUTED)
    arrow(d, (644, 190), (676, 190), MUTED)
    text_at(d, (322, 168), "XCom", f_small, MUTED)
    text_at(d, (642, 168), "XCom", f_small, MUTED)

    # Payload panels
    rounded(d, (40, 320, 470, 470), 10, fill=PANEL, outline=BORDER, width=2)
    text_at(d, (56, 332), "XCom #1 — raw API payload (excerpt)", f_stage, INK)
    raw = [
        '{ "latitude": 51.5, "longitude": -0.12,',
        '  "generationtime_ms": 0.02,',
        '  "utc_offset_seconds": 0,',
        '  "current_weather": {',
        '      "time": "2026-07-28T07:00",',
        '      "temperature": 17.4, "windspeed": 11.2,',
        '      "winddirection": 243, "weathercode": 3 } }',
    ]
    yy = 356
    for ln in raw:
        text_at(d, (56, yy), ln, f_mono, INK)
        yy += 15

    rounded(d, (510, 320, 960, 470), 10, fill=PANEL, outline=BORDER, width=2)
    text_at(d, (526, 332), "XCom #2 — transformed row -> weather_data", f_stage, INK)
    cols = [
        ("latitude", "FLOAT", "'51.5074'  (str from constant)"),
        ("longitude", "FLOAT", "'-0.1278'  (str from constant)"),
        ("temperature", "FLOAT", "17.4"),
        ("windspeed", "FLOAT", "11.2"),
        ("winddirection", "FLOAT", "243"),
        ("weathercode", "INT", "3"),
        ("timestamp", "TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP"),
    ]
    yy = 356
    for name, typ, val in cols:
        text_at(d, (526, yy), f"{name:<14}", f_mono_b, LOAD)
        text_at(d, (640, yy), f"{typ:<10}", f_mono, MUTED)
        text_at(d, (720, yy), val, f_mono, INK)
        yy += 15

    text_at(d, (30, 496), "Note: latitude/longitude are inserted as Python strings; Postgres coerces them to FLOAT on write.",
            f_small, MUTED)
    text_at(d, (30, 514), "Note: the DAG is append-only — re-running a date adds another row rather than replacing it.",
            f_small, MUTED)
    text_at(d, (30, 532), "Note: with retries unset, any transient API or DB error fails the run immediately.",
            f_small, MUTED)

    save(img, "dag_flow.png")


if __name__ == "__main__":
    diagram_architecture()
    diagram_dag_flow()
