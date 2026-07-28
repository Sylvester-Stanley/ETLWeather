"""Render the ETLWeather Markdown docs to a single styled PDF.

Uses ReportLab's Platypus so it works without Pango/Cairo/Chromium, which are
unavailable in many minimal environments.

Supported Markdown subset (sufficient for these documents):
    #/##/### headings, paragraphs, bullet and numbered lists, fenced code
    blocks, pipe tables, block quotes, images, horizontal rules, and inline
    **bold** / *italic* / `code` / [links](url).

Usage:
    python docs/tools/md_to_pdf.py docs/CASE_STUDY.md docs/IMPLEMENTATION_GUIDE.md \
        -o docs/ETLWeather_Case_Study.pdf
"""

from __future__ import annotations

import argparse
import html
import os
import re

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate, CondPageBreak, Frame, HRFlowable, Image, KeepTogether,
    ListFlowable, ListItem, PageBreak, PageTemplate, Paragraph, Preformatted,
    Spacer, Table, TableStyle,
)

INK = colors.HexColor("#1f2933")
MUTED = colors.HexColor("#6b7280")
ACCENT = colors.HexColor("#2563eb")
RULE = colors.HexColor("#cbd5e1")
CODE_BG = colors.HexColor("#f6f8fa")
TH_BG = colors.HexColor("#eef2f7")
QUOTE_BG = colors.HexColor("#fffbeb")
QUOTE_BAR = colors.HexColor("#f59e0b")

PAGE_W, PAGE_H = A4
MARGIN = 18 * mm


def build_styles():
    ss = getSampleStyleSheet()
    s = {}
    s["title"] = ParagraphStyle("title", parent=ss["Title"], fontName="Helvetica-Bold",
                                fontSize=26, leading=31, textColor=INK, spaceAfter=6)
    s["subtitle"] = ParagraphStyle("subtitle", parent=ss["Normal"], fontName="Helvetica",
                                   fontSize=12, leading=17, textColor=MUTED, spaceAfter=18)
    s["h1"] = ParagraphStyle("h1", parent=ss["Heading1"], fontName="Helvetica-Bold",
                             fontSize=17, leading=22, textColor=INK,
                             spaceBefore=16, spaceAfter=8)
    s["h2"] = ParagraphStyle("h2", parent=ss["Heading2"], fontName="Helvetica-Bold",
                             fontSize=13.5, leading=18, textColor=INK,
                             spaceBefore=13, spaceAfter=6)
    s["h3"] = ParagraphStyle("h3", parent=ss["Heading3"], fontName="Helvetica-Bold",
                             fontSize=11.5, leading=15, textColor=ACCENT,
                             spaceBefore=10, spaceAfter=4)
    s["body"] = ParagraphStyle("body", parent=ss["BodyText"], fontName="Helvetica",
                               fontSize=9.6, leading=14.2, textColor=INK,
                               alignment=TA_LEFT, spaceAfter=7)
    s["bullet"] = ParagraphStyle("bullet", parent=s["body"], spaceAfter=3, leading=13.6)
    s["code"] = ParagraphStyle("code", parent=ss["Code"], fontName="Courier",
                               fontSize=7.9, leading=10.4, textColor=INK,
                               backColor=CODE_BG, borderPadding=6,
                               leftIndent=2, spaceBefore=3, spaceAfter=9)
    s["quote"] = ParagraphStyle("quote", parent=s["body"], fontSize=9.2, leading=13.4,
                                textColor=INK, leftIndent=9, spaceAfter=4, spaceBefore=2)
    s["cell"] = ParagraphStyle("cell", parent=s["body"], fontSize=8.4, leading=11.6,
                               spaceAfter=0)
    s["cellh"] = ParagraphStyle("cellh", parent=s["cell"], fontName="Helvetica-Bold")
    s["caption"] = ParagraphStyle("caption", parent=s["body"], fontSize=8.3,
                                  textColor=MUTED, spaceBefore=3, spaceAfter=11)
    s["toc"] = ParagraphStyle("toc", parent=s["body"], fontSize=10, leading=16,
                              spaceAfter=1)
    return s


INLINE_CODE = re.compile(r"`([^`]+)`")
BOLD = re.compile(r"\*\*([^*]+)\*\*")
ITALIC = re.compile(r"(?<![*\w])\*([^*\n]+)\*(?!\*)")
LINK = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
BARE_URL = re.compile(r"<(https?://[^>]+)>")


# The core PDF fonts have no emoji glyphs, so emoji render as tofu boxes.
# Markdown keeps the emoji (GitHub renders it); the PDF gets a text equivalent.
EMOJI_MAP = {
    "\u26a0\ufe0f": "<b>IMPORTANT</b>",
    "\u26a0": "<b>IMPORTANT</b>",
    "\u2705": "[ok]",
    "\u274c": "[x]",
    "\u25b6": ">",
}


def inline(text: str) -> str:
    """Convert inline Markdown to ReportLab mini-HTML."""
    for emoji, repl in EMOJI_MAP.items():
        text = text.replace(emoji, repl)
    out = html.escape(text, quote=False)
    # Un-escape the replacement markup introduced above.
    out = out.replace("&lt;b&gt;IMPORTANT&lt;/b&gt;", "<b>IMPORTANT</b>")
    # Restore escaped entities inside the patterns we handle.
    out = BARE_URL.sub(r'<font color="#2563eb">\1</font>', out)
    out = LINK.sub(r'<font color="#2563eb">\1</font>', out)
    out = INLINE_CODE.sub(
        r'<font face="Courier" size="8.4" backColor="#f1f5f9">\1</font>', out)
    out = BOLD.sub(r"<b>\1</b>", out)
    out = ITALIC.sub(r"<i>\1</i>", out)
    return out


def split_row(line: str) -> list[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    return [c.strip() for c in line.split("|")]


def is_separator(line: str) -> bool:
    return bool(re.fullmatch(r"\|?[\s:\-|]+\|?", line.strip())) and "-" in line


def make_table(rows: list[list[str]], styles, avail: float):
    header, body = rows[0], rows[1:]
    ncols = max(len(r) for r in rows)
    rows = [r + [""] * (ncols - len(r)) for r in rows]
    header, body = rows[0], rows[1:]

    # Width proportional to content, clamped so no column collapses.
    weights = []
    for c in range(ncols):
        longest = max(len(str(r[c])) for r in rows)
        weights.append(max(longest, 6))
    total = sum(weights)
    widths = [max(avail * w / total, 16 * mm) for w in weights]
    scale = avail / sum(widths)
    widths = [w * scale for w in widths]

    # A header of all-empty cells is a key/value table written without headings.
    headerless = not any(c.strip() for c in header)
    if headerless:
        rows = body
        data = [[Paragraph(inline(c), styles["cell"]) for c in r] for r in rows]
    else:
        data = [[Paragraph(inline(c), styles["cellh"]) for c in header]]
        for r in body:
            data.append([Paragraph(inline(c), styles["cell"]) for c in r])

    t = Table(data, colWidths=widths, repeatRows=0 if headerless else 1, hAlign="LEFT")
    style = [
        ("GRID", (0, 0), (-1, -1), 0.4, RULE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    if headerless:
        style.append(("BACKGROUND", (0, 0), (0, -1), TH_BG))
    else:
        style += [
            ("BACKGROUND", (0, 0), (-1, 0), TH_BG),
            ("LINEBELOW", (0, 0), (-1, 0), 0.8, RULE),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1),
             [colors.white, colors.HexColor("#fbfcfd")]),
        ]
    t.setStyle(TableStyle(style))
    return t


# Courier (a core PDF font) has no box-drawing glyphs; fall back to ASCII.
BOX_MAP = str.maketrans({
    "\u251c": "|", "\u2514": "`", "\u2500": "-",
    "\u2502": "|", "\u252c": "+", "\u2518": "'", "\u2510": ".",
})


def code_block(lines: list[str], styles, avail: float):
    # Hard-wrap long lines so nothing runs off the page.
    max_chars = int(avail / (7.9 * 0.6))
    wrapped: list[str] = []
    for ln in lines:
        ln = ln.replace("\t", "    ").translate(BOX_MAP)
        while len(ln) > max_chars:
            cut = ln.rfind(" ", 0, max_chars)
            if cut < max_chars // 2:
                cut = max_chars
            wrapped.append(ln[:cut])
            ln = "    " + ln[cut:].lstrip()
        wrapped.append(ln)
    return Preformatted("\n".join(wrapped), styles["code"])


def quote_block(lines: list[str], styles, avail: float):
    flows = [Paragraph(inline(l), styles["quote"]) for l in lines if l.strip()]
    t = Table([[flows]], colWidths=[avail], hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), QUOTE_BG),
        ("LINEBEFORE", (0, 0), (0, -1), 2.5, QUOTE_BAR),
        ("LEFTPADDING", (0, 0), (-1, -1), 9),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return [t, Spacer(1, 8)]


def parse_markdown(md: str, base_dir: str, styles, avail: float, first_doc: bool):
    story = []
    lines = md.split("\n")
    i = 0
    pending_list: list[tuple[str, str]] = []  # (kind, text)

    def flush_list():
        nonlocal pending_list
        if not pending_list:
            return
        kind = pending_list[0][0]
        items = [ListItem(Paragraph(inline(t), styles["bullet"]), leftIndent=13)
                 for _, t in pending_list]
        story.append(ListFlowable(
            items,
            bulletType="1" if kind == "ol" else "bullet",
            bulletFontSize=8.4, leftIndent=15, bulletOffsetY=-0.7,
            start="1" if kind == "ol" else None,
        ))
        story.append(Spacer(1, 7))
        pending_list = []

    while i < len(lines):
        raw = lines[i]
        line = raw.rstrip()
        stripped = line.strip()

        # HTML details blocks -> keep the inner content, drop the tags.
        if stripped.startswith("<details") or stripped.startswith("</details")            or stripped.startswith("<summary") or stripped.startswith("</summary"):
            inner = re.sub(r"<[^>]+>", "", stripped).strip()
            if inner:
                flush_list()
                story.append(Paragraph(f"<b>{inline(inner)}</b>", styles["body"]))
            i += 1
            continue

        # Fenced code
        if stripped.startswith("```"):
            flush_list()
            i += 1
            buf = []
            while i < len(lines) and not lines[i].strip().startswith("```"):
                buf.append(lines[i])
                i += 1
            i += 1
            story.append(code_block(buf, styles, avail))
            continue

        # Blank
        if not stripped:
            flush_list()
            i += 1
            continue

        # Horizontal rule
        if re.fullmatch(r"-{3,}|\*{3,}|_{3,}", stripped):
            flush_list()
            story.append(Spacer(1, 4))
            story.append(HRFlowable(width="100%", thickness=0.7, color=RULE))
            story.append(Spacer(1, 9))
            i += 1
            continue

        # Image
        m = re.fullmatch(r"!\[([^\]]*)\]\(([^)]+)\)", stripped)
        if m:
            flush_list()
            alt, src = m.group(1), m.group(2)
            path = src if os.path.isabs(src) else os.path.join(base_dir, src)
            if os.path.exists(path):
                from reportlab.lib.utils import ImageReader
                iw, ih = ImageReader(path).getSize()
                w = min(avail, iw)
                h = ih * (w / iw)
                max_h = PAGE_H - 2 * MARGIN - 40 * mm
                if h > max_h:
                    h = max_h
                    w = iw * (h / ih)
                block = [Image(path, width=w, height=h)]
                if alt:
                    block.append(Paragraph(inline(alt), styles["caption"]))
                story.append(CondPageBreak(h + 24 * mm))
                story.extend(block)
            i += 1
            continue

        # Table
        if stripped.startswith("|") and i + 1 < len(lines) and is_separator(lines[i + 1]):
            flush_list()
            rows = [split_row(stripped)]
            i += 2
            while i < len(lines) and lines[i].strip().startswith("|"):
                rows.append(split_row(lines[i]))
                i += 1
            story.append(make_table(rows, styles, avail))
            story.append(Spacer(1, 10))
            continue

        # Block quote
        if stripped.startswith(">"):
            flush_list()
            buf = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                buf.append(lines[i].strip().lstrip(">").strip())
                i += 1
            story.extend(quote_block(buf, styles, avail))
            continue

        # Headings
        m = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if m:
            flush_list()
            level, text = len(m.group(1)), m.group(2).strip()
            if level == 1:
                if story or not first_doc:
                    story.append(PageBreak())
                story.append(Paragraph(inline(text), styles["title"]))
                story.append(HRFlowable(width="100%", thickness=1.2, color=ACCENT))
                story.append(Spacer(1, 12))
            elif level == 2:
                story.append(CondPageBreak(34 * mm))
                story.append(Paragraph(inline(text), styles["h1"]))
                story.append(HRFlowable(width="100%", thickness=0.6, color=RULE))
                story.append(Spacer(1, 6))
            elif level == 3:
                story.append(CondPageBreak(26 * mm))
                story.append(Paragraph(inline(text), styles["h2"]))
            else:
                story.append(Paragraph(inline(text), styles["h3"]))
            i += 1
            continue

        # Lists
        m = re.match(r"^\s*[-*+]\s+(.*)$", line)
        if m:
            pending_list.append(("ul", m.group(1)))
            i += 1
            continue
        m = re.match(r"^\s*\d+[.)]\s+(.*)$", line)
        if m:
            pending_list.append(("ol", m.group(1)))
            i += 1
            continue

        # Paragraph (join continuation lines)
        flush_list()
        buf = [stripped]
        i += 1
        while i < len(lines):
            nxt = lines[i].strip()
            if (not nxt or nxt.startswith(("#", "|", ">", "```", "!["))
                    or re.match(r"^\s*([-*+]|\d+[.)])\s+", lines[i])
                    or re.fullmatch(r"-{3,}|\*{3,}|_{3,}", nxt)
                    or nxt.startswith("<")):
                break
            buf.append(nxt)
            i += 1
        story.append(Paragraph(inline(" ".join(buf)), styles["body"]))

    flush_list()
    return story


def cover(styles, title, subtitle, meta_rows, avail):
    flows = [
        Spacer(1, 46 * mm),
        Paragraph(title, styles["title"]),
        HRFlowable(width="100%", thickness=1.6, color=ACCENT),
        Spacer(1, 10),
        Paragraph(subtitle, styles["subtitle"]),
        Spacer(1, 10),
    ]
    data = [[Paragraph(f"<b>{k}</b>", styles["cell"]), Paragraph(v, styles["cell"])]
            for k, v in meta_rows]
    t = Table(data, colWidths=[avail * 0.32, avail * 0.68], hAlign="LEFT")
    t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LINEBELOW", (0, 0), (-1, -2), 0.4, RULE),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
    ]))
    flows.append(t)
    return flows


def make_footer(title):
    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.6)
        canvas.setFillColor(MUTED)
        if doc.page > 1:
            canvas.drawString(MARGIN, 11 * mm, title)
            canvas.drawRightString(PAGE_W - MARGIN, 11 * mm, f"Page {doc.page}")
            canvas.setStrokeColor(RULE)
            canvas.setLineWidth(0.4)
            canvas.line(MARGIN, 14.5 * mm, PAGE_W - MARGIN, 14.5 * mm)
        canvas.restoreState()
    return footer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+")
    ap.add_argument("-o", "--output", required=True)
    args = ap.parse_args()

    styles = build_styles()
    avail = PAGE_W - 2 * MARGIN

    doc = BaseDocTemplate(
        args.output, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN, bottomMargin=MARGIN + 6 * mm,
        title="ETLWeather — Case Study & Implementation Guide",
        author="ETLWeather analysis",
    )
    frame = Frame(MARGIN, MARGIN + 6 * mm, avail,
                  PAGE_H - 2 * MARGIN - 6 * mm, id="body")
    doc.addPageTemplates([
        PageTemplate(id="main", frames=[frame],
                     onPage=make_footer("ETLWeather — Case Study & Implementation Guide"))
    ])

    story = cover(
        styles,
        "ETLWeather",
        "Case Study &amp; Implementation Guide — an Apache Airflow ETL pipeline "
        "from the Open-Meteo API to PostgreSQL",
        [
            ("Repository", "Sylvester-Stanley/ETLWeather"),
            ("Pipeline", "weather_etl_pipeline"),
            ("Orchestrator", "Apache Airflow 2.10.2 (Astro Runtime 12.1.1)"),
            ("Source", "Open-Meteo Forecast API"),
            ("Target", "PostgreSQL 13 — weather_data"),
            ("Schedule", "@daily, catchup disabled"),
            ("Contents", "Part 1 — Case Study &nbsp;·&nbsp; Part 2 — Implementation Guide"),
            ("Date", "28 July 2026"),
        ],
        avail,
    )

    for n, path in enumerate(args.inputs):
        with open(path, encoding="utf-8") as fh:
            md = fh.read()
        base = os.path.dirname(os.path.abspath(path))
        story.extend(parse_markdown(md, base, styles, avail, first_doc=False))

    doc.build(story)
    size = os.path.getsize(args.output)
    print(f"wrote {args.output}  ({size/1024:.0f} KB)")


if __name__ == "__main__":
    main()
