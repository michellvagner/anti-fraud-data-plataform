"""Gera o PDF da documentacao tecnica a partir do Markdown (docs/)."""

import re
from pathlib import Path

import markdown
from weasyprint import HTML

DOCS = Path(__file__).parent
SOURCE = DOCS / "documentacao-tecnica-pipeline.md"
OUTPUT = DOCS / "documentacao-tecnica-pipeline.pdf"

DIAGRAM = """<pre class="diagram">
  CSV locais (data/)
          |
   Python + Polars
          |
    Parquet (temp/)
          |
  AWS S3  raw/transactions/  --(evento s3:ObjectCreated -> SQS)--+
          |                                                      |
  Stage externo STAGE_S3_TRANSACTIONS  --------------------------+
          |
  Snowpipe BRONZE_TRANSACTIONS_PIPE (AUTO_INGEST)
          |
  Tabela Bronze  BRONZE_TRANSACTIONS
          |
  Stream APPEND_ONLY  BRONZE_TRANSACTIONS_STREAM
          |
  Task SILVER_TRANSACTIONS_TASK (schedule 1 MINUTE)
          |
  Tabela Silver  SILVER_TRANSACTIONS
          |
  Task GOLD_TRANSACTIONS_TASK (AFTER Silver)
          |
  Camada Gold  GOLD_TRANSACTIONS
</pre>"""

CSS = """
@page { size: A4; margin: 1.4cm 1.6cm; @bottom-center { content: counter(page); font-size: 8pt; color: #666; } }
body { font-family: "DejaVu Sans", sans-serif; font-size: 8.7pt; line-height: 1.35; color: #111; text-align: justify; }
h1 { font-size: 15pt; margin: 0 0 6px; border-bottom: 2px solid #1f4e79; padding-bottom: 4px; color: #1f4e79; }
h2 { font-size: 10.5pt; margin: 11px 0 4px; color: #1f4e79; page-break-after: avoid; }
p, li { margin: 3px 0; }
ul, ol { margin: 3px 0 3px 16px; padding: 0; }
code { font-family: "DejaVu Sans Mono", monospace; font-size: 7.6pt; background: #f2f4f7; padding: 0 2px; }
pre { background: #f6f8fa; border: 1px solid #dfe3e8; padding: 5px 7px; margin: 5px 0; page-break-inside: avoid; }
pre code { background: none; font-size: 7.3pt; }
pre.diagram { font-family: "DejaVu Sans Mono", monospace; font-size: 7.2pt; line-height: 1.15; text-align: left; }
table { border-collapse: collapse; width: 100%; margin: 5px 0; font-size: 7.9pt; page-break-inside: avoid; }
th, td { border: 1px solid #cfd6dd; padding: 2px 5px; text-align: left; vertical-align: top; }
th { background: #eaeef3; }
"""


def build() -> Path:
    text = SOURCE.read_text(encoding="utf-8")
    text = re.sub(r"```mermaid.*?```", "@@DIAGRAM@@", text, flags=re.DOTALL)

    body = markdown.markdown(text, extensions=["tables", "fenced_code"])
    body = body.replace("<p>@@DIAGRAM@@</p>", DIAGRAM)

    html = f"<html><head><meta charset='utf-8'><style>{CSS}</style></head><body>{body}</body></html>"
    HTML(string=html).write_pdf(OUTPUT)

    return OUTPUT


if __name__ == "__main__":
    print(f"PDF gerado: {build()}")
