"""Gera o PDF da documentacao tecnica a partir do Markdown (docs/)."""

import re
from pathlib import Path

import markdown
from weasyprint import HTML

DOCS = Path(__file__).parent
SOURCE = DOCS / "documentacao-tecnica-pipeline.md"
OUTPUT = DOCS / "documentacao-tecnica-pipeline.pdf"

AUTHOR = "Vagner Michell"
TITLE = "Anti-Fraud Data Platform"
SUBTITLE = "Pipeline de Dados com AWS S3 e Snowflake"


def _node(label, sub, kind):
    return (
        f'<div class="node {kind}"><span class="lbl">{label}</span>'
        f'<span class="sub">{sub}</span></div>'
    )


ARROW = '<div class="arrow">&#9660;</div>'

DIAGRAM = f"""
<figure class="diagram">
  <div class="track">
    <div class="stage-tag src">Origem &amp; preparação</div>
    {_node("Dados de origem", "arquivos CSV (;)", "src")}
    {ARROW}
    {_node("Python + Polars", "leitura e tipagem", "src")}
    {ARROW}
    {_node("Parquet", "formato colunar", "src")}
    {ARROW}
    <div class="stage-tag aws">AWS (suporte)</div>
    {_node("AWS S3", "raw/transactions/", "aws")}
    <div class="arrow evt">&#9660;<span>evento S3 &#8594; SQS</span></div>
    <div class="stage-tag snow">Snowflake</div>
    {_node("Stage externo &#8594; Snowpipe", "AUTO_INGEST", "snow")}
    {ARROW}
    {_node("Bronze", "BRONZE_TRANSACTIONS", "bronze")}
    {ARROW}
    {_node("Stream", "APPEND_ONLY — só o que é novo", "snow")}
    <div class="arrow task">&#9660;<span>Task SILVER (1 min)</span></div>
    {_node("Silver", "SILVER_TRANSACTIONS", "silver")}
    <div class="arrow task">&#9660;<span>Task GOLD (AFTER Silver)</span></div>
    {_node("Gold", "GOLD_TRANSACTIONS", "gold")}
  </div>
  <figcaption>Terraform provisiona o bucket e os prefixos S3 (componente complementar);
  as Snowflake Tasks automatizam o avanço entre as camadas.</figcaption>
</figure>
"""

COVER = f"""
<section class="cover">
  <div class="cover-rule"></div>
  <p class="kicker">Documentação Técnica &#183; Engenharia de Dados</p>
  <h1 class="cover-title">{TITLE}</h1>
  <p class="cover-sub">{SUBTITLE}</p>
  <div class="chips">
    <span class="chip py">Python</span><span class="chip s3">AWS S3</span>
    <span class="chip pq">Parquet</span><span class="chip sf">Snowflake</span>
    <span class="chip dp">Data Pipeline</span>
  </div>
  <div class="cover-flow">
    CSV &#8594; Python &#8594; Parquet &#8594; S3 &#8594; Snowpipe &#8594;
    Bronze &#8594; Stream &#8594; Silver &#8594; Gold
  </div>
  <p class="cover-author">{AUTHOR}</p>
  <p class="cover-note">Trabalho de pós-graduação &#183; repositório <code>anti-fraud-data-plataform</code></p>
</section>
"""

CSS = """
@page { size: A4; margin: 1.5cm 1.7cm 1.4cm;
        @bottom-right { content: counter(page); font-size: 7.5pt; color: #8a9199; } }
@page :first { margin: 0; @bottom-right { content: none; } }

body { font-family: "DejaVu Sans", sans-serif; font-size: 8.2pt; line-height: 1.34;
       color: #1c2530; text-align: justify; }

/* Capa */
.cover { height: 297mm; padding: 38mm 24mm 0; page-break-after: always; text-align: left;
         border-top: 9mm solid #0f2b46; border-bottom: 4mm solid #29b5e8; }
.cover h1.cover-title { display: block; }
.cover-rule { width: 46mm; height: 3px; background: #29b5e8; margin-bottom: 9mm; }
.kicker { font-size: 8.5pt; letter-spacing: 2.6px; text-transform: uppercase;
          color: #5b6b7c; margin: 0 0 6mm; }
.cover-title { font-size: 30pt; line-height: 1.1; color: #0f2b46; margin: 0; font-weight: bold; }
.cover-sub { font-size: 13pt; color: #29b5e8; margin: 4mm 0 0; }
.chips { margin: 13mm 0 0; }
.chip { display: inline-block; font-size: 7.6pt; padding: 2px 9px; margin: 0 5px 5px 0;
        border-radius: 11px; border: 1px solid #d3dce4; color: #3b4a59; background: #f6f9fb; }
.chip.py { border-color: #ffd34d; } .chip.s3 { border-color: #f0913a; }
.chip.pq { border-color: #9aa7b3; } .chip.sf { border-color: #29b5e8; }
.chip.dp { border-color: #6fbf73; }
.cover-flow { margin-top: 10mm; padding: 5mm 6mm; background: #f4f8fb; border-left: 3px solid #29b5e8;
              font-family: "DejaVu Sans Mono", monospace; font-size: 7.6pt; color: #23394f; }
.cover-author { margin: 70mm 0 0; font-size: 11pt; color: #0f2b46; font-weight: bold; }
.cover-note { margin: 1mm 0 0; font-size: 8pt; color: #6b7885; }

/* Corpo */
h1 { display: none; }
h2 { font-size: 10.5pt; color: #0f2b46; margin: 10px 0 4px; padding-left: 7px;
     border-left: 3px solid #29b5e8; page-break-after: avoid; }
p, li { margin: 3px 0; }
ul, ol { margin: 3px 0 3px 15px; padding: 0; }
code { font-family: "DejaVu Sans Mono", monospace; font-size: 7.5pt; color: #16425b; }
pre { background: #f4f8fb; border-left: 3px solid #cfe3ee; padding: 5px 8px; margin: 5px 0;
      page-break-inside: avoid; }
pre code { font-size: 7.3pt; color: #23394f; }
table { border-collapse: collapse; width: 100%; margin: 5px 0; font-size: 7.8pt;
        page-break-inside: avoid; }
th, td { border-bottom: 1px solid #e2e8ee; padding: 2px 6px; text-align: left; vertical-align: top; }
th { background: #f2f6f9; color: #0f2b46; border-bottom: 1px solid #c9d6e0; }

/* Diagrama */
.diagram { margin: 4px 0 6px; padding: 2px 0; page-break-inside: avoid; text-align: center; }
.track { display: block; }
.node { display: block; width: 72mm; margin: 0 auto; padding: 1.5px 8px; border-radius: 4px;
        border: 1px solid #cfdae4; background: #fbfdfe; }
.node .lbl { display: block; font-size: 8pt; font-weight: bold; color: #0f2b46; }
.node .sub { display: block; font-size: 6.9pt; color: #66757f; }
.node.src { border-left: 4px solid #ffd34d; }
.node.aws { border-left: 4px solid #f0913a; }
.node.snow { border-left: 4px solid #29b5e8; }
.node.bronze { border-left: 4px solid #b07d3f; background: #fdf9f3; }
.node.silver { border-left: 4px solid #9aa7b3; background: #f9fafb; }
.node.gold { border-left: 4px solid #d4a017; background: #fdfaf0; }
.arrow { font-size: 6.2pt; color: #9fb1c0; line-height: 1; margin: 0; }
.arrow span { font-size: 6.6pt; color: #5b6b7c; margin-left: 5px; font-style: italic; }
.stage-tag { font-size: 6.4pt; letter-spacing: 1.4px; text-transform: uppercase;
             color: #7b8894; margin: 3px 0 1px; }
figcaption { font-size: 6.8pt; color: #6b7885; margin-top: 4px; text-align: center; }
"""


def build() -> Path:
    text = SOURCE.read_text(encoding="utf-8")
    text = re.sub(r"```mermaid.*?```", "@@DIAGRAM@@", text, flags=re.DOTALL)

    body = markdown.markdown(text, extensions=["tables", "fenced_code"])
    body = body.replace("<p>@@DIAGRAM@@</p>", DIAGRAM)

    html = (
        "<html><head><meta charset='utf-8'>"
        f"<style>{CSS}</style></head><body>{COVER}{body}</body></html>"
    )
    HTML(string=html).write_pdf(OUTPUT)

    return OUTPUT


if __name__ == "__main__":
    print(f"PDF gerado: {build()}")
