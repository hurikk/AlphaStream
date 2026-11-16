#!/usr/bin/env python
"""Inspeção das anomalias da Execução 3 no banco de avaliação (somente leitura).

Coloque este arquivo ao lado de evaluate_pipeline.py (raiz do projeto ou
`scripts/`), com o PostgreSQL do docker-compose em execução e o `.env`
preenchido, e rode:

    uv run python inspect_anomalies.py
    # ou: python inspect_anomalies.py [--db stock_eval] [--out-dir evaluation_results]

O script NÃO altera nada: só executa SELECTs. Ele responde a duas perguntas:

  1. Os 5 registros duplicados na Landing são os mesmos 5 que violam a regra
     de preço/volume na Bronze e que a Silver descartou na Execução 3?
  2. Os registros originais (válidos) desses pares permaneceram na Silver e
     na Gold?

A saída é gravada em <out-dir>/anomalias_<db>.md.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def find_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "src" / "alphastream").is_dir() and (p / "pyproject.toml").exists():
            return p
    sys.exit("Não encontrei a raiz do projeto (pyproject.toml + src/alphastream).")


ROOT = find_root(Path(__file__).resolve().parent)
os.chdir(ROOT)  # o projeto lê Path(".env") relativo ao diretório atual
sys.path.insert(0, str(ROOT))

import psycopg2  # noqa: E402

from src.alphastream.database.helpers import start_conn  # noqa: E402

ENV = Path(".env")

# Mesma regra de validade usada pela Silver e pelo evaluate_pipeline.py
VALID = ("COALESCE(open > 0 AND high > 0 AND low > 0 AND close > 0 "
         "AND volume >= 0, false)")

DUP_PAIRS = ("SELECT ticker, reference_date FROM landing.main_landing "
             "GROUP BY ticker, reference_date HAVING count(*) > 1")

BAD_PAIRS = ("SELECT DISTINCT ticker, reference_date FROM bronze.main_bronze "
             f"WHERE NOT {VALID}")

QUERIES = [
    ("1. Pares (ticker, data) duplicados na Landing",
     "SELECT ticker, reference_date, count(*) AS n_versoes, "
     "min(ingested_at) AS primeira_ingestao, max(ingested_at) AS ultima_ingestao "
     "FROM landing.main_landing GROUP BY ticker, reference_date "
     "HAVING count(*) > 1 ORDER BY ticker, reference_date"),

    ("2. Versões (payload bruto) dos pares duplicados na Landing",
     "SELECT l.ticker, l.reference_date, l.ingested_at, l.payload::text AS payload "
     "FROM landing.main_landing l "
     f"JOIN ({DUP_PAIRS}) d ON d.ticker = l.ticker AND d.reference_date = l.reference_date "
     "ORDER BY l.ticker, l.reference_date, l.ingested_at"),

    ("3. Registros da Bronze que violam a regra de preço/volume (todas as versões)",
     "SELECT ticker, reference_date, ingested_at, open, high, low, close, volume "
     f"FROM bronze.main_bronze WHERE NOT {VALID} "
     "ORDER BY ticker, reference_date, ingested_at"),

    ("4. Esses pares em cada camada (a versão inválida não deve estar na Silver/Gold)",
     "SELECT * FROM ("
     "SELECT 'bronze' AS camada, b.ticker, b.reference_date, b.ingested_at, "
     "b.open, b.high, b.low, b.close, b.volume "
     f"FROM bronze.main_bronze b JOIN ({BAD_PAIRS}) x "
     "ON x.ticker = b.ticker AND x.reference_date = b.reference_date "
     "UNION ALL "
     "SELECT 'silver', s.ticker, s.reference_date, s.ingested_at, "
     "s.open, s.high, s.low, s.close, s.volume "
     f"FROM silver.main_silver s JOIN ({BAD_PAIRS}) x "
     "ON x.ticker = s.ticker AND x.reference_date = s.reference_date "
     "UNION ALL "
     "SELECT 'gold', g.ticker, g.reference_date, g.ingested_at, "
     "g.open, g.high, g.low, g.close, g.volume "
     f"FROM gold.main_gold g JOIN ({BAD_PAIRS}) x "
     "ON x.ticker = g.ticker AND x.reference_date = g.reference_date"
     ") t ORDER BY ticker, reference_date, camada, ingested_at"),

    ("5. Os inválidos da Bronze chegaram depois da última ingestão da Silver? (= lote da Execução 3)",
     "SELECT (SELECT max(ingested_at) FROM silver.main_silver) AS ultimo_ingested_at_silver, "
     "count(*) AS n_invalidos_bronze, "
     "min(ingested_at) AS menor_ingested_at_invalidos, "
     "max(ingested_at) AS maior_ingested_at_invalidos "
     f"FROM bronze.main_bronze WHERE NOT {VALID}"),
]


def cell(v) -> str:
    if v is None:
        return "—"
    s = str(v).replace("|", "\\|").replace("\n", " ")
    return s if len(s) <= 600 else s[:600] + "…"


def md_table(cols: list[str], rows: list[tuple]) -> str:
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join(["---"] * len(cols)) + "|"]
    lines += ["| " + " | ".join(cell(v) for v in r) + " |" for r in rows]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", default="stock_eval", help="banco de avaliação")
    ap.add_argument("--out-dir", default="evaluation_results")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)

    conn = start_conn(ENV, args.db)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET default_transaction_read_only = on")  # garantia extra

    parts = [f"# Anomalias da Execução 3 — banco `{args.db}`\n"]
    for title, sql in QUERIES:
        parts.append(f"## {title}\n")
        try:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            parts.append(f"{len(rows)} linha(s).\n")
            if rows:
                parts.append(md_table(cols, rows) + "\n")
        except psycopg2.Error as exc:
            parts.append(f"**ERRO:** {str(exc).strip().splitlines()[0]}\n"
                         "(confira o nome das colunas/tabelas e ajuste a consulta)\n")

    cur.close()
    conn.close()

    report = "\n".join(parts)
    target = out_dir / f"anomalias_{args.db}.md"
    target.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nGravado em {target.resolve()}")


if __name__ == "__main__":
    main()
