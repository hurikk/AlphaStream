#!/usr/bin/env python
"""Avaliação enxuta do pipeline AlphaStream (subconjunto de tickers).

Coloque este arquivo na raiz do projeto (ao lado de pyproject.toml) ou em
`scripts/`, com o PostgreSQL do docker-compose em execução e o `.env`
preenchido, e rode:

    uv run python evaluate_pipeline.py
    # ou: python evaluate_pipeline.py

O que o script faz
------------------
1. Recria um banco isolado (`stock_eval`), sem tocar no `stock_database`.
2. Restringe o universo a 5 tickers (sem chamar a Brapi).
3. Executa o pipeline real (Landing -> Bronze -> Silver -> Gold) em sequência:
     Execução 1: carga inicial. A fonte é "congelada" em hoje - CUTOFF dias
                 (o código do projeto não é alterado; só o download é
                 truncado em memória).
     Execução 2: incremental real, com os dias novos desde o corte.
     Execução 3: reexecução imediata, sobre o mesmo estado da fonte
                 (teste de idempotência).
   Com `--cutoff-days 0` são feitas só duas execuções (carga + reexecução).
4. Mede tempo por camada, contagem de registros, pares (ticker, data)
   duplicados, completude, validade e retenção entre camadas.
5. Grava `evaluation_results/results.json` e `evaluation_results/results.md`.

Nada é inventado: todos os números vêm de consultas ao banco e de cronômetros.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

DEFAULT_TICKERS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3"]
LAYERS = {
    "landing": "landing.main_landing",
    "bronze": "bronze.main_bronze",
    "silver": "silver.main_silver",
    "gold": "gold.main_gold",
}
OHLCV = ["open", "high", "low", "close", "volume"]
GOLD_FEATURES = ["return_21d", "close_to_ma_21", "rsi_14", "volatility_21d", "relative_volume"]


# --------------------------------------------------------------------------
# Localização da raiz do projeto e imports do próprio projeto
# --------------------------------------------------------------------------
def find_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "src" / "alphastream").is_dir() and (p / "pyproject.toml").exists():
            return p
    sys.exit("Não encontrei a raiz do projeto (pyproject.toml + src/alphastream). "
             "Coloque o script na raiz ou em scripts/.")


ROOT = find_root(Path(__file__).resolve().parent)
os.chdir(ROOT)  # as camadas leem Path(".env") relativo ao diretório atual
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import psycopg2  # noqa: E402

from src.alphastream.database.helpers import start_conn  # noqa: E402
from src.alphastream.pipelines import (  # noqa: E402
    bronze_layer, gold_layer, landing_layer, silver_layer,
)
from src.alphastream.transformations.bronze import parse_payload_key  # noqa: E402

ENV = Path(".env")
logger = logging.getLogger("evaluate_pipeline")


# --------------------------------------------------------------------------
# Preparação do ambiente de avaliação
# --------------------------------------------------------------------------
def reset_database(db: str) -> None:
    """Remove o banco de avaliação (apenas nomes que começam com 'stock_eval')."""
    if not db.startswith("stock_eval"):
        sys.exit("Por segurança, o banco de avaliação deve se chamar 'stock_eval*'.")
    conn = start_conn(ENV, "postgres")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = %s AND pid <> pg_backend_pid()", (db,))
    cur.execute(f'DROP DATABASE IF EXISTS "{db}"')
    cur.close()
    conn.close()


def restrict_universe(tickers: list[str]) -> None:
    """Faz as camadas usarem só `tickers` (sem '.SA'; o código acrescenta o sufixo)."""
    def fixed() -> list[str]:
        return list(tickers)
    for mod in (landing_layer, bronze_layer, silver_layer, gold_layer):
        if hasattr(mod, "get_tickers"):
            mod.get_tickers = fixed


def freeze_source_at(cutoff: date):
    """Trunca em memória o download da yfinance em `cutoff` (inclusive)."""
    original = landing_layer._download_raw

    def truncated(ticker, **kwargs):
        df = original(ticker, **kwargs)
        if df.empty:
            return df
        dates = pd.to_datetime(df[df.columns[0]]).dt.date
        return df[dates <= cutoff].reset_index(drop=True)

    landing_layer._download_raw = truncated
    return original


# --------------------------------------------------------------------------
# Execução cronometrada do pipeline
# --------------------------------------------------------------------------
def run_once(db: str) -> dict:
    steps = [
        ("landing", landing_layer.insert_into_landing_layer, "landing", "main_landing"),
        ("bronze", bronze_layer.insert_into_bronze_layer, "bronze", "main_bronze"),
        ("silver", silver_layer.insert_into_silver_layer, "silver", "main_silver"),
        ("gold", gold_layer.insert_into_gold_layer, "gold", "main_gold"),
    ]
    timings: dict[str, float] = {}
    error = None
    for name, fn, schema, table in steps:
        t0 = time.perf_counter()
        try:
            fn(db_name=db, schema_name=schema, table_name=table)
        except Exception as exc:  # registra e interrompe esta execução
            logger.exception("Falha na camada %s", name)
            error = f"{name}: {type(exc).__name__}: {exc}"
            timings[name] = round(time.perf_counter() - t0, 2)
            break
        timings[name] = round(time.perf_counter() - t0, 2)
    timings["total"] = round(sum(v for k, v in timings.items() if k != "total"), 2)
    return {"timings": timings, "error": error}


# --------------------------------------------------------------------------
# Consultas de medição
# --------------------------------------------------------------------------
def _connect(db: str):
    conn = start_conn(ENV, db)
    conn.autocommit = True
    return conn


def _one(cur, sql: str, params=None):
    cur.execute(sql, params)
    return cur.fetchone()


def pct(num, den):
    return round(100.0 * float(num) / float(den), 2) if den else None


def snapshot(db: str, cutoff: date | None) -> dict:
    """Contagens e duplicidade por camada, no estado atual do banco."""
    conn = _connect(db)
    cur = conn.cursor()
    out = {}
    for layer, table in LAYERS.items():
        try:
            rows = _one(cur, f"SELECT count(*) FROM {table}")[0]
            distinct_pairs = _one(
                cur, f"SELECT count(*) FROM (SELECT 1 FROM {table} "
                     f"GROUP BY ticker, reference_date) t")[0]
            dup_pairs, dup_extra = _one(
                cur, f"SELECT count(*), coalesce(sum(c - 1), 0) FROM ("
                     f"SELECT count(*) c FROM {table} GROUP BY ticker, reference_date "
                     f"HAVING count(*) > 1) t")
            entry = {
                "rows": int(rows),
                "distinct_pairs": int(distinct_pairs),
                "duplicated_pairs": int(dup_pairs),
                "extra_rows_from_duplicates": int(dup_extra),
            }
            if cutoff is not None:
                entry["rows_after_cutoff"] = int(_one(
                    cur, f"SELECT count(*) FROM {table} WHERE reference_date > %s",
                    (cutoff,))[0])
            out[layer] = entry
        except psycopg2.Error as exc:
            out[layer] = {"error": str(exc).strip().splitlines()[0]}
    cur.close()
    conn.close()
    return out


def quality_profile(db: str, cutoff: date | None) -> dict:
    """Completude, validade e nulos das features (estado atual do banco)."""
    conn = _connect(db)
    cur = conn.cursor()
    prof: dict = {}

    # Completude na Landing: campos OHLCV dentro do payload JSON
    cur.execute(
        "SELECT key, count(*), count(*) FILTER (WHERE jsonb_typeof(value) <> 'null') "
        "FROM landing.main_landing, jsonb_each(payload) GROUP BY key")
    fields: dict[str, list[int]] = {}
    for key, total, non_null in cur.fetchall():
        name = parse_payload_key(key).lower()
        acc = fields.setdefault(name, [0, 0])
        acc[0] += int(total)
        acc[1] += int(non_null)
    prof["landing_completeness_pct"] = {
        c: pct(fields[c][1], fields[c][0]) if c in fields else None for c in OHLCV}

    # Completude na Gold: OHLCV e as cinco features persistidas
    cols = OHLCV + GOLD_FEATURES
    counts = ", ".join(f"count({c})" for c in cols)
    row = _one(cur, f"SELECT count(*), {counts} FROM gold.main_gold")
    total = row[0]
    prof["gold_rows"] = int(total)
    prof["gold_completeness_pct"] = {c: pct(row[i + 1], total) for i, c in enumerate(cols)}

    # Nulos das features: carga inicial x lote incremental
    if cutoff is not None:
        split = {}
        for label, op in (("initial_batch", "<="), ("incremental_batch", ">")):
            r = _one(cur, f"SELECT count(*), {counts} FROM gold.main_gold "
                          f"WHERE reference_date {op} %s", (cutoff,))
            split[label] = {
                "rows": int(r[0]),
                "feature_null_pct": {
                    c: (round(100.0 - pct(r[len(OHLCV) + 1 + i], r[0]), 2) if r[0] else None)
                    for i, c in enumerate(GOLD_FEATURES)},
            }
        prof["gold_feature_nulls_by_batch"] = split

    # Validade na Bronze (antes da Silver), com as mesmas regras do código da Silver
    # (conta pares únicos, para que duplicatas da Bronze não inflem o resultado)
    valid_expr = ("COALESCE(open > 0 AND high > 0 AND low > 0 AND close > 0 "
                  "AND volume >= 0, false)")
    r = _one(cur, f"""
        SELECT count(*),
               count(*) FILTER (WHERE open IS NULL OR high IS NULL OR low IS NULL
                                  OR close IS NULL OR volume IS NULL),
               count(*) FILTER (WHERE NOT {valid_expr}),
               count(*) FILTER (WHERE reference_date > current_date),
               count(*) FILTER (WHERE NOT {valid_expr} OR reference_date > current_date)
        FROM (SELECT DISTINCT ON (ticker, reference_date) *
              FROM bronze.main_bronze
              ORDER BY ticker, reference_date, ingested_at DESC) b""")
    prof["bronze_validity"] = {
        "rows": int(r[0]),
        "rows_with_null_ohlcv": int(r[1]),
        "rows_violating_price_volume_rule": int(r[2]),
        "rows_with_future_date": int(r[3]),
        "rows_violating_any_rule": int(r[4]),
        "violating_any_rule_pct": pct(r[4], r[0]),
    }

    # Linhas por ticker na Gold
    cur.execute("SELECT ticker, count(*), min(reference_date), max(reference_date) "
                "FROM gold.main_gold GROUP BY ticker ORDER BY ticker")
    prof["gold_by_ticker"] = [
        {"ticker": t, "rows": int(n), "first_date": str(a), "last_date": str(b)}
        for t, n, a, b in cur.fetchall()]
    cur.close()
    conn.close()
    return prof


# --------------------------------------------------------------------------
# Relatório
# --------------------------------------------------------------------------
def md_table(header: list[str], rows: list[list]) -> str:
    def cell(v):
        return "—" if v is None else str(v)
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join(["---"] * len(header)) + "|"]
    lines += ["| " + " | ".join(cell(v) for v in r) + " |" for r in rows]
    return "\n".join(lines)


def layer_value(snap: dict, layer: str, key: str):
    return snap.get(layer, {}).get(key)


def build_report(cfg: dict, runs: list[dict], quality: dict) -> str:
    names = [r["label"] for r in runs]
    short = [f"Exec. {i + 1}" for i in range(len(runs))]
    out = [f"# Resultados da avaliação enxuta do AlphaStream\n",
           f"- Data da execução: {cfg['executed_at']}",
           f"- Tickers ({len(cfg['tickers'])}): {', '.join(cfg['tickers'])}",
           f"- Banco de avaliação: `{cfg['db']}`",
           f"- Corte da fonte na carga inicial: {cfg['cutoff'] or 'sem corte'}",
           f"- Python {cfg['python']}, pandas {cfg['pandas']}, yfinance {cfg['yfinance']}\n",
           "Execuções:\n"]
    out += [f"- {s}: {n}" + (f" — **ERRO** {r['error']}" if r["error"] else "")
            for s, n, r in zip(short, names, runs)]

    out.append("\n## Tempo de execução por camada (segundos)\n")
    out.append(md_table(["Execução", "Landing", "Bronze", "Silver", "Gold", "Total"],
                        [[s] + [r["timings"].get(k) for k in
                                ("landing", "bronze", "silver", "gold", "total")]
                         for s, r in zip(short, runs)]))

    out.append("\n## Registros por camada ao final de cada execução\n")
    out.append(md_table(["Execução"] + list(LAYERS),
                        [[s] + [layer_value(r["snapshot"], l, "rows") for l in LAYERS]
                         for s, r in zip(short, runs)]))

    out.append("\n## Pares (ticker, data) duplicados por camada\n")
    out.append("Cada célula: pares duplicados / linhas excedentes.\n")
    out.append(md_table(["Execução"] + list(LAYERS),
                        [[s] + [f"{layer_value(r['snapshot'], l, 'duplicated_pairs')} / "
                                f"{layer_value(r['snapshot'], l, 'extra_rows_from_duplicates')}"
                                for l in LAYERS]
                         for s, r in zip(short, runs)]))

    out.append("\n## Variação de registros entre execuções consecutivas\n")
    rows = []
    for i in range(1, len(runs)):
        rows.append([f"{short[i - 1]} → {short[i]}"] + [
            (layer_value(runs[i]["snapshot"], l, "rows") or 0)
            - (layer_value(runs[i - 1]["snapshot"], l, "rows") or 0) for l in LAYERS])
    out.append(md_table(["Transição"] + list(LAYERS), rows))
    last, prev = runs[-1]["snapshot"], runs[-2]["snapshot"] if len(runs) > 1 else None
    if prev:
        same = all(last[l] == prev[l] for l in LAYERS if "error" not in last[l])
        out.append("\nReexecução imediata (últimas duas execuções): "
                   + ("**nenhuma camada variou** em registros nem em duplicidade."
                      if same else "**houve variação** em ao menos uma camada (ver tabelas)."))

    if cfg["cutoff"]:
        out.append("\n## Linhas com data de referência posterior ao corte (lote incremental)\n")
        out.append(md_table(["Execução"] + list(LAYERS),
                            [[s] + [layer_value(r["snapshot"], l, "rows_after_cutoff")
                                    for l in LAYERS] for s, r in zip(short, runs)]))

    out.append("\n## Completude (% de valores não nulos)\n")
    lc, gc = quality["landing_completeness_pct"], quality["gold_completeness_pct"]
    out.append(md_table(["Coluna", "Landing", "Gold"],
                        [[c, lc.get(c), gc.get(c)] for c in OHLCV]
                        + [[f, "—", gc.get(f)] for f in GOLD_FEATURES]))

    v = quality["bronze_validity"]
    out.append("\n## Validade na Bronze (antes da Silver)\n")
    out.append(md_table(["Medida", "Valor"], [
        ["Pares (ticker, data) únicos na Bronze", v["rows"]],
        ["Com algum valor nulo em OHLCV", v["rows_with_null_ohlcv"]],
        ["Violando regra de preço/volume", v["rows_violating_price_volume_rule"]],
        ["Com data futura", v["rows_with_future_date"]],
        ["Violando qualquer regra", v["rows_violating_any_rule"]],
        ["% violando qualquer regra", v["violating_any_rule_pct"]],
    ]))

    first = runs[0]["snapshot"]
    out.append("\n## Retenção entre camadas após a carga inicial\n")
    b, s, g = (layer_value(first, l, "rows") for l in ("bronze", "silver", "gold"))
    out.append(md_table(["Etapa", "Entrada", "Saída", "Retenção (%)"], [
        ["Bronze → Silver", b, s, pct(s, b)],
        ["Silver → Gold", s, g, pct(g, s)],
    ]))

    if "gold_feature_nulls_by_batch" in quality:
        out.append("\n## Nulos nas features da Gold: carga inicial × lote incremental (%)\n")
        sp = quality["gold_feature_nulls_by_batch"]
        out.append(md_table(["Feature",
                             f"Carga inicial ({sp['initial_batch']['rows']} linhas)",
                             f"Lote incremental ({sp['incremental_batch']['rows']} linhas)"],
                            [[f, sp["initial_batch"]["feature_null_pct"][f],
                              sp["incremental_batch"]["feature_null_pct"][f]]
                             for f in GOLD_FEATURES]))

    out.append("\n## Gold por ticker\n")
    out.append(md_table(["Ticker", "Linhas", "Primeira data", "Última data"],
                        [[t["ticker"], t["rows"], t["first_date"], t["last_date"]]
                         for t in quality["gold_by_ticker"]]))
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS,
                    help="tickers sem '.SA' (padrão: %(default)s)")
    ap.add_argument("--db", default="stock_eval", help="banco de avaliação (stock_eval*)")
    ap.add_argument("--cutoff-days", type=int, default=7,
                    help="congela a fonte em hoje - N dias na carga inicial; 0 = sem corte")
    ap.add_argument("--no-reset", action="store_true", help="não recriar o banco")
    ap.add_argument("--out-dir", default="evaluation_results")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.FileHandler(out_dir / "pipeline.log", mode="w", encoding="utf-8"),
                  logging.StreamHandler()])
    for noisy in ("yfinance", "peewee", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    if not args.no_reset:
        print(f"Recriando o banco '{args.db}'...")
        reset_database(args.db)
    restrict_universe(args.tickers)

    cutoff = date.today() - timedelta(days=args.cutoff_days) if args.cutoff_days > 0 else None
    labels = (["Carga inicial (fonte congelada no corte)",
               "Incremental (dias novos desde o corte)",
               "Reexecução imediata (mesmo estado da fonte)"] if cutoff else
              ["Carga inicial", "Reexecução imediata (mesmo estado da fonte)"])

    runs: list[dict] = []
    original_download = None
    for i, label in enumerate(labels):
        print(f"\n=== Execução {i + 1}/{len(labels)}: {label} ===")
        if i == 0 and cutoff:
            original_download = freeze_source_at(cutoff)
        if i == 1 and cutoff and original_download:
            landing_layer._download_raw = original_download  # fonte volta ao estado real
        res = run_once(args.db)
        res["label"] = label
        res["snapshot"] = snapshot(args.db, cutoff)
        runs.append(res)
        print("Tempos (s):", res["timings"])
        print("Registros:", {l: res["snapshot"][l].get("rows") for l in LAYERS})
        if res["error"]:
            print("ERRO:", res["error"])

    quality = quality_profile(args.db, cutoff)
    try:
        import yfinance
        yf_version = yfinance.__version__
    except Exception:
        yf_version = "?"
    cfg = {
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tickers": [f"{t}.SA" for t in args.tickers], "db": args.db,
        "cutoff": str(cutoff) if cutoff else None,
        "python": platform.python_version(), "pandas": pd.__version__, "yfinance": yf_version,
    }
    (out_dir / "results.json").write_text(
        json.dumps({"config": cfg, "runs": runs, "quality": quality},
                   indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    report = build_report(cfg, runs, quality)
    (out_dir / "results.md").write_text(report, encoding="utf-8")
    print("\n" + report)
    print(f"Arquivos gravados em {out_dir.resolve()}: results.md, results.json, pipeline.log")


if __name__ == "__main__":
    main()
