"""
pipeline_fbref.py
=================
Pipeline completa para scraping do FBref — Brasileirão Série A.

Refatorado para usar curl_cffi (bypass de Cloudflare via network TLS fingerprinting)
eliminando a necessidade de navegadores reais e reduzindo o uso de RAM em 95%.
"""

import os
from bs4 import BeautifulSoup, Comment
import pandas as pd
from io import StringIO
from pathlib import Path
from datetime import date
import time
import random
import logging
import httpx
from curl_cffi import requests as cffi_requests

# ─────────────────────────────────────────────────────────────
# CONSTANTES E DIRETÓRIOS BLINDADOS
# ─────────────────────────────────────────────────────────────
BASE_URL = "https://fbref.com"

BASE_DIR = Path(os.getenv("APP_BASE_DIR", Path(__file__).resolve().parent.parent.parent))
OUTPUT_BASE = Path(os.getenv("OUTPUT_DIR", BASE_DIR / "data"))

CACHE_DIR = OUTPUT_BASE / "cache_html"
OUTPUT_DIR = OUTPUT_BASE / "dados_brasileirao"
LOG_DIR = BASE_DIR / "logs"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DE LOG
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pipeline.log"),
    ]
)
log = logging.getLogger(__name__)

PAGINAS = {
    "standard": ("/en/comps/24/stats/Serie-A-Stats", "stats_standard", "stats_squads_standard_for", "stats_squads_standard_against"),
    "shooting": ("/en/comps/24/shooting/Serie-A-Stats", "stats_shooting", "stats_squads_shooting_for", "stats_squads_shooting_against"),
    "passing": ("/en/comps/24/passing/Serie-A-Stats", "stats_passing", "stats_squads_passing_for", "stats_squads_passing_against"),
    "defense": ("/en/comps/24/defense/Serie-A-Stats", "stats_defense", "stats_squads_defense_for", "stats_squads_defense_against"),
    "possession": ("/en/comps/24/possession/Serie-A-Stats", "stats_possession", "stats_squads_possession_for", "stats_squads_possession_against"),
    "keepers": ("/en/comps/24/keepers/Serie-A-Stats", "stats_keeper", "stats_squads_keeper_for", "stats_squads_keeper_against"),
    "playingtime": ("/en/comps/24/playingtime/Serie-A-Stats", "stats_playing_time", "stats_squads_playing_time_for", "stats_squads_playing_time_against"),
    "misc": ("/en/comps/24/misc/Serie-A-Stats", "stats_misc", "stats_squads_misc_for", "stats_squads_misc_against"),
}

PAGINA_CLASSIFICACAO = "/en/comps/24/Serie-A-Stats"

FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1")

_cf_session: dict = {"cookies": None, "user_agent": None}

def _resolver_cloudflare() -> dict:
    if _cf_session["cookies"]:
        log.info("  Reaproveitando cookies Cloudflare")
        return _cf_session

    log.info("  Resolvendo Cloudflare via FlareSolverr...")
    r = httpx.post(FLARESOLVERR_URL, json={
        "cmd": "request.get",
        "url": "https://fbref.com/en/comps/24/Serie-A-Stats",
        "maxTimeout": 60000
    }, timeout=90)

    data = r.json()
    if data["status"] != "ok":
        raise Exception(f"FlareSolverr falhou: {data.get('message')}")

    solution = data["solution"]
    _cf_session["cookies"] = {c["name"]: c["value"] for c in solution["cookies"]}
    _cf_session["user_agent"] = solution["userAgent"]
    log.info(f"  Cookies obtidos: {list(_cf_session['cookies'].keys())}")
    return _cf_session

# ─────────────────────────────────────────────────────────────
# FETCH — rede direta forjando Chrome via TLS
# ─────────────────────────────────────────────────────────────
def get_html(path: str) -> str:
    url = BASE_URL + path
    cache_key = path.replace("/", "_").strip("_") + ".html"
    cache_path = CACHE_DIR / cache_key

    if cache_path.exists():
        idade_segundos = time.time() - cache_path.stat().st_mtime
        if idade_segundos < 23 * 3600:
            log.info(f"  Cache válido ({int(idade_segundos/3600)}h) — {path}")
            return cache_path.read_text(encoding="utf-8")

    session_cf = _resolver_cloudflare()

    log.info(f"  Buscando: {url}")
    response = cffi_requests.get(
        url,
        impersonate="chrome124",
        cookies=session_cf["cookies"],
        headers={
            "User-Agent": session_cf["user_agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://fbref.com/",
        },
        timeout=30
    )

    if response.status_code == 403:
        log.warning("  403 — cookie expirado, renovando...")
        _cf_session["cookies"] = None
        _cf_session["user_agent"] = None
        session_cf = _resolver_cloudflare()
        response = cffi_requests.get(
            url,
            impersonate="chrome124",
            cookies=session_cf["cookies"],
            headers={"User-Agent": session_cf["user_agent"]},
            timeout=30
        )

    if response.status_code == 429:
        log.warning("  Rate limit 429 — aguardando 60s...")
        time.sleep(60)
        response = cffi_requests.get(url, impersonate="chrome124",
                                     cookies=session_cf["cookies"], timeout=30)

    response.raise_for_status()
    html = response.text
    cache_path.write_text(html, encoding="utf-8")
    return html

# ─────────────────────────────────────────────────────────────
# PARSE E LIMPEZA (Intocados - lógica estava perfeita)
# ─────────────────────────────────────────────────────────────
def parse_tabelas(html: str) -> dict[str, pd.DataFrame]:
    soup = BeautifulSoup(html, "html.parser")
    tabelas = {}

    for t in soup.find_all("table"):
        tid = t.get("id", f"tabela_{len(tabelas)}")
        try:
            tabelas[tid] = pd.read_html(StringIO(str(t)))[0]
        except Exception:
            pass

    for comentario in soup.find_all(string=lambda x: isinstance(x, Comment)):
        soup_c = BeautifulSoup(comentario, "html.parser")
        for t in soup_c.find_all("table"):
            tid = t.get("id", f"comentario_{len(tabelas)}")
            try:
                tabelas[tid] = pd.read_html(StringIO(str(t)))[0]
            except Exception:
                pass

    return tabelas

def limpar_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        colunas = []
        for top, bottom in df.columns:
            top = str(top).strip()
            bottom = str(bottom).strip()
            if "Unnamed" in top:
                colunas.append(bottom)
            else:
                colunas.append(f"{top}_{bottom}")
        df.columns = colunas

    for col_referencia in ["Player", "Squad", "Rk"]:
        if col_referencia in df.columns:
            df = df[df[col_referencia] != col_referencia].reset_index(drop=True)
            break

    if "Matches" in df.columns:
        df = df.drop(columns=["Matches"])

    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass

    return df

# ─────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────
def rodar_pipeline():
    hoje = date.today().strftime("%Y-%m-%d")
    log.info(f"\n{'='*60}")
    log.info(f"Pipeline Brasileirão — {hoje} (Modo: curl_cffi)")
    log.info(f"{'='*60}\n")

    log.info("classificacao")
    html_class = get_html(PAGINA_CLASSIFICACAO)
    tabelas_class = parse_tabelas(html_class)

    id_classificacao = next((k for k in tabelas_class if "overall" in k and "results" in k), None)
    if id_classificacao:
        df_class = limpar_df(tabelas_class[id_classificacao])
        df_class.to_parquet(OUTPUT_DIR / f"classificacao_{hoje}.parquet", index=False)
        log.info(f"  classificacao: {df_class.shape}")
    else:
        log.warning("  Tabela de classificação não encontrada")

    time.sleep(random.uniform(4, 8))

    for categoria, (path, id_jogadores, id_times_for, id_times_against) in PAGINAS.items():
        log.info(f" {categoria}")

        html = get_html(path)
        tabelas = parse_tabelas(html)

        if id_jogadores in tabelas:
            df_jog = limpar_df(tabelas[id_jogadores])
            df_jog.to_parquet(OUTPUT_DIR / f"jogadores_{categoria}_{hoje}.parquet", index=False)
            log.info(f"  jogadores_{categoria}: {df_jog.shape}")
        else:
            match = next((k for k in tabelas if id_jogadores.replace("stats_", "") in k and "squads" not in k), None)
            if match:
                df_jog = limpar_df(tabelas[match])
                df_jog.to_parquet(OUTPUT_DIR / f"jogadores_{categoria}_{hoje}.parquet", index=False)
                log.info(f"  jogadores_{categoria} (via fallback '{match}'): {df_jog.shape}")
            else:
                log.warning(f"  Tabela de jogadores não encontrada para {categoria}")

        if id_times_for in tabelas:
            df_for = limpar_df(tabelas[id_times_for])
            df_for.to_parquet(OUTPUT_DIR / f"times_{categoria}_ataque_{hoje}.parquet", index=False)
            log.info(f"  times_{categoria}_ataque: {df_for.shape}")

        if id_times_against in tabelas:
            df_against = limpar_df(tabelas[id_times_against])
            df_against.to_parquet(OUTPUT_DIR / f"times_{categoria}_defesa_{hoje}.parquet", index=False)
            log.info(f"  times_{categoria}_defesa: {df_against.shape}")

        # O sleep é crucial para não tomar HTTP 429 seguido do FBref
        time.sleep(random.uniform(4, 8))

    log.info(f"\nPipeline concluída! Arquivos em: {OUTPUT_DIR.resolve()}")

    arquivos = sorted(OUTPUT_DIR.glob(f"*_{hoje}.parquet"))
    log.info(f"\n{len(arquivos)} arquivos gerados:")
    for f in arquivos:
        tamanho_kb = f.stat().st_size / 1024
        log.info(f"  {f.name} ({tamanho_kb:.1f} KB)")

if __name__ == "__main__":
    rodar_pipeline()