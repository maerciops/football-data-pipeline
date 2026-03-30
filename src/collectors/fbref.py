"""
pipeline_fbref.py
=================
Pipeline completa para scraping do FBref — Brasileirão Série A.

Coleta dados de jogadores e times em todas as categorias disponíveis:
padrões, chutes, passes, defesas, posse de bola, goleiros, tempo jogado e diversos.

Uso:
    python fbref.py
"""

# ─────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────
import os
os.environ['WDM_LOG'] = '0'  # Suprime log bugado do webdriver_manager

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup, Comment

import pandas as pd
from io import StringIO
from pathlib import Path
from datetime import date
import time
import random
import logging

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DE LOG
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),                          # imprime no terminal
        logging.FileHandler("logs/pipeline.log"),        # salva em arquivo
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────
BASE_URL   = "https://fbref.com"
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CACHE_DIR = BASE_DIR / "data" / "cache_html"
OUTPUT_DIR = BASE_DIR / "data" / "dados_brasileirao"

CACHE_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# Cada página do FBref tem tabelas de jogadores E times.
# O FBref organiza assim:
#   - Tabelas de times: stats_squads_<categoria>_for / _against
#   - Tabela de jogadores: stats_<categoria>
#
# Mapeamento: nome_amigavel -> (path_da_url, id_tabela_jogadores, id_tabela_times_for, id_tabela_times_against)
PAGINAS = {
    "standard": (
        "/en/comps/24/stats/Serie-A-Stats",
        "stats_standard",
        "stats_squads_standard_for",
        "stats_squads_standard_against",
    ),
    "shooting": (
        "/en/comps/24/shooting/Serie-A-Stats",
        "stats_shooting",
        "stats_squads_shooting_for",
        "stats_squads_shooting_against",
    ),
    "passing": (
        "/en/comps/24/passing/Serie-A-Stats",
        "stats_passing",
        "stats_squads_passing_for",
        "stats_squads_passing_against",
    ),
    "defense": (
        "/en/comps/24/defense/Serie-A-Stats",
        "stats_defense",
        "stats_squads_defense_for",
        "stats_squads_defense_against",
    ),
    "possession": (
        "/en/comps/24/possession/Serie-A-Stats",
        "stats_possession",
        "stats_squads_possession_for",
        "stats_squads_possession_against",
    ),
    "keepers": (
        "/en/comps/24/keepers/Serie-A-Stats",
        "stats_keeper",
        "stats_squads_keeper_for",
        "stats_squads_keeper_against",
    ),
    "playingtime": (
        "/en/comps/24/playingtime/Serie-A-Stats",
        "stats_playing_time",
        "stats_squads_playing_time_for",
        "stats_squads_playing_time_against",
    ),
    "misc": (
        "/en/comps/24/misc/Serie-A-Stats",
        "stats_misc",
        "stats_squads_misc_for",
        "stats_squads_misc_against",
    ),
}

# Página de classificação — separada pois tem estrutura diferente
PAGINA_CLASSIFICACAO = "/en/comps/24/Serie-A-Stats"


# ─────────────────────────────────────────────────────────────
# SELENIUM — cria o driver do Chromium
# ─────────────────────────────────────────────────────────────
def criar_driver() -> webdriver.Chrome:

    options = Options()
    options.binary_location = "/usr/bin/chromium-browser"
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--user-data-dir=/home/dev/.config/chromium")

    return webdriver.Chrome(
        service=Service("/usr/bin/chromedriver"),
        options=options,
    )


# ─────────────────────────────────────────────────────────────
# FETCH — busca o HTML com cache de 23h
# ─────────────────────────────────────────────────────────────
def get_html(path: str) -> str:
    """
    Busca o HTML de uma página do FBref.

    Estratégia de cache:
    - Se o HTML já foi buscado há menos de 23h, usa o arquivo em disco.
    - Caso contrário, abre o Chromium, aguarda o Cloudflare resolver,
      e salva o HTML em disco para as próximas chamadas.

    Args:
        path: Caminho relativo da URL, ex: "/en/comps/24/stats/Serie-A-Stats"

    Returns:
        HTML completo da página como string.
    """
    url = BASE_URL + path

    cache_key = path.replace("/", "_").strip("_") + ".html"
    cache_path = CACHE_DIR / cache_key

    # Verifica se o cache ainda é válido (menos de 23h)
    if cache_path.exists():
        idade_segundos = time.time() - cache_path.stat().st_mtime
        if idade_segundos < 23 * 3600:
            log.info(f"  Cache válido ({int(idade_segundos/3600)}h) — {path}")
            return cache_path.read_text(encoding="utf-8")

    log.info(f"  Buscando: {url}")
    driver = criar_driver()

    try:
        driver.get(url)

        for i in range(30):
            time.sleep(1)
            titulo = driver.title
            n_tabelas = driver.page_source.count("<table")
            cloudflare_ativo = "Just a moment" in titulo or "Um momento" in titulo

            if not cloudflare_ativo and n_tabelas > 0:
                log.info(f"Carregou em {i+1}s — {n_tabelas} tabelas")
                break
        else:
            raise TimeoutError(f"Página não carregou em 30s: {url}")

        html = driver.page_source
        cache_path.write_text(html, encoding="utf-8")
        return html

    finally:
        driver.quit()


# ─────────────────────────────────────────────────────────────
# PARSE — extrai tabelas do HTML
# ─────────────────────────────────────────────────────────────
def parse_tabelas(html: str) -> dict[str, pd.DataFrame]:
    """
    Extrai todas as tabelas de um HTML do FBref.

    Detalhe importante: o FBref esconde as tabelas de stats avançadas
    dentro de comentários HTML (<!-- -->). O BeautifulSoup sozinho não
    as encontra — é preciso processar os comentários explicitamente.

    Returns:
        Dicionário {table_id: DataFrame} com todas as tabelas encontradas.
    """
    soup = BeautifulSoup(html, "html.parser")
    tabelas = {}

    # 1. Tabelas visíveis normalmente no HTML
    for t in soup.find_all("table"):
        tid = t.get("id", f"tabela_{len(tabelas)}")
        try:
            tabelas[tid] = pd.read_html(StringIO(str(t)))[0]
        except Exception:
            pass

    # 2. Tabelas dentro de comentários HTML (stats avançadas do FBref)
    for comentario in soup.find_all(string=lambda x: isinstance(x, Comment)):
        soup_c = BeautifulSoup(comentario, "html.parser")
        for t in soup_c.find_all("table"):
            tid = t.get("id", f"comentario_{len(tabelas)}")
            try:
                tabelas[tid] = pd.read_html(StringIO(str(t)))[0]
            except Exception:
                pass

    return tabelas


# ─────────────────────────────────────────────────────────────
# LIMPEZA — normaliza os DataFrames do FBref
# ─────────────────────────────────────────────────────────────
def limpar_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza um DataFrame bruto do FBref.

    Returns:
        DataFrame limpo e pronto para uso.
    """
    # 1. Deixa as multi-level columns no mesmo nivel
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

    # 2. Remove linhas que repetem o cabeçalho
    for col_referencia in ["Player", "Squad", "Rk"]:
        if col_referencia in df.columns:
            df = df[df[col_referencia] != col_referencia].reset_index(drop=True)
            break

    # 3. Remove coluna de links inútil
    if "Matches" in df.columns:
        df = df.drop(columns=["Matches"])

    # 4. Converte colunas numéricas
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
    log.info(f"Pipeline Brasileirão — {hoje}")
    log.info(f"{'='*60}\n")

    # ── 1. Classificação geral ────────────────────────────────
    log.info("classificacao")
    html_class = get_html(PAGINA_CLASSIFICACAO)
    tabelas_class = parse_tabelas(html_class)

    # Busca dinamicamente pelo padrão em vez de hardcodar o ID
    id_classificacao = next(
        (k for k in tabelas_class if "overall" in k and "results" in k), None
    )
    if id_classificacao:
        df_class = limpar_df(tabelas_class[id_classificacao])
        df_class.to_parquet(OUTPUT_DIR / f"classificacao_{hoje}.parquet", index=False)
        log.info(f"  classificacao: {df_class.shape}")
    else:
        log.warning("  Tabela de classificação não encontrada")

    # Delay entre requests
    time.sleep(random.uniform(4, 8))

    # ── 2. Categorias de stats ────────────────────────────────
    for categoria, (path, id_jogadores, id_times_for, id_times_against) in PAGINAS.items():
        log.info(f" {categoria}")

        html = get_html(path)
        tabelas = parse_tabelas(html)

        # DataFrame de jogadores
        if id_jogadores in tabelas:
            df_jog = limpar_df(tabelas[id_jogadores])
            df_jog.to_parquet(
                OUTPUT_DIR / f"jogadores_{categoria}_{hoje}.parquet", index=False
            )
            log.info(f"  jogadores_{categoria}: {df_jog.shape}")
        else:
            # Tenta buscar com padrão parcial (IDs podem variar)
            match = next((k for k in tabelas if id_jogadores.replace("stats_", "") in k
                         and "squads" not in k), None)
            if match:
                df_jog = limpar_df(tabelas[match])
                df_jog.to_parquet(
                    OUTPUT_DIR / f"jogadores_{categoria}_{hoje}.parquet", index=False
                )
                log.info(f"  jogadores_{categoria} (via fallback '{match}'): {df_jog.shape}")
            else:
                log.warning(f"  Tabela de jogadores não encontrada para {categoria}")
                log.warning(f"  Tabelas disponíveis: {list(tabelas.keys())}")

        # DataFrame de times — ataque (for)
        if id_times_for in tabelas:
            df_for = limpar_df(tabelas[id_times_for])
            df_for.to_parquet(
                OUTPUT_DIR / f"times_{categoria}_ataque_{hoje}.parquet", index=False
            )
            log.info(f"times_{categoria}_ataque: {df_for.shape}")
        else:
            log.warning(f"Tabela de times (ataque) não encontrada para {categoria}")

        # DataFrame de times — defesa (against)
        if id_times_against in tabelas:
            df_against = limpar_df(tabelas[id_times_against])
            df_against.to_parquet(
                OUTPUT_DIR / f"times_{categoria}_defesa_{hoje}.parquet", index=False
            )
            log.info(f"times_{categoria}_defesa: {df_against.shape}")
        else:
            log.warning(f"Tabela de times (defesa) não encontrada para {categoria}")

        # Delay entre páginas
        time.sleep(random.uniform(4, 8))

    log.info(f"\nPipeline concluída! Arquivos em: {OUTPUT_DIR.resolve()}")

    # Lista os arquivos gerados
    arquivos = sorted(OUTPUT_DIR.glob(f"*_{hoje}.parquet"))
    log.info(f"\n{len(arquivos)} arquivos gerados:")
    for f in arquivos:
        tamanho_kb = f.stat().st_size / 1024
        log.info(f"  {f.name} ({tamanho_kb:.1f} KB)")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    rodar_pipeline()