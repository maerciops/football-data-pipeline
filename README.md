# ⚽ Football Data Pipeline — Brasileirão 2024

Este projeto é uma pipeline de dados end-to-end focada em estatísticas avançadas do Campeonato Brasileiro (Série A). O objetivo é extrair, processar e visualizar métricas que vão além do placar, como **xG (Gols Sperados)**, **Passes Progressivos** e **Redes de Interação**.

---

## Arquitetura e Stack
O projeto foi desenhado para escalar de um script local para uma infraestrutura moderna de dados:

- **Extração:** Python com Selenium & BeautifulSoup (Bypass de Cloudflare/Anti-bot).
- **Armazenamento:** Arquivos colunares **Apache Parquet** (Preservação de Schema e Performance).
- **Orquestração:** Apache Airflow & Docker (Em breve).
- **Modelagem:** dbt & Apache Spark (Em breve).
- **Dashboard:** Streamlit (Em breve).

---

## Configuração do Ambiente

### 1. Dependências do Sistema
Para rodar o extrator (Selenium), você precisará do navegador e do driver instalados:
```bash
sudo apt-get install -y chromium-browser chromium-chromedriver
```

### 2. Dependências Python
Recomenda-se o uso de um ambiente virtual (venv):
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt 
```

## Como Funciona o Extrator (fbref.py)

### O script de extração foi construído com foco em resiliência:

1. Simulação Humana: Utiliza rotação de User-Agents e delays randômicos para evitar bloqueios.

2. Data Scraping: Captura dados escondidos em comentários HTML (estratégia do FBref para proteger dados avançados).

3. Conversão: Transforma HTML cru em DataFrames limpos via Pandas.

4. Persistência: Salva os dados na camada data/raw/ no formato .parquet.

## Estrutura do Projeto

/football-data-pipeline
├── /data                 # Armazenamento dos arquivos Parquet (Raw/Trusted)
├── /logs                 # Logs de execução da pipeline
├── fbref.py              # Script principal de extração (Selenium + BS4)
├── requirements.txt      # Dependências do projeto
└── .gitignore            # Proteção para dados e binários

## Próximos Passos

# [x] Extração robusta via Selenium (Fase 1).

# [ ] Dockerização do ambiente e script.

# [ ] Orquestração das coletas semanais via Apache Airflow.

# [ ] Modelagem dos dados com dbt para o Data Warehouse.

# Desenvolvido por Maercio Paulino de Sousa
Engenheiro de Software focando em Engenharia de Dados