import streamlit as st
import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
from sqlalchemy import create_engine
from dotenv import load_dotenv
from queries import (
    eventos_query,
    mopp_query,
    classificados_query,
    updated_query,
    rank_frota_query,
)
import pytz
import os
from bs4 import BeautifulSoup
from util import (
    calcular_tempo,
    reiniciar_ord,
    render_table_with_red_header,
    aplicar_filtragem,
)
import streamlit.components.v1 as components

# Database Connection
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
st.set_page_config(layout="wide")


@st.cache_resource
def connect_db():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@database.datalake.kmm.app.br:5430/datalake"
    engine = create_engine(url)
    return engine


def precisa_recarregar(nome):
    return (
        nome not in st.session_state
        or (datetime.now() - st.session_state[nome + "_last"]).total_seconds()
        > st.session_state[nome + "_ttl"]
    )


def carregar_silenciosamente(nome, loader_func, ttl):
    if precisa_recarregar(nome):
        st.session_state[nome] = loader_func()
        st.session_state[nome + "_last"] = datetime.now()
        st.session_state[nome + "_ttl"] = ttl


@st.cache_data(ttl=900)
def load_eventos(query):
    engine = connect_db()
    return pd.read_sql(query, engine)


@st.cache_data(ttl=600)
def load_rank(query):
    engine = connect_db()
    return pd.read_sql(query, engine)


@st.cache_data(ttl=6400)
def load_classificados(query):
    engine = connect_db()
    return pd.read_sql(query, engine)


@st.cache_data(ttl=3200)
def load_mopp(query):
    engine = connect_db()
    return pd.read_sql(query, engine)


@st.cache_data(ttl=900)
def load_updated(query):
    engine = connect_db()
    return pd.read_sql(query, engine)


carregar_silenciosamente("df_eventos", lambda: load_eventos(eventos_query), 900)
carregar_silenciosamente("df_rank", lambda: load_rank(rank_frota_query), 600)
carregar_silenciosamente(
    "df_classificados", lambda: load_classificados(classificados_query), 6400
)
carregar_silenciosamente("df_mopp", lambda: load_mopp(mopp_query), 3200)
carregar_silenciosamente("df_updated", lambda: load_updated(updated_query), 900)

df_rank = st.session_state.df_rank
df_classificados = st.session_state.df_classificados
df_mopp = st.session_state.df_mopp
df_updated = st.session_state.df_updated
df_eventos = st.session_state.df_eventos
# df_rank = load_rank(rank_frota_query)
# df_classificados = load_classificados(classificados_query)
# df_mopp = load_mopp(mopp_query)
# df_updated = load_updated(updated_query)
# df_eventos = load_eventos(eventos_query)

# Hide top white bar
last_update = df_updated.iloc[0, 0]
last_update_str = last_update.strftime("%d/%m/%Y %H:%M")

st.markdown(
    f"""
    <style>
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
        .stApp {{
            background-color: #1F3066;
            color: white;
        }}
        .stApp * {{
            color: white;
        }}
        html, body, .main {{
            height: 50%;
        }}
        .block-container {{
            display: block !important;  
            align-items: flex-start !important;  
            justify-content: flex-start !important;
            height: 50%;
            flex-direction: column;
            text-align: center;
            padding-top: 0px !important;
            margin-top: 0px !important;
        }}
        h2 {{
            margin-top: 0px !important;
            padding-top: 0px !important;
        }}
        .streamlit-expanderHeader {{
            padding-top: 0px !important;
            margin-top: 0px !important;
        }}
        .logo-overlay {{
            position: fixed;
            top: 50px;
            left: 170px;
            z-index: 9999;
            width: 250px; 
            height: auto;
            padding: 4px;
            border-radius: 15px;
        }}
        .last-update-box {{
            position: fixed;
            top: 10px;
            right: 10px;
            font-weight: bold;
            font-size: 16px;
            color: rgba(255,255,255,0.35);
            padding: 5px 10px;
            border-radius: 5px;
            z-index: 9999;
        }}
    </style>

    <img class="logo-overlay" src="https://i.imgur.com/aYVcHeE.png" />
    <div class="last-update-box">DADOS ATUALIZADOS EM {last_update_str}</div>
""",
    unsafe_allow_html=True,
)
st.markdown(
    """
    <style>
    .marquee {
      width: 300px; /* ou ajuste conforme a sua tabela */
      overflow: hidden;
      white-space: nowrap;
      box-sizing: border-box;
    }

    .marquee span {
      display: inline-block;
      padding-left: 5%;
      animation: marquee 5s linear infinite alternate;
    }

    @keyframes marquee {
      0%   { transform: translateX(10%); }
      100% { transform: translateX(-20%); }
    }
    
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    f"""
    <style>
        .custom-image {{
            height: 700px;  /* 80% da altura da viewport */
            object-fit: fill;  /* Mantém a proporção sem distorcer a imagem */
            width: 1300px;  /* Ajusta a largura da imagem ao container */
        }}
    </style>
""",
    unsafe_allow_html=True,
)


# STATUS_CARRETA Column
def calcular_status_carreta(df):
    df = df.copy()
    df["DATA"] = pd.to_datetime(df["DATA"])
    df.sort_values(by=["PLACA_REFERENCIA", "DATA"], inplace=True)
    df = df.drop_duplicates(subset=["PLACA_REFERENCIA", "DATA"])
    status_df = df[df["EVENTO"].isin(["Carregado", "Vazio"])][
        ["PLACA_REFERENCIA", "DATA", "EVENTO"]
    ].copy()
    status_df = status_df.rename(columns={"EVENTO": "STATUS_CARRETA"})
    status_df.set_index(["PLACA_REFERENCIA", "DATA"], inplace=True)

    df.set_index(["PLACA_REFERENCIA", "DATA"], inplace=True)
    df["STATUS_CARRETA"] = status_df["STATUS_CARRETA"]
    df["STATUS_CARRETA"] = df.groupby(level=0)["STATUS_CARRETA"].ffill()
    mask = df["EVENTO"].isin(["Carregado", "Vazio"])
    df.loc[mask, "STATUS_CARRETA"] = df.groupby(level=0)["STATUS_CARRETA"].shift(1)
    df.reset_index(inplace=True)
    return df


# ULTIMOS_EVENTOS Table
def gerar_ultimos_eventos(df_eventos):
    condicao = ~(
        df_eventos["EVENTO"].isin(["Desengate", "Vazio", "Saída", "Recebimento"])
        & (df_eventos["STATUS_CARRETA"] != "Carregado")
    )
    eventos_filtrados = df_eventos[condicao]
    ultimos_eventos = eventos_filtrados.groupby(
        ["COD_PESSOA", "EVENTO"], as_index=False
    )["CONTROLE_EVO_ID"].max()
    return ultimos_eventos


# PESSOAS_EVENTO Table
def gerar_pessoas_evento(df_ultimos_eventos):
    pessoas = df_ultimos_eventos["COD_PESSOA"].dropna().unique()
    df_pessoas = pd.DataFrame({"COD_PESSOA": pessoas})
    eventos_grupo_1 = ["Engate", "Recebimento", "Carregado"]
    eventos_grupo_2 = ["Desengate", "Saída", "Vazio"]
    grupo_1 = (
        df_ultimos_eventos[df_ultimos_eventos["EVENTO"].isin(eventos_grupo_1)]
        .groupby("COD_PESSOA", as_index=False)["CONTROLE_EVO_ID"]
        .max()
        .rename(columns={"CONTROLE_EVO_ID": "ENGATE_RECEBIMENTO_CARREGADO"})
    )
    grupo_2 = (
        df_ultimos_eventos[df_ultimos_eventos["EVENTO"].isin(eventos_grupo_2)]
        .groupby("COD_PESSOA", as_index=False)["CONTROLE_EVO_ID"]
        .max()
        .rename(columns={"CONTROLE_EVO_ID": "DESENGATE_VAZIO_SAIDA"})
    )
    df_pessoas_evento = df_pessoas.merge(grupo_1, on="COD_PESSOA", how="left").merge(
        grupo_2, on="COD_PESSOA", how="left"
    )
    return df_pessoas_evento


df_eventos = calcular_status_carreta(df_eventos)
df_ultimos_eventos = gerar_ultimos_eventos(df_eventos)
df_ultimos_eventos = df_ultimos_eventos.merge(
    df_eventos[
        [
            "CONTROLE_EVO_ID",
            "MUNICIPIO",
            "UF_PROV",
            "PAIS",
            "PLACA",
            "PLACA_REFERENCIA",
            "RAZAO_SOCIAL",
            "DATA",
            "STATUS_CARRETA",
        ]
    ],
    on="CONTROLE_EVO_ID",
    how="left",
)
df_pessoas_evento = gerar_pessoas_evento(df_ultimos_eventos)
df_data_dvs = df_ultimos_eventos[
    [
        "CONTROLE_EVO_ID",
        "DATA",
        "PLACA",
        "MUNICIPIO",
        "UF_PROV",
        "PAIS",
        "PLACA_REFERENCIA",
        "EVENTO",
        "RAZAO_SOCIAL",
    ]
].rename(columns={"CONTROLE_EVO_ID": "DESENGATE_VAZIO_SAIDA", "DATA": "DATA_DVS"})
df_pessoas_evento = df_pessoas_evento.merge(
    df_data_dvs, on="DESENGATE_VAZIO_SAIDA", how="left"
)
df_data_erc = df_ultimos_eventos[["CONTROLE_EVO_ID", "DATA"]].rename(
    columns={"CONTROLE_EVO_ID": "ENGATE_RECEBIMENTO_CARREGADO", "DATA": "DATA_ERC"}
)
df_pessoas_evento = df_pessoas_evento.merge(
    df_data_erc, on="ENGATE_RECEBIMENTO_CARREGADO", how="left"
)
df_pessoas_evento["TEMPO"] = df_pessoas_evento["DATA_DVS"].apply(calcular_tempo)
df_rank_filtrado = df_rank[df_rank["COM_MOTORISTA"] == "Sim"]
df_rank_filtrado = df_rank_filtrado[
    [
        "NOME_MOTORISTA",
        "PLACA_CONTROLE",
        "STATUS",
        "PLACA_REFERENCIA",
        "ENGATADA",
        "MUNICIPIO_ATUAL",
        "UFP_ATUAL",
        "PAIS_ATUAL",
    ]
].rename(
    columns={
        "NOME_MOTORISTA": "RAZAO_SOCIAL",
        "PLACA_CONTROLE": "ULTIMA_PLACA",
        "STATUS": "SITUACAO",
        "PLACA_REFERENCIA": "CARRETA",
    }
)
df_pessoas_evento = df_pessoas_evento.merge(
    df_rank_filtrado, on="RAZAO_SOCIAL", how="left"
)

df_pessoas_evento = df_pessoas_evento.merge(
    df_classificados, left_on="CARRETA", right_on="PLACA", how="left"
)
df_pessoas_evento = df_pessoas_evento.rename(
    columns={
        "TIPO_CARRETA": "TIPO_CARRETA_REAL",
        "CLASSIFICADO": "CLASSIFICADO_CARRETA",
    }
)
df_pessoas_evento = df_pessoas_evento.merge(
    df_classificados, left_on="ULTIMA_PLACA", right_on="PLACA", how="left"
)
df_pessoas_evento = df_pessoas_evento.rename(
    columns={"TIPO_CARRETA": "TIPO_CAVALO", "CLASSIFICADO": "CLASSIFICADO_CAVALO"}
)
df_pessoas_evento = df_pessoas_evento.merge(df_mopp, on="COD_PESSOA", how="left")
df_pessoas_evento["DATA_DVS"] = df_pessoas_evento["DATA_DVS"].fillna(
    pd.Timestamp("1900-11-11 11:11:11")
)
df_pessoas_evento["DATA_ERC"] = df_pessoas_evento["DATA_ERC"].fillna(
    pd.Timestamp("1900-11-11 11:11:11")
)
df_pessoas_evento["STATUS_FILA"] = np.select(
    [
        df_pessoas_evento["DATA_DVS"] > df_pessoas_evento["DATA_ERC"],
        df_pessoas_evento["DATA_ERC"] > df_pessoas_evento["DATA_DVS"],
    ],
    ["Na Fila", "Em viagem"],
    default="Desconhecido",
)


def ordena_mun(df):
    df = df.copy()
    df["DATA_DVS"] = pd.to_datetime(df["DATA_DVS"])
    df.sort_values(by=["UFP_ATUAL", "DATA_DVS"], inplace=True)
    return df


df_pessoas_evento["LOCALIZACAO_ATUAL"] = (
    df_pessoas_evento["MUNICIPIO_ATUAL"].fillna("")
    + ", "
    + df_pessoas_evento["UFP_ATUAL"].fillna("")
    + " - "
    + df_pessoas_evento["PAIS_ATUAL"].fillna("")
)
df_pessoas_evento["LOCALIZACAO_EVENTO"] = (
    df_pessoas_evento["MUNICIPIO"].fillna("")
    + ", "
    + df_pessoas_evento["UF_PROV"].fillna("")
    + " - "
    + df_pessoas_evento["PAIS"].fillna("")
)
df_pessoas_evento["CLASS"] = df_pessoas_evento["TIPO_CARRETA_REAL"].fillna(
    ""
) + df_pessoas_evento["CLASSIFICADO_CARRETA"].fillna("")
df_pessoas_evento["ULTIMA_PLACA"] = np.where(
    df_pessoas_evento["ULTIMA_PLACA"] == df_pessoas_evento["PLACA_x"],
    df_pessoas_evento["ULTIMA_PLACA"],
    "(Anterior: "
    + df_pessoas_evento["PLACA_x"]
    + ") "
    + df_pessoas_evento["ULTIMA_PLACA"],
)
df_pessoas_evento_filtrado = df_pessoas_evento[
    (df_pessoas_evento["STATUS_FILA"] == "Na Fila")
    & (df_pessoas_evento["ULTIMA_PLACA"].notnull())
    & (df_pessoas_evento["PAIS_ATUAL"] == "Brasil")
    & (df_pessoas_evento["UFP_ATUAL"] != "RS")
].copy()
df_pessoas_evento_filtrado = ordena_mun(df_pessoas_evento_filtrado)
countes = 1
df_pessoas_evento_filtrado["ORD"] = (
    df_pessoas_evento_filtrado["UFP_ATUAL"].fillna("")
    + " - "
    + (df_pessoas_evento_filtrado.groupby("UFP_ATUAL").cumcount() + 1).astype(str)
)

df_pessoas_evento_filtrado["TIPO_PLACA"] = df_pessoas_evento_filtrado.apply(
    lambda row: (
        "DESENGATADO"
        if pd.isna(row["TIPO_CARRETA_REAL"])
        else f'{row["TIPO_CARRETA_REAL"]} {row["ENGATADA"]}'
    ),
    axis=1,
)
df_pessoas_evento_filtrado["FILTRAGEM"] = df_pessoas_evento_filtrado.apply(
    aplicar_filtragem, axis=1
)
df_pessoas_evento_filtrado["CARRETA_CLASS_TABELA"] = (
    df_pessoas_evento_filtrado["CLASS"] + " - " + df_pessoas_evento_filtrado["CARRETA"]
)

icon = "☣️  "
df_pessoas_evento_filtrado["MOTORISTA_COM_ICONE"] = df_pessoas_evento_filtrado.apply(
    lambda row: (
        f"{icon}{row['RAZAO_SOCIAL']} "
        if pd.notnull(row.get("DATA_VENCIMENTO"))
        else row["RAZAO_SOCIAL"]
    ),
    axis=1,
)


def aplicar_marquee(df, coluna):
    df[coluna] = df[coluna].apply(
        lambda x: f"<div class='marquee'><span>{x}</span></div>"
    )
    return df


df_pessoas_evento_filtrado = df_pessoas_evento_filtrado.rename(
    columns={"RAZAO_SOCIAL": "MOTORISTA_OLD"}
)
df_pessoas_evento_filtrado["RAZAO_SOCIAL"] = df_pessoas_evento_filtrado[
    "MOTORISTA_COM_ICONE"
]
df_pessoas_evento_filtrado.drop(
    columns=["MOTORISTA_OLD", "MOTORISTA_COM_ICONE"], inplace=True, errors="ignore"
)
df_pessoas_evento_filtrado["CARRETA_CLASS_TABELA"] = df_pessoas_evento_filtrado[
    "CARRETA_CLASS_TABELA"
].fillna("DESENGATADO")
df_pessoas_evento_filtrado = df_pessoas_evento_filtrado[
    [
        "ORD",
        "UFP_ATUAL",
        "RAZAO_SOCIAL",
        "EVENTO",
        "DATA_DVS",
        "LOCALIZACAO_EVENTO",
        "TEMPO",
        "ULTIMA_PLACA",
        "LOCALIZACAO_ATUAL",
        "CARRETA_CLASS_TABELA",
        "FILTRAGEM",
    ]
]
df_pessoas_evento_filtrado = df_pessoas_evento_filtrado.rename(
    columns={
        "ORD": "ORDEM",
        "UFP_ATUAL": "ESTADO",
        "RAZAO_SOCIAL": "MOTORISTA",
        "EVENTO": "ÚLTIMO EVENTO",
        "DATA_DVS": "DATA EVENTO",
        "LOCALIZACAO_EVENTO": "LOCAL EVENTO",
        "TEMPO": "TEMPO NA FILA",
        "ULTIMA_PLACA": "CAVALO ATUAL",
        "LOCALIZACAO_ATUAL": "LOCAL ATUAL",
        "CARRETA_CLASS_TABELA": "CLASSIFICAÇÃO / CARRETA",
        "FILTRAGEM": "FILTRAGEM",
    }
)
df_pessoas_evento_filtrado["DATA EVENTO"] = df_pessoas_evento_filtrado[
    "DATA EVENTO"
].dt.strftime("%d/%m/%Y %H:%M")
df_sdc = df_pessoas_evento_filtrado[
    df_pessoas_evento_filtrado["FILTRAGEM"] == "Sider Class"
].drop(columns=["FILTRAGEM"])
df_sdd = df_pessoas_evento_filtrado[
    df_pessoas_evento_filtrado["FILTRAGEM"].isin(["Sider Div", "DESENGATADO"])
].drop(columns=["FILTRAGEM"])
df_bau = df_pessoas_evento_filtrado[
    df_pessoas_evento_filtrado["FILTRAGEM"] == "Bau"
].drop(columns=["FILTRAGEM"])
df_rodo = df_pessoas_evento_filtrado[
    df_pessoas_evento_filtrado["FILTRAGEM"] == "Rodo"
].drop(columns=["FILTRAGEM"])
df_outros = df_pessoas_evento_filtrado[
    df_pessoas_evento_filtrado["FILTRAGEM"].notna()
].drop(columns=["FILTRAGEM"])
df_sdc = reiniciar_ord(df_sdc)
df_sdd = reiniciar_ord(df_sdd)
df_bau = reiniciar_ord(df_bau)
df_rodo = reiniciar_ord(df_rodo)
df_sdc = aplicar_marquee(df_sdc, "MOTORISTA")
df_sdd = aplicar_marquee(df_sdd, "MOTORISTA")
df_bau = aplicar_marquee(df_bau, "MOTORISTA")
df_rodo = aplicar_marquee(df_rodo, "MOTORISTA")
dfs = [df_sdc, df_sdd, df_bau, df_rodo, df_outros]
nomes = [
    "SIDER CLASSIFICADO",
    "SIDER DIVERSOS",
    "BAÚ CLASSIFICADO / DIVERSOS",
    "BITREM CLASSIFICADO / DIVERSOS",
    "AVISOS",
]

slideshow_images = ["ACERTO.png"]
slideshow_duration = len(slideshow_images)
avisos_index = len(nomes) - 1
count = st_autorefresh(interval=30000, limit=None, key="auto_refresh")
dfs_validos = []
nomes_validos = []
for df_, nome_ in zip(dfs, nomes):
    if not df_.empty:
        dfs_validos.append(df_)
        nomes_validos.append(nome_)

main_loop_len = len(dfs_validos) - (1 if "AVISOS" in nomes_validos else 0)
slideshow_phase = count % (main_loop_len + slideshow_duration)

if "AVISOS" in nomes_validos and slideshow_phase >= main_loop_len:
    img_index = slideshow_phase - main_loop_len
    st.markdown(f"<h2 style='font-size:10px;'></h2>", unsafe_allow_html=True)
    st.image(slideshow_images[img_index], use_container_width=True)
else:
    index = slideshow_phase % len(dfs_validos)
    nome = nomes_validos[index]
    if nome != "AVISOS":
        st.markdown(f"<h2 style='font-size:58px;'>{nome}</h2>", unsafe_allow_html=True)
        render_table_with_red_header(dfs_validos[index])
    else:
        pass

components.html("""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <title>Vídeo Aleatório – Transporte Rodoviário</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <style>
    /* Faz o iframe ocupar toda a tela */
    html, body {
      margin: 0;
      padding: 0;
      height: 100%;
      background-color: #000;
      opacity:1%;
    }
    #player {
      position: absolute;
      top: 0;
      left: 0;
      width: 10%;
      height: 10%;
      border: none;
      opacity:1%;
    }
    /* Mensagem exibida antes de carregar o vídeo */
    #loading {
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      color: #fff;
      font-family: sans-serif;
      font-size: 1.2em;
    }
  </style>
</head>
<body>
  <div id="loading">Carregando vídeo...</div>
  <iframe
    id="player"
    allow="autoplay; encrypted-media"
    allowfullscreen
  ></iframe>

  <script>
    // Lista de IDs de vídeos do YouTube sobre transporte rodoviário
    const videos = [
      "1dPvL0TQ5rY"
    ];

    // Escolhe aleatoriamente um índice dentro do array
    const aleatorio = Math.floor(Math.random() * videos.length);
    const videoId = videos[aleatorio];

    // Monta a URL de embed (autoplay e muted para permitir reprodução automática em Smart TV)
    const embedURL = `https://www.youtube.com/embed/${videoId}?autoplay=1&mute=1&rel=0&controls=1`;

    // Ao carregar o script, define o src do iframe e remove a mensagem de "Carregando"
    const iframe = document.getElementById("player");
    iframe.src = embedURL;

    // Quando o iframe carregar, esconde o texto de carregamento
    iframe.addEventListener("load", () => {
      const loadDiv = document.getElementById("loading");
      if (loadDiv) loadDiv.style.display = "none";
    });
  </script>
</body>
</html>""", height=10)