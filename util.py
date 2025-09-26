import streamlit as st
import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
from sqlalchemy import create_engine
from queries import eventos_query, mopp_query,classificados_query,rank_frota_query
import pytz
import os
from bs4 import BeautifulSoup

fuso_brasilia = pytz.timezone("America/Sao_Paulo")
agora = datetime.now(fuso_brasilia).replace(tzinfo=None)  # remove timezone

def calcular_tempo(data_dvs):
    if pd.isna(data_dvs):
        return None
    fuso_brasilia = pytz.timezone("America/Sao_Paulo")
    agora = datetime.now(fuso_brasilia).replace(tzinfo=None)
    delta = agora - data_dvs
    total_segundos = int(delta.total_seconds())
    dias = total_segundos // 86400
    horas = (total_segundos % 86400) // 3600
    minutos = (total_segundos % 3600) // 60
    return f"{dias} D, {horas} H, {minutos} M"

def reiniciar_ord(df):
    df = df.copy()
    df["ORD"] = df.groupby("ESTADO").cumcount() + 1
    df["ORDEM"] = df["ESTADO"] + " - " + df["ORD"].astype(str)
    df = df.drop(columns=["ORD"])  # remove a coluna ORD original
    return df



ufp_colors = {
    "PB": "#17244D",  
    "MG": "#0F1C2E",  
    "SC": "#5F4545",  
    "SP": "#2C3E50",  
    "RJ": "#556270",  
    "MT": "#37474F", 
    "PR": "#6B7177",  
}

def style_table(row):
    color = ufp_colors.get(row['ESTADO'], '#444')
    return [f'background-color: {color}; color: white; border: 1px solid #1F3066;' for _ in row]

def render_table_with_red_header(df):
    df = df.reset_index(drop=True)
    styled_html = (
        df.style
        .hide(axis="index")
        .set_table_styles([
            {'selector': 'table', 'props': [('border-collapse', 'collapse')]},
            {'selector': 'th, td', 'props': [('border', '2px solid #1F3066'), ('padding', '0px 6px'),('white-space', 'nowrap'),('overflow', 'hidden'),('text-overflow', 'ellipsis')]},
            {'selector': 'thead th', 'props': [('background-color', '#70050A'), ('color', 'white'), ('font-weight', 'bold'), ('font-size', '18px') ]},
            {'selector': 'tbody td', 'props': [('min-width', '170px'), ('max-width', '170px'), ('text-align', 'center'),('font-weight', 'bold'), ('font-size', '22px') ]},
            {'selector': 'th.col1', 'props': [('display', 'none')]},
            {'selector': 'td.col1', 'props': [('display', 'none')]},
            {'selector': 'td.col0', 'props': [('min-width', '70px')]},
            {'selector': 'th.col0', 'props': [('min-width', '70px')]},
            {'selector': 'th.col2', 'props': [('min-width', '300px')]},
            {'selector': 'td.col2', 'props': [('min-width', '300px'), ('text-align', 'left')]},
            {'selector': 'th.col3', 'props': [('min-width', '100px'),('white-space', 'wrap')]},
            {'selector': 'td.col3', 'props': [('min-width', '100px')]},
            {'selector': 'th.col4', 'props': [('font-size', '18px')]},
            {'selector': 'td.col4', 'props': [('font-size', '18px')]},
            {'selector': 'th.col5', 'props': [('font-size', '18px'),('min-width', '300px')]},
            {'selector': 'td.col5', 'props': [('font-size', '18px'),('min-width', '300px')]},
            {'selector': 'th.col7', 'props': [('min-width', '280px'),('overflow', 'visible'),('font-size', '19px')]},
            {'selector': 'td.col7', 'props': [('min-width', '280px'),('overflow', 'visible'),('font-size', '19px')]},
            {'selector': 'th.col8', 'props': [('min-width', '300px'),('font-size', '18px')]},
            {'selector': 'td.col8', 'props': [('min-width', '300px'),('font-size', '18px')]},
            {'selector': 'th.col9', 'props': [('min-width', '170px'),('white-space', 'wrap')]},
            {'selector': 'td.col9', 'props': [('overflow', 'visible'),('min-width', '170px')]},
        ], overwrite=False)
        .apply(style_table, axis=1)
        .to_html(index=False)
    )
    soup = BeautifulSoup(styled_html, 'html.parser')
    style_tag = soup.find('style')
    table_tag = soup.find('table')

    container_html = f"""
    <div style="width: 100%; display: flex; justify-content: center; margin: 0; padding: 0;">
        {str(style_tag) if style_tag else ''}
        {str(table_tag)}
    </div>
    """

    st.markdown(container_html, unsafe_allow_html=True)
    return container_html

def aplicar_filtragem(row):
    vencimento = row["DATA_VENCIMENTO"]
    cavalo = row["CLASSIFICADO_CAVALO"]
    carreta = row["CLASSIFICADO_CARRETA"]
    tipo_cr = row["TIPO_CARRETA_REAL"]

    habilitado = "Sim" if pd.notnull(vencimento) and cavalo == "C" and carreta == "C" else "Não"

    if tipo_cr == "SD":
        return "Sider Class" if habilitado == "Sim" else "Sider Div"
    elif tipo_cr == "BA":
        return "Bau"
    elif tipo_cr == "RO":
        return "Rodo"
    elif pd.isna(tipo_cr):
        return row["TIPO_PLACA"]
    return None