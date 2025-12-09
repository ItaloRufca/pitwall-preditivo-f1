import streamlit as st
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.inference.predictor import F1Predictor
from dotenv import load_dotenv

# Load Env
load_dotenv()
BUCKET = os.environ.get('S3_BUCKET_NAME')

# Page Config
st.set_page_config(
    page_title="Pitwall Preditivo F1",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    /* Card Container */
    .driver-card {
        background-color: #262730;
        padding: 5px 15px; /* Compact vertical padding */
        border-radius: 8px;
        margin-bottom: 8px; /* Restored spacing */
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    
    /* Metrics clean up */
    [data-testid="stMetricValue"] {
        font-size: 1.5rem !important; /* Limit size */
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem;
    }
    
    /* Selection Button - Target only the small chart buttons if possible, 
       but here we just ensure 'secondary' buttons (like the chevron) don't break layout */
    div.stButton > button {
        /* Reset any aggressive width overrides */
        width: auto; 
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def load_predictor():
    model_path = os.path.join(os.getcwd(), 'notebooks', 'xgboost_f1_model.json')
    if not os.path.exists(model_path):
        st.error(f"Modelo não encontrado em {model_path}")
        return None
    return F1Predictor(model_path)

@st.cache_data
def load_data():
    try:
        race_path = f"s3://{BUCKET}/gold/gold_race_wt/"
        practice_path = f"s3://{BUCKET}/gold/gold_practice_wt/"
        driver_path = f"s3://{BUCKET}/gold/dim_driver/"
        
        df_race = pd.read_parquet(race_path)
        df_practice = pd.read_parquet(practice_path)
        df_driver = pd.read_parquet(driver_path)
        
        df_driver = df_driver[['meeting_key', 'driver_number', 'full_name', 'name_acronym', 'team_name', 'team_colour', 'headshot_url']].drop_duplicates()
        
        return df_race, df_practice, df_driver
    except Exception as e:
        st.error(f"Erro ao carregar dados do S3: {e}")
        return None, None, None

def format_time(seconds):
    if pd.isna(seconds) or seconds == 0:
        return "-"
    try:
        s = float(seconds)
        minutes = int(s // 60)
        rem_seconds = s % 60
        return f"{minutes}:{rem_seconds:06.3f}".replace('.', ',')
    except (ValueError, TypeError):
        return str(seconds)

def main():
    st.title("🏎️ Pitwall Preditivo F1")
    # st.markdown("### Simulador de Estratégia e Previsão")

    predictor = load_predictor()
    df_race, df_practice, df_driver = load_data()

    if predictor is None or df_race is None:
        st.warning("Por favor verifique se o modelo e os dados estão disponíveis.")
        return

    # Sidebar Selection
    st.sidebar.header("Seleção da Corrida")
    
    years = sorted(df_race['year'].unique(), reverse=True)
    selected_year = st.sidebar.selectbox("Ano", years)
    
    meetings = df_race[df_race['year'] == selected_year][['meeting_key', 'country_name', 'circuit_short_name']].drop_duplicates()
    meetings_formatted = {f"{row['country_name']} - {row['circuit_short_name']}": row['meeting_key'] for _, row in meetings.iterrows()}
    
    selected_meeting_label = st.sidebar.selectbox("Grande Prêmio", list(meetings_formatted.keys()))
    selected_meeting_key = meetings_formatted[selected_meeting_label]

    # Reset state if race changes
    if 'last_meeting_key' in st.session_state and st.session_state['last_meeting_key'] != selected_meeting_key:
        if 'last_results' in st.session_state:
            del st.session_state['last_results']
            st.session_state['selected_driver'] = None
            st.rerun()

    # Main Content - Adjusted Ratio for more space on right
    col1, col2 = st.columns([1.8, 1.2])

    with col1:
        st.subheader(f"🏁 Tabela de Classificação: {selected_meeting_label}")
        
        race_data = df_race[df_race['meeting_key'] == selected_meeting_key].copy()
        practice_data = df_practice[df_practice['meeting_key'] == selected_meeting_key].copy()
        
        # Action Bar in columns to prevent button wrapping weirdness
        ac1, ac2 = st.columns([0.4, 0.6])
        with ac1:
            prediction_triggered = st.button("Executar Previsão", type="primary", use_container_width=True)
        
        has_results = 'last_results' in st.session_state
        
        if prediction_triggered or has_results:
            if prediction_triggered:
                 with st.spinner("Calculando estratégia..."):
                    results = predictor.predict(race_data, practice_data)
                    results = pd.merge(results, df_driver, on=['meeting_key', 'driver_number'], how='left')
                    st.session_state['last_results'] = results
                    st.session_state['last_meeting_key'] = selected_meeting_key
                    st.session_state['selected_driver'] = None 
            
            results = st.session_state['last_results']
            
            # Sort Controls in the second column of Action Bar or below
            with ac2:
                sort_option = st.radio("Ordenar por:", ["Grid de Largada", "Resultado Previsto"], horizontal=True, index=1 if not prediction_triggered else 1, label_visibility="collapsed")
            
            if sort_option == "Grid de Largada":
                final_df = results.sort_values('grid_position')
            else:
                final_df = results.sort_values('predicted_position_int')
            
            final_df['team_colour'] = final_df['team_colour'].fillna('808080')
            final_df['full_name'] = final_df['full_name'].fillna(final_df['driver_number'].astype(str))
            final_df['team_name'] = final_df['team_name'].fillna("Unknown Team")
            
            winner_row = results.sort_values('predicted_position_int').iloc[0]
            if 'selected_driver' not in st.session_state or st.session_state['selected_driver'] is None:
                st.session_state['selected_driver'] = winner_row['driver_number']

            st.markdown("<br>", unsafe_allow_html=True) 

            # Display List
            for _, row in final_df.iterrows():
                    d_num = row['driver_number']
                    driver_name = row['full_name']
                    team_name = row['team_name']
                    team_color = f"#{row['team_colour']}" if row['team_colour'] and not str(row['team_colour']).startswith('#') else (row['team_colour'] or "#808080")
                    if not team_color.startswith('#'): team_color = f"#{team_color}" 
                    
                    start_pos = int(row['grid_position'])
                    pred_pos = int(row['predicted_position_int'])
                    delta = start_pos - pred_pos
                    
                    delta_color = "#4CAF50" if delta > 0 else "#F44336" if delta < 0 else "#888"
                    delta_str = f"▲{delta}" if delta > 0 else f"▼{abs(delta)}" if delta < 0 else "="
                    
                    # Highlight selected
                    is_selected = st.session_state['selected_driver'] == d_num
                    card_bg = "#3A3A3A" if is_selected else "#262730"
                    
                    # Layout
                    c_card, c_btn = st.columns([0.9, 0.1])
                    
                    with c_card:
                        # Improved Card HTML with strict width control
                        st.markdown(f"""
                        <div class="driver-card" style="background-color: {card_bg}; border-left: 5px solid {team_color};">
                            <div style="flex: 3; overflow: hidden; white-space: nowrap; margin-right: 10px;">
                                <div style="font-weight: bold; font-size: 1.05rem; color: white; text-overflow: ellipsis; overflow: hidden;">{driver_name}</div>
                                <div style="font-size: 0.8rem; color: #bbb; text-overflow: ellipsis; overflow: hidden;">{team_name}</div>
                            </div>
                            <div style="flex: 1; text-align: center;">
                                <div style="font-size: 0.65rem; color: #aaa;">LARGADA</div>
                                <div style="font-weight: bold;">P{start_pos}</div>
                            </div>
                            <div style="flex: 1; text-align: center;">
                                <div style="font-size: 0.65rem; color: #aaa;">PREVISÃO</div>
                                <div style="font-weight: bold; color: {delta_color};">P{pred_pos}</div>
                            </div>
                             <div style="flex: 0.5; text-align: right;">
                                <div style="font-weight: bold; color: {delta_color}; font-size: 0.9rem;">{delta_str}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with c_btn:
                        # Center align the button vertically roughly
                        st.markdown("<div style='height: 8px'></div>", unsafe_allow_html=True)
                        if st.button("›", key=f"sel_{d_num}"):
                            st.session_state['selected_driver'] = d_num
                            st.rerun()

    with col2:
        if 'last_results' in st.session_state and st.session_state.get('last_meeting_key') == selected_meeting_key:
            res = st.session_state['last_results']
            sel_id = st.session_state.get('selected_driver')
            
            sel_row = res[res['driver_number'] == sel_id]
            if not sel_row.empty:
                driver = sel_row.iloc[0]
                
                d_name = driver['full_name']
                d_team = driver['team_name']
                d_img = driver.get('headshot_url')
                t_color = f"#{driver['team_colour']}" if driver['team_colour'] and not str(driver['team_colour']).startswith('#') else (driver['team_colour'] or "#808080")
                if not t_color.startswith('#'): t_color = f"#{t_color}"
                
                best_lap = format_time(driver.get('best_lap_time'))
                avg_lap = format_time(driver.get('avg_lap_time'))
                s_pos = int(driver['grid_position'])
                f_pos = int(driver['predicted_position_int'])
                
                # PROFILE UI FIXES
                # 1. Smaller Image (width=180)
                # 2. Clean Name
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                if d_img and str(d_img).startswith('http'):
                    # Center Image trick
                    c_im1, c_im2, c_im3 = st.columns([1,2,1])
                    with c_im2:
                        st.image(d_img, width=180)
                
                st.markdown(f"""
                <div style="text-align: center; margin-top: 5px;">
                    <h2 style="margin:0; font-size: 1.8rem;">{d_name}</h2>
                    <h4 style="margin:5px 0 20px 0; color: {t_color}; font-weight: 300;">{d_team}</h4>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("---")
                
                # Metrics in a cleaner Grid
                # Avoid clipping by allowing more width per metric or reducing font
                c1, c2 = st.columns(2)
                c1.metric("🏁 Largada", f"P{s_pos}")
                c2.metric("🏁 Chegada", f"P{f_pos}")
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                c3, c4 = st.columns(2)
                c3.metric("Melhor Volta", best_lap)
                c4.metric("Ritmo Médio", avg_lap)

        else:
            st.info("Aguardando prévisão...")

if __name__ == "__main__":
    main()
