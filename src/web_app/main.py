from fastapi import FastAPI, Request, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import uvicorn
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.inference.predictor import F1Predictor
from dotenv import load_dotenv
# Force Reload Triggered
# Force Reload Triggered Again for RF Validation
# Force Reload Triggered for Rain Heuristic
# Force Reload Triggered for Display Logic
# Force Reload Triggered for Rain Mode Fix
# Force Reload Triggered for Proportional Rain Logic
# Force Reload Triggered for Differential Skill Logic
# Force Reload Triggered for Historical Aptitude Logic
# Force Reload Triggered for Grid Re-Ranking Fix
# Force Reload Triggered for Rain Factor Amplification
# Force Reload Triggered for Additive Swing Logic
# Force Reload Triggered for Latest Grid Workflow
# Load Env
load_dotenv()
BUCKET = os.environ.get('S3_BUCKET_NAME')

app = FastAPI()

# Mount Static
app.mount("/static", StaticFiles(directory="src/web_app/static"), name="static")

# Templates
templates = Jinja2Templates(directory="src/web_app/templates")

# Global Data Cache (Simple in-memory for demo)
class DataCache:
    def __init__(self):
        self.predictor = None
        self.df_race = None
        self.df_practice = None
        self.df_driver = None
        self.last_prediction = None

    def load(self):
        print("Loading Data...")
        # Updated for Random Forest: Pass Directory, not file
        model_dir = os.path.join(os.getcwd(), 'notebooks')
        self.predictor = F1Predictor(model_dir)
        
        race_path = f"s3://{BUCKET}/gold/gold_race_widetable/"
        practice_path = f"s3://{BUCKET}/gold/gold_practice_widetable/"
        driver_path = f"s3://{BUCKET}/gold/dim_driver/"
        
        # Load and Ensure grid_position is numeric
        self.df_race = pd.read_parquet(race_path)
        if 'grid_position' in self.df_race.columns:
            self.df_race['grid_position'] = pd.to_numeric(self.df_race['grid_position'], errors='coerce').fillna(0).astype(int)
        self.df_practice = pd.read_parquet(practice_path)
        self.df_driver = pd.read_parquet(driver_path)
        # Dedup driver
        self.df_driver = self.df_driver[['meeting_key', 'driver_number', 'full_name', 'name_acronym', 'team_name', 'team_colour', 'headshot_url']].drop_duplicates()
        print("Data Loaded.")

data = DataCache()

@app.on_event("startup")
async def startup_event():
    data.load()
    print("DEBUG RACE COLS:", data.df_race.columns.tolist())
    if data.df_practice is not None:
        print("DEBUG PRACTICE COLS:", data.df_practice.columns.tolist())

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    # SIMPLIFIED WORKFLOW (User Request):
    # Select Circuit -> Predict (Using Grid from most recent year available)
    
    # 1. Get unique circuits
    # We want to display "Country - Circuit"
    # We need the Latest meeting_key for each circuit
    
    df = data.df_race[['meeting_key', 'year', 'country_name', 'circuit_short_name', 'date_start']].drop_duplicates()
    
    # Sort by date descending to find latest easily
    df = df.sort_values('date_start', ascending=False)
    
    # Group by Circuit Identifier (using country + circuit name to be safe)
    # We take the FIRST occurrence (which is latest due to sort)
    latest_meetings = df.groupby(['country_name', 'circuit_short_name']).first().reset_index()
    
    # Sort alphabetically for UI
    latest_meetings = latest_meetings.sort_values('country_name')
    
    # Create List
    # Value = meeting_key (The engine will use this to get the grid)
    # Label = Country - Circuit (Year) <- Helpful to show which year is being used
    circuits_list = []
    for _, row in latest_meetings.iterrows():
        circuits_list.append({
            "key": int(row['meeting_key']),
            "name": f"{row['country_name']} - {row['circuit_short_name']}"
        })
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "circuits": circuits_list,
        # No years needed
    })

# Helper: Format Time
def format_time(seconds):
    if pd.isna(seconds) or seconds == 0:
        return "-"
    try:
        s = float(seconds)
        minutes = int(s // 60)
        rem_seconds = s % 60
        # Format: 1:23,456 (using comma as requested)
        return f"{minutes}:{rem_seconds:06.3f}".replace('.', ',')
    except (ValueError, TypeError):
        return str(seconds)

@app.post("/predict", response_class=HTMLResponse)
async def predict(request: Request, year: int = Form(0), meeting_key: int = Form(...), rain_prob: int = Form(0)):
    # Calculate
    race_data = data.df_race[data.df_race['meeting_key'] == meeting_key].copy()
    practice_data = data.df_practice[data.df_practice['meeting_key'] == meeting_key].copy()
    
    # --- WEATHER SIMULATION ---
    # Logic: Pass probability to Predictor for proportional scaling
    # We DO NOT randomize the grid here. The model handles performance changes.
    results = data.predictor.predict(race_data, practice_data, rain_prob=rain_prob)
    results = pd.merge(results, data.df_driver, on=['meeting_key', 'driver_number'], how='left')
    
    # Fill NA
    results['team_colour'] = results['team_colour'].fillna('808080')
    results['full_name'] = results['full_name'].fillna(results['driver_number'].astype(str))
    
    # Sort by Predicted by default
    final_df = results.sort_values('predicted_position_int')
    
    # Cache for Comparison (and map position column)
    final_df['position'] = final_df['predicted_position_int']
    data.last_prediction = final_df.copy()
    
    # Format for Template
    drivers = []
    for _, row in final_df.iterrows():
        t_col = f"#{row['team_colour']}" if not str(row['team_colour']).startswith('#') else row['team_colour']
        if not t_col.startswith('#'): t_col = f"#{t_col}"
        
        delta = int(row['grid_position']) - int(row['predicted_position_int'])
        d_str = "▲" if delta > 0 else "▼" if delta < 0 else "="
        d_val = abs(delta) if delta != 0 else ""
        d_col = "text-green-500" if delta > 0 else "text-red-500" if delta < 0 else "text-gray-500"
        
        # Times
        best = format_time(row.get('best_lap_time'))
        avg = format_time(row.get('avg_lap_time'))

        drivers.append({
            "number": row['driver_number'],
            "name": row['full_name'],
            "team": row['team_name'],
            "color": t_col,
            "start": int(row['grid_position']),
            "end": int(row['predicted_position_int']),
            "delta_str": d_str,
            "delta_val": d_val,
            "delta_class": d_col,
            "img": row['headshot_url'] if pd.notna(row['headshot_url']) and str(row['headshot_url']).startswith('http') else '',
            "best_lap": best,
            "avg_lap": avg
        })
        
    return templates.TemplateResponse("results_partial.html", {
        "request": request,
        "drivers": drivers,
        "winner": drivers[0],
        "meeting_key": meeting_key,
        "circuit": {
            "name": race_data.iloc[0]['circuit_short_name'],
            "country": race_data.iloc[0]['country_name']
        }
    })

@app.post("/compare", response_class=HTMLResponse)
async def compare_drivers(request: Request, meeting_key: int = Form(...), d1: str = Form(...), d2: str = Form(...)):
    try:
        meeting_key = int(meeting_key)
    except:
        return "Invalid Meeting Key"

    # Fetch Data
    df = data.df_race[data.df_race['meeting_key'] == meeting_key]
    
    # Use Cache if available and matches this meeting
    if data.last_prediction is not None and not data.last_prediction.empty:
        if data.last_prediction['meeting_key'].iloc[0] == meeting_key:
            print("DEBUG: Using Cached Prediction for Compare")
            df = data.last_prediction
    
    # Filter Drivers (Convert input to int if df uses int)
    # Check type of driver_number in df
    try:
         d1_int = int(float(d1))
         d2_int = int(float(d2))
    except:
         return "Invalid Driver ID"

    d1_data = df[df['driver_number'] == d1_int].iloc[0] if not df[df['driver_number'] == d1_int].empty else None
    d2_data = df[df['driver_number'] == d2_int].iloc[0] if not df[df['driver_number'] == d2_int].empty else None
    
    if d1_data is None or d2_data is None:
        print(f"DEBUG: Data Insufficient. D1({d1_int}) found? {d1_data is not None}. D2({d2_int}) found? {d2_data is not None}")
        return "Erro: Dados insuficientes."

    # Helper to clean time
    def clean_time(x):
        return x if pd.notna(x) and x > 0 else 9999
        
    # Stats to Compare (Lower is better for times)
    metrics = {
        'best_lap_time': {'label': 'Volta Rápida', 'desc': 'A melhor volta registrada pelo piloto durante a prova.'}, 
        'avg_lap_time': {'label': 'Ritmo de Corrida', 'desc': 'Média de tempo de todas as voltas válidas (excluindo pit stops).'},
        'std_lap_time': {'label': 'Consistência', 'desc': 'Desvio padrão dos tempos de volta. Quanto menor, mais constante foi o piloto.'},
        'avg_pit_duration': {'label': 'Pit Stop', 'desc': 'Tempo médio de duração dos pit stops (entrada e saída).'}
    }
    
    # Calculate Scores (0-100) and Prepare Display Strings
    chart_data = {'labels': [m['label'] for m in metrics.values()], 'd1': [], 'd2': []}
    stats_display = []

    for k, meta in metrics.items():
        # Get solid numbers
        all_vals = df[k].apply(clean_time)
        min_v = all_vals.min()
        max_v = all_vals[all_vals != 9999].max() # Ignore 9999 for max range
        if pd.isna(max_v): max_v = min_v # Fallback if all NaN/9999
        range_v = max_v - min_v
        
        # Raw Values
        raw_v1 = d1_data.get(k, 9999)
        raw_v2 = d2_data.get(k, 9999)
        
        # Clean Logic for Scoring
        val1 = clean_time(raw_v1)
        val2 = clean_time(raw_v2)
        
        if val1 == 9999: score1 = 0
        elif range_v <= 0: score1 = 50 # If only 1 driver or equal times, show mid
        
        else: score1 = 100 - ((val1 - min_v) / range_v * 100)
        
        if val2 == 9999: score2 = 0
        elif range_v <= 0: score2 = 50
        else: score2 = 100 - ((val2 - min_v) / range_v * 100)
        
        # Clamp scores
        score1 = max(0, min(100, score1))
        score2 = max(0, min(100, score2))

        chart_data['d1'].append(round(score1))
        chart_data['d2'].append(round(score2))
        
        # Format for Display
        def fmt(v):
            if v == 9999 or pd.isna(v): return "-"
            # Time formatting
            try:
                s = float(v)
                m = int(s // 60)
                sec = s % 60
                return f"{m}:{sec:06.3f}"
            except:
                return str(v)

        stats_display.append({
            "label": meta['label'],
            "desc": meta['desc'],
            "v1": fmt(raw_v1),
            "v2": fmt(raw_v2)
        })

    # Prepare Position Logic
    def get_pos_data(row):
        try:
            start = int(row.get('grid_position', 0))
            end = int(row.get('position', 0))
            # Treat 0 as invalid/None
            if start == 0: start = end # Fallback
        except:
            start, end = 0, 0
            
        delta = start - end
        sign = 'pos' if delta > 0 else 'neg' if delta < 0 else 'neutral'
        
        # Display Mapping
        d_start = str(start) if start > 0 else "-"
        d_end = str(end) if end > 0 else "-"
        
        return {
            'start': d_start, 
            'end': d_end, 
            'val': abs(delta) if (start > 0 and end > 0) else "", 
            'sign': sign if (start > 0 and end > 0) else 'neutral'
        }

    pos1 = get_pos_data(d1_data)
    pos2 = get_pos_data(d2_data)

    # Get Names/Colors from Driver Dim (Corrected for Meeting)
    def get_info(d_num):
        dims = data.df_driver[(data.df_driver['driver_number'] == d_num) & (data.df_driver['meeting_key'] == meeting_key)]
        if dims.empty:
            dims = data.df_driver[data.df_driver['driver_number'] == d_num]
        if dims.empty:
             return {'name': str(d_num), 'full_name': str(d_num), 'color': '#888888', 'img': ''}
        info = dims.iloc[0]
        img_url = info.get('headshot_url', '')
        return {
            'number': str(d_num), # Added number for modal
            'name': info.get('name_acronym', str(d_num)),
            'full_name': info.get('full_name', str(d_num)),
            'color': f"#{info.get('team_colour', '888888')}" if not str(info.get('team_colour')).startswith('#') else info.get('team_colour'),
            'img': img_url if pd.notna(img_url) and str(img_url).startswith('http') else ''
        }

    info1 = get_info(d1_int)
    info2 = get_info(d2_int)

    return templates.TemplateResponse("compare_modal.html", {
        "request": request,
        "d1": info1,
        "d2": info2,
        "p1": pos1,
        "p2": pos2,
        "chart": chart_data,
        "stats": stats_display
    })

@app.get("/compare/select", response_class=HTMLResponse)
async def select_opponent(request: Request, current_driver: str, meeting_key: int):
    try:
        meeting_key = int(meeting_key)
    except:
         return templates.TemplateResponse("select_driver_modal.html", {"request": request, "drivers": [], "current_driver": current_driver, "meeting_key": meeting_key})

    # Strict Filtering: Only drivers present in this race AND with valid position data (excludes some reserves/FPS)
    race_drivers = data.df_race[data.df_race['meeting_key'] == meeting_key]
    
    if race_drivers.empty:
        print(f"DEBUG: No race drivers found for meeting {meeting_key}")
        return templates.TemplateResponse("select_driver_modal.html", {"request": request, "drivers": [], "current_driver": current_driver, "meeting_key": meeting_key})

    # Filter out drivers with no position (likely didn't race)
    # But be careful, DNF might have position NaN in some datasets, but usually has 'position' or 'classified'.
    # We'll use presence in df_race as the main filter, and maybe distinct driver numbers.
    active_drivers = race_drivers['driver_number'].unique()
    
    # Filter df_driver to only these Active Drivers AND this meeting
    # This removes historical/future entries for the same driver number
    relevant_drivers = data.df_driver[
        (data.df_driver['meeting_key'] == meeting_key) & 
        (data.df_driver['driver_number'].isin(active_drivers))
    ]

    drivers_list = []
    for _, row in relevant_drivers.iterrows():
        # Clean team color
        t_col = f"#{row['team_colour']}" if not str(row['team_colour']).startswith('#') else row['team_colour']
        if not t_col.startswith('#'): t_col = f"#{t_col}"
        
        drivers_list.append({
            "number": row['driver_number'],
            "name": row['name_acronym'],
            "full_name": row['full_name'],
            "team_color": t_col,
            "img": row['headshot_url']
        })
    
    # Remove current driver from options
    drivers_list = [d for d in drivers_list if str(d['number']) != str(current_driver)]
    
    return templates.TemplateResponse("select_driver_modal.html", {
        "request": request, 
        "drivers": drivers_list,
        "current_driver": current_driver,
        "meeting_key": meeting_key
    })
