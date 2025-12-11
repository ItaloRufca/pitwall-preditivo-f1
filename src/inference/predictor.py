
import os
import pandas as pd
import numpy as np
import joblib
import json

class F1Predictor:
    def __init__(self, model_dir):
        """
        Initialize the predictor with a saved Random Forest model.
        
        Args:
            model_dir (str): Directory containing model (.joblib) and features (.json).
        """
        model_path = os.path.join(model_dir, 'rf_f1_model.joblib')
        features_path = os.path.join(model_dir, 'model_features.json')
        
        print(f"Loading model from {model_path}...")
        try:
            self.model = joblib.load(model_path)
        except Exception as e:
            print(f"Error loading model: {e}")
            self.model = None
            
        self.feature_names = []
        try:
            with open(features_path, 'r') as f:
                self.feature_names = json.load(f)
            print(f"Loaded {len(self.feature_names)} features configuration.")
        except Exception as e:
            print(f"Warning: Could not load feature names from {features_path}: {e}")
        
        # Categorical columns used in training - MUST MATCH TRAINING EXACTLY
        # These are dummy-encoded, so we need to generate columns matching training
        self.cat_cols = ['team_name', 'circuit_short_name', 'country_name', 'first_stint_compound']

        # Load Rain Factors and AMPLIFY Differences
        self.rain_factors = {}
        rain_factors_path = os.path.join(model_dir, 'driver_rain_factors.json')
        try:
             with open(rain_factors_path, 'r') as f:
                 raw_factors = json.load(f)
                 # 1. Load Raw
                 temp_factors = {int(k): v for k, v in raw_factors.items()}
                 
                 # 2. Normalize/Amplify
                 # We want the best rain driver to have factor ~0.8 (bonus)
                 # We want the worst rain driver to have factor ~1.2 (penalty)
                 # Current range is tight (e.g. 0.98 to 1.02)
                 
                 vals = list(temp_factors.values())
                 if vals:
                     min_val = min(vals)
                     max_val = max(vals)
                     # Avoid div/0
                     spread = max_val - min_val if (max_val - min_val) > 0.0001 else 1.0
                     
                     for k, v in temp_factors.items():
                         # Normalize to 0..1
                         norm = (v - min_val) / spread
                         # Scale to 0.8..1.2
                         # 0.0 -> 0.8 (Best)
                         # 1.0 -> 1.2 (Worst)
                         amplified = 0.8 + (norm * 0.4)
                         self.rain_factors[k] = amplified
                         
                 print(f"Loaded and Amplified {len(self.rain_factors)} rain factors (Range: 0.8 - 1.2).")
                 
        except Exception as e:
             print(f"Warning: Could not load rain factors: {e}")

    def preprocess(self, df_race, df_practice=None):
        """
        Prepares raw race/practice data for inference, matching the training pipeline.
        
        Args:
            df_race (pd.DataFrame): Race data (must valid columns like meeting_key, etc.)
            df_practice (pd.DataFrame): Optional practice data.
            
        Returns:
            pd.DataFrame: Processed dataframe ready for prediction.
        """
        # 1. Join Logic (if practice data exists)
        if df_practice is not None:
            # Cast 'year' to int if present
            if 'year' in df_practice.columns:
                df_practice['year'] = df_practice['year'].astype(int)

            join_keys = ['meeting_key', 'driver_number']
            
            # AGGREGATION FIX: Handle multiple sessions (FP1, FP2, FP3)
            numeric_cols = df_practice.select_dtypes(include=[np.number]).columns.tolist()
            cols_to_agg = [c for c in numeric_cols if c not in join_keys]
            
            # Groupby
            df_practice_agg = df_practice.groupby(join_keys)[cols_to_agg].mean().reset_index()
            df_practice = df_practice_agg

            # Rename practice columns
            cols_to_rename = {col: f"practice_{col}" for col in df_practice.columns if col not in join_keys}
            df_practice_renamed = df_practice.rename(columns=cols_to_rename)
            
            # Merge
            df_final = pd.merge(
                df_race, 
                df_practice_renamed, 
                on=join_keys, 
                how='left',
                suffixes=('', '_dup')
            )
            df_final = df_final.loc[:, ~df_final.columns.str.endswith('_dup')]
            
            # Impute Nulls (using logic matching training)
            practice_cols = [c for c in df_final.columns if c.startswith('practice_') and pd.api.types.is_numeric_dtype(df_final[c])]
            for col in practice_cols:
                meeting_median = df_final.groupby('meeting_key')[col].transform('median')
                df_final[col] = df_final[col].fillna(meeting_median)
                df_final[col] = df_final[col].fillna(0) # Fallback
        else:
            df_final = df_race.copy()

        # 2. Feature Engineering (Relative Metrics) - CRITICAL FOR WEATHER
        # Must replicate update_features.py logic dynamically for the inference batch
        
        # Meeting Averages
        if 'avg_lap_time' in df_final.columns:
            meeting_avg = df_final.groupby('meeting_key')['avg_lap_time'].transform('mean')
            df_final['ratio_avg_lap_time'] = df_final['avg_lap_time'] / (meeting_avg + 1e-6)
            df_final['diff_avg_lap_time'] = df_final['avg_lap_time'] - meeting_avg
            
        if 'best_lap_time' in df_final.columns:
            meeting_best = df_final.groupby('meeting_key')['best_lap_time'].transform('mean')
            df_final['ratio_best_lap_time'] = df_final['best_lap_time'] / (meeting_best + 1e-6)

        # Rain Flag Cast
        if 'rain_flag' in df_final.columns:
             df_final['rain_flag'] = df_final['rain_flag'].fillna(0).astype(int)
             
        # --- RAIN HEURISTIC (User Request) ---
        # If rain_flag is detected (1), artificially simulate the "percentage increase" in variability
        # We increase the ratio/diff because in rain, gaps are larger.
        # This forces the model to see "worse" relative times for everyone, but differential impact on slower drivers.
        mask_rain = df_final['rain_flag'] == 1
        if mask_rain.any():
            print("DEBUG: Rain simulation active. Adjusting relative metrics.")
            # Increase variability: Assume rain makes standard gaps 15% larger
            if 'ratio_avg_lap_time' in df_final.columns:
                 df_final.loc[mask_rain, 'ratio_avg_lap_time'] = df_final.loc[mask_rain, 'ratio_avg_lap_time'] * 1.05 # 5% spread
            if 'diff_avg_lap_time' in df_final.columns:
                 df_final.loc[mask_rain, 'diff_avg_lap_time'] = df_final.loc[mask_rain, 'diff_avg_lap_time'] * 1.10 # 10% spread

        # 3. One-Hot Encoding
        valid_cats = [c for c in self.cat_cols if c in df_final.columns]
        if valid_cats:
            df_final = pd.get_dummies(df_final, columns=valid_cats, drop_first=True)

        # 4. Align Columns with Model
        if self.feature_names:
            # Robust Reindex: Adds missing cols with 0, removes extra, and enforces order
            df_final = df_final.reindex(columns=self.feature_names, fill_value=0)
        
        # Ensure numeric types
        df_final = df_final.fillna(0)
        
        return df_final

    def predict(self, df_race, df_practice=None, rain_prob=0):
        """
        Generates predictions for the given data.
        
        Args:
            df_race (pd.DataFrame): Race data
            df_practice (pd.DataFrame): Practice data
            rain_prob (int): 0-100% probability of rain.
        
        Returns:
            pd.DataFrame: Original dataframe with 'predicted_position' column.
        """
        if self.model is None:
            print("Error: Model not loaded.")
            # Fail safe
            result = df_race.copy()
            result['predicted_position_int'] = result['grid_position']
            result['predicted_position'] = result['grid_position']
            return result
            
        try:
            X = self.preprocess(df_race, df_practice)
            
            # RAIN MODE TRIGGER
            if rain_prob > 20 and 'rain_flag' in X.columns:
                 X['rain_flag'] = 1
            elif 'rain_flag' in X.columns:
                 pass 
            
            # Random Forest Prediction
            preds = self.model.predict(X)
        except Exception as e:
            print(f"Prediction Error: {e}")
            # Fallback to prevent App Crash 500
            result = df_race.copy()
            result['predicted_position_int'] = result.get('grid_position', 0)
            result['predicted_position'] = result.get('grid_position', 0)
            result['raw_prediction'] = result.get('grid_position', 0)
            return result
        
        # Clip predictions to realistic F1 range (for raw score)
        preds = np.clip(preds, 1, 20)
        
        
        result = df_race.copy()
        result['raw_prediction'] = preds
        
        # TIE-BREAKER FIX: Use rank() to force unique integer positions 1-N
        result['predicted_position_int'] = result['raw_prediction'].rank(method='first').astype(int)
        result['predicted_position'] = result['raw_prediction'] 
        
        # --- RAIN SIMULATION DISPLAY FIX (User Request) ---
        has_rain = False
        if rain_prob > 0:
             has_rain = True
        elif 'rain_flag' in df_race.columns and (df_race['rain_flag'] == 1).any():
             has_rain = True
             rain_prob = 100 
             
        # Force scaling if detected
        if has_rain:
            # HISTORICAL SKILL SCALING
            # Base penalty derived from rain_prob
            base_rain_impact = 0.18 * (rain_prob / 100.0)
            
            # Get historical factor per driver
            # Default to 1.1 (slightly worse in rain) if unknown
            def get_skill_factor(row):
                d_num = int(row.get('driver_number', 0))
                # Factor < 1.0 means GOOD in rain (e.g. 0.98)
                # Factor > 1.0 means BAD in rain (e.g. 1.05)
                # We use this to modulate the *Impact*
                
                # If factor is 0.9, we want penalty to be LESS.
                # If factor is 1.1, we want penalty to be MORE.
                factor = self.rain_factors.get(d_num, 1.05) 
                
                # We scale the Base Impact by this factor
                # e.g. Base 0.18 * 0.9 = 0.16 (Less penalty for rain master)
                # e.g. Base 0.18 * 1.1 = 0.198 (More penalty for bad driver)
                
                # Amplified Difference: Let's power it to make it visible
                return factor * factor
                
            skill_mults = result.apply(get_skill_factor, axis=1)
            scale_factors = 1.0 + (base_rain_impact * skill_mults)
            
            print(f"DEBUG: Scaling Display Times for Rain Simulation (Historical Skill)")
            print(f"DEBUG: Applying Rain Penalty to Grid Ordering...")
            
            # --- CRITICAL FIX: Additive Swing for Grid Re-Ranking ---
            # Multiplicative scaling failed to change order because base gaps were too large.
            # We switch to ADDITIVE SWING:
            # NewScore = RawScore + (RainIntensity * MaxSwing * (Factor - Neutral))
            
            # MaxSwing: At 100% rain, how many positions can a driver lose/gain solely on skill?
            # Let's say +/- 5 positions.
            MAX_SWING = 10.0 
            
            def get_position_swing(row):
                d_num = int(row.get('driver_number', 0))
                # Recall Factor Range: 0.8 (Best) to 1.2 (Worst)
                # Neutral is 1.0
                factor = self.rain_factors.get(d_num, 1.05)
                
                # Difference from neutral
                # Best: 0.8 - 1.0 = -0.2
                # Worst: 1.2 - 1.0 = +0.2
                diff = factor - 1.0
                
                # Calculate Swing
                # RainProb (0-1) * 10 * (-0.2) = -2.0 positions (Gain)
                # RainProb (0-1) * 10 * (+0.2) = +2.0 positions (Loss)
                
                # Amplify the diff for drama? 
                # If we want -5 positions for the best:
                # 1.0 * 10 * X = -5  => X = -0.5. Current is -0.2.
                # Let's multiply diff by 2.5 to map 0.2 to 0.5
                
                amplified_diff = diff * 3.0 # Stronger effect
                
                swing = (rain_prob / 100.0) * MAX_SWING * amplified_diff
                return swing

            swings = result.apply(get_position_swing, axis=1)
            
            print(f"DEBUG: Applying Additive Swings to Grid (Min: {swings.min():.2f}, Max: {swings.max():.2f})")
            
            # Update Score
            result['raw_prediction'] = result['raw_prediction'] + swings
            
            # RE-RANK
            result['predicted_position_int'] = result['raw_prediction'].rank(method='first').astype(int)
            result['predicted_position'] = result['raw_prediction']
            
            # Scale Best Lap (Keep multiplicative for PHYSICS/Times)
            if 'best_lap_time' in result.columns:
                 mask = (result['best_lap_time'] > 0) & (result['best_lap_time'].notna())
                 result.loc[mask, 'best_lap_time'] = result.loc[mask, 'best_lap_time'] * scale_factors[mask]
                 
            # Scale Avg Lap
            if 'avg_lap_time' in result.columns:
                 mask = (result['avg_lap_time'] > 0) & (result['avg_lap_time'].notna())
                 result.loc[mask, 'avg_lap_time'] = result.loc[mask, 'avg_lap_time'] * scale_factors[mask]
                 
            # Scale Std Dev (Consistency)
            if 'std_lap_time' in result.columns:
                 mask = (result['std_lap_time'] > 0) & (result['std_lap_time'].notna())
                 chaos_scale = 1.0 + (0.50 * (rain_prob / 100.0)) 
                 result.loc[mask, 'std_lap_time'] = result.loc[mask, 'std_lap_time'] * chaos_scale
            
            # RE-SORT DF to ensure the table is ordered by the new position
            result = result.sort_values('predicted_position_int')

        return result

# Simple test block
if __name__ == "__main__":
    print("Predictor module (RF) loaded.")
