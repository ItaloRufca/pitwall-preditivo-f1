import os
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib

class F1Predictor:
    def __init__(self, model_path):
        """
        Initialize the predictor with a saved XGBoost model.
        
        Args:
            model_path (str): Path to the .json model file.
        """
        self.model = xgb.Booster()
        self.model.load_model(model_path)
        self.feature_names = None
        
        # Categorical columns used in training - MUST MATCH TRAINING EXACTLY
        self.cat_cols = ['team_name', 'circuit_short_name', 'country_name', 'first_stint_compound']
        
        # We need a reference to the exact columns/order the model expects.
        # Since we don't have a preserved feature list artifact from training yet, 
        # we will rely on DMatrix feature names if available, or assume standard OneHot alignment.
        try:
            self.feature_names = self.model.feature_names
        except AttributeError:
            print("Warning: Model does not have feature_names metadata.")

    def preprocess(self, df_race, df_practice=None):
        """
        Prepares raw race/practice data for inference, matching the training pipeline.
        
        Args:
            df_race (pd.DataFrame): Race data (must valid columns like meeting_key, etc.)
            df_practice (pd.DataFrame): Optional practice data.
            
        Returns:
            pd.DataFrame: Processed dataframe ready for prediction (DMatrix-ready).
        """
        # 1. Join Logic (if practice data exists)
        if df_practice is not None:
            # Cast 'year' to int if present
            if 'year' in df_practice.columns:
                df_practice['year'] = df_practice['year'].astype(int)

            join_keys = ['meeting_key', 'driver_number']
            
            # AGGREGATION FIX: Handle multiple sessions (FP1, FP2, FP3)
            # Group by meeting/driver and take mean of numeric columns to ensure 1:1 merge
            numeric_cols = df_practice.select_dtypes(include=[np.number]).columns.tolist()
            
            # EXCLUDE grouping keys from aggregation list to avoid duplication on reset_index
            cols_to_agg = [c for c in numeric_cols if c not in join_keys]
            
            # Groupby
            df_practice_agg = df_practice.groupby(join_keys)[cols_to_agg].mean().reset_index()
            
            # Update df_practice to use the aggregated version
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
                # In inference, we might not have a full 'meeting' to groupby.
                # If we have multiple rows, we try. If single row, this does nothing useful but is safe.
                meeting_median = df_final.groupby('meeting_key')[col].transform('median')
                df_final[col] = df_final[col].fillna(meeting_median)
                df_final[col] = df_final[col].fillna(0) # Fallback
        else:
            df_final = df_race.copy()
            # Note: If model expects practice features, they must be present.
            # Ideally we add them as 0s if missing, but let's assume valid input for now.

        # 2. One-Hot Encoding
        valid_cats = [c for c in self.cat_cols if c in df_final.columns]
        if valid_cats:
            df_final = pd.get_dummies(df_final, columns=valid_cats, drop_first=True)

        # 3. Align Columns with Model
        if self.feature_names:
            # Add missing columns with 0
            for col in self.feature_names:
                if col not in df_final.columns:
                    df_final[col] = 0
            
            # Reorder and select strict columns
            df_final = df_final[self.feature_names]
        
        # Ensure numeric types
        df_final = df_final.select_dtypes(include=[np.number])
        
        return df_final

    def predict(self, df_race, df_practice=None):
        """
        Generates predictions for the given data.
        
        Returns:
            pd.DataFrame: Original dataframe with 'predicted_position' column.
        """
        X = self.preprocess(df_race, df_practice)
        dmatrix = xgb.DMatrix(X)
        
        preds = self.model.predict(dmatrix)
        
        # Clip predictions to realistic F1 range (for raw score)
        preds = np.clip(preds, 1, 20)
        
        result = df_race.copy()
        result['raw_prediction'] = preds
        
        # TIE-BREAKER FIX: Use rank() to force unique integer positions 1-N
        # We rank the raw scores (lower score = better position)
        result['predicted_position_int'] = result['raw_prediction'].rank(method='first').astype(int)
        result['predicted_position'] = result['raw_prediction'] # Keep raw for reference
        
        return result

# Simple test block
if __name__ == "__main__":
    # Mock usage
    print("Predictor module loaded.")
