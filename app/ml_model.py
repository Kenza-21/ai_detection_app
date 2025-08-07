from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import os
import pandas as pd
import numpy as np
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_input_data(df):
    """Valide que le DataFrame contient les colonnes nécessaires"""
    required_columns = ['amount']
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        raise ValueError(f"Colonnes requises manquantes: {missing}")
    
    if df['amount'].isnull().any():
        raise ValueError("La colonne 'amount' contient des valeurs nulles")

def prepare_features(df):
    """Prépare les features pour le modèle"""
    try:
        # Création de amount_log si absent
        if 'amount_log' not in df.columns:
            df['amount_log'] = np.log1p(df['amount'])
            
        return df[['amount', 'amount_log']]
    except Exception as e:
        raise ValueError(f"Erreur préparation features: {str(e)}")


def detect_anomalies(df):
    """
    Version robuste qui garantit la création de is_anomaly
    """
    try:
        # Vérification des colonnes requises
        if 'amount' not in df.columns:
            raise ValueError("La colonne 'amount' est requise")
            
        # Création de amount_log si nécessaire
        if 'amount_log' not in df.columns:
            df['amount_log'] = np.log1p(df['amount'])
            
        # Entraînement du modèle
        model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
        model.fit(df[['amount', 'amount_log']])
        
        # Ajout des colonnes résultats
        df['anomaly_score'] = model.decision_function(df[['amount', 'amount_log']])
        df['is_anomaly'] = model.predict(df[['amount', 'amount_log']])
        df['is_anomaly'] = df['is_anomaly'].apply(lambda x: 1 if x == -1 else 0)
        
        return df
        
    except Exception as e:
        raise ValueError(f"Erreur dans detect_anomalies: {str(e)}")

def explain_anomalies(df):
    """Génère un rapport d'anomalies avec vérifications"""
    try:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input doit être un DataFrame")
            
        required_cols = ['is_anomaly', 'anomaly_score', 'amount']
        missing = [col for col in required_cols if col not in df.columns]
        
        if missing:
            return {"error": f"Colonnes manquantes: {missing}"}
        
        anomalies = df[df['is_anomaly'] == 1]
        
        if anomalies.empty:
            return {"info": "Aucune anomalie détectée"}
        
        return {
            "count": len(anomalies),
            "mean_amount": anomalies['amount'].mean(),
            "max_amount": anomalies['amount'].max(),
            "min_score": anomalies['anomaly_score'].min(),
            "top_anomalies": anomalies.nlargest(5, 'amount').to_dict('records')
        }
        
    except Exception as e:
        return {"error": f"Erreur génération rapport: {str(e)}"}