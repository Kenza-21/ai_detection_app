import streamlit as st
from validation import validate_xml_with_xsd
from etl import extract_transactions, transform_data
from ml_model import detect_anomalies
from db_operations import db_manager
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

def main():
    st.title("🏦 Détection de Fraude Bancaire")
    st.markdown("""
    **Upload un fichier XML (PACS.008/PACS.001)**  
    Le système validera le fichier, détectera les anomalies et sauvegardera les résultats.
    """)

    uploaded_file = st.file_uploader("Déposer le fichier XML ici", type=["xml"])
    
    if uploaded_file:
        xml_content = uploaded_file.read().decode("utf-8")
        
        # Validation XSD
        is_valid, message = validate_xml_with_xsd(xml_content)
        if not is_valid:
            st.error(f"❌ Erreur de validation : {message}")
            return
        
        st.success("✅ Fichier XML validé avec succès !")
        
        # Extraction ETL
        with st.spinner("Extraction des transactions..."):
            try:
                transactions = extract_transactions(xml_content)
                df = transform_data(transactions)
                st.write(f"📊 {len(df)} transactions extraites")
                st.write("Aperçu des données:", df.head())
            except Exception as e:
                st.error(f"❌ Erreur extraction données: {str(e)}")
                return

        # Détection d'anomalies
        with st.spinner("Analyse des anomalies..."):
            try:
                df = detect_anomalies(df)
                
                if 'is_anomaly' not in df.columns:
                    st.error("La colonne 'is_anomaly' est manquante")
                    st.write("Colonnes disponibles:", df.columns.tolist())
                    return
                
                anomalies = df[df['is_anomaly'] == 1]
                st.warning(f"⚠️ {len(anomalies)} anomalies détectées")
                
            except Exception as e:
                st.error(f"❌ Erreur détection anomalies: {str(e)}")
                return

        # Sauvegarde en base
        if st.button("💾 Sauvegarder les résultats"):
            try:
                success = db_manager.save_transactions(df, xml_content)
                if success:
                    st.success("✅ Données sauvegardées avec succès!")
                else:
                    st.error("❌ Échec de la sauvegarde")
            except Exception as e:
                st.error(f"🚨 Erreur critique: {str(e)}")

        # Affichage des résultats
        st.dataframe(df)

if __name__ == "__main__":
    main()