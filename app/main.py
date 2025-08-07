import streamlit as st
from validation import validate_xml_with_xsd
from etl import extract_transactions, transform_data
from ml_model import detect_anomalies, explain_anomalies
from db_operations import db_manager
import os
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Configuration
load_dotenv()
st.set_page_config(layout="wide")
sns.set_style("whitegrid")

# ==================================================================== #
# =================== FONCTIONS DE VISUALISATION ===================== #
# ==================================================================== #

def show_data_cleaning(df):
    """Affiche les visualisations sur la qualité des données."""
    st.subheader("🔍 Visualisation de la Qualité des Données")
    
    with st.expander("Statistiques Générales du Fichier", expanded=True):
        cols = st.columns(3)
        cols[0].metric("Transactions Traitées", len(df))
        cols[1].metric("Anomalies Détectées", df['is_anomaly'].sum())
        cols[2].metric("Montant Moyen (MAD)", f"{df['amount'].mean():,.2f}")
    
    with st.expander("Qualité des Données (Complétude)", expanded=True):
        cleaned_fields = ['debtor_account', 'creditor_account', 'debtor_name', 'creditor_name']
        completeness = [df[field].replace('', np.nan).count() / len(df) * 100 for field in cleaned_fields]
        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.barh(cleaned_fields, completeness, color='#3498db')
        ax.set_title('Complétude des Champs Clés (%)', fontsize=16)
        ax.set_xlabel('Pourcentage de complétude')
        ax.set_xlim(0, 105)
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 1, bar.get_y() + bar.get_height()/2, f'{width:.1f}%', va='center')
        st.pyplot(fig)
    
    with st.expander("Distribution des Montants des Transactions", expanded=True):
        fig, ax = plt.subplots(figsize=(12, 5))
        sns.histplot(df['amount'], bins=40, kde=True, color="#7559b6", ax=ax)
        ax.set_title('Distribution des Montants (MAD)', fontsize=16)
        ax.set_xlabel('Montant')
        ax.set_ylabel('Nombre de Transactions')
        st.pyplot(fig)

def show_anomaly_report(df):
    """Génère et affiche le rapport d'anomalies et le graphique de fraude."""
    st.subheader("📈 Rapport et Visualisation des Anomalies (Fraudes Potentielles)")
    
    report = explain_anomalies(df)
    
    if "error" in report:
        st.error(f"Erreur lors de la génération du rapport : {report['error']}")
        return
    
    if report.get('count', 0) == 0:
        st.info("Le rapport d'anomalies est vide car aucune anomalie n'a été détectée dans ce fichier.")
        return

    with st.expander("Statistiques des Anomalies", expanded=True):
        cols = st.columns(4)
        cols[0].metric("Nombre d'Anomalies", report['count'])
        cols[1].metric("Montant Moyen Anomalie", f"{report['mean_amount']:,.2f} MAD")
        cols[2].metric("Montant Max Anomalie", f"{report['max_amount']:,.2f} MAD")
        cols[3].metric("Score d'Anomalie Min", f"{report['min_score']:.2f}")

    with st.expander("Visualisation Graphique des Fraudes", expanded=True):
        fig, ax = plt.subplots(figsize=(12, 7))
        anomalies = df[df['is_anomaly'] == 1]
        normales = df[df['is_anomaly'] == 0]
        
        ax.scatter(normales.index, normales['amount'], color='#3498db', label='Transactions Normales', alpha=0.6, s=50)
        ax.scatter(anomalies.index, anomalies['amount'], color='#e74c3c', label='Anomalies (Fraudes Potentielles)', alpha=1, s=100, edgecolors='black')
        
        ax.set_title('Visualisation des Transactions et des Anomalies Détectées', fontsize=16)
        ax.set_xlabel('Index de la Transaction')
        ax.set_ylabel('Montant de la Transaction (MAD)')
        ax.legend()
        ax.grid(True)
        ax.set_yscale('log')
        
        st.pyplot(fig)
        st.info("Note : L'axe des montants est en échelle logarithmique pour mieux visualiser les transactions de faible et de grande valeur sur le même graphique.")

def main():
    st.title("🏦 Système de Détection de Fraude Bancaire")
    st.markdown("""
    **Déposez un fichier XML (PACS.008/PACS.001)**. Le système affichera chaque transaction 
    dans un format structuré et encadré pour une clarté maximale.
    """)

    uploaded_file = st.file_uploader("Choisir un fichier XML", type=["xml"])
    
    if uploaded_file:
        xml_content = uploaded_file.read().decode("utf-8")
        
        is_valid, message = validate_xml_with_xsd(xml_content)
        if not is_valid:
            st.error(f"❌ Erreur de validation : {message}")
            return
        
        st.success("✅ Fichier XML validé avec succès")
        
        with st.spinner("Extraction et analyse en cours..."):
            try:
                transactions = extract_transactions(xml_content)
                df = transform_data(transactions)
                df = detect_anomalies(df)
            except Exception as e:
                st.error(f"❌ Erreur lors du traitement : {str(e)}")
                return

        anomalies = df[df['is_anomaly'] == 1]
        if not anomalies.empty:
            st.warning(f"⚠️ **{len(anomalies)} anomalies détectées** dans le fichier.")
        else:
            st.info("✅ Aucune anomalie détectée dans ce fichier.")

        # ==================================================================== #
        # ============== SECTION D'AFFICHAGE PRINCIPALE (FINALE) =============== #
        # ==================================================================== #
        st.subheader("📋 Transactions Analysées")
        
        transactions_list = df.to_dict('records')

        for i, transaction in enumerate(transactions_list):
            if i > 0:
                st.divider()

            is_anomaly = transaction.get('is_anomaly') == 1
            
            if is_anomaly:
                st.markdown(f"### 🔴 Transaction #{i+1} : {transaction.get('transaction_id', 'N/A')} (ANOMALIE)")
            else:
                st.markdown(f"### Transaction #{i+1} : {transaction.get('transaction_id', 'N/A')}")

            # Itérer sur chaque champ et valeur pour les afficher dans une case
            for field, value in transaction.items():
                
                # Formater la valeur
                if pd.isna(value) or str(value).strip() == '':
                    value_display = "<i>N/A</i>"
                elif isinstance(value, float):
                    if field == 'amount':
                        value_display = f"<b>{value:,.2f} {transaction.get('currency', 'MAD')}</b>"
                    else:
                        value_display = f"{value:.4f}"
                elif isinstance(value, pd.Timestamp):
                    value_display = value.strftime('%d-%m-%Y %H:%M:%S')
                else:
                    value_display = str(value)
                
                # Définir la couleur de la bordure et du champ
                border_color = "#001c38" if is_anomaly and field in ['is_anomaly', 'anomaly_score', 'amount'] else "#001c38"
                field_color = "#34495e" # Couleur sobre pour le nom du champ

                # Création de la case avec du HTML et CSS
                field_box_html = f"""
                <div style="
                    border: 1px solid {border_color}; 
                    border-left: 5px solid {border_color};
                    border-radius: 5px; 
                    padding: 10px; 
                    margin: 5px 0; 
                    background-color: rgb(16, 20, 24);
                    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
                ">
                    <span style='color: {field_color}; font-weight: bold;'>{field.replace('_', ' ').title()}</span> : {value_display}
                </div>
                """
                st.markdown(field_box_html, unsafe_allow_html=True)

        st.divider()
        # ==================================================================== #
        # ======================= FIN DE LA SECTION ========================== #
        # ==================================================================== #

        if st.button("💾 Sauvegarder et Afficher les Analyses Complètes"):
            with st.spinner("Sauvegarde et génération des rapports..."):
                try:
                    success = db_manager.save_transactions(df, xml_content)
                    if success:
                        st.success("✅ Données sauvegardées avec succès.")
                        show_data_cleaning(df)
                        show_anomaly_report(df)
                    else:
                        st.error("❌ Échec de la sauvegarde des données.")
                except Exception as e:
                    st.error(f"🚨 Erreur critique lors de la sauvegarde : {str(e)}")

if __name__ == "__main__":
    main()
