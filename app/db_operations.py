import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import logging
import sys

# Configuration
load_dotenv()
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.conn_params = {
            "host": os.getenv("DB_HOST", "localhost"),
            "database": os.getenv("DB_NAME", "bank_fraud"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgres"),
            "port": os.getenv("DB_PORT", "5432"),
            "connect_timeout": 5
        }
        self.connection = None
        self._init_db()
        self._upgrade_db()

    def _get_connection(self):
        """Établit une connexion à la base de données"""
        if self.connection is None or self.connection.closed:
            self.connection = psycopg2.connect(**self.conn_params)
        return self.connection

    def _init_db(self):
        """Initialise la structure de base de la base de données"""
        commands = [
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(50) UNIQUE NOT NULL,
                amount DECIMAL(15, 2) NOT NULL,
                currency VARCHAR(3) NOT NULL,
                creation_date TIMESTAMP,
                acceptance_datetime TIMESTAMP,
                debtor_name TEXT,
                creditor_name TEXT,
                debtor_account VARCHAR(50),
                creditor_account VARCHAR(50),
                is_anomaly BOOLEAN DEFAULT FALSE,
                anomaly_score DECIMAL(10, 4),
                file_type VARCHAR(10) CHECK (file_type IN ('PACS.008', 'PACS.001')),
                processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id)",
            "CREATE INDEX IF NOT EXISTS idx_anomalies ON transactions(is_anomaly)"
        ]
        
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            for command in commands:
                cursor.execute(command)
            conn.commit()
            logger.info("Database tables initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def _upgrade_db(self):
        """Met à jour le schéma de la base de données"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Supprime la colonne RIB si elle existe
            cursor.execute("""
                ALTER TABLE transactions DROP COLUMN IF EXISTS RIB
            """)
            # Supprime la colonne original_xml si elle existe
            cursor.execute("""
                ALTER TABLE transactions DROP COLUMN IF EXISTS original_xml
            """)
            conn.commit()
            logger.info("Removed unused columns (RIB, original_xml)")

        except Exception as e:
            logger.error(f"Error upgrading database: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def save_transactions(self, df, xml_content):
        """Sauvegarde les transactions dans la base de données"""
        file_type = 'PACS.008' if 'pacs.008' in xml_content.lower() else 'PACS.001'
        query = """
        INSERT INTO transactions (
            transaction_id, amount, currency, creation_date,
            acceptance_datetime, debtor_name, creditor_name,
            debtor_account, creditor_account, is_anomaly,
            anomaly_score, file_type
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (transaction_id) 
        DO UPDATE SET 
            processing_date = EXCLUDED.processing_date,
            amount = EXCLUDED.amount,
            debtor_account = EXCLUDED.debtor_account,
            creditor_account = EXCLUDED.creditor_account
        RETURNING id
        """
        
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                cursor.execute(query, (
                    str(row['transaction_id']),
                    float(row['amount']),
                    str(row['currency']),
                    row['creation_date'],
                    row['acceptance_datetime'],
                    str(row['debtor_name']),
                    str(row['creditor_name']),
                    str(row.get('debtor_account', '')),
                    str(row.get('creditor_account', '')),
                    bool(row.get('is_anomaly', False)),
                    float(row.get('anomaly_score', 0)),
                    file_type
                ))
            
            conn.commit()
            logger.info(f"Saved {len(df)} transactions")
            return True
        except Exception as e:
            logger.error(f"Error saving transactions: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

# Singleton instance for the application
db_manager = DatabaseManager()