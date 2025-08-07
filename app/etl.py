import pandas as pd
from xml.etree import ElementTree as ET
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_rib(rib):
    if pd.isna(rib) or not isinstance(rib, str):
        return ''
    # Remove all whitespace and make uppercase
    rib_cleaned = ''.join(rib.split()).upper()
    # Basic validation - at least some characters
    if not rib_cleaned:
        return ''
    return rib_cleaned

def get_text(element, xpath, ns):
    if element is None:
        return None
    found = element.find(xpath, ns)
    if found is not None and found.text:
        return found.text.strip()
    return None

def get_birth_date(dbtr):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    return get_text(dbtr, ".//ns:DtAndPlcOfBirth/ns:BirthDt", ns)

def get_birth_city(dbtr):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    return get_text(dbtr, ".//ns:DtAndPlcOfBirth/ns:CityOfBirth", ns)

def get_birth_country(dbtr):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    return get_text(dbtr, ".//ns:DtAndPlcOfBirth/ns:CtryOfBirth", ns)

def get_account_id(acct):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    if acct is None:
        return None
    # First try Othr/Id
    account_id = get_text(acct, ".//ns:Othr/ns:Id", ns)
    if account_id:
        return account_id
    # Fallback to other possible locations if needed
    return None

def get_service_level_code(pmt_tp_inf):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    if pmt_tp_inf is None:
        return None
    return get_text(pmt_tp_inf, "ns:SvcLvl/ns:Cd", ns)

def get_local_instrument(pmt_tp_inf):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    if pmt_tp_inf is None:
        return None
    return get_text(pmt_tp_inf, "ns:LclInstrm/ns:Prtry", ns)

def get_category_purpose(pmt_tp_inf):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    if pmt_tp_inf is None:
        return None
    return get_text(pmt_tp_inf, "ns:CtgyPurp/ns:Prtry", ns)

def get_bic(agent):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    if agent is None:
        return None
    return get_text(agent, ".//ns:BICFI", ns)

def get_member_id(agent):
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
    if agent is None:
        return None
    return get_text(agent, ".//ns:ClrSysMmbId/ns:MmbId", ns)

def extract_transactions(xml_content):
    """Extrait tous les champs nécessaires du XML"""
    try:
        ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"}
        root = ET.fromstring(xml_content)
        grp_hdr = root.find(".//ns:GrpHdr", ns)
        message_id = get_text(grp_hdr, "ns:MsgId", ns)

        transactions = []
        for tx in root.findall(".//ns:CdtTrfTxInf", ns):
            pmt_id = tx.find("ns:PmtId", ns)
            pmt_tp_inf = tx.find("ns:PmtTpInf", ns)
            dbtr = tx.find("ns:Dbtr", ns)
            cdtr = tx.find("ns:Cdtr", ns)
            dbtr_acct = tx.find("ns:DbtrAcct", ns)
            cdtr_acct = tx.find("ns:CdtrAcct", ns)
            dbtr_agt = tx.find("ns:DbtrAgt", ns)
            cdtr_agt = tx.find("ns:CdtrAgt", ns)

            amount = None
            currency = None
            amount_elem = tx.find(".//ns:IntrBkSttlmAmt", ns)
            if amount_elem is not None and amount_elem.text:
                try:
                    amount = float(amount_elem.text)
                    currency = amount_elem.attrib.get("Ccy")
                except ValueError:
                    amount = None

            transaction = {
                "message_id": message_id,
                "transaction_id": get_text(tx, ".//ns:TxId", ns),
                "instruction_id": get_text(pmt_id, "ns:InstrId", ns),
                "end_to_end_id": get_text(pmt_id, "ns:EndToEndId", ns),
                "clearing_system_ref": get_text(pmt_id, "ns:ClrSysRef", ns),
                "amount": amount,
                "currency": currency,
                "creation_date": get_text(grp_hdr, "ns:CreDtTm", ns),
                "acceptance_datetime": get_text(tx, ".//ns:AccptncDtTm", ns),
                "debtor_name": get_text(dbtr, "ns:Nm", ns),
                "debtor_birth_date": get_birth_date(dbtr),
                "debtor_birth_city": get_birth_city(dbtr),
                "debtor_birth_country": get_birth_country(dbtr),
                "debtor_account": get_account_id(dbtr_acct),
                "creditor_name": get_text(cdtr, "ns:Nm", ns),
                "creditor_account": get_account_id(cdtr_acct),  # Ajouté cette ligne
                "service_level_code": get_service_level_code(pmt_tp_inf),
                "local_instrument": get_local_instrument(pmt_tp_inf),
                "category_purpose": get_category_purpose(pmt_tp_inf),
                "charge_bearer": get_text(tx, "ns:ChrgBr", ns),
                "debtor_bic": get_bic(dbtr_agt),
                "creditor_bic": get_bic(cdtr_agt),
                "debtor_member_id": get_member_id(dbtr_agt),
                "creditor_member_id": get_member_id(cdtr_agt)
            }
            transactions.append(transaction)

        return transactions

    except Exception as e:
        logger.error(f"Erreur extraction XML: {str(e)}", exc_info=True)
        raise ValueError(f"Erreur d'extraction XML: {str(e)}")

def transform_data(transactions):
    """Transforme les données extraites et applique le nettoyage"""
    try:
        df = pd.DataFrame(transactions)

        if 'amount' in df.columns:
            df['amount'] = df['amount'].abs()

        date_cols = ['creation_date', 'acceptance_datetime']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').fillna(pd.Timestamp('2000-01-01'))

        for name_col in ['debtor_name', 'creditor_name']:
            if name_col in df.columns:
                df[name_col] = df[name_col].fillna('UNKNOWN')
                df[name_col] = df[name_col].apply(lambda x: 'UNKNOWN' if pd.isna(x) or str(x).strip() == '' else x)

        if 'currency' in df.columns:
            df['currency'] = df['currency'].fillna('MAD')
            df['currency'] = df['currency'].apply(lambda x: 'MAD' if pd.isna(x) or str(x).strip() == '' else x)

        # Nettoyage des RIBs
        for account_col in ['debtor_account', 'creditor_account']:
            if account_col in df.columns:
                df[account_col] = df[account_col].fillna('')
                df[account_col] = df[account_col].apply(clean_rib)

        if 'amount' in df.columns:
            df['amount_log'] = df['amount'].apply(lambda x: round(x, 2))

        logger.info("Nettoyage des données terminé avec succès")
        return df

    except Exception as e:
        logger.error(f"Erreur lors du nettoyage des données: {str(e)}", exc_info=True)
        raise ValueError(f"Erreur transformation données: {str(e)}")