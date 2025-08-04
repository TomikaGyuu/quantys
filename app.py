import os
import json
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import openpyxl 
from datetime import datetime, date 
import uuid
from werkzeug.utils import secure_filename
import logging
import re
import io
from typing import Tuple, Dict, List, Union
from threading import Lock 

app = Flask(__name__)
# Exposer l'en-tête Content-Disposition pour le frontend
CORS(app, expose_headers=['Content-Disposition']) 

# Configuration de l'application
class Config:
    def __init__(self):
        self.UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'uploads')
        self.PROCESSED_FOLDER = os.getenv('PROCESSED_FOLDER', 'processed') 
        self.FINAL_FOLDER = os.getenv('FINAL_FOLDER', 'final')
        self.ARCHIVE_FOLDER = os.getenv('ARCHIVE_FOLDER', 'archive')
        self.LOG_FOLDER = os.getenv('LOG_FOLDER', 'logs')
        self.MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE', 16 * 1024 * 1024))  # 16MB
        
        # Créer les répertoires si ils n'existent pas
        for folder in [self.UPLOAD_FOLDER, self.PROCESSED_FOLDER, 
                      self.FINAL_FOLDER, self.ARCHIVE_FOLDER, self.LOG_FOLDER]:
            os.makedirs(folder, exist_ok=True)

config = Config()
app.config.from_object(config)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_FOLDER, 'inventory_processor.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SageX3Processor:
    """
    Classe principale pour le traitement des fichiers d'inventaire Sage X3.
    Gère les données en mémoire pour les sessions actives.
    """
    
    def __init__(self):
        # Dictionnaire pour stocker les données de chaque session en mémoire
        # Clé: session_id (str), Valeur: dictionnaire contenant les DataFrames et métadonnées
        self.sessions: Dict[str, dict] = {} 
        self._lock = Lock() # Verrou pour sécuriser l'accès concurrent aux sessions en mémoire

        # Définition des colonnes du fichier Sage X3 (indices basés sur 0)
        # ATTENTION : Les indices ont été décalés pour inclure la nouvelle colonne 'QUANTITE_REELLE_IN_INPUT'
        self.SAGE_COLUMNS = {
            'TYPE_LIGNE': 0,
            'NUMERO_SESSION': 1,
            'NUMERO_INVENTAIRE': 2, # Contient la date de l'inventaire
            'RANG': 3,
            'SITE': 4,
            'QUANTITE': 5,
            'QUANTITE_REELLE_IN_INPUT': 6, # NOUVELLE COLONNE : Quantité réelle présente dans le fichier d'entrée
            'INDICATEUR_COMPTE': 7,      # Décalé de 6
            'CODE_ARTICLE': 8,           # Décalé de 7
            'EMPLACEMENT': 9,            # Décalé de 8
            'STATUT': 10,                # Décalé de 9
            'UNITE': 11,                 # Décalé de 10
            'VALEUR': 12,                # Décalé de 11
            'ZONE_PK': 13,               # Décalé de 12
            'NUMERO_LOT': 14,            # Décalé de 13
        }
        # Ordre des noms de colonnes pour la reconstruction du fichier Sage X3
        # Cette liste doit correspondre aux clés de SAGE_COLUMNS et être dans le bon ordre
        self.SAGE_COLUMN_NAMES_ORDERED = [
            'TYPE_LIGNE', 'NUMERO_SESSION', 'NUMERO_INVENTAIRE', 'RANG', 'SITE',
            'QUANTITE', 'QUANTITE_REELLE_IN_INPUT', 'INDICATEUR_COMPTE', 'CODE_ARTICLE', 
            'EMPLACEMENT', 'STATUT', 'UNITE', 'VALEUR', 'ZONE_PK', 'NUMERO_LOT'
        ]

    def extract_date_from_lot(self, lot_number: str) -> Union[datetime, None]:
        """Extrait une date d'un numéro de lot Sage X3"""
        if pd.isna(lot_number):
            return None
            
        # Pattern pour les lots de format CPKU###MMYY####
        match = re.search(r'CPKU\d{3}(\d{2})(\d{2})\d{4}', str(lot_number))
        if match:
            try:
                month = int(match.group(1))
                year = int(match.group(2)) + 2000
                return datetime(year, month, 1)
            except ValueError:
                logger.warning(f"Date invalide dans le lot: {lot_number}")
        return None
    
    def _extract_inventory_date_from_num_inventaire(self, numero_inventaire: str, session_creation_timestamp: datetime) -> Union[date, None]:
        """
        Extrait la date (jour, mois) du numéro d'inventaire et utilise l'année de création de la session.
        Ex: ABJ012507INV00000002 -> 25/07/<session_creation_year>
        """
        # Regex pour capturer DDMM avant 'INV'
        match = re.search(r'(\d{2})(\d{2})INV', numero_inventaire)
        if match:
            try:
                day = int(match.group(1))
                month = int(match.group(2))
                # Utilise l'année de la création de la session pour la date de l'inventaire
                year = session_creation_timestamp.year
                return date(year, month, day)
            except ValueError:
                logger.warning(f"Date invalide (jour/mois) dans le numéro d'inventaire: {numero_inventaire}")
        return None

    def validate_sage_file(self, filepath: str, file_extension: str, session_creation_timestamp: datetime) -> Tuple[bool, Union[str, pd.DataFrame], List[str], Union[date, None]]:
        """
        Valide la structure d'un fichier Sage X3 (CSV ou XLSX), charge les données en DataFrame
        et extrait les lignes d'en-tête et la date d'inventaire.
        """
        headers = []
        data_rows = []
        original_s_lines_raw = []
        first_s_line_numero_inventaire = None
        
        expected_num_cols_for_data = len(self.SAGE_COLUMN_NAMES_ORDERED)

        try:
            if file_extension == '.csv':
                with open(filepath, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f):
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith('E;') or line.startswith('L;'):
                            headers.append(line)
                        elif line.startswith('S;'):
                            parts = line.split(';')
                            # Vérifier que la ligne a suffisamment de parties pour les colonnes attendues
                            if len(parts) < expected_num_cols_for_data:
                                return False, f"Ligne {i+1} : Format de colonnes invalide. Minimum {expected_num_cols_for_data} colonnes requises pour les données S;.", [], None
                            
                            # Capture du numéro d'inventaire de la première ligne S; pour la date
                            if first_s_line_numero_inventaire is None:
                                first_s_line_numero_inventaire = parts[self.SAGE_COLUMNS['NUMERO_INVENTAIRE']]
                            
                            # Tronquer ou padder les parties pour qu'elles aient exactement le nombre de colonnes attendu
                            processed_parts = parts[:expected_num_cols_for_data]
                            if len(processed_parts) < expected_num_cols_for_data:
                                processed_parts.extend([''] * (expected_num_cols_for_data - len(processed_parts)))

                            data_rows.append(processed_parts)
                            original_s_lines_raw.append(';'.join(processed_parts)) # Stocker la ligne traitée pour la reconstruction

            elif file_extension == '.xlsx':
                # Lire le fichier Excel, en s'assurant que toutes les cellules sont lues comme des chaînes
                # et sans interpréter la première ligne comme un en-tête.
                temp_df = pd.read_excel(filepath, header=None, dtype=str)
                
                # Itérer sur les lignes du DataFrame temporaire
                for i, row_series in temp_df.iterrows():
                    # Convertir la série en liste de chaînes, gérer les NaN et tronquer/padder
                    # pour assurer que 'parts' a la bonne longueur.
                    # On prend le maximum de l'indice de la dernière colonne attendue + 1
                    parts = [str(val).strip() if pd.notna(val) else '' for val in row_series.iloc[:max(self.SAGE_COLUMNS.values()) + 1]]
                    
                    # S'assurer que la ligne a au moins une colonne pour vérifier le type
                    if not parts:
                        logger.warning(f"Ligne XLSX vide ignorée à l'index {i}.")
                        continue

                    line_type = parts[self.SAGE_COLUMNS['TYPE_LIGNE']] if len(parts) > self.SAGE_COLUMNS['TYPE_LIGNE'] else ''

                    if line_type == 'E' or line_type == 'L':
                        headers.append(';'.join(parts))
                    elif line_type == 'S':
                        # Vérifier que la ligne S; a suffisamment de colonnes pour les données attendues
                        if len(parts) < expected_num_cols_for_data:
                            return False, f"Ligne {i+1} (S;): Format de colonnes invalide dans le fichier XLSX. Minimum {expected_num_cols_for_data} colonnes requises pour les données S;.", [], None
                        
                        # Tronquer ou padder les parties pour qu'elles aient exactement le nombre de colonnes attendu
                        processed_parts = parts[:expected_num_cols_for_data]
                        if len(processed_parts) < expected_num_cols_for_data:
                            processed_parts.extend([''] * (expected_num_cols_for_data - len(processed_parts)))

                        if first_s_line_numero_inventaire is None:
                            first_s_line_numero_inventaire = processed_parts[self.SAGE_COLUMNS['NUMERO_INVENTAIRE']]
                        
                        data_rows.append(processed_parts)
                        original_s_lines_raw.append(';'.join(processed_parts))
                    else:
                        logger.warning(f"Ligne XLSX non de type 'E;', 'L;', ou 'S;' ignorée à l'index {i}: {row_series.to_dict()}")

                if not data_rows:
                    return False, "Aucune donnée de type 'S;' trouvée dans le fichier XLSX.", [], None

            else:
                return False, "Extension de fichier non supportée. Seuls .csv et .xlsx sont acceptés.", [], None

            # --- Logique commune de traitement après lecture (CSV ou XLSX) ---
            # Créer le DataFrame à partir des data_rows déjà nettoyées et de la bonne longueur
            df_processed = pd.DataFrame(data_rows, columns=self.SAGE_COLUMN_NAMES_ORDERED)
            
            # Conversion des types
            df_processed['QUANTITE'] = pd.to_numeric(df_processed['QUANTITE'], errors='coerce')
            if df_processed['QUANTITE'].isna().any():
                return False, "Valeurs de quantité invalides détectées dans le fichier source.", [], None
            
            # Extraction des dates de lot
            df_processed['Date_Lot'] = df_processed['NUMERO_LOT'].apply(self.extract_date_from_lot)
            
            # Assurez-vous que original_s_line_raw est bien aligné avec df_processed
            df_processed['original_s_line_raw'] = original_s_lines_raw

            # Extraire la date d'inventaire (utilisée la première ligne S; trouvée)
            if first_s_line_numero_inventaire is None:
                return False, "Impossible d'extraire le numéro d'inventaire de la première ligne de données.", [], None
            inventory_date = self._extract_inventory_date_from_num_inventaire(first_s_line_numero_inventaire, session_creation_timestamp)
            if inventory_date is None:
                return False, "Impossible d'extraire une date d'inventaire valide du numéro d'inventaire.", [], None
            
            return True, df_processed, headers, inventory_date
            
        except Exception as e:
            logger.error(f"Erreur de validation: {str(e)}", exc_info=True)
            return False, str(e), [], None
    
    def aggregate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrège les données par Code_Article, Statut, Emplacement, Dépôt et Unité.
        """
        try:
            if df.empty:
                raise ValueError("DataFrame vide pour l'agrégation.")

            # Clés d'agrégation
            aggregation_keys = [
                'CODE_ARTICLE', 'STATUT', 'EMPLACEMENT', 'ZONE_PK', 'UNITE'
            ]

            aggregated = df.groupby(aggregation_keys).agg(
                Quantite_Theorique_Totale=('QUANTITE', 'sum'),
                Numero_Session=('NUMERO_SESSION', 'first'),
                Numero_Inventaire=('NUMERO_INVENTAIRE', 'first'),
                Site=('SITE', 'first'),
                Date_Min=('Date_Lot', lambda x: min(d for d in x if d is not None) if any(d for d in x if d is not None) else None)
            ).reset_index()
            
            return aggregated.sort_values('Date_Min', na_position='last')
            
        except Exception as e:
            logger.error(f"Erreur d'agrégation: {str(e)}", exc_info=True)
            raise
    
    def generate_template(self, aggregated_df: pd.DataFrame, session_id: str) -> str:
        """Génère un template Excel pour la saisie à partir des données agrégées."""
        try:
            if aggregated_df.empty:
                raise ValueError(f"Aucune donnée agrégée trouvée pour la session {session_id}.")

            # Récupérer Numero Session, Numero Inventaire et Site de la première ligne agrégée
            session_num = aggregated_df['Numero_Session'].iloc[0]
            inventory_num = aggregated_df['Numero_Inventaire'].iloc[0]
            site_code = aggregated_df['Site'].iloc[0] 

            template_data = {
                'Numéro Session': [session_num] * len(aggregated_df),
                'Numéro Inventaire': [inventory_num] * len(aggregated_df),
                'Code Article': aggregated_df['CODE_ARTICLE'],
                'Statut Article': aggregated_df['STATUT'], 
                'Quantité Théorique': 0, # initialiser la quantité théorique à 0
                'Quantité Réelle': 0, # Toujours 0 comme demandé pour la saisie manuelle
                'Unites': aggregated_df['UNITE'],
                'Depots': aggregated_df['ZONE_PK'], 
                'Emplacements': aggregated_df['EMPLACEMENT'], 
            }
            
            template_df = pd.DataFrame(template_data)
            
            # Construction du nom de fichier
            filename = f"{site_code}_{inventory_num}_{session_id}.xlsx"
            filepath = os.path.join(config.PROCESSED_FOLDER, filename)
            
            # Écriture Excel avec ajustement des colonnes
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                template_df.to_excel(writer, index=False, sheet_name='Inventaire')
                
                worksheet = writer.sheets['Inventaire']
                for column in worksheet.columns:
                    max_length = max(len(str(cell.value)) for cell in column if cell.value is not None)
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column[0].column_letter].width = adjusted_width
            
            return filepath
            
        except Exception as e:
            logger.error(f"Erreur génération template: {str(e)}", exc_info=True)
            raise
    
    def validate_completed_template(self, df: pd.DataFrame) -> bool:
        """Valide le fichier Excel complété par l'utilisateur."""
        required_columns = {
            'Numéro Session', 'Numéro Inventaire', 'Code Article', 
            'Statut Article', 'Quantité Théorique', 'Quantité Réelle',
            'Unites', 'Depots', 'Emplacements'
        }
        if not required_columns.issubset(df.columns):
            logger.error(f"Colonnes manquantes dans le fichier complété: {required_columns - set(df.columns)}")
            return False
        
        df['Quantité Réelle'] = pd.to_numeric(df['Quantité Réelle'], errors='coerce')
        if df['Quantité Réelle'].isna().any():
            logger.error("La colonne 'Quantité Réelle' contient des valeurs non numériques ou vides.")
            return False
        return True
    
    def process_completed_file(self, session_id: str, filepath: str) -> pd.DataFrame:
        """
        Traite le fichier Excel complété, calcule les écarts
        et met à jour le DataFrame agrégé en mémoire avec les quantités réelles.
        """
        with self._lock:
            session = self.sessions.get(session_id)
            if not session:
                raise ValueError("Session invalide ou non trouvée.")
            
            # Copie du DataFrame agrégé original pour travailler dessus
            original_aggregated_df = session['aggregated_df'].copy() 
            completed_df = pd.read_excel(filepath)
            
            if not self.validate_completed_template(completed_df):
                raise ValueError("Fichier complété invalide: vérifiez les colonnes ou les quantités réelles.")
            
            completed_df['Quantité Réelle'] = pd.to_numeric(completed_df['Quantité Réelle'], errors='coerce').fillna(0)

            # Renommer les colonnes du DataFrame agrégé pour correspondre au template
            original_aggregated_df.rename(columns={
                'CODE_ARTICLE': 'Code Article',
                'STATUT': 'Statut Article',
                'EMPLACEMENT': 'Emplacements',
                'ZONE_PK': 'Depots',
                'UNITE': 'Unites'
            }, inplace=True)

            # Clés de fusion basées sur l'agrégation
            merge_keys = [
                'Code Article', 'Statut Article', 'Emplacements', 'Depots', 'Unites'
            ]
            
            # Fusionner les données théoriques agrégées avec les quantités réelles saisies
            merged = pd.merge(
                original_aggregated_df[merge_keys + ['Quantite_Theorique_Totale']],
                completed_df[merge_keys + ['Quantité Réelle']],
                on=merge_keys,
                how='left'
            )
            
            merged['Quantité Réelle'] = merged['Quantité Réelle'].fillna(0)
            merged['Ecart'] = merged['Quantite_Theorique_Totale'] - merged['Quantité Réelle']
            
            # Stocker le DataFrame fusionné dans la session
            session['merged_df'] = merged.copy() 

            # Mettre à jour les métadonnées de la session
            session['completed_file_path'] = filepath
            session['status'] = 'completed_file_processed'
            session['total_discrepancy'] = float(merged['Ecart'].sum()) # Écart total pour la session
            
            return merged[['Code Article', 'Statut Article', 'Emplacements', 'Depots', 'Unites', 'Quantité Réelle', 'Ecart']].copy()
    
    def distribute_discrepancies(self, session_id: str, strategy: str = 'FIFO') -> pd.DataFrame:
        """
        Répartit les écarts sur les lots individuels (lignes S;) selon la stratégie spécifiée (FIFO/LIFO).
        Met à jour le DataFrame original de la session avec les quantités corrigées.
        """
        with self._lock:
            session = self.sessions.get(session_id)
            if not session or 'merged_df' not in session or 'original_df' not in session:
                raise ValueError("Session invalide ou données manquantes pour la distribution des écarts.")
                
            original_df_full = session['original_df'].copy() 
            merged_df = session['merged_df'] # Contient les écarts par combinaison d'articles

            if original_df_full.empty or merged_df.empty:
                raise ValueError(f"Données d'inventaire ou d'écarts manquantes pour la session {session_id}.")

            # Initialiser la colonne des quantités corrigées avec les quantités théoriques initiales
            original_df_full['Quantite_Corrigee'] = original_df_full['QUANTITE'].astype(float)
            
            adjusted_items_count = 0 # Compteur d'articles/lots ajustés

            # Itérer sur les lignes agrégées qui ont un écart
            for _, aggregated_row in merged_df[merged_df['Ecart'] != 0].iterrows():
                code = aggregated_row['Code Article']
                statut = aggregated_row['Statut Article']
                emplacement = aggregated_row['Emplacements']
                depot = aggregated_row['Depots']
                unite = aggregated_row['Unites']
                ecart = float(aggregated_row['Ecart'])

                # Filtrer les lignes de stock originales pour cette combinaison spécifique
                filter_mask = (
                    (original_df_full['CODE_ARTICLE'] == code) &
                    (original_df_full['STATUT'] == statut) &
                    (original_df_full['EMPLACEMENT'] == emplacement) &
                    (original_df_full['ZONE_PK'] == depot) &
                    (original_df_full['UNITE'] == unite)
                )
                
                # Obtenir les indices des lots pertinents pour cette combinaison
                relevant_indices = original_df_full[filter_mask].index.tolist()
                
                if not relevant_indices:
                    logger.warning(f"Aucun lot trouvé pour {code}/{statut}/{emplacement}/{depot}/{unite} malgré un écart.")
                    continue

                # Tri des lots pertinents selon la stratégie (FIFO/LIFO)
                if strategy == 'FIFO':
                    # Tri ascendant par Date_Lot (les plus anciens en premier)
                    relevant_indices.sort(key=lambda idx: original_df_full.loc[idx, 'Date_Lot'] if original_df_full.loc[idx, 'Date_Lot'] is not None else datetime.max)
                elif strategy == 'LIFO':
                    # Tri descendant par Date_Lot (les plus récents en premier)
                    relevant_indices.sort(key=lambda idx: original_df_full.loc[idx, 'Date_Lot'] if original_df_full.loc[idx, 'Date_Lot'] is not None else datetime.min, reverse=True)
                else:
                    raise ValueError("Stratégie non supportée. Utilisez 'FIFO' ou 'LIFO'.")

                if ecart > 0:  # Écart positif: il manque des articles (Théorique > Réel)
                    remaining_discrepancy = ecart
                    for idx in relevant_indices:
                        if remaining_discrepancy <= 0:
                            break
                        current_qty_in_lot = original_df_full.loc[idx, 'Quantite_Corrigee']
                        
                        # L'ajustement ne peut pas dépasser la quantité actuelle du lot
                        ajust = min(current_qty_in_lot, remaining_discrepancy)
                        
                        original_df_full.loc[idx, 'Quantite_Corrigee'] -= ajust
                        remaining_discrepancy -= ajust
                        if ajust > 0: # Compter seulement si un ajustement a réellement eu lieu
                            adjusted_items_count += 1
                
                elif ecart < 0:  # Écart négatif: il y a plus d'articles que prévu (Réel > Théorique)
                    amount_to_add = abs(ecart)
                    # Pour les ajouts, on applique généralement à un seul lot (le premier selon le tri)
                    idx_to_adjust = relevant_indices[0] 
                    original_df_full.loc[idx_to_adjust, 'Quantite_Corrigee'] += amount_to_add
                    adjusted_items_count += 1
            
            # Sauvegarde du DataFrame original mis à jour dans la session
            session['final_df'] = original_df_full
            session['status'] = 'discrepancies_distributed'
            session['strategy_used'] = strategy
            session['adjusted_items_count'] = adjusted_items_count

            # Retourner un DataFrame des quantités corrigées pour l'aperçu frontend
            return original_df_full[['CODE_ARTICLE', 'STATUT', 'EMPLACEMENT', 'ZONE_PK', 'UNITE', 'Quantite_Corrigee']].copy()
    
    def generate_final_file(self, session_id: str) -> str:
        """
        Génère le fichier final pour l'export Sage X3 à partir des données corrigées en mémoire.
        """
        with self._lock:
            session = self.sessions.get(session_id)
            if not session or 'final_df' not in session:
                raise ValueError("Données finales non disponibles pour la session.")
                
            final_df = session['final_df'] # C'est le original_df_full avec Quantite_Corrigee
            header_lines = session.get('header_lines', [])
            
            reconstructed_lines = []
            
            # Itérer sur le DataFrame final_df (qui contient les lignes originales + Quantite_Corrigee)
            for _, row in final_df.iterrows():
                original_line_raw = row['original_s_line_raw'] 
                parts = original_line_raw.split(';')
                
                # Remplacer la quantité (colonne 5, index 0-based) par la Quantite_Corrigee
                # Le QUANTITE_REELLE_IN_INPUT (index 6) est laissé tel quel car il n'est pas utilisé pour la sortie Sage X3
                if len(parts) > self.SAGE_COLUMNS['QUANTITE']:
                    # Assurez-vous que la quantité est un entier et convertie en string
                    parts[self.SAGE_COLUMNS['QUANTITE']] = str(int(row['Quantite_Corrigee']))
                    reconstructed_lines.append(';'.join(parts))
                else:
                    logger.warning(f"Ligne originale trop courte pour l'index quantité: {original_line_raw}. Ligne non modifiée.")
                    reconstructed_lines.append(original_line_raw) # Ajouter la ligne originale non modifiée

            # Concaténer en-têtes et lignes de données
            final_content = header_lines + reconstructed_lines
            
            # Génération du nom de fichier final
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"sage_x3_inventaire_corrige_{session_id}_{timestamp}.csv"
            filepath = os.path.join(config.FINAL_FOLDER, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                for line in final_content:
                    f.write(line + '\n')
            
            session['final_file_path'] = filepath # Mettre à jour le chemin du fichier final dans la session
            session['status'] = 'final_file_generated' 
            
            return filepath

# Initialisation du processeur
processor = SageX3Processor()

# Endpoints API
@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Endpoint pour l'upload initial d'un fichier Sage X3 (CSV ou XLSX)."""
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier fourni'}), 400
    
    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'Nom de fichier vide'}), 400
    
    # Vérification de la taille du fichier
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)
    
    if file_size > config.MAX_FILE_SIZE:
        return jsonify({'error': 'Fichier trop volumineux'}), 413
    
    file_extension = os.path.splitext(file.filename)[1].lower()
    if file_extension not in ['.csv', '.xlsx']: # Accepte CSV et XLSX
        return jsonify({'error': 'Format de fichier non supporté. Seuls les fichiers CSV et XLSX sont acceptés'}), 400
    
    session_id = str(uuid.uuid4())[:8] # Génère un ID de session court
    filepath = None # Initialiser filepath pour le bloc finally
    session_creation_timestamp = datetime.now() # Capture le timestamp de création de la session
    
    try:
        filename_on_disk = secure_filename(f"{session_id}_{file.filename}")
        filepath = os.path.join(config.UPLOAD_FOLDER, filename_on_disk)
        file.save(filepath)
        
        # Validation et traitement du fichier Sage X3
        is_valid, result_data, headers, inventory_date = processor.validate_sage_file(filepath, file_extension, session_creation_timestamp)
        if not is_valid:
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'error': str(result_data)}), 400 # result_data contient le message d'erreur ici
        
        original_df = result_data # Renommer pour plus de clarté
        
        # Agrégation des données
        aggregated_df = processor.aggregate_data(original_df)
        
        # Sauvegarde de la session en mémoire
        with processor._lock:
            processor.sessions[session_id] = {
                'original_file_path': filepath, # Chemin du fichier original sur le disque
                'original_df': original_df, # DataFrame complet des lignes S;
                'header_lines': headers, # Lignes d'en-tête du fichier
                'aggregated_df': aggregated_df, # DataFrame agrégé
                'timestamp': session_creation_timestamp,
                'status': 'uploaded', # Statut initial
                'inventory_date': inventory_date, # Date d'inventaire extraite
                'total_discrepancy': 0, # Initialisé à 0, mis à jour après traitement du fichier complété
                'adjusted_items_count': 0, # Initialisé à 0, mis à jour après distribution des écarts
                'strategy_used': 'N/A', # Initialisé à N/A, mis à jour après distribution
                'template_file_path': None, # Chemin du template généré
                'completed_file_path': None, # Chemin du fichier complété
                'final_file_path': None # Chemin du fichier final généré
            }
        
        # Génération du template Excel
        template_file_path = processor.generate_template(aggregated_df, session_id)
        
        # Mettre à jour le chemin du template et le statut dans la session en mémoire
        with processor._lock:
            processor.sessions[session_id]['template_file_path'] = template_file_path
            processor.sessions[session_id]['status'] = 'template_generated' 

        return jsonify({
            'success': True,
            'session_id': session_id,
            'template_url': f"/api/download/template/{session_id}",
            'stats': {
                'nb_articles': len(aggregated_df),
                'total_quantity': float(aggregated_df['Quantite_Theorique_Totale'].sum()),
                'nb_lots': len(original_df), # Nombre de lots (lignes S;) dans le fichier original
                'inventory_date': inventory_date.isoformat() if inventory_date else None
            }
        })
    
    except Exception as e:
        logger.error(f"Erreur upload: {str(e)}", exc_info=True)
        if filepath and os.path.exists(filepath):
            os.remove(filepath) # Nettoyer le fichier uploadé si erreur
        # Nettoyage de la session en mémoire en cas d'erreur
        if session_id in processor.sessions:
            del processor.sessions[session_id]
        return jsonify({'error': 'Erreur interne du serveur lors de l\'upload initial'}), 500

@app.route('/api/process', methods=['POST'])
def process_completed_file_route():
    """Endpoint pour traiter le fichier complété, calculer les écarts et générer le fichier final."""
    if 'file' not in request.files or 'session_id' not in request.form:
        return jsonify({'error': 'Paramètres manquants'}), 400
    
    try:
        session_id = request.form['session_id']
        file = request.files['file']
        strategy = request.form.get('strategy', 'FIFO') # Stratégie par défaut FIFO
        
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            return jsonify({'error': 'Seuls les fichiers Excel sont acceptés'}), 400
        
        filename_on_disk = secure_filename(f"completed_{session_id}_{file.filename}")
        filepath = os.path.join(config.PROCESSED_FOLDER, filename_on_disk)
        file.save(filepath)
        
        # Traitement du fichier complété et mise à jour des écarts
        processed_summary_df = processor.process_completed_file(session_id, filepath)
        
        # Distribution des écarts et mise à jour des quantités corrigées
        distributed_summary_df = processor.distribute_discrepancies(session_id, strategy)
        
        # Génération du fichier final
        final_file_path = processor.generate_final_file(session_id)
        
        # Récupérer la session mise à jour pour les stats
        session_data = processor.sessions.get(session_id, {})

        return jsonify({
            'success': True,
            'final_url': f"/api/download/final/{session_id}",
            'stats': {
                'total_discrepancy': session_data.get('total_discrepancy', 0),
                'adjusted_items': session_data.get('adjusted_items_count', 0), 
                'strategy_used': session_data.get('strategy_used', 'N/A')
            }
        })
    
    except ValueError as e:
        logger.error(f"Erreur de validation/logique: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Erreur traitement du fichier complété: {str(e)}", exc_info=True)
        return jsonify({'error': 'Erreur interne du serveur'}), 500

@app.route('/api/distribute/<strategy>', methods=['POST'])
def redistribute(strategy: str):
    """Endpoint pour re-répartir les écarts avec une autre stratégie (agit sur les données en mémoire)."""
    if 'session_id' not in request.form:
        return jsonify({'error': 'Session ID manquant'}), 400
    
    if strategy not in ['FIFO', 'LIFO']:
        return jsonify({'error': 'Stratégie non supportée'}), 400
    
    try:
        session_id = request.form['session_id']
        
        # Répartition avec nouvelle stratégie
        distributed_summary_df = processor.distribute_discrepancies(session_id, strategy)
        final_file_path = processor.generate_final_file(session_id)
        
        # Récupérer la session mise à jour pour les stats
        session_data = processor.sessions.get(session_id, {})

        return jsonify({
            'success': True,
            'final_url': f"/api/download/final/{session_id}",
            'strategy_used': session_data.get('strategy_used', 'N/A'),
            'adjusted_items': session_data.get('adjusted_items_count', 0)
        })
    
    except ValueError as e:
        logger.error(f"Erreur de validation/logique redistribution: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Erreur redistribution: {str(e)}", exc_info=True)
        return jsonify({'error': 'Erreur interne du serveur'}), 500

@app.route('/api/download/<file_type>/<session_id>', methods=['GET'])
def download_file(file_type: str, session_id: str):
    """Endpoint de téléchargement unifié pour les templates et les fichiers finaux."""
    try:
        session_data = processor.sessions.get(session_id)
        if not session_data:
            return jsonify({'error': 'Session invalide ou non trouvée'}), 404
        
        filepath = None
        download_name = None
        mimetype = None

        if file_type == 'template':
            filepath = session_data.get('template_file_path')
            if not filepath:
                return jsonify({'error': 'Chemin du template non trouvé pour cette session.'}), 404
            download_name = os.path.basename(filepath)
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        elif file_type == 'final':
            filepath = session_data.get('final_file_path') # Utilise 'final_file_path'
            if not filepath:
                return jsonify({'error': 'Fichier final non généré'}), 404
            download_name = os.path.basename(filepath)
            mimetype = 'text/csv'
        else:
            return jsonify({'error': 'Type de fichier invalide'}), 400
        
        if not os.path.exists(filepath):
            return jsonify({'error': 'Fichier non trouvé sur le serveur.'}), 404
        
        return send_file(
            filepath,
            as_attachment=True,
            download_name=download_name,
            mimetype=mimetype
        )
    
    except Exception as e:
        logger.error(f"Erreur téléchargement: {str(e)}", exc_info=True)
        return jsonify({'error': 'Erreur interne du serveur'}), 500

@app.route('/api/sessions', methods=['GET'])
def list_sessions():
    """Liste les sessions existantes en mémoire avec leurs statuts et statistiques."""
    try:
        sessions_list = []
        with processor._lock: # Verrouiller l'accès aux sessions pour éviter les modifications concurrentes
            for sid, data in processor.sessions.items():
                sessions_list.append({
                    'id': sid,
                    'status': data.get('status', 'unknown'),
                    'created': data.get('timestamp').isoformat() if data.get('timestamp') else None,
                    'original_file': os.path.basename(data.get('original_file_path', '')),
                    'stats': {
                        'nb_articles': len(data.get('aggregated_df', pd.DataFrame())),
                        'total_quantity': float(data.get('aggregated_df', pd.DataFrame())['Quantite_Theorique_Totale'].sum()) if not data.get('aggregated_df', pd.DataFrame()).empty else 0,
                        'total_discrepancy': data.get('total_discrepancy', 0),
                        'adjusted_items': data.get('adjusted_items_count', 0),
                        'strategy_used': data.get('strategy_used', 'N/A'),
                        'inventory_date': data.get('inventory_date').isoformat() if data.get('inventory_date') else None
                    }
                })
        
        # Trier les sessions par date de création descendante
        sessions_list.sort(key=lambda x: x['created'] or '', reverse=True)
        
        return jsonify({'sessions': sessions_list})
    
    except Exception as e:
        logger.error(f"Erreur listage sessions: {str(e)}", exc_info=True)
        return jsonify({'error': 'Erreur interne du serveur'}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint de santé pour vérifier le statut de l'application (en mémoire)."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'active_sessions_count': len(processor.sessions) # Compte les sessions en mémoire
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
