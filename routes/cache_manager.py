import os
import json
import hashlib
import logging
from datetime import datetime
from typing import Optional, Dict, List
import config
import glob

logger = logging.getLogger(__name__)

class CacheManager:
    """
    Gère le cache robuste basé sur la signature de tous les fichiers impliqués.
    Invalide automatiquement si UN fichier source change.
    """

    def __init__(self, cache_folder: str = "cache/"):
        self.cache_folder = cache_folder
        self.cache_metadata_file = os.path.join(cache_folder, "cache_metadata.json")

        # Créer le dossier cache s'il n'existe pas
        os.makedirs(cache_folder, exist_ok=True)

        # Initialiser le fichier de métadonnées
        self._init_metadata_file()

    def _init_metadata_file(self):
        """Initialise le fichier JSON de métadonnées du cache."""
        if not os.path.exists(self.cache_metadata_file):
            with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
                json.dump({}, f)

    def _get_file_signature(self, file_path: str) -> str:
        """
        Calcule la signature d'un fichier (timestamp + taille).
        Retourne "" si le fichier n'existe pas.
        """
        try:
            if not os.path.exists(file_path):
                return ""

            stat = os.stat(file_path)
            timestamp = stat.st_mtime
            size = stat.st_size
            return f"{timestamp}_{size}"
        except Exception as e:
            logger.error(f"Erreur lors du calcul de signature pour {file_path}: {e}")
            return ""

    def _get_files_for_report(self, report_type: str, company_code: str, year: str,
                             bp_type: Optional[str] = None, bnk: bool = False) -> List[str]:
        """
        Retourne la liste de TOUS les fichiers impliqués pour un type de rapport donné.
        """
        files = []

        # Fichiers de transactions
        if report_type in [config.GRAND_LIVRE_COMPTA_GEN, config.BALANCE_GEN,
                          config.GRAND_LIVRE_BNK, config.BALANCE_GEN_BNK]:
            # Transactions générales
            transactions_pattern = os.path.join(config.transactions_data_folder,
                                               company_code, year, "*")
            files.extend(glob.glob(transactions_pattern))

            # Balance d'ouverture générale
            initial_balance_file = f"{config.initial_balance_file_path} {company_code} {year}.xlsx"
            files.append(initial_balance_file)

            # Fichier bancaire si applicable
            if bnk or report_type in [config.GRAND_LIVRE_BNK, config.BALANCE_GEN_BNK]:
                files.append("bnk_gls.txt")

            # Fichier Plan Comptable pour balance générale
            if report_type in [config.BALANCE_GEN, config.BALANCE_GEN_BNK]:
                files.append(config.general_balance_mapping_file_path)

        elif report_type in [config.GRAND_LIVRE_FOURN, config.BALANCE_GEN_FOURN]:
            # Transactions fournisseurs
            vendors_pattern = os.path.join(config.vendors_transactions_data_folder,
                                          company_code, year, "*")
            files.extend(glob.glob(vendors_pattern))

            # Balance d'ouverture fournisseurs
            vendor_balance_file = f"{config.vendor_initial_balance_file_path} {company_code} {year}.xlsx"
            files.append(vendor_balance_file)

            # Fichier Plan Comptable pour balance
            if report_type == config.BALANCE_GEN_FOURN:
                files.append(config.general_balance_mapping_file_path)

        elif report_type in [config.GRAND_LIVRE_CLIENT, config.BALANCE_GEN_CLIENT]:
            # Transactions clients
            customers_pattern = os.path.join(config.customers_transactions_data_folder,
                                            company_code, year, "*")
            files.extend(glob.glob(customers_pattern))

            # Balance d'ouverture clients
            customer_balance_file = f"{config.customer_initial_balance_file_path} {company_code} {year}.xlsx"
            files.append(customer_balance_file)

            # Fichier Plan Comptable pour balance
            if report_type == config.BALANCE_GEN_CLIENT:
                files.append(config.general_balance_mapping_file_path)

        return files

    def _compute_signature(self, files: List[str]) -> str:
        """
        Compute une signature unique basée sur tous les fichiers.
        Signature = hash(concat de toutes les signatures individuelles).
        """
        file_signatures = []

        # Trier pour consistance
        for file_path in sorted(files):
            sig = self._get_file_signature(file_path)
            file_signatures.append(f"{file_path}:{sig}")

        # Créer un hash global
        combined = "|".join(file_signatures)
        return hashlib.md5(combined.encode()).hexdigest()

    def get_cache_key(self, report_type: str, company_code: str, year: str,
                     start_month: int, end_month: int,
                     bp_type: Optional[str] = None, bnk: bool = False) -> str:
        """
        Génère une clé cache unique incluant la signature de tous les fichiers.
        Format: {report_type}:{company_code}:{year}:{start_month}:{end_month}:{signature}
        """
        # Identifier tous les fichiers impliqués
        files = self._get_files_for_report(report_type, company_code, year, bp_type, bnk)

        # Calculer la signature
        signature = self._compute_signature(files)

        # Créer la clé
        cache_key = f"{report_type}:{company_code}:{year}:{start_month}:{end_month}:{signature}"
        return cache_key

    def get_cache(self, cache_key: str) -> Optional[str]:
        """
        Récupère un fichier du cache si:
        1. Le fichier cache existe
        2. La signature est toujours valide

        Cache lives forever (no TTL).
        Cache is invalidated only when source files change (signature-based invalidation).
        Retourne le chemin du fichier en cache, ou None si invalide/inexistant.
        """
        # Charger les métadonnées
        with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        # Vérifier si le cache existe dans les métadonnées
        if cache_key not in metadata:
            return None

        cache_info = metadata[cache_key]
        cache_file = cache_info.get("file_path")

        # Vérifier que le fichier cache existe
        if not cache_file or not os.path.exists(cache_file):
            # Supprimer l'entrée du cache si le fichier n'existe plus
            del metadata[cache_key]
            with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f)
            return None

        # Cache is valid if file exists (no TTL check)
        return cache_file

    def set_cache(self, cache_key: str, file_path: str) -> None:
        """
        Stocke un fichier en cache et met à jour les métadonnées.
        """
        # Charger les métadonnées actuelles
        with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        # Ajouter la nouvelle entrée
        metadata[cache_key] = {
            "file_path": file_path,
            "created_at": datetime.now().isoformat(),
            "accessed_at": datetime.now().isoformat()
        }

        # Sauvegarder les métadonnées
        with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

    def access_cache(self, cache_key: str) -> None:
        """Mise à jour du timestamp d'accès au cache."""
        with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        if cache_key in metadata:
            metadata[cache_key]["accessed_at"] = datetime.now().isoformat()
            with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)

    def clear_cache(self, cache_key: Optional[str] = None) -> None:
        """
        Supprime une entrée de cache spécifique, ou tout le cache si cache_key est None.
        """
        with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        if cache_key:
            # Supprimer l'entrée spécifique
            if cache_key in metadata:
                cache_file = metadata[cache_key].get("file_path")
                if cache_file and os.path.exists(cache_file):
                    try:
                        os.remove(cache_file)
                    except:
                        pass
                del metadata[cache_key]
        else:
            # Supprimer tout le cache
            for cache_info in metadata.values():
                cache_file = cache_info.get("file_path")
                if cache_file and os.path.exists(cache_file):
                    try:
                        os.remove(cache_file)
                    except:
                        pass
            metadata = {}

        with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

    def get_cache_stats(self) -> Dict:
        """Retourne des statistiques sur le cache."""
        with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        total_entries = len(metadata)
        total_size = 0

        for cache_info in metadata.values():
            cache_file = cache_info.get("file_path")
            if cache_file and os.path.exists(cache_file):
                try:
                    total_size += os.path.getsize(cache_file)
                except:
                    pass

        return {
            "total_entries": total_entries,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "cache_folder": self.cache_folder
        }
