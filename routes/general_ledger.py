from flask import Blueprint, Response, request, send_from_directory, jsonify
from datetime import datetime
import config
import os
from routes.grand_livre import generate_gl_compta_gen
from routes.general_balance import generate_bal_gen
from routes.grand_livre_bp import generate_gl_bp
from routes.general_balance_bp import generate_bal_bp
from routes.cache_manager import CacheManager

general_ledger = Blueprint("general_ledger", __name__)

# Initialiser le gestionnaire de cache
cache_manager = CacheManager()

# Redirect the end-user submition to the right function
@general_ledger.route('/redirect-submit', methods=['POST'])
def redirect_submit():
    data = request.form.to_dict()  # Convertir en dict mutable
    report_type = data.get("report_type")
    company_code = data.get("company_code")
    year = data.get("year")

    # Ajouter le nom de l'entreprise au dict data (thread-safe)
    company_name = config.COMPANY_MAPPING.get(company_code, "Unknown")
    data['company_name'] = company_name

    if company_code == "CI14" and report_type == config.GRAND_LIVRE_COMPTA_GEN and year == "2022":
        return send_from_directory(directory=os.getcwd(), path=config.grand_livre_mantra_deux, as_attachment=True), 200
    elif company_code == "CI14" and report_type == config.GRAND_LIVRE_COMPTA_GEN and year == "2023":
        return send_from_directory(directory=os.getcwd(), path=config.grand_livre_mantra_trois, as_attachment=True), 200
    elif company_code == "CI14" and report_type == config.BALANCE_GEN_CLIENT and year == "2022":
        return send_from_directory(directory=os.getcwd(), path=config.bl_client_mantra_deux, as_attachment=True), 200
    elif company_code == "CI14" and report_type == config.BALANCE_GEN_CLIENT and year == "2023":
        return send_from_directory(directory=os.getcwd(), path=config.bl_client_mantra_trois, as_attachment=True), 200
    elif company_code == "CI14" and report_type == config.BALANCE_GEN_FOURN and year == "2022":
        return send_from_directory(directory=os.getcwd(), path=config.bl_fourn_mantra_deux, as_attachment=True), 200
    elif company_code == "CI14" and report_type == config.BALANCE_GEN_FOURN and year == "2023":
        return send_from_directory(directory=os.getcwd(), path=config.bl_fourn_mantra_trois, as_attachment=True), 200
    elif company_code == "CI14" and report_type == config.BALANCE_GEN and year == "2022":
        return send_from_directory(directory=os.getcwd(), path=config.bl_mantra_deux, as_attachment=True), 200
    elif company_code == "CI14" and report_type == config.BALANCE_GEN and year == "2023":
        return send_from_directory(directory=os.getcwd(), path=config.bl_mantra_trois, as_attachment=True), 200
    else:
        pass

    if report_type == config.GRAND_LIVRE_COMPTA_GEN:
        return _get_or_generate_report(data, generate_gl_compta_gen, report_type, company_code, year)
    elif report_type == config.GRAND_LIVRE_FOURN:
        return _get_or_generate_report(data, generate_gl_bp, report_type, company_code, year, bp_type="Vendor")
    elif report_type == config.GRAND_LIVRE_CLIENT:
        return _get_or_generate_report(data, generate_gl_bp, report_type, company_code, year, bp_type="Customer")
    elif report_type == config.GRAND_LIVRE_BNK:
        return _get_or_generate_report(data, generate_gl_compta_gen, report_type, company_code, year, bnk=True)
    elif report_type == config.BALANCE_GEN:
        return _get_or_generate_report(data, generate_bal_gen, report_type, company_code, year)
    elif report_type == config.BALANCE_GEN_CLIENT:
        return _get_or_generate_report(data, generate_bal_bp, report_type, company_code, year, bp_type="Customer")
    elif report_type == config.BALANCE_GEN_FOURN:
        return _get_or_generate_report(data, generate_bal_bp, report_type, company_code, year, bp_type="Vendor")
    elif report_type == config.BALANCE_GEN_BNK:
        return _get_or_generate_report(data, generate_bal_gen, report_type, company_code, year, bnk=True)
    else:
        return Response("Not Yet Implemented", 404)


def _get_or_generate_report(data, generate_func, report_type, company_code, year, bnk=False, bp_type=None):
    """
    Vérifie le cache et retourne le rapport en cache s'il existe,
    sinon génère un nouveau rapport et le met en cache.
    """
    start_month = int(data.get('start_month', 1))
    end_month = int(data.get('end_month', 12))

    # Créer la clé de cache
    cache_key = cache_manager.get_cache_key(report_type, company_code, year, start_month, end_month, bp_type, bnk)

    # Vérifier si le rapport est en cache
    cached_file = cache_manager.get_cache(cache_key)
    if cached_file:
        print(f"✓ Cache hit pour {cache_key}")
        cache_manager.access_cache(cache_key)
        return send_from_directory(directory=os.getcwd(), path=cached_file, as_attachment=True), 200

    # Générer le rapport avec les paramètres de cache
    print(f"✗ Cache miss pour {cache_key} - génération en cours...")

    # Déterminer la fonction correcte avec le bon nombre de paramètres
    if bp_type:
        # Pour generate_gl_bp et generate_bal_bp
        result = generate_func(data, bp_type=bp_type, cache_manager=cache_manager, cache_key=cache_key)
    elif bnk:
        # Pour les rapports bancaires
        result = generate_func(data, bnk=bnk, cache_manager=cache_manager, cache_key=cache_key)
    else:
        # Pour generate_gl_compta_gen et generate_bal_gen
        result = generate_func(data, cache_manager=cache_manager, cache_key=cache_key)

    return result


# Endpoint pour vider le cache manuellement
@general_ledger.route('/clear-cache', methods=['POST'])
def clear_cache_endpoint():
    """
    Endpoint pour vider complètement le cache.
    Utile quand le code change ou pour maintenance du serveur.

    Exemple d'utilisation:
    POST http://localhost:5051/clear-cache
    """
    try:
        cache_manager.clear_cache()
        return jsonify({
            "status": "success",
            "message": "Cache cleared successfully",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to clear cache: {str(e)}"
        }), 500


# Endpoint pour voir les statistiques du cache
@general_ledger.route('/cache-stats', methods=['GET'])
def cache_stats_endpoint():
    """
    Endpoint pour voir les statistiques du cache.
    Utile pour monitoring et debugging.

    Exemple d'utilisation:
    GET http://localhost:5051/cache-stats

    Réponse:
    {
        "status": "success",
        "total_entries": 15,
        "total_size_mb": 45.2,
        "cache_folder": "cache/"
    }
    """
    try:
        stats = cache_manager.get_cache_stats()
        return jsonify({
            "status": "success",
            "total_entries": stats['total_entries'],
            "total_size_mb": stats['total_size_mb'],
            "cache_folder": stats['cache_folder'],
            "ttl": "infinite",
            "invalidation": "signature-based (when source files change)"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to get cache stats: {str(e)}"
        }), 500