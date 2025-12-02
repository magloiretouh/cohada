from flask import Blueprint
from .general_ledger import general_ledger
from .print_journal import other_actions


def register_routes(app):
    app.register_blueprint(general_ledger)
    app.register_blueprint(other_actions)