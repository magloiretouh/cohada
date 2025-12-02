grand_livre_mantra_deux = "Data/STATIC/ibrahim test.pdf"
grand_livre_mantra_trois = "Data/STATIC/TG13_2024_PowerBi1.xlsx"
bl_mantra_deux = "Data/STATIC/TG13_2024_PowerBi1.xlsx"
bl_mantra_trois = "Data/STATIC/TG13_2024_PowerBi1.xlsx"
bl_client_mantra_deux = "Data/STATIC/TG13_2024_PowerBi1.xlsx"
bl_client_mantra_trois = "Data/STATIC/TG13_2024_PowerBi1.xlsx"
bl_fourn_mantra_deux = "Data/STATIC/TG13_2024_PowerBi1.xlsx"
bl_fourn_mantra_trois = "Data/STATIC/TG13_2024_PowerBi1.xlsx"

# Global Static Parameters
general_balance_mapping_file_path = "Data/STATIC/Plan_Comptable_OHADA.xlsx"
transactions_data_folder = "Data/ALL_TRANSACTIONS/"
vendors_transactions_data_folder = "Data/ALL_VENDORS_TRANSACTIONS/"
customers_transactions_data_folder = "Data/ALL_CUSTOMERS_TRANSACTIONS/"
output_folder = "output/"
initial_balance_file_path = "Data/INITIAL BALANCE/Initial Balance"
vendor_initial_balance_file_path = "Data/VENDORS INITIAL BALANCE/Initial Balance"
customer_initial_balance_file_path = "Data/CUSTOMERS INITIAL BALANCE/Initial Balance"
SYSCOHADA_column_in_initial_balance = "numéro de compte SYSCOHADA"
amount_column = "Amount in local currency"
SYSCOHADA_column_in_main_data = "Alternative Account No."  # Change to the column you want to use
filter_column = "Company Code"  # Column to filter on
debit_column_label = "Soldes débiteurs"
credit_column_label = "Soldes créditeurs"
offset_account_column_name = "Offsetting acct no."
posting_date_column_name = "Posting Date"
company_code_name = ""
year = 2024
start_month = 1
end_month = 12

# Mapping des codes entreprise vers leurs noms (thread-safe)
COMPANY_MAPPING = {
    "BF10": "OLAM BURKINA SARL",
    "CI13": "SECO",
    "CI14": "MANTRA",
    "CI22": "OLAM AGRI RUBBER C.I",
    "SN11": "OLAM SENEGAL S.A",
    "SN14": "ARISE IIP SENEGAL",
    "SN15": "AVISEN SARL",
    "TD10": "COTONTCHAD SN",
    "TG13": "NOUVELLE SOCIETE COTON SR",
}
SYSCOHADA_desc_column_in_initial_balance = "Intitulés de compte SYSCOHADA"
IFRS_code_column_in_initial_balance = "Numéro de compte IFRS"
selected_columns = ["Company Code", "Company code Name", "Fiscal Year", "G/L Account", "G/L Acct Long Text",
                    "Alternative Account No.", "Posting Date", "Document Number",
                    "Amount in local currency", "Text", "Reference", "Document Type",
                    "Offsetting acct no.", "Désignation", "Entry Date", "Time of Entry", "User ID"]  # Columns to load

vendor_selected_columns = ["Company Code", "Company code Name", "Fiscal Year", "Document Date",
                           "Posting Date", "Vendor", "Vendor Name", "Alternative Account No.",
                           "Amount in LC", "Document Number", "Document Header Text", "Amount in local currency",
                           "Reference", "Text", "Offsetting acct no.", "Offseet A/C Description", "Document Type", "Désignation"]

customer_selected_columns = ["Company Code", "Company code Name", "Fiscal Year", "Document Date",
                           "Posting Date", "Customer", "Customer Name", "Alternative Account No.",
                           "Amount in LC", "Document Number", "Document Header Text", "Amount in local currency",
                           "Reference", "Text", "Offsetting acct no.", "Offseet A/C Description", "Document Type", "Désignation"]

renamed_columns = {"Company Code": "Code Entreprise", "Company code Name": "Nom Entreprise",
                   "Fiscal Year": "Année Fiscale", "G/L Account": "Compte IFRS",
                    "G/L Acct Long Text": "Desc Compte IFRS", "Posting Date": "Date",
                    "Désignation": "Désignation Type de pièce", "Document Number": "Pièce",
                    "Reference": "Référence", "Text": "Libellé",
                    "Offsetting acct no.": "Contrepartie IFRS",
                    "Intitulé de compte IFRS": "Contrepartie IFRS Desc",
                    "numéro de compte SYSCOHADA": "Contrepartie SYSCOHADA",
                    "Intitulés de compte SYSCOHADA": "Contrepartie SYSCOHADA Desc",
                    "Document Type": "Type de pièce", "Entry Date" : "Date de Saisie", "Time of Entry": "Heure de Saisie",
                    "User ID" : "Utilisateur SAP"}

vendor_renamed_columns = {"Company Code": "Code Entreprise", "Company code Name": "Nom Entreprise",
                   "Fiscal Year": "Année Fiscale", "Posting Date": "Date",
                    "Désignation": "Désignation Type de pièce", "Document Number": "Pièce",
                    "Reference": "Référence", "Text": "Libellé",
                    "Offsetting acct no.": "Contrepartie IFRS",
                    "Offseet A/C Description": "Contrepartie IFRS Desc",
                    "Document Type": "Type de pièce"}

customer_renamed_columns = {"Company Code": "Code Entreprise", "Company code Name": "Nom Entreprise",
                   "Fiscal Year": "Année Fiscale", "Posting Date": "Date",
                    "Désignation": "Désignation Type de pièce", "Document Number": "Pièce",
                    "Reference": "Référence", "Text": "Libellé",
                    "Offsetting acct no.": "Contrepartie IFRS",
                    "Offseet A/C Description": "Contrepartie IFRS Desc",
                    "Document Type": "Type de pièce"}

reordered_columns = ["Code Entreprise", "Nom Entreprise", "Année Fiscale", "Compte SYSCOHADA", "Compte SYSCOHADA Desc",
                 "Compte IFRS", "Desc Compte IFRS", "Date",
                 "Type de pièce", "Désignation Type de pièce", "Pièce", "Référence", "Débit", "Crédit", "Solde", "Libellé",
                 "Date de Saisie", "Heure de Saisie", "Utilisateur SAP", "Contrepartie IFRS",
                 "Contrepartie IFRS Desc", "Contrepartie SYSCOHADA", "Contrepartie SYSCOHADA Desc"]

vendor_reordered_columns = ["Code Entreprise", "Nom Entreprise", "Année Fiscale", "Date",
                 "Type de pièce", "Désignation Type de pièce", "Pièce", "Référence", "Débit", "Crédit", "Solde", "Libellé", "Contrepartie IFRS",
                 "Contrepartie IFRS Desc"]

customer_reordered_columns = ["Code Entreprise", "Nom Entreprise", "Année Fiscale", "Date",
                 "Type de pièce", "Désignation Type de pièce", "Pièce", "Référence", "Débit", "Crédit", "Solde", "Libellé", "Contrepartie IFRS",
                 "Contrepartie IFRS Desc"]

GRAND_LIVRE_COMPTA_GEN = "gl_compta_gen"
BALANCE_GEN = "bal_gen"
BALANCE_GEN_CLIENT = "bal_gen_client"
BALANCE_GEN_FOURN = "bal_gen_fourn"
BALANCE_GEN_BNK = "bal_gen_bnk"
COMPTE_RESULTAT = "compte_res"
GRAND_LIVRE_CLIENT = "gl_client"
GRAND_LIVRE_FOURN = "gl_fourn"
GRAND_LIVRE_BNK = "gl_bnk"
JOURNAL_ACHAT = "jrnl_ach"
JOURNAL_VENTE = "jrnl_vte"

bnk_gls = []

with open("bnk_gls.txt", "r") as f:
    bnk_gls = [line.strip() for line in f]

# Authoritative expected dtypes for transaction files (column name -> polars dtype name)
# Edit this mapping if your source files have known types.
expected_dtypes = {
    "Company Code": "Utf8",
    "Company code Name": "Utf8",
    "Fiscal Year": "Int64",
    "G/L Account": "Utf8",
    "G/L Acct Long Text": "Utf8",
    "Alternative Account No.": "Int64",
    "Posting Date": "Date",
    "Document Number": "Utf8",
    "Amount in local currency": "Int64",
    "Text": "Utf8",
    "Reference": "Utf8",
    "Document Type": "Utf8",
    "Offsetting acct no.": "Utf8",
    "Désignation": "Utf8",
    "Entry Date": "Date",
    "Time of Entry": "Time",
    "User ID": "Utf8"
}