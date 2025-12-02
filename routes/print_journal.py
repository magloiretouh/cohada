from flask import Blueprint, Response, request, send_from_directory
import polars as pl
import calendar
import config
import os
import uuid
from datetime import datetime
from xlsxwriter import Workbook
from routes.customs_functions import *

other_actions = Blueprint("other_actions", __name__)

# Generate a document number journal in Excel format
@other_actions.route('/print_journal', methods=['POST'])
def print_journal():

    data = request.form
    document_number = data.get("document_number")
    company_code = data.get("company_code")
    year = str(data.get('year'))
    sum_debit = 0
    sum_credit = 0

    output_file = config.output_folder + str(uuid.uuid4()) + '.xlsx'
    # Load data file and all gl initial balance
    df = load_data(config.transactions_data_folder, config.filter_column, company_code, config.selected_columns,
                   config.amount_column, f"01/01/{year}", f"31/12/{year}", company_code, year, data.get('document_number'))
    
    if df.is_empty():
        return Response(f"Aucune correspondance pour la pièce {document_number}", 500)

    company_name = df["Company code Name"].to_list()[0]
    posting_date = df["Posting Date"].to_list()[0]
    date_comptable = datetime.strftime(posting_date, "%d/%m/%Y")
    period = datetime.strftime(posting_date, "%m/%Y")
    reference = df["Reference"].to_list()[0]
    journal = df["Désignation"].to_list()[0]


    start_row = 13

    with Workbook(output_file) as writer:
        worksheet = writer.add_worksheet()

        # Merge cells and add a title and some header description for both sheets
        number_fmt = writer.add_format({'num_format': '#,##0_);[Red](#,##0);'})
        merge_format = writer.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 11, 'border': 1})
        format_2 = writer.add_format({'bold': True, 'font_size': 11})

        # Journal Sheet
        worksheet.merge_range('A1:G1', f"{company_code} - {company_name}", writer.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 24}))

        # General Details
        worksheet.write('D2', f"Fiche Comptable", writer.add_format({'font_size':16}))
        worksheet.write('F4', f"Numero:", format_2)
        worksheet.write('F5', f"Date Comptable:", format_2)
        worksheet.write('F6', "Période:", format_2)
        worksheet.write('F7', "Reference:", format_2)
        worksheet.write('F8', "Journal", format_2)
        
        worksheet.write('G4', f"{document_number}")
        worksheet.write('G5', f"{date_comptable}")
        worksheet.write('G6', period)
        worksheet.write('G7', reference)
        worksheet.write('G8', journal)

        # Table Header
        worksheet.merge_range('A11:B11', "Details Compte Groupe", merge_format)
        worksheet.write('A12', "Compte Groupe", merge_format)
        worksheet.write('B12', "Libelle Compte Groupe", merge_format)
        
        worksheet.merge_range('C11:D11', "Details Compte OHADA", merge_format)
        worksheet.write('C12', "Compte OHADA", merge_format)
        worksheet.write('D12', "Libelle Compte OHADA", merge_format)

        worksheet.write('E11', "Narration/Description", merge_format)
        worksheet.write('E12', "Narration", merge_format)

        worksheet.merge_range('F11:G11', "Montant en Devise Locale", merge_format)
        worksheet.write('F12', "Debit", merge_format)
        worksheet.write('G12', "Credit", merge_format)

        # Table details
        for row in df.iter_rows(named=True):
            worksheet.write(f'A{start_row}', row["G/L Account"])
            worksheet.write(f'B{start_row}', row["G/L Acct Long Text"])
            worksheet.write(f'C{start_row}', row["Alternative Account No."])
            worksheet.write(f'D{start_row}', "")
            worksheet.write(f'E{start_row}', row["Text"])
            if row["Amount in local currency"] > 0:
                sum_debit += row["Amount in local currency"]
                worksheet.write(f'F{start_row}', row["Amount in local currency"], number_fmt)
            else:
                sum_credit += row["Amount in local currency"]
                worksheet.write(f'G{start_row}', abs(row["Amount in local currency"]), number_fmt)

            start_row += 1
        
        # Grand Total
        worksheet.write(f'E{start_row}', "Montant Total", merge_format)
        worksheet.write(f'F{start_row}', sum_debit, number_fmt)
        worksheet.write(f'G{start_row}', sum_credit, number_fmt)

        # Footer
        worksheet.write(f'A{start_row+2}', "Préparé Par:", format_2)
        worksheet.write(f'A{start_row+3}', "Nom:", format_2)
        worksheet.write(f'C{start_row+2}', "Vérifié Par:", format_2)
        worksheet.write(f'C{start_row+3}', "Nom:", format_2)
        worksheet.write(f'E{start_row+2}', "Autorisé par:", format_2)
        worksheet.write(f'E{start_row+3}', "Nom:", format_2)
        worksheet.write(f'G{start_row+2}', "Reçu Par:", format_2)
        worksheet.write(f'G{start_row+3}', "Nom Société:", format_2)

    return send_from_directory(directory=os.getcwd(), path=output_file, as_attachment=True), 200