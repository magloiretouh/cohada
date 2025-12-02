from flask import send_from_directory
import polars as pl
import calendar
import config
import os
import uuid
from datetime import datetime
from xlsxwriter import Workbook
from routes.customs_functions import *

def generate_bal_bp(data, bp_type, cache_manager=None, cache_key=None):

    output_file = config.output_folder + str(uuid.uuid4()) + '.xlsx'

    # Define French month abbreviations
    french_months = {
        1: "Jan", 2: "Fev", 3: "Mar", 4: "Avr",
        5: "Mai", 6: "Juin", 7: "Juil", 8: "Aou",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    # Get current date and format it as DD-MMM-YYYY
    now = datetime.now()
    formatted_date = f"{now.day:02d}-{french_months[now.month]}-{now.year}"

    # Get year and months sent by user
    year = int(data.get('year'))
    start_date = f"01/{int(data.get('start_month')):02d}/{year}"
    end_date = f"{calendar.monthrange(year, int(data.get('end_month')))[1]}/{int(data.get('end_month')):02d}/{year}"

    # Load data file and all gl initial balance
    folder_path = config.vendors_transactions_data_folder if bp_type == "Vendor" else config.customers_transactions_data_folder
    initial_balance_file_path = config.vendor_initial_balance_file_path if bp_type == "Vendor" else config.customer_initial_balance_file_path
    df = load_bp_data(folder_path, config.filter_column, data.get("company_code"),
                      config.vendor_selected_columns if bp_type=="Vendor" else config.customer_selected_columns,
                      start_date, end_date, data.get('company_code'), str(year), bp_type)
    df_initial_balance = load_bp_initial_balance(initial_balance_file_path,
                                                "Total",
                                                data.get("company_code"),
                                                data.get("year"), bp_type)

    bp_unique_values = df_initial_balance.select([bp_type, f"{bp_type} Name", "Total"]).unique()
    bp_unique_values.sort(bp_type)

    start_row = 6

    with Workbook(output_file) as writer:
        pl.DataFrame().write_excel(writer, worksheet=f"Balance General Format {bp_type}")

        # Access the workbook and worksheet_details
        worksheet = writer.get_worksheet_by_name(f"Balance General Format {bp_type}")

        # Merge cells and add a title and some header description for both sheets
        number_fmt = writer.add_format({'num_format': '#,##0_);[Red](#,##0);'})
        merge_format = writer.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 9, 'border': 2})
        merge_format_2 = writer.add_format({'align': 'center', 'valign': 'vcenter', 'font_size': 14, 'border': 2})
        merge_format_3 = writer.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 9, 'border': 2, 'bg_color': '#00b6e9', 'font_color': 'white'})
        merge_format_4 = writer.add_format({'bold': True, 'font_size': 12})

        # GENERAL SHEET HEADER
        worksheet.write('A2', f"{data.get('company_code')}", merge_format)
        worksheet.write('A3', f"COMPTE: {'411101' if bp_type == 'Customer' else '401100'}", merge_format)
        worksheet.merge_range('B2:B3', f"{data.get('company_name', '')}", merge_format)
        worksheet.merge_range('C2:H2', f"BALANCE COMPTABILITE - {'CLIENTS' if bp_type == 'Customer' else 'FOURNISSEURS'}", merge_format_2)
        worksheet.merge_range('C3:H3', f"En FCFA du {start_date} Au {end_date}", merge_format)
        worksheet.merge_range('I2:I3', f"Date Printed:", merge_format)
        worksheet.merge_range('J2:J3', f"{formatted_date}", merge_format)

        worksheet.merge_range('A4:A5', f"COMPTE", merge_format_3)
        worksheet.merge_range('B4:B5', f"LIBELLE", merge_format_3)
        worksheet.merge_range('C4:D4', f"A NOUVEAU", merge_format_3)
        worksheet.write('C5', f"DEBIT", merge_format_3)
        worksheet.write('D5', f"CREDIT", merge_format_3)
        worksheet.merge_range('E4:F4', f"MOUVEMENTS", merge_format_3)
        worksheet.write('E5', f"DEBIT", merge_format_3)
        worksheet.write('F5', f"CREDIT", merge_format_3)
        worksheet.merge_range('G4:H4', f"CUMULS", merge_format_3)
        worksheet.write('G5', f"DEBIT", merge_format_3)
        worksheet.write('H5', f"CREDIT", merge_format_3)
        worksheet.merge_range('I4:J4', f"SOLDE", merge_format_3)
        worksheet.write('I5', f"DEBIT", merge_format_3)
        worksheet.write('J5', f"CREDIT", merge_format_3)

        # TOTAL PARAMS
        gen_init_balance_credit_all = 0
        gen_init_balance_debit_all = 0
        mvts_sum_credit_all = 0
        mvts_sum_debit_all = 0
        cum_credit_all = 0
        cum_debit_all = 0
        solde_credit_all = 0
        solde_debit_all = 0

        # ADD values to general table
        for row in bp_unique_values.iter_rows():
            value, bp_name, bp_balance = row  # Unpack values
            filtered_df = df.filter(pl.col(bp_type) == str(value))
            worksheet.write(f"A{str(start_row)}", str(value))

            worksheet.write(f"B{str(start_row)}", bp_name)
            if bp_balance > 0:
                worksheet.write(f"C{str(start_row)}", bp_balance, number_fmt)
                gen_init_balance_debit_all += bp_balance
            else:
                gen_init_balance_credit_all += bp_balance
                worksheet.write(f"D{str(start_row)}", abs(bp_balance), number_fmt)

            # Create 'credit' and 'debit' columns based on 'amount' value and Contrepartie column
            filtered_df = filtered_df.with_columns([
                pl.when(pl.col(config.amount_column) <= 0)
                .then(pl.col(config.amount_column))
                .otherwise(0)
                .alias("Crédit"),

                pl.when(pl.col(config.amount_column) > 0)
                .then(pl.col(config.amount_column).abs())
                .otherwise(0)
                .alias("Débit")
            ])

            mvts_cum_credit = filtered_df['Crédit'].sum()
            mvts_cum_debit = filtered_df['Débit'].sum()

            worksheet.write(f"E{str(start_row)}", mvts_cum_debit, number_fmt)
            worksheet.write(f"F{str(start_row)}", abs(mvts_cum_credit), number_fmt)

            mvts_sum_credit_all += mvts_cum_credit # For below credit total
            mvts_sum_debit_all += mvts_cum_debit # For below debit total
            
            cum_debit = mvts_cum_debit + (bp_balance if bp_balance > 0 else 0)
            cum_credit = mvts_cum_credit + (bp_balance if bp_balance <= 0 else 0)

            cum_credit_all += cum_credit
            cum_debit_all += cum_debit

            worksheet.write(f"G{str(start_row)}", cum_debit, number_fmt)
            worksheet.write(f"H{str(start_row)}", abs(cum_credit), number_fmt)

            solde = cum_credit + cum_debit

            if solde > 0:
                solde_debit_all += solde
                worksheet.write(f"I{str(start_row)}", abs(solde), number_fmt)
            else:
                solde_credit_all += solde
                worksheet.write(f"J{str(start_row)}", abs(solde), number_fmt)
            
            start_row += 1
        
        # Below Totals Rows
        worksheet.write(f"B{str(start_row)}", f"Total à Reporter", merge_format_4)
        # Comptes de Bilan values row
        worksheet.write(f"C{str(start_row)}", abs(gen_init_balance_debit_all), number_fmt)
        worksheet.write(f"D{str(start_row)}", abs(gen_init_balance_credit_all), number_fmt)
        worksheet.write(f"E{str(start_row)}", abs(mvts_sum_debit_all), number_fmt)
        worksheet.write(f"F{str(start_row)}", abs(mvts_sum_credit_all), number_fmt)
        worksheet.write(f"G{str(start_row)}", abs(cum_debit_all), number_fmt)
        worksheet.write(f"H{str(start_row)}", abs(cum_credit_all), number_fmt)
        worksheet.write(f"I{str(start_row)}", abs(solde_debit_all), number_fmt)
        worksheet.write(f"J{str(start_row)}", abs(solde_credit_all), number_fmt)

    # Mettre en cache si le cache_manager est fourni
    if cache_manager and cache_key:
        cache_manager.set_cache(cache_key, output_file)

    return send_from_directory(directory=os.getcwd(), path=output_file, as_attachment=True), 200