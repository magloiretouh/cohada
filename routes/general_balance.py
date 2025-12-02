from flask import send_from_directory
import polars as pl
import calendar
import config
import os
import uuid
from datetime import datetime
from xlsxwriter import Workbook
from routes.customs_functions import *
import operator

def generate_bal_gen(data, bnk=False, cache_manager=None, cache_key=None):

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
    df = load_data(config.transactions_data_folder, config.filter_column, data.get("company_code"), config.selected_columns,
                   config.amount_column, start_date, end_date, data.get('company_code'), str(year), bank=bnk)

    if df.is_empty():
        with Workbook(output_file) as writer:
            df.write_excel(writer, worksheet="Empty", table_style="Table Style Light 10",
                                    autofit=True, autofilter=False)
        # Mettre en cache si le cache_manager est fourni
        if cache_manager and cache_key:
            cache_manager.set_cache(cache_key, output_file)
        return send_from_directory(directory=os.getcwd(), path=output_file, as_attachment=True), 200
    
    df_initial_balance = load_initial_balance_mapping_data(config.initial_balance_file_path,
                                                                            config.debit_column_label,
                                                                            config.credit_column_label,
                                                                            data.get("company_code"),
                                                                            data.get("year"), bank=bnk)


    general_balance_mapping = fetch_general_balance_mapping_data()
    unique_values_general = sorted(df_initial_balance[config.SYSCOHADA_column_in_initial_balance].unique().to_list())
    unique_values_details = df_initial_balance.select([config.IFRS_code_column_in_initial_balance, config.SYSCOHADA_column_in_initial_balance]).unique()
    unique_values_details = unique_values_details.sort([
        config.SYSCOHADA_column_in_initial_balance,
        config.IFRS_code_column_in_initial_balance
    ])

    start_row = 6

    with Workbook(output_file) as writer:
        pl.DataFrame().write_excel(writer, worksheet="Balance General Format")
        pl.DataFrame().write_excel(writer, worksheet="Balance General Format Detail")

        # Access the workbook and worksheet_details
        worksheet_general = writer.get_worksheet_by_name("Balance General Format")
        worksheet_details = writer.get_worksheet_by_name("Balance General Format Detail")

        # Merge cells and add a title and some header description for both sheets
        number_fmt = writer.add_format({'num_format': '#,##0_);[Red](#,##0);'})
        merge_format = writer.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 9, 'border': 2})
        merge_format_2 = writer.add_format({'align': 'center', 'valign': 'vcenter', 'font_size': 14, 'border': 2})
        merge_format_3 = writer.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 9, 'border': 2, 'bg_color': '#00b6e9', 'font_color': 'white'})
        merge_format_4 = writer.add_format({'bold': True, 'font_size': 12})
        # Subtotal formats: bold and highlighted
        subtotal_label_fmt = writer.add_format({'bold': True, 'bg_color': '#F0F8FF'})
        subtotal_number_fmt = writer.add_format({'bold': True, 'num_format': '#,##0_);[Red](#,##0);', 'bg_color': '#F0F8FF'})

        # GENERAL SHEET HEADER
        worksheet_general.merge_range('A2:A3', f"{data.get('company_code')}", merge_format)
        worksheet_general.merge_range('B2:B3', f"{data.get('company_name', '')}", merge_format)
        worksheet_general.merge_range('C2:H2', f"BALANCE COMPTABILITE GENERALE", merge_format_2)
        worksheet_general.merge_range('C3:H3', f"En FCFA du {start_date} Au {end_date}", merge_format)
        worksheet_general.merge_range('I2:I3', f"Date Printed:", merge_format)
        worksheet_general.merge_range('J2:J3', f"{formatted_date}", merge_format)

        worksheet_general.merge_range('A4:A5', f"COMPTE", merge_format_3)
        worksheet_general.merge_range('B4:B5', f"LIBELLE", merge_format_3)
        worksheet_general.merge_range('C4:D4', f"A NOUVEAU", merge_format_3)
        worksheet_general.write('C5', f"DEBIT", merge_format_3)
        worksheet_general.write('D5', f"CREDIT", merge_format_3)
        worksheet_general.merge_range('E4:F4', f"MOUVEMENTS", merge_format_3)
        worksheet_general.write('E5', f"DEBIT", merge_format_3)
        worksheet_general.write('F5', f"CREDIT", merge_format_3)
        worksheet_general.merge_range('G4:H4', f"CUMULS", merge_format_3)
        worksheet_general.write('G5', f"DEBIT", merge_format_3)
        worksheet_general.write('H5', f"CREDIT", merge_format_3)
        worksheet_general.merge_range('I4:J4', f"SOLDE", merge_format_3)
        worksheet_general.write('I5', f"DEBIT", merge_format_3)
        worksheet_general.write('J5', f"CREDIT", merge_format_3)

        # DETAILS SHEET HEADER

        worksheet_details.merge_range('B2:D3', f"{data.get('company_code')}", merge_format)
        worksheet_details.merge_range('E2:G3', f"{data.get('company_name', '')}", merge_format)
        worksheet_details.merge_range('H2:P2', f"BALANCE COMPTABILITE GENERALE", merge_format_2)
        worksheet_details.merge_range('H3:P3', f"En FCFA du {start_date} Au {end_date}", merge_format)
        worksheet_details.merge_range('Q2:Q3', f"Date Printed:", merge_format)
        worksheet_details.merge_range('R2:R3', f"{formatted_date}", merge_format)
        
        worksheet_details.merge_range('B4:B5', f"0L GL", merge_format_3)
        worksheet_details.merge_range('C4:C5', f"0L GL Description", merge_format_3)
        worksheet_details.merge_range('D4:D5', f"COMPTE", merge_format_3)
        worksheet_details.merge_range('E4:E5', f"Alternative GL Description", merge_format_3)
        worksheet_details.merge_range('F4:F5', f"Account Group", merge_format_3)
        worksheet_details.merge_range('G4:G5', f"Class", merge_format_3)
        worksheet_details.merge_range('H4:H5', f"Cumulative Balance", merge_format_3)
        worksheet_details.merge_range('I4:I5', f"Total Debit Postings", merge_format_3)
        worksheet_details.merge_range('J4:J5', f"Total Credit Postings", merge_format_3)
        worksheet_details.merge_range('K4:L4', f"A NOUVEAU", merge_format_3)
        worksheet_details.write('K5', f"DEBIT", merge_format_3)
        worksheet_details.write('L5', f"CREDIT", merge_format_3)
        worksheet_details.merge_range('M4:N4', f"MOUVEMENTS", merge_format_3)
        worksheet_details.write('M5', f"DEBIT", merge_format_3)
        worksheet_details.write('N5', f"CREDIT", merge_format_3)
        worksheet_details.merge_range('O4:P4', f"CUMULS", merge_format_3)
        worksheet_details.write('O5', f"DEBIT", merge_format_3)
        worksheet_details.write('P5', f"CREDIT", merge_format_3)
        worksheet_details.merge_range('Q4:R4', f"SOLDE", merge_format_3)
        worksheet_details.write('Q5', f"DEBIT", merge_format_3)
        worksheet_details.write('R5', f"CREDIT", merge_format_3)

        gen_init_balance_credit_all = {"bilan": 0, "gestion": 0}
        gen_init_balance_debit_all = {"bilan": 0, "gestion": 0}
        mvts_sum_credit_all = {"bilan": 0, "gestion": 0}
        mvts_sum_debit_all = {"bilan": 0, "gestion": 0}
        cum_credit_all = {"bilan": 0, "gestion": 0}
        cum_debit_all = {"bilan": 0, "gestion": 0}
        solde_credit_all = {"bilan": 0, "gestion": 0}
        solde_debit_all = {"bilan": 0, "gestion": 0}

        # ADD values to general table
        # We'll insert subtotal rows grouped by the first 2 characters (level2) and first character (level1) of the COMPTE (value)
        group2_accumulators = {}
        group1_accumulators = {}

        # helper to init accumulator
        def _init_group():
            return {
                'gl_debit': 0,
                'gl_credit': 0,
                'mvts_debit': 0,
                'mvts_credit': 0,
                'cum_debit': 0,
                'cum_credit': 0,
                'solde_debit': 0,
                'solde_credit': 0,
            }

        for idx, value in enumerate(unique_values_general):
            if value == "OHADA VIDES":
                continue

            key2 = str(value)[:2] if value is not None else ''
            key1 = str(value)[:1] if value is not None else ''

            if key2 not in group2_accumulators:
                group2_accumulators[key2] = _init_group()
            if key1 not in group1_accumulators:
                group1_accumulators[key1] = _init_group()

            filtered_df = df.filter(pl.col(config.SYSCOHADA_column_in_main_data) == str(value))
            worksheet_general.write(f"A{str(start_row)}", str(value))

            # Find a row which matches with current OHADA GL
            matching_rows = df_initial_balance.filter(pl.col(config.SYSCOHADA_column_in_initial_balance) == str(value))
            gl_debit_balance = 0
            gl_credit_balance = 0
            gl_desc = ""

            # check if SYSCOHADA Account start with 6 or 7 so we can put it's starting balance as zero (0)
            if not matching_rows.is_empty():
                gl_desc = matching_rows[config.SYSCOHADA_desc_column_in_initial_balance][0]
                if str(value)[0] not in ['6', '7', '8'] :
                    gl_debit_balance = matching_rows["Soldes débiteurs"].sum()
                    gl_credit_balance = -(abs(matching_rows["Soldes créditeurs"].sum()))

            # Determine whether 'value' belongs to 'gestion' or 'bilan'
            key = 'gestion' if str(value)[0] in {'6', '7', '8'} else 'bilan'

            gen_init_balance_credit_all[key] += gl_credit_balance
            gen_init_balance_debit_all[key] += gl_debit_balance

            worksheet_general.write(f"B{str(start_row)}", gl_desc)
            worksheet_general.write(f"C{str(start_row)}", "" if gl_debit_balance == 0 else gl_debit_balance, number_fmt)
            worksheet_general.write(f"D{str(start_row)}", "" if abs(gl_credit_balance) == 0 else abs(gl_credit_balance), number_fmt)

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
            mvts_sum_credit_all[key] += mvts_cum_credit
            mvts_sum_debit_all[key] += mvts_cum_debit

            worksheet_general.write(f"E{str(start_row)}", "" if mvts_cum_debit == 0 else mvts_cum_debit, number_fmt)
            worksheet_general.write(f"F{str(start_row)}", "" if abs(mvts_cum_credit) == 0 else abs(mvts_cum_credit), number_fmt)

            cum_credit = mvts_cum_credit + gl_credit_balance
            cum_debit = mvts_cum_debit + gl_debit_balance
            cum_credit_all[key] += cum_credit
            cum_debit_all[key] += cum_debit

            worksheet_general.write(f"G{str(start_row)}", "" if cum_debit == 0 else cum_debit, number_fmt)
            worksheet_general.write(f"H{str(start_row)}", "" if abs(cum_credit) == 0 else abs(cum_credit), number_fmt)

            solde = cum_credit + cum_debit

            if solde > 0:
                solde_debit_all[key] += solde
                worksheet_general.write(f"I{str(start_row)}", abs(solde), number_fmt)
            else:
                solde_credit_all[key] += solde
                worksheet_general.write(f"J{str(start_row)}", abs(solde), number_fmt)

            # accumulate into both group totals (explicit variables)
            g2 = group2_accumulators[key2]
            g1 = group1_accumulators[key1]
            g2['gl_debit'] += gl_debit_balance
            g1['gl_debit'] += gl_debit_balance
            g2['gl_credit'] += gl_credit_balance
            g1['gl_credit'] += gl_credit_balance
            g2['mvts_debit'] += mvts_cum_debit
            g1['mvts_debit'] += mvts_cum_debit
            g2['mvts_credit'] += mvts_cum_credit
            g1['mvts_credit'] += mvts_cum_credit
            g2['cum_debit'] += cum_debit
            g1['cum_debit'] += cum_debit
            g2['cum_credit'] += cum_credit
            g1['cum_credit'] += cum_credit
            if solde > 0:
                g2['solde_debit'] += solde
                g1['solde_debit'] += solde
            else:
                g2['solde_credit'] += solde
                g1['solde_credit'] += solde

            start_row += 1

            # determine next group's keys (peek ahead) and write subtotals if group changes
            next_key2 = None
            next_key1 = None
            for j in range(idx + 1, len(unique_values_general)):
                nv = unique_values_general[j]
                if nv == "OHADA VIDES":
                    continue
                next_key2 = str(nv)[:2] if nv is not None else ''
                next_key1 = str(nv)[:1] if nv is not None else ''
                break

            # when level2 group ends, write level2 subtotal
            if next_key2 != key2:
                subtotal2 = group2_accumulators[key2]
                desc2 = general_balance_mapping.get(key2, "")
                # Highlight the entire subtotal row
                worksheet_general.set_row(start_row - 1, None, subtotal_label_fmt)
                worksheet_general.write(f"A{str(start_row)}", "")
                worksheet_general.write(f"B{str(start_row)}", f"{key2}-{desc2}", subtotal_label_fmt)
                worksheet_general.write(f"C{str(start_row)}", "" if subtotal2['gl_debit'] == 0 else subtotal2['gl_debit'], subtotal_number_fmt)
                worksheet_general.write(f"D{str(start_row)}", "" if abs(subtotal2['gl_credit']) == 0 else abs(subtotal2['gl_credit']), subtotal_number_fmt)
                worksheet_general.write(f"E{str(start_row)}", "" if subtotal2['mvts_debit'] == 0 else subtotal2['mvts_debit'], subtotal_number_fmt)
                worksheet_general.write(f"F{str(start_row)}", "" if abs(subtotal2['mvts_credit']) == 0 else abs(subtotal2['mvts_credit']), subtotal_number_fmt)
                worksheet_general.write(f"G{str(start_row)}", "" if subtotal2['cum_debit'] == 0 else subtotal2['cum_debit'], subtotal_number_fmt)
                worksheet_general.write(f"H{str(start_row)}", "" if abs(subtotal2['cum_credit']) == 0 else abs(subtotal2['cum_credit']), subtotal_number_fmt)
                worksheet_general.write(f"I{str(start_row)}", "" if subtotal2['solde_debit'] == 0 else abs(subtotal2['solde_debit']), subtotal_number_fmt)
                worksheet_general.write(f"J{str(start_row)}", "" if subtotal2['solde_credit'] == 0 else abs(subtotal2['solde_credit']), subtotal_number_fmt)
                start_row += 1

            # when level1 group ends, write level1 subtotal just below level2 subtotals
            if next_key1 != key1:
                subtotal1 = group1_accumulators[key1]
                desc1 = general_balance_mapping.get(key1, "")
                worksheet_general.set_row(start_row - 1, None, subtotal_label_fmt)
                worksheet_general.write(f"A{str(start_row)}", "")
                worksheet_general.write(f"B{str(start_row)}", f"{key1}-{desc1}", subtotal_label_fmt)
                worksheet_general.write(f"C{str(start_row)}", "" if subtotal1['gl_debit'] == 0 else subtotal1['gl_debit'], subtotal_number_fmt)
                worksheet_general.write(f"D{str(start_row)}", "" if abs(subtotal1['gl_credit']) == 0 else abs(subtotal1['gl_credit']), subtotal_number_fmt)
                worksheet_general.write(f"E{str(start_row)}", "" if subtotal1['mvts_debit'] == 0 else subtotal1['mvts_debit'], subtotal_number_fmt)
                worksheet_general.write(f"F{str(start_row)}", "" if abs(subtotal1['mvts_credit']) == 0 else abs(subtotal1['mvts_credit']), subtotal_number_fmt)
                worksheet_general.write(f"G{str(start_row)}", "" if subtotal1['cum_debit'] == 0 else subtotal1['cum_debit'], subtotal_number_fmt)
                worksheet_general.write(f"H{str(start_row)}", "" if abs(subtotal1['cum_credit']) == 0 else abs(subtotal1['cum_credit']), subtotal_number_fmt)
                worksheet_general.write(f"I{str(start_row)}", "" if subtotal1['solde_debit'] == 0 else abs(subtotal1['solde_debit']), subtotal_number_fmt)
                worksheet_general.write(f"J{str(start_row)}", "" if subtotal1['solde_credit'] == 0 else abs(subtotal1['solde_credit']), subtotal_number_fmt)
                start_row += 1
                # reset level1 accumulator to avoid double-counting if desired
                group1_accumulators[key1] = _init_group()
        
        start_row+=2
        # Below Totals Rows
        worksheet_general.write(f"A{str(start_row)}", f"Comptes de Bilan", merge_format_4)
        worksheet_general.write(f"A{str(start_row+1)}", f"Comptes de Gestion", merge_format_4)
        worksheet_general.write(f"A{str(start_row+2)}", f"Grand Total General", merge_format_4)
        # Comptes de Bilan values row
        worksheet_general.write(f"C{str(start_row)}", abs(gen_init_balance_debit_all['bilan']), number_fmt)
        worksheet_general.write(f"D{str(start_row)}", abs(gen_init_balance_credit_all['bilan']), number_fmt)
        worksheet_general.write(f"E{str(start_row)}", abs(mvts_sum_debit_all['bilan']), number_fmt)
        worksheet_general.write(f"F{str(start_row)}", abs(mvts_sum_credit_all['bilan']), number_fmt)
        worksheet_general.write(f"G{str(start_row)}", abs(cum_debit_all['bilan']), number_fmt)
        worksheet_general.write(f"H{str(start_row)}", abs(cum_credit_all['bilan']), number_fmt)
        worksheet_general.write(f"I{str(start_row)}", abs(solde_debit_all['bilan']), number_fmt)
        worksheet_general.write(f"J{str(start_row)}", abs(solde_credit_all['bilan']), number_fmt)
        # Comptes de gestion values row
        worksheet_general.write(f"C{str(start_row+1)}", abs(gen_init_balance_debit_all['gestion']), number_fmt)
        worksheet_general.write(f"D{str(start_row+1)}", abs(gen_init_balance_credit_all['gestion']), number_fmt)
        worksheet_general.write(f"E{str(start_row+1)}", abs(mvts_sum_debit_all['gestion']), number_fmt)
        worksheet_general.write(f"F{str(start_row+1)}", abs(mvts_sum_credit_all['gestion']), number_fmt)
        worksheet_general.write(f"G{str(start_row+1)}", abs(cum_debit_all['gestion']), number_fmt)
        worksheet_general.write(f"H{str(start_row+1)}", abs(cum_credit_all['gestion']), number_fmt)
        worksheet_general.write(f"I{str(start_row+1)}", abs(solde_debit_all['gestion']), number_fmt)
        worksheet_general.write(f"J{str(start_row+1)}", abs(solde_credit_all['gestion']), number_fmt)
        # Grand total values row
        worksheet_general.write(f"C{str(start_row+2)}", abs(sum(gen_init_balance_debit_all.values())), number_fmt)
        worksheet_general.write(f"D{str(start_row+2)}", abs(sum(gen_init_balance_credit_all.values())), number_fmt)
        worksheet_general.write(f"E{str(start_row+2)}", abs(sum(mvts_sum_debit_all.values())), number_fmt)
        worksheet_general.write(f"F{str(start_row+2)}", abs(sum(mvts_sum_credit_all.values())), number_fmt)
        worksheet_general.write(f"G{str(start_row+2)}", abs(sum(cum_debit_all.values())), number_fmt)
        worksheet_general.write(f"H{str(start_row+2)}", abs(sum(cum_credit_all.values())), number_fmt)
        worksheet_general.write(f"I{str(start_row+2)}", abs(sum(solde_debit_all.values())), number_fmt)
        worksheet_general.write(f"J{str(start_row+2)}", abs(sum(solde_credit_all.values())), number_fmt)
    
        start_row = 6
        # ADD values to details table
        for row in unique_values_details.iter_rows():
            ifrs_value, value = row  # Unpack values
            filtered_df = df.filter((pl.col(config.SYSCOHADA_column_in_main_data) == str(value)) & (pl.col("G/L Account") == str(ifrs_value)))
            worksheet_details.write(f"D{str(start_row)}", str(value))
            
            # Find a row which matches with current OHADA GL
            matching_rows = df_initial_balance.filter((pl.col(config.SYSCOHADA_column_in_initial_balance) == str(value)) & 
                                                      (pl.col(config.IFRS_code_column_in_initial_balance) == str(ifrs_value)))
            gl_debit_balance = 0
            gl_credit_balance = 0
            gl_desc = ""

            # check if SYSCOHADA Account start with 6 or 7 so we can put it's starting balance as zero (0)
            if not matching_rows.is_empty():
                gl_desc = matching_rows[config.SYSCOHADA_desc_column_in_initial_balance][0]
                worksheet_details.write(f"B{str(start_row)}", matching_rows['Numéro de compte IFRS'][0])
                worksheet_details.write(f"C{str(start_row)}", matching_rows['Intitulé de compte IFRS'][0])
                if str(value)[0] not in ['6', '7', '8'] :
                    gl_debit_balance = matching_rows["Soldes débiteurs"].sum()
                    gl_credit_balance = -(abs(matching_rows["Soldes créditeurs"].sum()))
                        
            worksheet_details.write(f"E{str(start_row)}", gl_desc)
            worksheet_details.write(f"K{str(start_row)}", "" if gl_debit_balance == 0 else gl_debit_balance, number_fmt)
            worksheet_details.write(f"L{str(start_row)}", "" if abs(gl_credit_balance) == 0 else abs(gl_credit_balance), number_fmt)
            
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

            worksheet_details.write(f"M{str(start_row)}", "" if mvts_cum_debit == 0 else mvts_cum_debit, number_fmt)
            worksheet_details.write(f"N{str(start_row)}", "" if abs(mvts_cum_credit) == 0 else abs(mvts_cum_credit), number_fmt)
            
            worksheet_details.write(f"I{str(start_row)}", "" if mvts_cum_debit == 0 else mvts_cum_debit, number_fmt)
            worksheet_details.write(f"J{str(start_row)}", "" if abs(mvts_cum_credit) == 0 else abs(mvts_cum_credit), number_fmt)

            cum_credit = mvts_cum_credit + gl_credit_balance
            cum_debit = mvts_cum_debit + gl_debit_balance

            worksheet_details.write(f"O{str(start_row)}", "" if cum_debit == 0 else cum_debit, number_fmt)
            worksheet_details.write(f"P{str(start_row)}", "" if abs(cum_credit) == 0 else abs(cum_credit), number_fmt)

            solde = cum_credit + cum_debit

            if solde > 0:
                worksheet_details.write(f"Q{str(start_row)}", abs(solde), number_fmt)
            else:
                worksheet_details.write(f"R{str(start_row)}", abs(solde), number_fmt)
            
            worksheet_details.write(f"H{str(start_row)}", solde, number_fmt)

            start_row += 1

    # Mettre en cache si le cache_manager est fourni
    if cache_manager and cache_key:
        cache_manager.set_cache(cache_key, output_file)

    return send_from_directory(directory=os.getcwd(), path=output_file, as_attachment=True), 200