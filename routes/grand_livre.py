from flask import send_from_directory
import polars as pl
import calendar
import config
import os
import uuid
from xlsxwriter import Workbook
from routes.customs_functions import *

def generate_gl_compta_gen(data, bnk=False, cache_manager=None, cache_key=None):

    output_file = config.output_folder + str(uuid.uuid4()) + '.xlsx'

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

    unique_values = df_initial_balance[config.SYSCOHADA_column_in_initial_balance].unique().to_list()
    unique_values.sort()
    pd_dfs_comptes_bilan = []
    pd_dfs_comptes_gestion = []

    # Set locale to French
    french_months = {
        1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
        5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
        9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre"
    }

    company_name = df["Company code Name"].to_list()[0]

    with Workbook(output_file) as writer:
        for value in unique_values:
            if value == "OHADA VIDES":
                continue
            filtered_df = df.filter(pl.col(config.SYSCOHADA_column_in_main_data) == str(value))

            empty_df = filtered_df.is_empty()

            # Merge data based on Offset GL to create contrepartie columns
            df_initial_balance_unique = df_initial_balance.unique(("Numéro de compte IFRS"), keep='first', maintain_order=True) # only keep one line for an IFRS account to avoid multiple rows generate from the below merging
            filtered_df = filtered_df.join(df_initial_balance_unique, left_on=config.offset_account_column_name,
                                   right_on="Numéro de compte IFRS", how="left")

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

            # Remove unwanted columns
            filtered_df = filtered_df.drop([config.amount_column, config.SYSCOHADA_column_in_main_data])

            # Rename columns to french for output
            filtered_df = filtered_df.rename(config.renamed_columns)

            # Find a row which matches with current OHADA GL
            matching_rows = df_initial_balance.filter(pl.col(config.SYSCOHADA_column_in_initial_balance) == value)
            gl_debit_balance = 0
            gl_credit_balance = 0
            initial_balance = 0
            gl_desc = ""

            # check if SYSCOHADA Account start with 6 or 7 so we can put it's starting balance as zero (0)
            if not matching_rows.is_empty():
                gl_desc = matching_rows[config.SYSCOHADA_desc_column_in_initial_balance][0]
                if str(value)[0] not in ['6', '7', '8'] :
                    gl_debit_balance = matching_rows["Soldes débiteurs"].sum()
                    gl_credit_balance = -(abs(matching_rows["Soldes créditeurs"].sum()))
            
            # Fill empty values in column Libellé with corresponding values from column Référence
            filtered_df = filtered_df.with_columns(
                pl.when(pl.col("Libellé").is_null() & pl.col("Référence").is_null())
                .then(pl.lit(gl_desc))  # SYSCOHADA Desc if both column are null
                .otherwise(pl.col("Libellé").fill_null(pl.col("Référence")))  # Fill Libellé with Référence
                .alias("Libellé")
            )

            # Add SYSCOHADA Code and Desc Columns
            filtered_df = filtered_df.with_columns(pl.lit(value).alias("Compte SYSCOHADA"))
            filtered_df = filtered_df.with_columns(pl.lit(gl_desc if gl_desc != None else "").alias("Compte SYSCOHADA Desc"))

            initial_balance = gl_credit_balance + gl_debit_balance

            filtered_df = filtered_df.with_columns([
                (pl.lit(initial_balance) + 
                pl.col("Crédit").cum_sum() + 
                pl.col("Débit").cum_sum()).alias("Solde")
            ])

            # Create new Month column to be able to group transactions of same month
            filtered_df = filtered_df.with_columns([
                pl.col("Date")
                .dt.strftime("%Y-%m")  # Format as Year-Month
                .alias("Month")
            ])

            # Convert the Date column to the desired format and overwrite it
            filtered_df = filtered_df.with_columns([
                pl.col("Date")
                .dt.strftime("%d/%m/%Y")  # Format to "day/month/year"
            ])

            # Create a new list to hold ledger year-months rows
            ledger_df = pl.DataFrame()

            # Process each month separately for the current ledger
            for month, group in filtered_df.group_by("Month", maintain_order=True):
                # Append all original rows of that month
                ledger_df = pl.concat([ledger_df, group], how="diagonal")

                # Compute monthly subtotal (sum of credit & debit, last balance)
                subtotal = pl.DataFrame({
                    "Date": ["Sous-Total"],
                    "Type de pièce": [french_months[int(month[0].split("-")[1])]],
                    "Débit": [group["Débit"].sum()],
                    "Crédit": [group["Crédit"].sum()],
                    "Solde": [group["Solde"].to_list()[-1]],  # Last balance of the month
                })

                # Append the subtotal row after the month
                ledger_df = pl.concat([ledger_df, subtotal], how="diagonal")

            # Compute and append total balance row
            total_balance = pl.DataFrame({
                "Code Entreprise": "",
                "Nom Entreprise": "",
                "Année Fiscale": "",
                "Compte IFRS": "",
                "Desc Compte IFRS": "",
                "Désignation Type de pièce": "", 
                "Pièce": "",
                "Référence": "", 
                "Libellé": "",
                "Contrepartie IFRS": "",
                "Contrepartie IFRS Desc": "",
                "Contrepartie SYSCOHADA": "",
                "Contrepartie SYSCOHADA Desc": "",
                "Compte SYSCOHADA": "",
                "Compte SYSCOHADA Desc": "",
                "Date": ["TOTAL"],
                "Type de pièce": [f"{len(filtered_df)} ligne(s)"],
                "Débit": [filtered_df["Débit"].sum()],
                "Crédit": [filtered_df["Crédit"].sum()],
                "Solde": [filtered_df["Solde"].to_list()[-1]] if not empty_df else float(initial_balance),
            })

            ledger_df = pl.concat([ledger_df, total_balance], how="diagonal")

            # Drop the temporary "Month" column
            ledger_df.drop(["Month"]) if not empty_df else ""
            filtered_df = ledger_df.clone() if not empty_df else pl.concat([filtered_df, total_balance], how="diagonal")
            filtered_df = filtered_df.with_columns(pl.col("Compte SYSCOHADA").cast(pl.Utf8))
            del ledger_df

            # Add thousand separator for columns 'debit', 'credit' and 'solde'
            filtered_df = filtered_df.with_columns([
                pl.col("Débit")
                .map_elements(lambda x: f"{x:,.0f}".replace(",", " ") if isinstance(x, (int, float)) else "", return_dtype=pl.Utf8),
                
                pl.col("Crédit")
                .map_elements(lambda x: f"{abs(x):,.0f}".replace(",", " ") if isinstance(x, (int, float)) else "", return_dtype=pl.Utf8),
                
                pl.col("Solde")
                .map_elements(lambda x: f"C {abs(x):,.0f}".replace(",", " ") if x <= 0 else f"D {x:,.0f}".replace(",", " "), return_dtype=pl.Utf8)
            ])

            # Reorder columns
            filtered_df = filtered_df[config.reordered_columns]

            # Add initial and closed balance rows in the tables
            closed_balance = filtered_df["Solde"].to_list()[-2] if not empty_df else f"C {abs(initial_balance):,.0f}".replace(",", " ") if initial_balance <= 0 else f"D {initial_balance:,.0f}".replace(",", " ")
            compte_ifrs = filtered_df["Compte IFRS"].to_list()[0] if not empty_df else matching_rows[config.IFRS_code_column_in_initial_balance][0]
            desc_compte_ifrs = filtered_df["Desc Compte IFRS"].to_list()[0] if not empty_df else matching_rows["Intitulé de compte IFRS"][0]
            initial_balance_row = {"Code Entreprise": data.get("company_code"), "Nom Entreprise": company_name, "Année Fiscale": "",
                                   "Compte SYSCOHADA": str(value), "Compte SYSCOHADA Desc": gl_desc, "Compte IFRS": compte_ifrs,
                                   "Desc Compte IFRS": desc_compte_ifrs, "Date": start_date, "Type de pièce": "",
                                   "Désignation Type de pièce": "", "Pièce": "",
                                   "Référence": "", "Débit": "", "Crédit": "",
                                   "Solde": f"C {abs(initial_balance):,.0f}".replace(",",
                                                                                     " ") if initial_balance <= 0 else f"D {initial_balance:,.0f}".replace(
                                       ",", " "), "Libellé": "REPORT AU " + start_date}
            
            close_balance_row = {"Code Entreprise": data.get("company_code"), "Nom Entreprise": company_name, "Année Fiscale": "",
                                 "Compte SYSCOHADA": str(value), "Compte SYSCOHADA Desc": gl_desc, "Compte IFRS": compte_ifrs,
                                 "Desc Compte IFRS": desc_compte_ifrs, "Date": end_date, "Type de pièce": "SOLDE",
                                 "Désignation Type de pièce": "", "Pièce": "",
                                 "Référence": "", "Débit": "", "Crédit": "", "Solde": f"{closed_balance}",
                                 "Libellé": "SOLDE AU " + end_date}

            filtered_df = pl.concat([pl.DataFrame([initial_balance_row]), filtered_df], how="diagonal_relaxed")
            filtered_df = pl.concat([filtered_df, pl.DataFrame([close_balance_row])], how="diagonal_relaxed")

            # Save transformation to output sheet
            filtered_df.write_excel(writer, worksheet=str(value), table_style="Table Style Light 10", 
                                    autofit=True, autofilter=False,
                                    position=(6, 0))

            # Append one GL table to all GL table
            if str(value)[0] in ['1', '2', '3', '4', '5']:
                pd_dfs_comptes_bilan.append({"df": filtered_df, "name": value, "desc": gl_desc})
            elif str(value)[0] in ['6', '7', '8']:
                pd_dfs_comptes_gestion.append({"df": filtered_df, "name": value, "desc": gl_desc})
            
            # Access the workbook and worksheet
            sheet_name = str(value)
            workbook = writer
            worksheet = writer.get_worksheet_by_name(sheet_name)

            # Merge cells and add a title and some header description
            merge_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 14})
            worksheet.merge_range('A1:K1', f"{company_name}", merge_format)  # Adjust column range as needed
            worksheet.merge_range('A3:K3', f"Compte <{value}> {gl_desc}", merge_format)  # Adjust column range as needed
            worksheet.merge_range('A4:K4', f"Grand-livre du {start_date} au {end_date}",
                                  merge_format)  # Adjust column range as needed

        # Concatenate all DataFrames and save to sheet Grand Livre
        current_row = 1

        pl.DataFrame().write_excel(writer, worksheet="Grand Livre - Comptes du bilan")
        pl.DataFrame().write_excel(writer, worksheet="Grand Livre-Comptes de gestion")

        for gl_df in pd_dfs_comptes_bilan:
            # Access the workbook and worksheet
            workbook = writer
            worksheet = writer.get_worksheet_by_name("Grand Livre - Comptes du bilan")

            # Merge cells and add a title and some header description
            merge_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 14})
            if current_row == 1:
                worksheet.merge_range(f'A{current_row}:K{current_row}', f"{company_name}",
                                      merge_format)  # Adjust column range as needed

            worksheet.merge_range(f'A{current_row + 2}:K{current_row + 2}', f"Compte <{gl_df['name']}> {gl_df['desc']}",
                                  merge_format)  # Adjust column range as needed
            worksheet.merge_range(f'A{current_row + 3}:K{current_row + 3}',
                                  f"Grand Livre - Comptes du bilan du {start_date} au {end_date}",
                                  merge_format)  # Adjust column range as needed
            # Write the DataFrame
            gl_df['df'].write_excel(writer, worksheet="Grand Livre - Comptes du bilan",  table_style="Table Style Light 10", 
                                    autofilter=False, position=(current_row + 5, 0))

            # Update the current row to write the next DataFrame below
            current_row += len(gl_df['df']) + 7  # Add 2 rows spaces between DataFrames

        # Concatenate all DataFrames and save to sheet Grand Livre starting at row 1
        current_row = 1

        for gl_df in pd_dfs_comptes_gestion:
            # Access the workbook and worksheet
            workbook = writer
            worksheet = writer.get_worksheet_by_name("Grand Livre-Comptes de gestion")

            # Merge cells and add a title and some header description
            merge_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 14})
            if current_row == 1:
                worksheet.merge_range(f'A{current_row}:K{current_row}', f"{company_name}",
                                      merge_format)  # Adjust column range as needed

            worksheet.merge_range(f'A{current_row + 2}:K{current_row + 2}', f"Compte <{gl_df['name']}> {gl_df['desc']}",
                                  merge_format)  # Adjust column range as needed
            worksheet.merge_range(f'A{current_row + 3}:K{current_row + 3}',
                                  f"Grand Livre - Comptes de gestion du {start_date} au {end_date}",
                                  merge_format)  # Adjust column range as needed
            # Write the DataFrame
            gl_df['df'].write_excel(writer, worksheet="Grand Livre-Comptes de gestion",  table_style="Table Style Light 10", 
                                    autofilter=False, position=(current_row + 5, 0))

            # Update the current row to write the next DataFrame below
            current_row += len(gl_df['df']) + 7  # Add 2 rows spaces between DataFrames

    # Mettre en cache si le cache_manager est fourni
    if cache_manager and cache_key:
        cache_manager.set_cache(cache_key, output_file)

    return send_from_directory(directory=os.getcwd(), path=output_file, as_attachment=True), 200

