from flask import send_from_directory
import polars as pl
import calendar
import config
import os
import uuid
from xlsxwriter import Workbook
from routes.customs_functions import *

def generate_gl_bp(data, bp_type, cache_manager=None, cache_key=None):
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

    unique_values = df_initial_balance.select([bp_type, f"{bp_type} Name", "Total"]).unique()
    unique_values.sort(bp_type)
    pd_dfs = []

    # Set locale to French
    french_months = {
        1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
        5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
        9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre"
    }

    output_file = config.output_folder + str(uuid.uuid4()) + '.xlsx'

    with Workbook(output_file) as writer:
        for row in unique_values.iter_rows():
            value, bp_name, bp_balance = row  # Unpack values
            filtered_df = df.filter(pl.col(bp_type) == str(value))
            if filtered_df.is_empty():
                continue

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
            filtered_df = filtered_df.rename(config.vendor_renamed_columns) if bp_type == "Vendor" else filtered_df.rename(config.customer_renamed_columns)
            
            # Fill empty values in column Libellé with corresponding values from column Référence
            filtered_df = filtered_df.with_columns(
                pl.when(pl.col("Libellé").is_null() & pl.col("Référence").is_null())
                .then(pl.lit(""))
                .otherwise(pl.col("Libellé").fill_null(pl.col("Référence")))  # Fill Libellé with Référence
                .alias("Libellé")
            )

            filtered_df = filtered_df.with_columns([
                (pl.lit(bp_balance) + 
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
                "Date": ["TOTAL"],
                "Type de pièce": [f"{len(filtered_df)} ligne(s)"],
                "Débit": [filtered_df["Débit"].sum()],
                "Crédit": [filtered_df["Crédit"].sum()],
                "Solde": [filtered_df["Solde"].to_list()[-1]],
            })

            ledger_df = pl.concat([ledger_df, total_balance], how="diagonal")

            # Drop the temporary "Month" column
            ledger_df.drop(["Month"])
            filtered_df = ledger_df.clone()
            del ledger_df

            # Add thousand separator for columns 'debit', 'credit' and 'solde'
            filtered_df = filtered_df.with_columns([
                pl.col("Débit")
                .map_elements(lambda x: f"{x:,.0f}".replace(",", " ") if isinstance(x, (int, float)) else "", return_dtype=pl.Utf8),
                
                pl.col("Crédit")
                .map_elements(lambda x: f"{abs(x):,.0f}".replace(",", " ") if isinstance(x, (int, float)) else "", return_dtype=pl.Utf8),
                
                pl.col("Solde")
                .map_elements(lambda x: f"C {abs(x):,.0f}".replace(",", " ") if x < 0 else f"D {x:,.0f}".replace(",", " "), return_dtype=pl.Utf8)
            ])

            # Reorder columns
            filtered_df = filtered_df[config.vendor_reordered_columns] if bp_type == "Vendor" else filtered_df[config.customer_reordered_columns]

            # Add initial and closed balance rows in the tables
            closed_balance = filtered_df["Solde"].to_list()[-2]
            company_name = filtered_df["Nom Entreprise"].to_list()[0]
            initial_balance_row = {"Code Entreprise": data.get("company_code"), "Nom Entreprise": company_name, "Année Fiscale": "",
                                   "Date": start_date, "Type de pièce": "",
                                   "Désignation Type de pièce": "", "Pièce": "",
                                   "Référence": "", "Débit": "", "Crédit": "",
                                   "Solde": f"C {abs(bp_balance):,.0f}".replace(",",
                                                                                     " ") if bp_balance < 0 else f"D {bp_balance:,.0f}".replace(
                                       ",", " "), "Libellé": "REPORT AU " + start_date}
            
            close_balance_row = {"Code Entreprise": data.get("company_code"), "Nom Entreprise": company_name, "Année Fiscale": "",
                                 "Date": end_date, "Type de pièce": "SOLDE",
                                 "Désignation Type de pièce": "", "Pièce": "",
                                 "Référence": "", "Débit": "", "Crédit": "", "Solde": f"{closed_balance}",
                                 "Libellé": "SOLDE AU " + end_date}

            filtered_df = pl.concat([pl.DataFrame([initial_balance_row]), filtered_df], how="diagonal")
            filtered_df = pl.concat([filtered_df, pl.DataFrame([close_balance_row])], how="diagonal")

            # Save transformation to output sheet
            filtered_df.write_excel(writer, worksheet=str(value), table_style="Table Style Light 10", 
                                    autofit=True, autofilter=False,
                                    position=(6, 0))

            # Append one GL table to all GL table
            pd_dfs.append({"df": filtered_df, "name": value, "desc": bp_name})
            
            # Access the workbook and worksheet
            sheet_name = str(value)
            workbook = writer
            worksheet = writer.get_worksheet_by_name(sheet_name)

            # Merge cells and add a title and some header description
            merge_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 14})
            worksheet.merge_range('A1:K1', f"{company_name}", merge_format)  # Adjust column range as needed
            worksheet.merge_range('A3:K3', f"{'Fournisseur' if bp_type == 'Vendor' else 'Client'} <{value}> {bp_name}", merge_format)  # Adjust column range as needed
            worksheet.merge_range('A4:K4', f"Grand-livre {'Fournisseurs' if bp_type == 'Vendor' else 'Clients'} du {start_date} au {end_date}",
                                  merge_format)  # Adjust column range as needed

        # Concatenate all DataFrames and save to sheet Grand Livre
        current_row = 1

        pl.DataFrame().write_excel(writer, worksheet=f"Grand Livre {'Fournisseurs' if bp_type == 'Vendor' else 'Clients'}")

        for gl_df in pd_dfs:
            # Access the workbook and worksheet
            workbook = writer
            worksheet = writer.get_worksheet_by_name(f"Grand Livre {'Fournisseurs' if bp_type == 'Vendor' else 'Clients'}")

            # Merge cells and add a title and some header description
            merge_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 14})
            if current_row == 1:
                worksheet.merge_range(f'A{current_row}:K{current_row}', f"{company_name}",
                                      merge_format)  # Adjust column range as needed

            worksheet.merge_range(f'A{current_row + 2}:K{current_row + 2}', f"Compte <{gl_df['name']}> {gl_df['desc']}",
                                  merge_format)  # Adjust column range as needed
            worksheet.merge_range(f'A{current_row + 3}:K{current_row + 3}',
                                  f"Grand Livre {'Fournisseurs' if bp_type == 'Vendor' else 'Clients'} du {start_date} au {end_date}",
                                  merge_format)  # Adjust column range as needed
            # Write the DataFrame
            gl_df['df'].write_excel(writer, worksheet=f"Grand Livre {'Fournisseurs' if bp_type == 'Vendor' else 'Clients'}",  table_style="Table Style Light 10", 
                                    autofilter=False, position=(current_row + 5, 0))

            # Update the current row to write the next DataFrame below
            current_row += len(gl_df['df']) + 7  # Add 2 rows spaces between DataFrames

        # Concatenate all DataFrames and save to sheet Grand Livre starting at row 1
        current_row = 1

    # Mettre en cache si le cache_manager est fourni
    if cache_manager and cache_key:
        cache_manager.set_cache(cache_key, output_file)

    return send_from_directory(directory=os.getcwd(), path=output_file, as_attachment=True), 200

