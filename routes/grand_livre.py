from flask import send_from_directory
import polars as pl
import calendar
import config
import os
import uuid
from xlsxwriter import Workbook
from routes.customs_functions import *
from layout_manager import LayoutManager
import time
import logging

# Configure logging for timing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def generate_gl_compta_gen(data, bnk=False, cache_manager=None, cache_key=None, layout_type=None):
    # ============================================================================
    # TIMING: Start request timer
    # ============================================================================
    request_start_time = time.time()
    timing_stages = {}

    stage_start = time.time()

    output_file = config.output_folder + str(uuid.uuid4()) + '.xlsx'

    # Initialize layout manager for this company
    company_code = data.get('company_code')
    layout_manager = LayoutManager()

    timing_stages['initialization'] = time.time() - stage_start

    # Get year and months sent by user
    year = int(data.get('year'))
    start_date = f"01/{int(data.get('start_month')):02d}/{year}"
    end_date = f"{calendar.monthrange(year, int(data.get('end_month')))[1]}/{int(data.get('end_month')):02d}/{year}"

    # TIMING: Data loading
    stage_start = time.time()

    # Load data file and all gl initial balance
    df = load_data(config.transactions_data_folder, config.filter_column, data.get("company_code"), config.selected_columns,
                   config.amount_column, start_date, end_date, data.get('company_code'), str(year), bank=bnk)
    if df.is_empty():
        total_time = time.time() - request_start_time
        logger.info(f"Grand Livre generation completed (empty) in {total_time:.2f}s for {company_code}")
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

    timing_stages['data_loading'] = time.time() - stage_start

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

    # ============================================================================
    # OPTIMIZATION: Data Preparation (DONE ONCE for ALL accounts)
    # ============================================================================

    # TIMING: Data preparation
    stage_start = time.time()

    # 1. Pre-compute unique initial balance (was inside loop - called 200+ times!)
    df_initial_balance_unique = df_initial_balance.unique(("Numéro de compte IFRS"), keep='first', maintain_order=True)

    # 2. Pre-build initial balance lookup dictionary (avoid filtering 200+ times)
    initial_balance_lookup = {}
    for value in unique_values:
        if value == "OHADA VIDES":
            continue
        matching_rows = df_initial_balance.filter(pl.col(config.SYSCOHADA_column_in_initial_balance) == value)
        if not matching_rows.is_empty():
            gl_desc = matching_rows[config.SYSCOHADA_desc_column_in_initial_balance][0]
            gl_debit = 0
            gl_credit = 0
            if str(value)[0] not in ['6', '7', '8']:
                gl_debit = matching_rows["Soldes débiteurs"].sum()
                gl_credit = -(abs(matching_rows["Soldes créditeurs"].sum()))
            initial_balance_lookup[value] = {
                'desc': gl_desc,
                'debit': gl_debit,
                'credit': gl_credit,
                'balance': gl_credit + gl_debit
            }
        else:
            initial_balance_lookup[value] = {
                'desc': "",
                'debit': 0,
                'credit': 0,
                'balance': 0
            }

    # 3. Join initial balance to entire dataset ONCE (was inside loop!)
    df = df.join(df_initial_balance_unique, left_on=config.offset_account_column_name,
                 right_on="Numéro de compte IFRS", how="left")

    # 4. Keep SYSCOHADA account number for filtering (add as separate column before dropping)
    df = df.with_columns(pl.col(config.SYSCOHADA_column_in_main_data).alias("SYSCOHADA_Account"))

    # 5. Create Debit/Credit columns ONCE for entire dataset (was inside loop!)
    df = df.with_columns([
        pl.when(pl.col(config.amount_column) <= 0)
        .then(pl.col(config.amount_column))
        .otherwise(0)
        .alias("Crédit"),

        pl.when(pl.col(config.amount_column) > 0)
        .then(pl.col(config.amount_column).abs())
        .otherwise(0)
        .alias("Débit")
    ])

    # 6. Remove unwanted columns ONCE (but keep SYSCOHADA_Account for filtering)
    df = df.drop([config.amount_column, config.SYSCOHADA_column_in_main_data])

    # 7. Rename columns ONCE for entire dataset
    df = df.rename(config.renamed_columns)

    # 8. Cast Date/Time columns ONCE
    cast_columns = []
    if "Date de Saisie" in df.columns:
        cast_columns.append(pl.col("Date de Saisie").cast(pl.Utf8))
    if "Heure de Saisie" in df.columns:
        cast_columns.append(pl.col("Heure de Saisie").cast(pl.Utf8))
    if cast_columns:
        df = df.with_columns(cast_columns)

    # ============================================================================
    # STRATEGY 3: Pre-compute layout-specific operations (avoid 200+ redundant calls)
    # ============================================================================

    # Get layout configuration based on user selection
    # Default to "default" layout if no selection is made
    if not layout_type:
        layout_type = "default"

    layout = layout_manager.config["layouts"].get(layout_type, layout_manager.config["layouts"]["default"])
    logger.info(f"Using layout: {layout_type}")

    column_labels = layout.get("column_labels", {})
    excluded_columns = layout.get("excluded_columns", [])

    # Pre-compute renamed column list for reordering (used in every iteration)
    renamed_reordered_columns = []
    for col in config.reordered_columns:
        new_col_name = column_labels.get(col, col)
        renamed_reordered_columns.append(new_col_name)

    # Pre-compute final columns after exclusions (apply exclusions from selected layout)
    final_columns_template = [col for col in renamed_reordered_columns if col not in excluded_columns]

    timing_stages['data_preparation'] = time.time() - stage_start

    # ============================================================================
    # Now loop through accounts for account-specific processing and Excel writing
    # ============================================================================

    # TIMING: Excel generation
    stage_start = time.time()

    # NOTE: constant_memory mode removed - incompatible with our write pattern
    # (we write data first, then headers above it)
    with Workbook(output_file) as writer:

        # ========================================================================
        # STRATEGY 6A: Pre-compute format objects to avoid creating 600+ times
        # ========================================================================

        # Create format object ONCE (reused throughout for all merge_range operations)
        merge_format_title = writer.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 14
        })

        # Initialize worksheet cache to avoid repeated get_worksheet_by_name() calls
        worksheet_cache = {}
        for value in unique_values:
            if value == "OHADA VIDES":
                continue

            # Filter for this account (now operating on pre-processed data)
            filtered_df = df.filter(pl.col("SYSCOHADA_Account") == str(value))

            empty_df = filtered_df.is_empty()

            # Get pre-computed initial balance
            balance_info = initial_balance_lookup.get(value, {'desc': '', 'debit': 0, 'credit': 0, 'balance': 0})
            gl_desc = balance_info['desc']
            gl_debit_balance = balance_info['debit']
            gl_credit_balance = balance_info['credit']
            initial_balance = balance_info['balance']

            # Fill empty values in column Libellé
            filtered_df = filtered_df.with_columns(
                pl.when(pl.col("Libellé").is_null() & pl.col("Référence").is_null())
                .then(pl.lit(gl_desc))
                .otherwise(pl.col("Libellé").fill_null(pl.col("Référence")))
                .alias("Libellé")
            )

            # Add SYSCOHADA Code and Desc Columns
            filtered_df = filtered_df.with_columns(pl.lit(value).alias("Compte SYSCOHADA"))
            filtered_df = filtered_df.with_columns(pl.lit(gl_desc if gl_desc != None else "").alias("Compte SYSCOHADA Desc"))

            # Apply layout-specific column renaming (business unit customization)
            # Must be done AFTER SYSCOHADA columns are added
            # OPTIMIZATION: Use pre-computed column_labels instead of calling layout_manager
            if column_labels:
                rename_mapping = {}
                for old_name, new_name in column_labels.items():
                    if old_name in filtered_df.columns:
                        rename_mapping[old_name] = new_name
                if rename_mapping:
                    filtered_df = filtered_df.rename(rename_mapping)

            initial_balance = gl_credit_balance + gl_debit_balance

            filtered_df = filtered_df.with_columns([
                (pl.lit(initial_balance) + 
                pl.col("Crédit").cum_sum() + 
                pl.col("Débit").cum_sum()).alias("Solde")
            ])

            # Create new Month column to be able to group transactions of same month
            # Only do this if we have transactions (empty DataFrames can't use dt.strftime properly)
            if not empty_df:
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
            else:
                # For empty DataFrames, explicitly cast Date/Time columns to String to match total_balance schema
                cast_columns = []
                if "Date" in filtered_df.columns:
                    cast_columns.append(pl.col("Date").cast(pl.Utf8))
                if "Date de Saisie" in filtered_df.columns:
                    cast_columns.append(pl.col("Date de Saisie").cast(pl.Utf8))
                if "Heure de Saisie" in filtered_df.columns:
                    cast_columns.append(pl.col("Heure de Saisie").cast(pl.Utf8))

                if cast_columns:
                    filtered_df = filtered_df.with_columns(cast_columns)

            # Create a new list to hold ledger year-months rows
            ledger_df = pl.DataFrame()

            # Process each month separately for the current ledger
            # Only group by month if we have transactions
            if not empty_df:
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

            # Compute and append total balance row (include all possible columns for layout flexibility)
            # Create with explicit schema to ensure all text columns are String type
            total_balance = pl.DataFrame({
                "Code Entreprise": [""],
                "Nom Entreprise": [""],
                "Année Fiscale": [""],
                "Compte IFRS": [""],
                "Desc Compte IFRS": [""],
                "Désignation Type de pièce": [""],
                "Pièce": [""],
                "Référence": [""],
                "Libellé": [""],
                "Contrepartie IFRS": [""],
                "Contrepartie IFRS Desc": [""],
                "Contrepartie SYSCOHADA": [""],
                "Contrepartie SYSCOHADA Desc": [""],
                "Compte SYSCOHADA": [""],
                "Compte SYSCOHADA Desc": [""],
                "Compte Général": [""],  # Renamed version for CIV_LAYOUT
                "Libelle du Compte": [""],  # Renamed version for CIV_LAYOUT
                "Contrepartie": [""],  # Renamed version for CIV_LAYOUT
                "Libelle Contrepartie": [""],  # Renamed version for CIV_LAYOUT
                "Date de Saisie": [""],
                "Heure de Saisie": [""],
                "Utilisateur SAP": [""],
                "Date": ["TOTAL"],
                "Type de pièce": [f"{len(filtered_df)} ligne(s)"],
                "Débit": [filtered_df["Débit"].sum()],
                "Crédit": [filtered_df["Crédit"].sum()],
                "Solde": [filtered_df["Solde"].to_list()[-1]] if not empty_df else [float(initial_balance)],
            })

            ledger_df = pl.concat([ledger_df, total_balance], how="diagonal")

            # Drop the temporary "Month" column
            ledger_df.drop(["Month"]) if not empty_df else ""
            filtered_df = ledger_df.clone() if not empty_df else pl.concat([filtered_df, total_balance], how="diagonal")
            filtered_df = filtered_df.with_columns(pl.col("Compte SYSCOHADA").cast(pl.Utf8))
            del ledger_df

            # Add thousand separator for columns 'debit', 'credit' and 'solde'
            # OPTIMIZATION: Using native Polars operations instead of map_elements (15-20% faster)
            filtered_df = filtered_df.with_columns([
                # Débit: Convert to string, add thousand separators (space as separator)
                pl.when(pl.col("Débit").is_null())
                .then(pl.lit(""))
                .otherwise(
                    pl.col("Débit")
                    .cast(pl.Int64)
                    .cast(pl.Utf8)
                    .str.reverse()  # Reverse to add separators from right to left
                    .str.replace_all(r"(\d{3})", "$1 ")  # Add space every 3 digits
                    .str.strip_chars()  # Remove trailing space
                    .str.reverse()  # Reverse back to normal
                )
                .alias("Débit"),

                # Crédit: Same as Débit but with abs value
                pl.when(pl.col("Crédit").is_null())
                .then(pl.lit(""))
                .otherwise(
                    pl.col("Crédit")
                    .abs()
                    .cast(pl.Int64)
                    .cast(pl.Utf8)
                    .str.reverse()
                    .str.replace_all(r"(\d{3})", "$1 ")
                    .str.strip_chars()
                    .str.reverse()
                )
                .alias("Crédit"),

                # Solde: Add C/D prefix based on sign, then format with thousand separator
                pl.when(pl.col("Solde").is_null())
                .then(pl.lit(""))
                .when(pl.col("Solde") <= 0)
                .then(
                    pl.lit("C ") +
                    pl.col("Solde")
                    .abs()
                    .cast(pl.Int64)
                    .cast(pl.Utf8)
                    .str.reverse()
                    .str.replace_all(r"(\d{3})", "$1 ")
                    .str.strip_chars()
                    .str.reverse()
                )
                .otherwise(
                    pl.lit("D ") +
                    pl.col("Solde")
                    .cast(pl.Int64)
                    .cast(pl.Utf8)
                    .str.reverse()
                    .str.replace_all(r"(\d{3})", "$1 ")
                    .str.strip_chars()
                    .str.reverse()
                )
                .alias("Solde")
            ])

            # Reorder columns and apply layout exclusions
            # OPTIMIZATION: Use pre-computed final_columns_template instead of recalculating
            # Only select columns that exist in the dataframe
            available_columns = [col for col in final_columns_template if col in filtered_df.columns]
            filtered_df = filtered_df[available_columns]

            # Add initial and closed balance rows in the tables
            closed_balance = filtered_df["Solde"].to_list()[-2] if not empty_df else f"C {abs(initial_balance):,.0f}".replace(",", " ") if initial_balance <= 0 else f"D {initial_balance:,.0f}".replace(",", " ")

            # OPTIMIZATION: Use pre-computed excluded_columns instead of calling layout_manager
            excluded_cols = excluded_columns

            # Get IFRS values only if columns are not excluded
            compte_ifrs = None
            desc_compte_ifrs = None
            if "Compte IFRS" not in excluded_cols:
                compte_ifrs = filtered_df["Compte IFRS"].to_list()[0] if not empty_df else matching_rows[config.IFRS_code_column_in_initial_balance][0]
            if "Desc Compte IFRS" not in excluded_cols:
                desc_compte_ifrs = filtered_df["Desc Compte IFRS"].to_list()[0] if not empty_df else matching_rows["Intitulé de compte IFRS"][0]

            # Build row data with all possible columns (both original and renamed versions)
            all_columns_data = {
                "Code Entreprise": data.get("company_code"),
                "Nom Entreprise": company_name,
                "Année Fiscale": "",
                "Compte SYSCOHADA": str(value),
                "Compte SYSCOHADA Desc": gl_desc,
                "Compte Général": str(value),  # Renamed version for CIV_LAYOUT
                "Libelle du Compte": gl_desc,  # Renamed version for CIV_LAYOUT
                "Compte IFRS": compte_ifrs,
                "Desc Compte IFRS": desc_compte_ifrs,
                "Date": start_date,
                "Type de pièce": "",
                "Désignation Type de pièce": "",
                "Pièce": "",
                "Référence": "",
                "Débit": "",
                "Crédit": "",
                "Solde": f"C {abs(initial_balance):,.0f}".replace(",", " ") if initial_balance <= 0 else f"D {initial_balance:,.0f}".replace(",", " "),
                "Libellé": "REPORT AU " + start_date,
                "Date de Saisie": "",
                "Heure de Saisie": "",
                "Utilisateur SAP": "",
                "Contrepartie IFRS": "",
                "Contrepartie IFRS Desc": "",
                "Contrepartie SYSCOHADA": "",
                "Contrepartie SYSCOHADA Desc": "",
                "Contrepartie": "",  # Renamed version for CIV_LAYOUT
                "Libelle Contrepartie": ""  # Renamed version for CIV_LAYOUT
            }

            # Filter to only include columns that exist in filtered_df
            initial_balance_row = {k: v for k, v in all_columns_data.items() if k in filtered_df.columns}

            # Build close balance row
            all_columns_data["Date"] = end_date
            all_columns_data["Type de pièce"] = "SOLDE"
            all_columns_data["Solde"] = f"{closed_balance}"
            all_columns_data["Libellé"] = "SOLDE AU " + end_date

            close_balance_row = {k: v for k, v in all_columns_data.items() if k in filtered_df.columns}

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

            # STRATEGY 6A: Use cached worksheet reference (avoid repeated get_worksheet_by_name calls)
            sheet_name = str(value)
            if sheet_name not in worksheet_cache:
                worksheet_cache[sheet_name] = writer.get_worksheet_by_name(sheet_name)
            worksheet = worksheet_cache[sheet_name]

            # STRATEGY 6A: Reuse pre-created format object (instead of creating 200+ times)
            worksheet.merge_range('A1:K1', f"{company_name}", merge_format_title)
            worksheet.merge_range('A3:K3', f"Compte <{value}> {gl_desc}", merge_format_title)
            worksheet.merge_range('A4:K4', f"Grand-livre du {start_date} au {end_date}", merge_format_title)

        # ========================================================================
        # STRATEGY 6B: Batch consolidation sheets
        # Pre-build merge range and write position lists, then apply in batch
        # ========================================================================

        pl.DataFrame().write_excel(writer, worksheet="Grand Livre - Comptes du bilan")
        pl.DataFrame().write_excel(writer, worksheet="Grand Livre-Comptes de gestion")

        # STRATEGY 6A: Get worksheet reference ONCE (not 50-100+ times in loop)
        worksheet_bilan = writer.get_worksheet_by_name("Grand Livre - Comptes du bilan")

        # STRATEGY 6B: Pre-build all merge ranges and positions for Bilan
        bilan_merges = []
        bilan_writes = []
        current_row = 1

        for i, gl_df in enumerate(pd_dfs_comptes_bilan):
            # Collect merge range information
            if i == 0:
                bilan_merges.append({
                    'range': f'A{current_row}:K{current_row}',
                    'text': company_name
                })

            bilan_merges.append({
                'range': f'A{current_row + 2}:K{current_row + 2}',
                'text': f"Compte <{gl_df['name']}> {gl_df['desc']}"
            })
            bilan_merges.append({
                'range': f'A{current_row + 3}:K{current_row + 3}',
                'text': f"Grand Livre - Comptes du bilan du {start_date} au {end_date}"
            })

            # Collect write information
            bilan_writes.append({
                'df': gl_df['df'],
                'position': (current_row + 5, 0)
            })

            current_row += len(gl_df['df']) + 7

        # STRATEGY 6B: Apply all merges in batch
        for merge_info in bilan_merges:
            worksheet_bilan.merge_range(merge_info['range'], merge_info['text'], merge_format_title)

        # STRATEGY 6B: Write all DataFrames
        for write_info in bilan_writes:
            write_info['df'].write_excel(writer, worksheet="Grand Livre - Comptes du bilan",
                                        table_style="Table Style Light 10",
                                        autofilter=False, position=write_info['position'])

        # STRATEGY 6A: Get worksheet reference ONCE (not 50-100+ times in loop)
        worksheet_gestion = writer.get_worksheet_by_name("Grand Livre-Comptes de gestion")

        # STRATEGY 6B: Pre-build all merge ranges and positions for Gestion
        gestion_merges = []
        gestion_writes = []
        current_row = 1

        for i, gl_df in enumerate(pd_dfs_comptes_gestion):
            # Collect merge range information
            if i == 0:
                gestion_merges.append({
                    'range': f'A{current_row}:K{current_row}',
                    'text': company_name
                })

            gestion_merges.append({
                'range': f'A{current_row + 2}:K{current_row + 2}',
                'text': f"Compte <{gl_df['name']}> {gl_df['desc']}"
            })
            gestion_merges.append({
                'range': f'A{current_row + 3}:K{current_row + 3}',
                'text': f"Grand Livre - Comptes de gestion du {start_date} au {end_date}"
            })

            # Collect write information
            gestion_writes.append({
                'df': gl_df['df'],
                'position': (current_row + 5, 0)
            })

            current_row += len(gl_df['df']) + 7

        # STRATEGY 6B: Apply all merges in batch
        for merge_info in gestion_merges:
            worksheet_gestion.merge_range(merge_info['range'], merge_info['text'], merge_format_title)

        # STRATEGY 6B: Write all DataFrames
        for write_info in gestion_writes:
            write_info['df'].write_excel(writer, worksheet="Grand Livre-Comptes de gestion",
                                        table_style="Table Style Light 10",
                                        autofilter=False, position=write_info['position'])

    timing_stages['excel_generation'] = time.time() - stage_start

    # ============================================================================
    # TIMING: Calculate and log total time
    # ============================================================================
    total_time = time.time() - request_start_time

    # Log detailed timing breakdown
    logger.info("="*80)
    logger.info(f"Grand Livre Generation Complete for {company_code}")
    logger.info(f"Period: {start_date} to {end_date}")
    logger.info(f"Total Accounts: {len(unique_values)}")
    logger.info("="*80)
    logger.info(f"TIMING BREAKDOWN:")
    logger.info(f"  Initialization:     {timing_stages.get('initialization', 0):.2f}s")
    logger.info(f"  Data Loading:       {timing_stages.get('data_loading', 0):.2f}s")
    logger.info(f"  Data Preparation:   {timing_stages.get('data_preparation', 0):.2f}s")
    logger.info(f"  Excel Generation:   {timing_stages.get('excel_generation', 0):.2f}s")
    logger.info("="*80)
    logger.info(f"TOTAL TIME:          {total_time:.2f}s ({total_time/60:.2f} minutes)")
    logger.info("="*80)

    # Mettre en cache si le cache_manager est fourni
    if cache_manager and cache_key:
        cache_manager.set_cache(cache_key, output_file)

    return send_from_directory(directory=os.getcwd(), path=output_file, as_attachment=True), 200

