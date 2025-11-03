import os
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

def _default_paths():
    """Return input/output directories relative to this script inside workspace/.

    If a pre-existing bronze directory exists INSIDE the raw directory (data/raw/bronze)
    we assume the user wants to keep that layout and write there. Otherwise we use the
    sibling directory data/bronze (recommended separation of raw vs bronze layers).
    """
    script_dir = os.path.dirname(__file__)              # .../workspace/scripts
    workspace_dir = os.path.dirname(script_dir)         # .../workspace
    input_dir = os.path.join(workspace_dir, "data", "raw")
    bronze_in_raw = os.path.join(input_dir, "bronze")
    if os.path.isdir(bronze_in_raw):
        output_dir = bronze_in_raw
    else:
        output_dir = os.path.join(workspace_dir, "data", "bronze")
    return input_dir, output_dir

def filter_gkg_for_pld(
    input_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    delete_after_transform: bool = True,
    theme_code: Optional[str] = "ECON_MONEYLAUNDERING",
    themes: Optional[List[str]] = None,
    use_regex: bool = False,
    output_mode: str = "per-file",  # 'per-file' | 'aggregate' | 'both'
    aggregate_filename: Optional[str] = None,
    add_source_column: bool = True,
    max_files: Optional[int] = None,    # process only first N files (debug / sampling)
):
    """Filter GKG files for specified theme codes.

    Matching logic:
        - If 'themes' provided (list), do exact membership: any token in semicolon-split THEMES column (col 8).
        - Else if 'theme_code' provided and use_regex=False: substring match (str.contains).
        - Else if use_regex=True: treat theme_code as regex.

    Parameters:
        input_dir: Directory containing *.gkg.csv (auto if None).
        output_dir: Destination directory (auto if None).
        delete_after_transform: Delete original only if matches found.
        theme_code: Single theme / pattern (ignored if 'themes' list is given).
        themes: List of exact theme codes to match (overrides theme_code when not None).
        use_regex: If True and themes is None, theme_code treated as regex.
        output_mode: 'per-file', 'aggregate', or 'both'.
        aggregate_filename: Custom name for aggregate file; defaults to 'gkg_money_laundering_agg.tsv'.
        add_source_column: Include source filename in aggregate output.
        max_files: If provided, only the first N files (sorted) are processed. Helpful for debugging.
    """
    if input_dir is None or output_dir is None:
        auto_input, auto_output = _default_paths()
        if input_dir is None:
            input_dir = auto_input
        if output_dir is None:
            output_dir = auto_output

    print(f"📥 Input dir: {os.path.abspath(input_dir)}")
    print(f"📤 Output dir: {os.path.abspath(output_dir)}")
    print(f"🔧 Output mode: {output_mode}")

    if not os.path.isdir(input_dir):
        print(f"❌ Input directory não existe: {input_dir}")
        return

    os.makedirs(output_dir, exist_ok=True)
    arquivos = sorted([f for f in os.listdir(input_dir) if f.endswith(".gkg.csv")])
    if max_files is not None:
        arquivos = arquivos[:max_files]

    if not arquivos:
        print("🔍 Nenhum arquivo .gkg.csv encontrado no input.")
        return

    total_matches = 0
    aggregate_rows = []

    # Prepare theme set for quick lookup if list provided
    theme_set = set(themes) if themes else None
    if theme_set:
        print(f"🎯 Themes (exact match list): {sorted(theme_set)}")
    else:
        print(f"🎯 Theme pattern: {theme_code} | regex={use_regex}")

    per_file_stats = []

    for arquivo in arquivos:
        input_path = os.path.join(input_dir, arquivo)
        per_file_output = os.path.join(output_dir, arquivo)

        try:
            df = pd.read_csv(input_path, sep="\t", header=None, dtype=str, encoding="utf-8", low_memory=False)

            if 8 not in df.columns:
                print(f"❌ Coluna 8 ausente: {arquivo}")
                continue

            if theme_set:
                def has_any_theme(val: str | None) -> bool:
                    if not isinstance(val, str):
                        return False
                    tokens = val.split(';')
                    return any(t in theme_set for t in tokens)
                mask = df[8].apply(has_any_theme)
            else:
                mask = df[8].str.contains(theme_code or "", na=False, regex=use_regex)

            df_sel = df[mask]
            match_count = len(df_sel)
            total_matches += match_count

            if match_count > 0:
                print(f"✅ {arquivo}: {match_count} linhas correspondentes")
                per_file_stats.append((arquivo, match_count))

                # Per-file output if requested
                if output_mode in ("per-file", "both"):
                    if os.path.exists(per_file_output):
                        print(f"🟡 Já existe (pulando escrita): {arquivo}")
                    else:
                        df_sel.to_csv(per_file_output, sep="\t", index=False, header=False)
                        print(f"💾 Salvo arquivo filtrado: {per_file_output}")

                # Aggregate collection if requested
                if output_mode in ("aggregate", "both"):
                    if add_source_column:
                        df_sel["__source_file"] = arquivo  # type: ignore[pd-dev-version]
                    aggregate_rows.append(df_sel)

                if delete_after_transform:
                    os.remove(input_path)
                    print(f"🗑️ Original deletado: {arquivo}")
            else:
                print(f"⚠️ {arquivo}: 0 linhas encontradas")
                per_file_stats.append((arquivo, 0))

        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {type(e).__name__} - {e}")

    # Write aggregate if needed
    if output_mode in ("aggregate", "both") and aggregate_rows:
        agg_df = pd.concat(aggregate_rows, ignore_index=True)
        if not aggregate_filename:
            stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            aggregate_filename = f"gkg_money_laundering_agg_{stamp}.tsv"
        aggregate_path = os.path.join(output_dir, aggregate_filename)
        agg_df.to_csv(aggregate_path, sep="\t", index=False, header=False)
        print(f"📦 Arquivo agregado salvo: {aggregate_path} ({len(agg_df)} linhas)")
    elif output_mode in ("aggregate", "both"):
        print("ℹ️ Nenhuma linha para agregar.")

    print(f"🏁 Processamento concluído. Total de linhas correspondentes: {total_matches}")

    # Build summary dict to return (useful for programmatic checks / tests)
    summary: Dict[str, Any] = {
        "input_dir": os.path.abspath(input_dir),
        "output_dir": os.path.abspath(output_dir),
        "total_matches": total_matches,
        "files_processed": len(per_file_stats),
        "per_file": per_file_stats,
        "aggregate_written": output_mode in ("aggregate", "both") and bool(aggregate_rows)
    }
    return summary

# Convenience wrapper for money laundering related themes
def filter_gkg_money_laundering(
    input_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    delete_after_transform: bool = True,
    output_mode: str = "both",
    max_files: Optional[int] = None,
):
    ml_themes: List[str] = [
        "ECON_MONEYLAUNDERING",
        "WB_2076_MONEY_LAUNDERING",
        "WB_328_FINANCIAL_INTEGRITY",
        "WB_1920_FINANCIAL_SECTOR_DEVELOPMENT"
    ]
    return filter_gkg_for_pld(
        input_dir=input_dir,
        output_dir=output_dir,
        delete_after_transform=delete_after_transform,
        themes=ml_themes,
        output_mode=output_mode,
        aggregate_filename="gkg_money_laundering_agg.tsv",
        max_files=max_files,
    )
