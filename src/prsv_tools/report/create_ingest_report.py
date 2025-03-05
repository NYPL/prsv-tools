import argparse
import re
from pathlib import Path
from datetime import datetime

import pandas as pd


def _make_parser():
    parser = argparse.ArgumentParser(
        description="""Generate an ingest report.
                       Require a folder with Excel files from
                       Preservica's unpack script
                       And require a file name"""
    )
    parser.add_argument(
        "--folder",
        "-f",
        type=str,
        required=True,
        help="a folder with Excel files",
    )
    parser.add_argument(
        "--filename",
        "-fn",
        type=str,
        required=True,
        help="file name for the csv file",
    )

    return parser


def combine_excel(folder: Path):
    """Combine all the XLSX files in designated folder together into
    a data frame"""
    file_ls = [
        x
        for x in folder.iterdir()
        if x.is_file()
        and x.suffix.lower() == ".xlsx"
        and not x.name.startswith(".")
        and not x.name.startswith("~$")
    ]
    print(file_ls)
    df = pd.DataFrame()
    for excel in file_ls:
        df_new = pd.read_excel(excel, index_col=0)
        df = pd.concat([df, df_new])

    return df


def determine_io_type(file_name):
    """Using regular expression of the file name to determine whether it is a
    metadata or asset file"""
    if re.search(r"^M\d+_(EM|DI|ER)_\d+", file_name):
        return "Metadata"
    else:
        return "Asset"


def determine_ingest_month(ingest_date):
    """Extract year-month information from the IngestDate column"""
    dt_obj = datetime.fromisoformat(ingest_date)
    year_month_str = f"{dt_obj.year}-{dt_obj.strftime('%m')}"

    return year_month_str


def add_columns(df):
    """Add IO Type column and Object Type column"""
    df["IO Type"] = df["File Name"].apply(determine_io_type)
    df["Object Type"] = "DigArch"
    df["Ingest Month"] = df["IngestDate"].apply(determine_ingest_month)

    return df


def get_summary_info(df):
    """Get summary information with specific columns"""
    summary_info = (
        df.groupby(["Ingest Month", "Object Type", "IO Type"])
        .agg({"File Size": "sum", "File Name": "count"})
        .reset_index()
    )
    summary_info.columns = [
        "Ingest Month",
        "Object Type",
        "IO Type",
        "Total File Size",
        "Number of Files",
    ]

    return summary_info


def main():
    parser = _make_parser()
    args = parser.parse_args()

    folder_path = Path(args.folder)
    file_name = args.filename
    combined_df = combine_excel(folder_path)

    extract_df = add_columns(combined_df)

    summary_info = get_summary_info(extract_df)

    summary_info.to_csv(f"{file_name}.csv", index=False)


if __name__ == "__main__":
    main()
