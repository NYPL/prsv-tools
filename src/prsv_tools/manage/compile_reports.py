import argparse
import logging
import pandas as pd 
import prsv_tools.utility.cli as prsvcli
import prsv_tools.manage.validate_reports as valrep

from pathlib import Path

def parse_args() -> argparse.Namespace:
    """Validate and return command-line args"""

    parser = prsvcli.Parser()
    parser.add_packagedirectory()

    return parser.parse_args()


def main():
    args = parse_args()

    # path + output variables
    directory = Path(args.directory)

    parent_folder = Path("/containers/metadata_exports/")
    output_folder = parent_folder / "Combined_Valid_CSV" / f"{directory.name[:3]}"

    outputfile_name = f"{directory.name[:3]}_Info_Merged.csv"
    outputfile_path = output_folder / outputfile_name

    # read csv + combine all dataframes
    new_df = pd.DataFrame()
    valid, invalid = valrep.sort_result(directory)
    for item in valid:
        for report in sorted(directory.iterdir()):
            if report.name == item:
                read_report = pd.read_csv(report)
                new_df = pd.concat([new_df, read_report], ignore_index=False, axis=0)


    #  write to new CSV
    output_folder.mkdir(parents=True, exist_ok=True)
    outputfile = open(outputfile_path, "w", encoding = 'utf-8')
    new_df.to_csv(outputfile, index=False)

if __name__ == '__main__':
    main()