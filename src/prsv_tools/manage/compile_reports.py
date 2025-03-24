import io
import pandas as pd 
import prsv_tools.utility.cli as prsvcli
import prsv_tools.manage.validate_reports as valrep

from pathlib import Path

# def parse_args() -> argparse.Namespace:
#     """Validate and return command-line args"""

#     parser = prsvcli.Parser()
#     parser.add_packagedirectory()

#     return parser.parse_args()



def main():
    source_dir = Path('/Users/emileebuytkins/containers/Export_Target/')
    parent_folder = Path('/Users/emileebuytkins/containers/combined_reports/')
    # for dir in source_dir.iterdir():
    #     csv_list = [parent_folder.joinpath(file) for file in dir.iterdir() if not dir.name.startswith("M") or dir.name.startswith(".")]

    new_df = pd.DataFrame()

    for dir in sorted(source_dir.iterdir()):
        if dir.name.startswith("M"):
            outputfile_name = "ER_Metadata_Info_Merged.csv"
            continue
        elif dir.name.startswith("."):
            continue
        else:
            print(dir.name)
            # output_folder = parent_folder / f"{dir.name[:3]}"
            outputfile_name = "AMI_Metadata_Info_Merged.csv"
            outputfile_path = parent_folder / outputfile_name
            # output_folder.mkdir(parents=True, exist_ok=True)

            for report in dir.iterdir():
                if report.name.startswith("."):
                    continue
                else:
                    print(report.name)
                    read_report = pd.read_csv(report)
                    new_df = pd.concat([new_df, read_report], ignore_index=False, axis=0)
    outputfile = io.open(outputfile_path, "w", encoding='utf-8')
    new_df.to_csv(outputfile, index=False)

if __name__ == '__main__':
    main()


