import logging
import argparse 
import re
import sys
import pandas as pd

from datetime import datetime
from pathlib import Path

import prsv_tools.utility.cli as prsvcli

# logger
LOGGER = logging.getLogger(__name__)

def _configure_logging(log_folder: Path):
    log_fn = datetime.now().strftime("lint_%Y_%m_%d_%H_%M.log")
    log_fpath = log_folder / log_fn

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(levelname)8s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        encoding="utf-8",
        handlers=[logging.FileHandler(log_fpath, mode="w"), logging.StreamHandler()],
    )

# arg parsers
def parse_args() -> argparse.Namespace:
    """Validate and return command-line args"""

    parser = prsvcli.Parser()

    parser.add_argument(
        "--report",
        "-r",
        type=str,
        required=False,
        help="Path of report to be validated, e.g. 'path/to/report/12345_INFO.csv'",
    )

    parser.add_argument(
        "--directory",
        "-d",
        type=prsvcli.extant_dir,
        requierd=False,
        help="Path to directory of reports to be validated, e.g. 'path/to/dir/of/csvs'",
    )

    parser.add_logdirectory()

    return parser.parse_args()

# validation tests
def metadata_contents_SO(report: pd.DataFrame) -> bool: 
    """Package must have _metadata and _contents SO"""
    contents = 0
    metadata = 0
    row_pos = ""
    # iterate through each row in dataframe
    for i in range(len(report.index)):
        # get local path info for row
        row_pos = report['Local_Path'].values[i]
        # bool for rows ending in _contents
        row_contents = str(row_pos).__contains__("_contents")
        # bool for rows ending in _metadata
        row_metadata = str(row_pos).__contains__("_met0adata")
        # if dataframe has _contents AND _metadata = true

        if row_contents or row_metadata:
            if row_contents:
                contents +=1
                continue
            else:
                metadata +=1
                continue

    if contents == 0:
        LOGGER.error(f"{row_pos[-15:-9]} is missing _contents SO")
        return False
    elif metadata ==0:
        LOGGER.error(f"{row_pos[-15:-9]} is missing _metadata SO")
        return False
    else:
        return True



def metadata_IO_count() -> bool:
    """Package_metadata may contain 0 to many IOs"""
    # does this need to be tested for if _metadata can have any number of IOs?
    # *******if failed, log ...
    pass

def contents_IO_count(report: pd.DataFrame) -> bool:
    """Package_contents must have at least 1 IO"""
    io_set = set()
    report_name = ""
    # iterate through each row in dataframe
    for i in range(len(report.index)):
        path_pos = report['Local_Path'].values[i]
        report_name = str(path_pos[-15:-9])
        # get IO Ref ID
        row_pos = report['IO Ref'].values[i]
        # check if row is for _contents
        row_contents = str(path_pos).__contains__("_contents")
        if row_contents:
            # if _contents, add IO Ref ID to set
            io_set.add(row_pos)
    # if unique IO Ref IDs count >= 1, is true
    if len(io_set) >=1:
        return True
    else:
        LOGGER.error(f"{report_name} has {len(io_set)} IOs")
        return False


def metadata_unqiue_names(report: pd.DataFrame) -> bool:
    """Package_metadata IOs must have unique names"""
    filtered_rows = pd.DataFrame(columns=report.columns)
    index = 0
    report_name = ""
    for i in range(len(report.index)):
        row_pos = report.values[i]
        path_pos = report['Local_Path'].values[i]
        report_name = str(path_pos[-15:-9])
        # bool for rows ending in _metadata
        row_metadata = str(row_pos).__contains__("_metadata")
        if row_metadata:
            # copy only _metadata rows to new df
            filtered_rows.loc[index] = row_pos
            index +=1
            filtered_rows.sort_index()
    # group rows by occurance, returns occurance count as new column
    name_count = filtered_rows.groupby(['File Name', 'IO Ref']).size().reset_index()
    for i in range(len(name_count.index)):
        if name_count.iloc[:, 2].values[i] == 1:
            return True
            # # occurance count
            # name_count.iloc[:, 2].values[i]
            # # IO Ref
            # name_count.iloc[:, 1].values[i]
            # # File Name
            # name_count.iloc[:, 0].values[i]
        else:
            LOGGER.error(f"{report_name}, file {name_count.iloc[:, 0].values[i]} has a non-unique name, occurs {name_count.iloc[:, 2].values[i]} time(s)")
            return False

def contents_unique_names(report) -> bool:
    """Package_contents IOs must have unique names"""
    filtered_rows = pd.DataFrame(columns=report.columns)
    index = 0
    report_name = ""
    for i in range(len(report.index)):
        row_pos = report.values[i]
        path_pos = report['Local_Path'].values[i]
        report_name = str(path_pos[-15:-9])
        # bool for rows ending in _contents
        row_contents = str(row_pos).__contains__("_contents")
        if row_contents:
            # copy only _metadata rows to new df
            filtered_rows.loc[index] = row_pos
            index +=1
            filtered_rows.sort_index()
    # group rows by occurance, returns occurance count as new column
    name_count = filtered_rows.groupby(['File Name', 'IO Ref']).size().reset_index()
    for i in range(len(name_count.index)):
        if name_count.iloc[:, 2].values[i] == 1:
            return True
            # # occurance count
            # name_count.iloc[:, 2].values[i]
            # # IO Ref
            # name_count.iloc[:, 1].values[i]
            # # File Name
            # name_count.iloc[:, 0].values[i]
        else:
            LOGGER.error(f"{report_name}, file {name_count.iloc[:, 0].values[i]}  has a non-unique name")
            return False

def metadata_bitstream_count(report: pd.DataFrame) -> bool:
    """Package_metadata IOs must have 1 bitstream each"""
    # check for unique checksums
    md5_check = report['MD5ChecksumVal'].unique()
    if len(md5_check) == len(report.index):
        return True
    else:
        return False

def contents_media_IO(report: pd.DataFrame) -> bool:
    """AMI Package_contents must contain 1 _media IO"""
    # may need to modify if reports should only contain 1 _media IO in _contents
    for i in range(len(report.index)):
        # get IO Title
        row_pos = report['IO Title'].values[i]
        # get Local Path 
        path_pos = report['Local_Path'].values[i]
        # check for _media in IO Title
        media_info = str(row_pos).__contains__("_media")
        # check for _contents in Local Path
        media_path = str(path_pos).__contains__("_contents")
        if media_path:
            # if _contents exists, check for _media files
            if not media_info:
                LOGGER.error(f"Index[{report.index[i]}]: {row_pos[:6]} does not contain _media IOs")
                return False
        
    return True

def contents_expected_names(report: pd.DataFrame) -> bool:
    """AMI Package_contents IOs must have expected names"""
    io_result = False
    io_result = False
    fn_result = False
    for i in range(len(report.index)):
        io_title = report['IO Title'].values[i]
        file_name = report['File Name'].values[i]
        path_pos = report['Local_Path'].values[i]
        if not str(path_pos).__contains__("_contents"):
            continue
        else:
            # io title pattern (ie. '338770_media')
            io_title_pattern = re.compile(r"^[0-9]+_media$")
            # file name pattern (ie. 'myd_338770_v01_pm.mkv')
            fn_pattern = re.compile(r"myd_[0-9]+_v[0-9]+_[A-Za-z]+\.[A-Za-z0-9]+")
            # file name pattern (ie. 'myh_336536_v01f01_sc.mp4')
            fn_alt_pattern = re.compile(r"my[A-Za-z]_[0-9]+_v[0-9]+[A-Za-z][0-9]+_[A-Za-z]+\.[A-Za-z0-9]+")
            if io_title_pattern.fullmatch(io_title):
                io_result = True
            else:
                io_result = False
                LOGGER.error(f"{io_title} does not have an expected IO Title name")
            if fn_pattern.fullmatch(file_name):
                fn_result = True
            elif fn_alt_pattern.fullmatch(file_name):
                fn_result = True
            else:
                fn_result = False
                LOGGER.error(f"{file_name} does not have an expected filename")

    return (io_result and fn_result)


    # if "File Name" matches pattern:
        # patterns here
    # if failed, log ...

def metadata_media_json(report: pd.DataFrame) -> bool:
    """AMI Package_media must have matching json in _metadata"""
    media_files = set()
    metadata_files = set()
    report_name = ""

    for i in range(len(report.index)):
        # get IO Title
        row_pos = report['IO Title'].values[i]
        # get Local Path 
        path_pos = report['Local_Path'].values[i]
        # get File Name
        fn_pos = report['File Name'].values[i]
        report_name = str(path_pos[-15:-9])
        #check for _media in IO Title
        media_info = str(row_pos).__contains__("_media")
        # check for _contents in Local Path
        metadata_path = str(path_pos).__contains__("_metadata")
        json_path = str(row_pos).__contains__(".json")
        if media_info:
            media_files.add(str(fn_pos).split('.')[0])
        if metadata_path:
            if json_path:
                metadata_files.add(str(fn_pos).split('.')[0])
    # set of file name stems that have _media files and jsons
    matched_jsons = media_files.intersection(metadata_files)
    if len(matched_jsons) == len(media_files):
        return True
    # more _media files than jsons
    elif len(matched_jsons) < len(media_files):
        json_diff = len(media_files)-len(matched_jsons)
        LOGGER.error(f"{report_name} is missing {json_diff} .json file(s)")
        return False
    # more jsons than _media files
    elif len(matched_jsons) > len(media_files):
        json_diff = len(matched_jsons)-len(media_files)
        LOGGER.error(f"{report_name} has {json_diff} too many .json files")
        return False


def io_category_id():
    # optional test
    pass

def so_category_id():
    # optional test
    pass

# validate reports
def validate_reports(report: pd.DataFrame, report_name):
    """Create a list of reports that are valid & invalid"""
    result = "valid"

    # op_tests = [
    #     io_category_id,
    #     so_category_id,
    # ]

    # for test in op_tests:
    #     if not test(report):
    #         invalid.append(report_name)
    #         LOGGER.error(f"{report_name} failed {test} [OPTIONAL].")

    req_tests = [
        metadata_contents_SO,
        # metadata_IO_count,
        contents_IO_count,
        metadata_unqiue_names,
        contents_unique_names,
        metadata_bitstream_count,
        contents_media_IO,
        contents_expected_names,
        metadata_media_json,
    ]

    for test in req_tests:
        if not test(report):
            result = "invalid"

    return result

def sort_result(report: Path):
    valid = list()
    invalid = list()
    
    read_report = pd.read_csv(report)
    result = validate_reports(read_report, report.name)
    if result == "invalid":
        invalid.append(report.name)
    elif result == "valid":
        valid.append(report.name)

    return valid, invalid

def main():
    args = parse_args()
    _configure_logging(args.log_folder)

    if args.report:
        sort_result(args.report)
    if args.directory:
        report_dir = Path(args.directory)
        for report in sorted(report_dir.iterdir()):
            sort_result(report)


if __name__ == '__main__':
    main()