#!/usr/bin/python3

# Andy Dean     andy.dean@preservica.com

# Date 18-05-2022
#
# ..................................... OPEX PAX Generator for NYPL Digiarch ......................................
#
#  THIS SCRIPTS IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, WITHOUT
#  LIMITATION, THE IMPLIED WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
#  Use at your own risk. Test it in your environment, check the resulting opex files BEFORE YOU UPLOAD THEM TO
#  PRESERVICA, and ingest a few examples to ensure the opex files are appropriate for your needs.
#  This is especially important if you are integrating with catalogs where the collection code or other attributes
#  may be used to influence catalog sync processes.
#
#  Do not use this script to "create" resulting .opex files into your primary file system but instead create the
#  opexes on your workstation and merge them with your digital files in your S3 bucket
#  Alternatively, COPY your content from your primatry file system to a temporary location that Preservica can access
#
#  If it works for you share it, and please provide feedback or share improvements at developers.preservica.com
#
#
#  Purpose: Create PAX based opex suitable for use with Incremental Ingest process.
#
#   Dispense with Working and WorkingPAX folders 
#   Create Container in Target, fCreatePAX in target, run "opex" creation in target
#
#   18-05-22 
#       Workflow overview
#           1. Place digiarch collection on Source directory
#           2. Inspect collection metadata ??
#           3. Identify key folders (FA Component SO, Metadaa SO, Content SO)
#           4. Specify container directory
#           5. Copy digiarch collection to upload directory
#           6. Create opex fragments
#           7. Initiate upload
#
#   24-05-22
#       Disabled fCheckWorkflowStatus to sped up ingest
#
#   19-08-22
#       Add variable for SOCategory, acquired from package name "Mxxxxx_##_xxxx"
#       Add variable for CMS COllection ID, acquired from package name "M#####_xx_xxxx"

#   <opex:Manifests> test ********************************************************************************
#
#
#   13-09-22
#       1. Add "exceptions" report for where certain files are intentionaally dropped i.e. .csv within metadata folder - done
#           Can the above be emailed?
#       2. Create metadata fragments as required - see agreed structures from Halley - done
#       3. Drop extra folders within "metadata" - done
#       4. Modify resulting content and metadata folder titles to include parent package pre-fix - done




##########################################################################################################
# Import modules
##########################################################################################################

from concurrent.futures import ThreadPoolExecutor, as_completed
from preservicatoken import securitytoken
import requests
import logging
# import pymsgbox
import time
from datetime import datetime
from pathlib import Path
import os
import io
from io import StringIO, BytesIO
import sys
import argparse
import getopt
import shutil
from shutil import unpack_archive
from shutil import rmtree
from os import listdir
from os.path import isfile, join
import lxml.etree
import json
import re
# import tkinter as tk
# from tkinter import *
# from tkinter import filedialog
# from tkinter import simpledialog
import configparser
import hashlib
import boto3
import botocore
from boto3.s3.transfer import S3Transfer
import threading

import zipfile
from zipfile import BadZipfile


##########################################################################################################
# Standard Functions
##########################################################################################################
def fDate():
    query_date = datetime.now().strftime("%Y-%m-%d")
    return query_date


def fIntTime():
    query_inttime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return query_inttime


def fTime():
    query_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return query_time


def fAltTime():
    # 2020-09-25T13:48:55.000Z
    alttime_A = datetime.now().strftime("%Y-%m-%d")
    alttime_B = datetime.now().strftime("%H:%M:%S")
    query_alttime = str(alttime_A + "T" + alttime_B + ".000Z")
    return query_alttime


def exit_all():
    print("exit_all")
    root_logger.info("exit_all")
    sys.exit(0)


def fInspectIllegalCharacters(fi_string):
    # root_logger.info("fInspectIllegalCharacters: Fragment as supplied : " + str(fi_string))
    for ic in range(len(list_chars)):
        print(list_chars[ic])
        index = fi_string.find(list_chars[ic])
        print(index)
        if index == -1:
            pass
        else:
            if ic == 0:
                if fi_string[index: index + 5] == "&amp;":
                    # root_logger.info("fInspectIllegalCharacters: Illegal characters no resolution required : " + str(fi_string))
                    continue
                elif fi_string[index: index + 2] == "&#":
                    # root_logger.info("fInspectIllegalCharacters: Illegal characters no resolution required : " + str(fi_string))
                    continue
                fi_string = fi_string.replace(fi_string[index], list_replace[ic])
                # root_logger.info("fInspectIllegalCharacters: Illegal characters resolved : " + str(fi_string))
            else:
                fi_string = fi_string.replace(fi_string[index], list_replace[ic])
                # root_logger.info("fInspectIllegalCharacters: Illegal characters resolved : " + str(fi_string))
    return fi_string


##########################################################################################################
# Global Lists, Dictionaries and Flags
##########################################################################################################

list_approved_bitstream_type = [("*", "Representation_Preservation_1")]

list_approved_pres_extension = [("STREAMING.bin", 1),("OBJ.jp2", 2),("OBJ.jpg", 3),("OBJ.mp4", 4),("OBJ.jp2", 5),("JP2.jp2", 6),("JPG.jpg", 7),("MP4.mp4", 8),("PDF.pdf", 9)]

list_workflows = ["DigArch"]

list_non_approved = [".db", ".DS_Store",".csv"]

list_add_identifiers_to_asset = ["asset"]

list_chars = ['&','<','>']

list_replace = ['&amp;','&lt;','&gt;']


list_manifest_directories = []
list_container = []     
list_folders_in_dir = []   
list_files_in_dir = []        
list_metadata_files = []        
list_unpacked_export_zips = []     
list_longest_path = []
list_greatest_file_depth = []
list_reference_folder = []
list_reference_folder_path = []
list_reference_folder_preamble = []
list_reference_folder_preamble_components = []
list_metadata_source = []
list_pax_zip_folders = []
list_delete_pax = []

list_working_folders = []

list_normalised_filenames = []

list_CD_Hdr_RunTS = []
list_CD_Hdr_RecordingTS = []
list_CD_Data_RunTS = []
list_CD_Data_RecordingDate = []


list_man_file_ext = []
list_man_ext_range = []
list_man_pkg_type = []

list_man_ext = []
list_man_pkg_type = []
list_man_package_ext = []
list_man_package_name = []
list_man_contained_files = []

list_parent_hierarchy = []

list_prepend = []
list_append = []
list_keywords = []
list_md_element = []

list_parent_path_parts = []
list_mkdir_name = []

list_json_key_val = []

list_idents = []

list_packages = []

list_sub_paths = []

list_available_pres_extension = []

list_excepted_files = []

c_list_folders_in_dir = []
c_list_files_in_dir = []

list_contents_folder = []
list_metadata_folder = []

dict_fs_resources = []

dict_folder_folderlevel = {}
dict_folder_folderrelpath = {}
dict_filepath = {}
dict_fs_file_path = {}
dict_directory_content = {}
dict_file_checksum = {}
dict_file_type = {}
dict_catalog = {}
dict_PAX_asset_x = {}
dict_PAX_asset_y = {}
dict_PAX_asset_z = {}
dict_PAX_asset = {}
dict_asset_parent = {}
dict_asset_parent_parent = {}

dict_asset_name = {}
dict_asset_year = {}
dict_asset_month = {}

dict_reference_foldercount_from_file = {}

dict_rfs_file_path = {}

dict_individual_file_checksum = {}
dict_checksum_manifest = {}

dictHeader_doc_no_filename = {}
dictHeader_doc_no_fragment = {}
dictInfo_doc_no_fragment = {}

dict_identifiers_biblio = {}

dict_containerf = {}

dict_metadata_dc = {}
dict_metadata_mods = {}

dict_approved_files = {}

dict_frh_orig_folder_name = {}
dict_frh_photographer = {}
dict_frh_title = {}
dict_frh_description = {}
dict_frh_box = {}
dict_frh_fromdate = {}
dict_frh_todate = {}
dict_frh_m_files = {}

##########################################################################################################
# Clear Lists and Dictionaries
##########################################################################################################
def fReset_Lists_Dicts():
    list_manifest_directories.clear()
    list_container.clear()
    list_files_in_dir.clear()
    list_folders_in_dir.clear()
    list_metadata_files.clear()
    list_unpacked_export_zips.clear()
    list_longest_path.clear()
    list_greatest_file_depth.clear()
    list_reference_folder_preamble.clear()
    list_reference_folder_preamble_components.clear()
    list_reference_folder.clear()
    list_reference_folder_path.clear()          # lists the parent folder common path for files lowest in the tree
    list_metadata_source.clear()
    list_pax_zip_folders.clear()
    list_delete_pax.clear()

    list_CD_Hdr_RunTS.clear()
    list_CD_Hdr_RecordingTS.clear()
    list_CD_Data_RunTS.clear()
    list_CD_Data_RecordingDate.clear()

    list_parent_hierarchy.clear()

    list_prepend.clear()
    list_append.clear()
    list_keywords.clear()
    list_md_element.clear()
    
    list_man_file_ext.clear()
    list_man_ext_range.clear()
    list_man_pkg_type.clear()

    list_man_ext.clear()
    list_man_pkg_type.clear()
    list_man_package_ext.clear()
    list_man_package_name.clear()
    list_man_contained_files.clear()
    
    list_man_contained_files.clear()
    
    list_parent_path_parts.clear()
    
    list_json_key_val.clear()
    
    list_idents.clear()
    
    list_packages.clear()
    
    list_sub_paths.clear()
    
    list_contents_folder.clear()
    list_metadata_folder.clear()
    
    c_list_folders_in_dir.clear()
    c_list_files_in_dir.clear()
    
    list_excepted_files.clear()
    
    dict_fs_resources.clear()

    dict_folder_folderlevel.clear()
    dict_folder_folderrelpath.clear()
    dict_filepath.clear()
    dict_fs_file_path.clear()
    dict_directory_content.clear()
    dict_file_checksum.clear()
    dict_file_type.clear()
    dict_catalog.clear()

    dict_PAX_asset_x.clear()
    dict_PAX_asset_y.clear()
    dict_PAX_asset_z.clear()
    dict_PAX_asset.clear()

    dict_asset_parent.clear()
    dict_asset_parent_parent.clear()

    dict_asset_name.clear()
    dict_asset_year.clear()
    dict_asset_month.clear()

    dict_reference_foldercount_from_file.clear()

    dict_rfs_file_path.clear()          # full local path for files in working folder

    dict_individual_file_checksum.clear()

    dictHeader_doc_no_filename.clear()
    dictHeader_doc_no_fragment.clear()
    dictInfo_doc_no_fragment.clear()
    
    dict_identifiers_biblio.clear()
    
    dict_containerf.clear()
    
    dict_metadata_dc.clear()
    dict_metadata_mods.clear()
    
    dict_approved_files.clear()
    
    #dict_frh_orig_folder_name.clear()
    #dict_frh_photographer.clear()
    #dict_frh_title.clear()
    #dict_frh_description.clear()
    #dict_frh_box.clear()
    #dict_frh_fromdate.clear()
    #dict_frh_todate.clear()
    #dict_frh_m_files.clear()


##########################################################################################################
# Classes
##########################################################################################################
class ProgressPercentage(object):
    global prog_val

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.write("\n" + "\n")
            sys.stdout.flush()

            prog_val = "\r%s  %s / %s  (%.2f%%)" % (self._filename, self._seen_so_far, self._size, percentage)


##########################################################################################################
# Functions
##########################################################################################################
def fCheckWorkflowStatus(fc_wf_id):
    root_logger.info("fCheckWorkflowStatus")
    r_wf_state = "starting"
    wf_loop_count = 0
    while True:
        if wf_loop_count > 30:
            print("LOG INTO PRESERVICA AND CHECK THE WORKFLOW STATUS")
            sys.exit()
        time.sleep(60)
        url = "https://" + hostval + "/sdb/rest/workflow/instances/" + fc_wf_id
        headers = {
            'Preservica-Access-Token': securitytoken(config_input),
            'Content-Type': "application/xml"
        }
        r_wf_start_response = requests.request("GET", url, headers=headers)
        root_logger.info("fCheckWorkflowStatus : Workflow Response " + r_wf_start_response.text)
        NSMAP = {"xip_wf": "http://workflow.preservica.com"}

        b_r_wf_start_response = bytes(r_wf_start_response.text, 'utf-8')
        parser = lxml.etree.XMLParser(remove_blank_text=True, ns_clean=True)
        r_wf_tree = lxml.etree.fromstring(b_r_wf_start_response, parser)
        r_workflow_state = r_wf_tree.xpath("//xip_wf:WorkflowInstance/xip_wf:State", namespaces = NSMAP)
        for r_wfstate in range(len(r_workflow_state)):
            r_wf_state = r_workflow_state[r_wfstate].text
            print("workflow state " + str(r_wf_state))
        if r_wf_state.lower() != "active":
            print("workflow state " + str(r_wf_state))
            sys.exit()
        wf_loop_count += 1


def fCopyAllFiles(ca_target_folder):
    root_logger.info("fCopyAllFiles")
    sub_r = "fCopyAllFiles"
    target_path = os.path.join(ca_target_folder, container)
    root_logger.info("fCopyAllFiles : target_path " + str(target_path))
    list_filepath = dict_filepath.keys()
    for ind_file in list_filepath:
        root_logger.info("fCopyAllFiles : PreAmble " + str(list_reference_folder_path[0]))
        root_logger.info("fCopyAllFiles : ind file " + str(ind_file))
        source_file = os.path.join(list_reference_folder_path[0], ind_file)
        root_logger.info("fCopyAllFiles : source_file " + str(source_file))
        target_file = os.path.join(target_path, ind_file)
        root_logger.info("fCopyAllFiles : target_file " + str(target_file))
        fCopyData(source_file, target_file, fCopyData)


def fCopyData(cd_package_source, cd_package_working, cd_sub_r):
    root_logger.info("fCopyData")
    root_logger.info("fCopyData : " + str(cd_sub_r))
    cd_working_parent = os.path.dirname(cd_package_working)
    if not os.path.isdir(cd_working_parent):
        fCreateFolderStructure(cd_working_parent)
    
    try:
        shutil.copy(cd_package_source, cd_package_working)
        root_logger.info(": fCopyData : " + str(cd_sub_r) + " : Copy completed from " + str(cd_package_source) + " to " + str(cd_package_working))
        return True
    except shutil.Error as err:
        print(err.args[0])
        root_logger.info(
            " : fCopyData : " + str(cd_sub_r) + " : Copy failed from " + str(cd_package_source) + " to " + str(cd_package_working))
        return False


def fCopytreeData(cd_package_source, cd_package_working):
    root_logger.info("fCopytreeData")
    try:
        shutil.copytree(cd_package_source, cd_package_working)
        root_logger.info(": fCopytreeData : Copy completed from " + str(cd_package_source) + " to " + str(cd_package_working))
        return True
    except:
        root_logger.info(": fCopytreeData : Copy failed from " + str(cd_package_source) + " to " + str(cd_package_working))
        return False


def fCreateContainerFolderOpexFragment(ccf_target_folder):
    root_logger.info("fCreateContainerFolderOpexFragment")
    c_folder_val = container
    root_logger.info("fCreateContainerFolderOpexFragment : directory in scope " + c_folder_val)
    c_list_folders_in_dir.clear()
    c_list_files_in_dir.clear()
    c_folder_val_full_path = os.path.join(ccf_target_folder, c_folder_val)
    if os.path.isdir(c_folder_val_full_path):
        c_opex_data_folder = ""
        c_opex_data_file = ""
        c_opex_file_name = os.path.basename(c_folder_val_full_path) + ".opex"
        c_temp_opex_file = os.path.join(c_folder_val_full_path, c_opex_file_name)
        for c_child in os.listdir(c_folder_val_full_path):
            if os.path.isdir(os.path.join(c_folder_val_full_path, c_child)):
                c_list_folders_in_dir.append(c_child)
            if os.path.isfile(os.path.join(c_folder_val_full_path, c_child)):
                list_files_in_dir.append(c_child)
        for c_lfd in range(len(c_list_folders_in_dir)):
            c_opex_data_folder = c_opex_data_folder + "<opex:Folder>" + c_list_folders_in_dir[c_lfd] + "</opex:Folder>"
            root_logger.info("fCreateContainerFolderOpexFragment : opex_data_folder " + c_opex_data_folder)
        for c_lff in range(len(c_list_files_in_dir)):
            if os.path.splitext(c_list_files_in_dir[c_lff])[1] == ".opex":
                c_opex_data_file = c_opex_data_file + "<opex:File type=\"metadata\">" \
                                   + c_list_files_in_dir[c_lff] + "</opex:File>"
            else:
                c_opex_data_file = c_opex_data_file + "<opex:File type=\"content\">" \
                                   + c_list_files_in_dir[c_lff] + "</opex:File>"
        source_ID = ""
        LegacyXIP = ""
        Identifiers_biblio = ""
        Identifiers_catalog = ""
        security_tag = ""
        ref_fldr_title = ""
        opex_desc_meta = ""
        ref_fldr_desc = ""
        opex_fixity_type = ""
        opex_fixity_checksum = ""
        
        c_xml_package = fCreateOpexFragment(c_opex_data_folder, c_opex_data_file, opex_fixity_type, opex_fixity_checksum,
                                            LegacyXIP, Ident_Biblio_Key, Identifiers_biblio, Identifiers_catalog, source_ID, security_tag, ref_fldr_title, ref_fldr_desc, opex_desc_meta)
        try:
            c_opex_temp = open(c_temp_opex_file, 'w', encoding='utf-8')
            c_opex_temp.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" + "\n")
            c_opex_temp.write(c_xml_package)
            c_opex_temp.close()
            root_logger.info("fCreateContainerFolderOpexFragment : file created " + c_temp_opex_file)
        except:
            root_logger.warning("fCreateContainerFolderOpexFragment : opex could not be created " + c_temp_opex_file)


def fCreateFileOpexFragments(cf_target_folder, security_tag, cf_package):
    Identifiers_biblio  = ""
    pax_type            = ""
    objects_io_flag     = 0
    metadata_io_flag    = 0
    root_logger.info("fCreateFileOpexFragments")
    for opex_r, opex_d, opex_f in os.walk(cf_target_folder):
        print("opex_d " + str(opex_d))
        for fil in opex_f:
            opex_file = os.path.join(opex_r, fil)
            if os.path.isfile(opex_file):
                list_path_comps = opex_file.split(os.sep)
                if (cf_package + "_contents") in list_path_comps:
                    objects_io_flag = 1
                elif (cf_package + "_metadata") in list_path_comps:
                    metadata_io_flag = 1
                
                
                opex_file_noext = os.path.splitext(os.path.basename(opex_file))[0]
                #if opex_file_noext[-4:] == ".pax":
                #    opex_file_noext = opex_file_noext.rstrip(".pax")
                
                print("opex_file_noext " + str(opex_file_noext))
                
                #array_opex_file_noext = opex_file_noext.split("_")
                #if len(array_opex_file_noext) < 2:
                #    root_logger.error(" : fCreateFileOpexFragments : cannot determine pax type " + str(opex_file_noext))
                #else:
                #    pax_type = array_opex_file_noext[-1]
                
                #list_idents.clear()
                #if pax_type in list_add_identifiers_to_asset:
                #    for ljkv in list_json_key_val:
                #        print(ljkv)
                #        Ident_type = ljkv[0]
                #        Ident_val  = ljkv[1]
                #        list_idents.append("<opex:Identifier type=\"" + str(Ident_type) + "\">" + str(Ident_val) + "</opex:Identifier>")
                    
                #    Identifiers_biblio = "<opex:Identifiers>" + "".join(list_idents) + "</opex:Identifiers>"
                #else:
                #    Identifiers_biblio = ""
                
                
                print("Identifiers_biblio " + str(Identifiers_biblio))
                
                # opex title
                opex_title          = dict_frh_title.get(cf_package, "")
                opex_description    = dict_frh_description.get(cf_package, "")
                
                
                if append_descriptive_metadata == 1:
                    root_logger.info("fCreateFileOpexFragments : opex_file " + str(opex_file))
                    
                    # bag_ref = str(os.path.basename(os.path.dirname(opex_file)))
                    bag_ref = opex_file_noext
                    
                    print("bag_ref " + str(bag_ref))
                    #print(dict_metadata_dc.keys())
                    #print(dict_metadata_mods.keys())
                    
                    root_logger.info("fCreateFileOpexFragments : metadata bag ref " + str(bag_ref))
                    metadata_payload = ""
                    #metadata_payload_dc = ""
                    #metadata_payload_mods = ""
                    #metadata_payload_dc = dict_metadata_dc.get(bag_ref, "")
                    #metadata_payload_mods = dict_metadata_mods.get(bag_ref, "")
                    #metadata_payload = metadata_payload_dc + metadata_payload_mods
                    if metadata_payload == "":
                        opex_desc_metadata = ""
                    #else:
                    #    opex_desc_metadata = "<opex:DescriptiveMetadata>" \
                    #                         + metadata_payload \
                    #                         + "</opex:DescriptiveMetadata>"
                else:
                    opex_desc_metadata = ""
                                
                opex_data_folder = ""
                opex_data_file  = ""
                opex_fixity_type = "MD5"
                opex_fixity_checksum = fv6Checksum(opex_file, "md5")
                LegacyXIP = ""
                Identifiers_catalog = ""
                source_ID = ""
                ref_file_title = ""
                ref_file_desc = ""
                
                if objects_io_flag      == 1:
                    Identifiers_biblio  = IOCategoryElement
                    Ident_Biblio_Key    = "ioCategory"
                elif metadata_io_flag   == 1:
                    Identifiers_biblio  = "string value to be confirmed" 
                    Ident_Biblio_Key    = "ioCategory"
                else:
                    Identifiers_biblio  = ""
                    Ident_Biblio_Key    = ""
                
                file_xml_package = fCreateOpexFragment(opex_data_folder, opex_data_file, opex_fixity_type, opex_fixity_checksum,
                                            LegacyXIP, Ident_Biblio_Key, Identifiers_biblio, Identifiers_catalog, source_ID, security_tag, ref_file_title, ref_file_desc, opex_desc_metadata)
                
                opex_file_withext = os.path.join(opex_file + ".opex")
                opex_filepath = os.path.join(cf_target_folder, container, opex_file_withext)
                
                if not os.path.exists(opex_filepath):
                    try:
                        opex_file = open(opex_filepath, 'w', encoding='utf-8')
                        opex_file.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" + "\n")
                        opex_file.write(file_xml_package)
                        opex_file.close()
                        root_logger.info("fCreateFileOpexFragments : file created " + opex_filepath)
                    except:
                        root_logger.warning("fCreateFileOpexFragments : opex could not be created " + opex_filepath)


def fCreateFolderOpexFragments(cf_target_folder, security_tag, ref_title, cf_wflow_type, cf_package):
    Ident_Biblio_Key = ""
    Identifiers_biblio = ""
    Identifiers_catalog = ""
    LegacyXIP = ""
    source_ID = ""
    objects_flag            = 0
    metadata_flag           = 0
    
    # track folder depth / folder name and apply appropriate identifietr value  
    for fol_root, fol_dir, fol_fol in os.walk(cf_target_folder):
        for fol_d in fol_dir:
            
            ## determine folder depth
            opex_fol = os.path.join(fol_root, fol_d)
            
            array_targetf_container_wf = targetf_container_wf.split(os.sep)
            baseline_folder_depth = len(array_targetf_container_wf)
            
            array_opex_folder = opex_fol.split(os.sep)
            current_folder_depth = len(array_opex_folder)
            
            actual_folder_depth = current_folder_depth - baseline_folder_depth
            print("actual_folder_depth " + str(actual_folder_depth))
            
            
            ## set source_ID
            if fol_d == cf_wflow_type:
                source_ID = cf_wflow_type + "_test"
            else:
                source_ID = ""


            ## set soCategory and folder title
            ref_fldr_title          = ""
            desc_metadata_xml       = ""
            print("fol_d " + str(fol_d.lower()))
            
            if actual_folder_depth == 1:
                curr_fol_identifier     = SOCategoryContainer
                Ident_Biblio_Key        = "soCategory"
                desc_metadata_xml       = fCreateDigArchMetadataFragments("mdfrag1", CMSCollectionID)
                ref_fldr_title          = fol_d
            elif actual_folder_depth    == 2:
                if fol_d.lower()        == cf_package.lower() + "_metadata":
                    metadata_flag       = 1
                    objects_flag        = 0
                    curr_fol_identifier = SOCategoryMetadata
                    Ident_Biblio_Key    = "soCategory"
                    ref_fldr_title      = opex_title_metadata
                elif fol_d.lower()      == cf_package.lower() + "_contents":
                    objects_flag        = 1
                    metadata_flag       = 0
                    desc_metadata_xml   = fCreateDigArchMetadataFragments("mdfrag4", FAComponentIdNo)
                    curr_fol_identifier = SOCategoryContents
                    Ident_Biblio_Key    = "soCategory"
                    ref_fldr_title      = opex_title_content
                else:
                    curr_fol_identifier = ""
            elif actual_folder_depth    >= 3:
                curr_fol_identifier     = SOCategoryElement
                Ident_Biblio_Key        = "soCategory"
            #elif actual_folder_depth    == 3 and metadata_flag == 1:
            #    curr_fol_identifier     = SOCategoryElement
            #    Ident_Biblio_Key        = "soCategory"
            
            else:
                curr_fol_identifier     = ""

       
            if os.path.isdir(opex_fol):
                LegacyXIP = ""
                Identifiers_catalog = ""
                list_folders_in_dir.clear()
                list_files_in_dir.clear()
                opex_data_folder = ""
                opex_data_file = ""
                opex_file_name = os.path.basename(opex_fol) + ".opex"
                temp_opex_file = os.path.join(opex_fol, opex_file_name)
                for child in os.listdir(opex_fol):
                    if os.path.isdir(os.path.join(opex_fol, child)):
                        list_folders_in_dir.append(child)
                    if os.path.isfile(os.path.join(opex_fol, child)):
                        list_files_in_dir.append(child)
                for lfd in range(len(list_folders_in_dir)):
                    opex_data_folder = opex_data_folder + "<opex:Folder>" + list_folders_in_dir[lfd] + "</opex:Folder>"
                    root_logger.info("fCreateFolderOpexFragments 4 : opex_data_folder " + opex_data_folder)
                for lff in range(len(list_files_in_dir)):
                    if os.path.splitext(list_files_in_dir[lff])[1] == ".opex":
                        opex_data_file = opex_data_file + "<opex:File type=\"metadata\">" + list_files_in_dir[lff] \
                                         + "</opex:File>"
                    else:
                        opex_data_file = opex_data_file + "<opex:File type=\"content\">" + list_files_in_dir[lff] \
                                         + "</opex:File>"
                    root_logger.info("fCreateFolderOpexFragments 5 : opex_data_file " + opex_data_folder)
                
                # opex title
                
                opex_desc_metadata_xml = desc_metadata_xml
                
               
                if curr_fol_identifier == "":
                    Identifiers_biblio = ""
                else:
                    Identifiers_biblio = curr_fol_identifier
                
                
                print("Identifiers_biblio " + str(Identifiers_biblio))
                
                
                # opex description
                ref_fldr_desc = dict_frh_description.get(fol_d, "NA")
                
                opex_fixity_type = ""
                opex_fixity_checksum = ""
           
                xml_package = fCreateOpexFragment(opex_data_folder, opex_data_file, opex_fixity_type, opex_fixity_checksum, 
                                                  LegacyXIP, Ident_Biblio_Key, Identifiers_biblio, Identifiers_catalog, source_ID, security_tag, ref_fldr_title, ref_fldr_desc, opex_desc_metadata_xml)
                try:
                    opex_temp = open(temp_opex_file, 'w', encoding='utf-8')
                    opex_temp.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" + "\n")
                    opex_temp.write(xml_package)
                    opex_temp.close()
                    root_logger.info("fCreateFolderOpexFragments 6 : file created " + temp_opex_file)
                except:
                    root_logger.warning("fCreateFolderOpexFragments : opex could not be created " + temp_opex_file)


def fCreateOpexFragment(opex_data_folder, opex_data_file, opex_fixity_type, opex_fixity_checksum, LegacyXIP, Ident_Biblio_Key, Identifiers_biblio, Identifiers_catalog, source_ID, security_tag, ref_fldr_title, ref_fldr_desc, opex_desc_md):
    sub_r = "fCreateOpexFragment"
    root_logger.info(sub_r)
    
    ## Opex metadata structure
    opex_master                 = ""
    opex_header_open                 = "<opex:OPEXMetadata xmlns:opex=\"http://www.openpreservationexchange.org/opex/v1.0\">"
    opex_header_close           = "</opex:OPEXMetadata>"
    
    # Opex Transfer
    opex_transfer_open          = "<opex:Transfer>"
    opex_transfer_close         = "</opex:Transfer>"
    
    
    ##Opex SourceID
    opex_source_id_open         = "<opex:SourceID>"
    opex_source_id_close        = "</opex:SourceID>"
    if source_ID == "":
        opex_source = ""
    else:
        opex_source             = opex_source_id_open + source_ID + opex_source_id_close
    
    ## Opex_Fixity
    opex_fixity                 = ""
    opex_fixity_open            = "<opex:Fixities>"
    opex_fixity_close           = "</opex:Fixities>"
    
    if opex_fixity_type == "" or opex_fixity_checksum == "":
        opex_fixity             = ""
    else:
        opex_fixity_string      = "<opex:Fixity type=\"" \
                                    + str(opex_fixity_type) \
                                    + "\" value=\"" \
                                    + str(opex_fixity_checksum) \
                                    + "\"/>"
        
        opex_fixity             = opex_fixity_open \
                                    + opex_fixity_string \
                                    + opex_fixity_close
    
    
    ## Opex_Manifest
    opex_manifest_open          = "<opex:Manifest>"
    opex_manifest_close         = "</opex:Manifest>"
    opex_folders_open           = "<opex:Folders>"
    opex_folders_close          = "</opex:Folders>"
    opex_files_open             = "<opex:Files>"
    opex_files_close            = "</opex:Files>"
    
    if opex_data_folder == "" and opex_data_file == "":
        opex_manifest = "<opex:Manifest/>"
    else:
        if opex_data_folder == "":
            opex_folders = ""
        else:
            opex_folders        = opex_folders_open + opex_data_folder + opex_folders_close
            
        if opex_data_file == "":
            opex_files = ""
        else:
            opex_files          = opex_files_open + opex_data_file + opex_files_close

        opex_manifest           = opex_manifest_open \
                                    + opex_folders \
                                    + opex_files \
                                    + opex_manifest_close
    
    # Opex Properties
    opex_properties_open        = "<opex:Properties>"
    opex_properties_close       = "</opex:Properties>"
    
    ## Opex_title
    if ref_fldr_title == "":
        opex_title = ""
    else:
        opex_title              = "<opex:Title>" \
                                    + ref_fldr_title \
                                    + "</opex:Title>"
                                    
    ## Opex_Description
    if ref_fldr_desc            == "":
        opex_description        = ""
    else:
        opex_description        = "<opex:Description>" \
                                    + ref_fldr_title \
                                    + "</opex:Description>"

    ## Opex Security Tag
    opex_sectag                 = "<opex:SecurityDescriptor>" \
                                    + security_tag \
                                    + "</opex:SecurityDescriptor>"
    
    
    # Opex Identifiers Biblio
    opex_identifiers_biblio     = ""
    opex_identifiers_catalog    = ""
    if Identifiers_biblio       == "":
        opex_identifiers_biblio = ""
    else:
        opex_identifiers_biblio = "<opex:Identifiers>" \
                                    + "<opex:Identifier type=\"" \
                                    + Ident_Biblio_Key \
                                    + "\">" \
                                    + Identifiers_biblio \
                                    + "</opex:Identifier>" \
                                    + "</opex:Identifiers>"
    
    if Identifiers_catalog       == "": 
        opex_identifiers_catalog = ""
    
    opex_identifiers            = opex_identifiers_biblio \
                                    + opex_identifiers_catalog      
    
    
    opex_properties             = opex_properties_open \
                                    + opex_title \
                                    + opex_description \
                                    + opex_sectag \
                                    + opex_identifiers \
                                    + opex_properties_close
    

    # Opex_DescriptiveMetadata
    opex_descript_meta_open         = "<opex:DescriptiveMetadata>"
    opex_descript_meta_close        = "</opex:DescriptiveMetadata>"
    
    if opex_desc_md == "" and LegacyXIP == "":
        opex_descript_meta          = ""
    else:
        opex_descrip_meta_desc      = opex_desc_md       
        opex_descript_meta_legacy   = LegacyXIP
        
        opex_descript_meta          = opex_descript_meta_open \
                                        + opex_descrip_meta_desc \
                                        + opex_descript_meta_legacy \
                                        + opex_descript_meta_close \

    # Opex_Master
    opex_master = opex_header_open \
                + opex_transfer_open \
                + opex_source \
                + opex_fixity \
                + opex_manifest \
                + opex_transfer_close \
                + opex_properties \
                + opex_descript_meta \
                + opex_header_close
                                
    print(ref_fldr_title)
    print(opex_master)
    root_logger.info("fCreateOpexFragment : opex master " + str(opex_master))
    parser = lxml.etree.XMLParser(remove_blank_text=True)
    opex_xml = lxml.etree.fromstring(opex_master, parser=parser)
    new_opex_xml = lxml.etree.tostring(opex_xml, encoding="unicode", pretty_print=True)
    # validate xml
    xmlschema_doc = lxml.etree.parse('OPEX-Metadata.xsd')
    xmlschema = lxml.etree.XMLSchema(xmlschema_doc)
    if xmlschema.validate(opex_xml):
        root_logger.info("fCreateOpexFragment : Metadata is valid")
    else:
        root_logger.warning("fCreateOpexFragment : Metadata validation failed for " + str(opex_master))
    return new_opex_xml


def fDelete_Content(full_path):
    root_logger.info("fDelete_Content")
    try:
        os.remove(full_path)
    except:
        root_logger.warning(": fDelete_Content : Could not delete " + str(full_path) + " contains a directory")


def fCreateOpexFragment_OLD(opex_data_folder, opex_data_file, LegacyXIP, Ident_Biblio_Key, Identifiers_biblio, Identifiers_catalog, source_ID, security_tag, ref_fldr_title, ref_fldr_desc, opex_desc_md):
    root_logger.info("fCreateOpexFragment")
    opex_package1 = "<opex:OPEXMetadata xmlns:opex=\"http://www.openpreservationexchange.org/opex/v1.0\">"
    opex_package2 = "<opex:Transfer>"
    opex_source_id_open = "<opex:SourceID>"
    opex_source_id_close = "</opex:SourceID>"
    if not source_ID == "":
        opex_source = opex_source_id_open + source_ID + opex_source_id_close
    else:
        opex_source = ""
    opex_package3_open = "<opex:Manifest>"
    opex_folders_open = "<opex:Folders>"
    opex_folders_close = "</opex:Folders>"
    opex_files_open = "<opex:Files>"
    opex_files_close = "</opex:Files>"
    if not opex_data_folder == "":
        opex_folders = opex_folders_open + opex_data_folder + opex_folders_close
    else:
        opex_folders = ""
    if not opex_data_file == "":
        opex_files = opex_files_open + opex_data_file + opex_files_close
    else:
        opex_files = ""
    opex_package4 = "</opex:Manifest>"
    opex_package5 = "</opex:Transfer>"
    opex_package6a = "<opex:Properties>"
    
    opex_package6b = ""
    
    opex_package6b1 = ""
    
    if not Identifiers_biblio == "":
        opex_package6b2 = "<opex:Identifiers><opex:Identifier type= \"SOCategory\">" + Identifiers_biblio + "</opex:Identifier></opex:Identifiers>"
    else:
        opex_package6b2 = ""
        
    opex_package6c = "<opex:SecurityDescriptor>" + security_tag + "</opex:SecurityDescriptor>"
    opex_package6d = "</opex:Properties>"
    opex_package6 = opex_package6a + opex_package6b + opex_package6b1 + opex_package6b2 + opex_package6c + Identifiers_catalog + opex_package6d
    if opex_desc_md != "":
        opex_desc_meta = "<opex:DescriptiveMetadata>" + opex_desc_md + "</opex:DescriptiveMetadata>"
    else:
        opex_desc_meta = ""
    opex_package7 = "</opex:OPEXMetadata>"
    opex_master = opex_package1 + opex_package2 + opex_source + opex_package3_open + opex_files \
                  + opex_folders + opex_package4 + opex_package5 + opex_package6 + opex_desc_meta + LegacyXIP + opex_package7
    root_logger.info("fCreateOpexFragment : opex master " + str(opex_master))
    parser = lxml.etree.XMLParser(remove_blank_text=True)
    opex_xml = lxml.etree.fromstring(opex_master, parser=parser)
    new_opex_xml = lxml.etree.tostring(opex_xml, encoding="unicode", pretty_print=True)
    # validate xml
    xmlschema_doc = lxml.etree.parse('OPEX-Metadata.xsd')
    xmlschema = lxml.etree.XMLSchema(xmlschema_doc)
    if xmlschema.validate(opex_xml):
        root_logger.info("fCreateOpexFragment : Metadata is valid")
    else:
        root_logger.warning("fCreateOpexFragment : Metadata validation failed for " + str(opex_master))
    return new_opex_xml


def fDelete_Content(full_path):
    root_logger.info("fDelete_Content")
    try:
        os.remove(full_path)
    except:
        root_logger.warning(": fDelete_Content : Could not delete " + str(full_path) + " contains a directory")


def fDeleteWorking_Folder(dw_workingf):
    root_logger.info("fDeleteWorking_Folder")
    for w_dir in os.listdir(dw_workingf):
        full_path_w_dir = os.path.join(dw_workingf, w_dir)
        if os.path.isdir(full_path_w_dir):
            shutil.rmtree(full_path_w_dir)
            root_logger.info("fDeleteWorking_Folder : Folder " + str(full_path_w_dir) + " deleted")


def fDeleteZip():
    root_logger.info("fDeleteZip")
    err_fDeleteZip = 0
    for zipper in list_unpacked_export_zips:
        if os.path.exists(zipper):
            try:
                os.remove(zipper)
                root_logger.info(": fDeleteZip : zip deleted " + str(zipper))
            except:
                root_logger.info(": fDeleteZip : delete failed for " + str(zipper))
                err_fDeleteZip = 1
        else:
            root_logger.info(": fDeleteZip : delete failed because the zip file does not exist " + str(zipper))
            err_fDeleteZip = 1
    if err_fDeleteZip == 0:
        return True
    elif err_fDeleteZip == 1:
        return False


def fSanitiseFolders():
    root_logger.info("fSanitiseFolders")
    print("fSanitiseFolders")
    sanitise_complete = 0
    for working_folder_area in list_working_folders:
        print("folder " + str(working_folder_area))
        for wf_root, wf_dir, wf_file in os.walk(working_folder_area):
            for wf_rm in wf_dir:
                print("fSanitiseFolders : Removing " + str(wf_rm))
                try:
                    shutil.rmtree(os.path.join(wf_root, wf_rm))
                    print("fSanitiseFolders : Removing " + str(wf_rm))
                except:
                    root_logger.warning("fSanitiseFolders : Failed on delete of " + str(os.path.join(wf_root, wf_rm)))
                    sanitise_complete = 1
            for wf_rmf in wf_file:
                try:
                    os.remove(os.path.join(wf_root, wf_rmf))
                except:
                    root_logger.warning("fSanitiseFolders : Failed on delete of " + str(os.path.join(wf_root, wf_rmf)))
                    sanitise_complete = 1
    if sanitise_complete == 0:
        return True
    elif sanitise_complete == 1:
        return False


def fGet_file_no_ext(full_path):
    no_ext = os.path.splitext(full_path)
    file_no_ext = os.path.basename(no_ext[0])
    return file_no_ext


def fGet_filesize(full_path):
    fsize = os.path.getsize(full_path)
    return fsize


def gettoken(config_input):
    accesstoken = securitytoken(config_input)
    return accesstoken


def fOutputDictionaries():
    root_logger.info("fOutputDictionaries ")
    for aa, bb in dict_filepath.items():
        root_logger.info("fOutputDictionaries : filepath = " + str(aa) + " : filename = " + str(bb) + "\n")
    for cc, dd in dict_folder_folderlevel.items():
        root_logger.info("fOutputDictionaries : filepath = " + str(cc) + " : folder level = " + str(dd) + "\n")
    for ee, ff in dict_file_checksum.items():
        root_logger.info("fOutputDictionaries : filepath = " + str(ee) + " : file MD5 value = " + str(ff) + "\n")


def fQuery_folder(qf_target_container, bucket_prefix):
    root_logger.info("fQuery_folder")
    global bucket
    packages = ""
    tl_container_folder = ""
    tlf_count = 0
    print("qf_target_container " + str(qf_target_container))
    for qroot, qd_names, qf_names in os.walk(qf_target_container):
        if tlf_count == 0:
            tl_container_folder = qf_target_container
        for qf in qf_names:
            print(qf)
            response = False
            if os.path.isfile(os.path.join(qroot, qf)):
                qfull_path = os.path.join(qroot, qf)
                print("qfull_path " + str(qfull_path) + "\n")
                f_size = fGet_filesize(qfull_path)
                f_no_ext = fGet_file_no_ext(qfull_path)
                path_no_ext = bucket_prefix \
                              + "/" \
                              + qroot.replace(qf_target_container, "").lstrip("\\").replace("\\", "/") + "/" \
                              + qf
                root_logger.info("path_no_ext " + str(path_no_ext))
                packages = qf
                response = fUpload_file(qfull_path, f_no_ext, packages, f_size, path_no_ext)
                if response == True:
                    fDelete_Content(qfull_path)
                elif response == False:
                    root_logger.info(": query_folder :Upload Error ")
            else:
                root_logger.info(": query_folder : File " + str(packages) + " is not a zip file")
        tlf_count += 1
    print("tl_container_folder " + str(tl_container_folder) + "\n")
    return tl_container_folder


def fQuery_container_folder(qcf_target_folder, bucket_prefix, selection_type):
    root_logger.info("fQuery_container_folder")
    global bucket
    packages = ""
    qcf_parent_folder = ""
    nom_container_folder = ""
    container_to_pass_back = ""
    tlf_count = 0
    if selection_type == "All":
        qcf_parent_folder = qcf_target_folder
    elif selection_type == "ind":
        qcf_parent_folder = os.path.dirname(qcf_target_folder)
   
    for qroot, qd_names, qf_names in os.walk(qcf_target_folder):
        if tlf_count == 0:
            container_to_pass_back = os.path.basename(qroot)
        for qf in qf_names:
            response = False
            if os.path.isfile(os.path.join(qroot, qf)):
                qfull_path = os.path.join(qroot, qf)
                print("qfull_path " + str(qfull_path))
                f_size = fGet_filesize(qfull_path)
                f_no_ext = fGet_file_no_ext(qfull_path)
                path_no_ext = bucket_prefix \
                              + qroot.replace(qcf_parent_folder, "").lstrip("\\").replace("\\", "/") + "/" \
                              + qf
                print("path_no_ext " + str(path_no_ext))
                root_logger.info("path_no_ext " + str(path_no_ext))
                packages = qf
                response = fUpload_file(qfull_path, f_no_ext, packages, f_size, path_no_ext)
                if response == True:
                    # fDelete_Content(qfull_path)
                    pass
                elif response == False:
                    root_logger.info(": fQuery_container_folder :Upload Error ")
        tlf_count += 1
    return container_to_pass_back


def fScanManifest(fs_p_f_path):
    # customised to support UoA use case where file names are in the form "OBJ.ext"
    
    with open(fs_p_f_path, "r", encoding='utf-8') as man_file:
        list_man_pkg_type.clear()
        list_man_file_ext.clear()
        list_man_ext_range.clear()
        while True:
            # read each line in turn
            man_line = man_file.readline()
            if man_line == "":
                break
            else:
                man_array = ""
                man_array = man_line.split("  ")
                if len(man_array) != 2:
                    root_logger.warning(": delimiter is not double spaced : " + str(fs_p_f_path))
                    man_array = man_line.split(" ")
                    if len(man_array) != 2:
                        root_logger.warning(": delimiter is not single spaced : " + str(fs_p_f_path))
                        continue
                # select file extension and folder type
                man_array_line = ""
                file_extension = ""
                man_array_line = man_array[1].strip("\r\n").split("/")
                print(len(man_array_line))
                list_man_contained_files.append(man_array_line[-1])
                if len(man_array_line) != 3:
                    root_logger.warning(": file path length is inconsistent : " + str(fs_p_f_path))
                else:
                    try:
                        array_file_extension = man_array_line[2].split(".")
                        file_extension = array_file_extension[len(array_file_extension)-1]
                    except:
                        md_fileout.write(fs_p_f_path + "||||" + man_array_line[2] + "|" + "\n")
                        root_logger.warning(": couldn't determine file extension : " + str(fs_p_f_path) + " : " + str(man_array_line[2]))
                    try:
                        package_name = ""
                        package_name = man_array_line[2].split("_")[1]
                    except:
                        root_logger.warning(": file name is inconsistent : " + str(fs_p_f_path) + " : " + str(man_array_line[2]))
                        continue
                package_type = man_array_line[1]
                package_type_file_extension = package_type + "_" + file_extension
                if package_type not in list_man_pkg_type:
                    list_man_pkg_type.append(package_type)
                if file_extension not in list_man_file_ext:
                    list_man_file_ext.append(file_extension)
                if str(package_type_file_extension) not in list_man_ext_range:
                    list_man_ext_range.append(str(package_type_file_extension))
    list_man_pkg_type.sort()
    list_man_file_ext.sort()
    list_man_ext_range.sort()

    print("list_man_pkg_type " + str(list_man_pkg_type))
    print("list_man_file_ext " + str(list_man_file_ext))
    print("list_man_ext_range " + str(list_man_ext_range))
    
    root_logger.info(" : fScanManifest : list_man_pkg_type " + str(list_man_pkg_type))
    root_logger.info(" : fScanManifest : list_man_file_ext " + str(list_man_file_ext))
    root_logger.info(" : fScanManifest : list_man_ext_range " + str(list_man_ext_range))



def fScanSource(p_directory):
    sub_r = "fScanSource"
    list_available_pres_extension.clear()
    root_logger.info(sub_r)
    root_logger.info(sub_r + " : p_directory " + str(p_directory))
    app_flag = 0
    bana = 0
    p_f_path = ""
    list_approved_bitstream_type_t0 = [x[0] for x in list_approved_bitstream_type]
    norm_p_f = ""
    for p_root, p_d_names, p_f_names in os.walk(p_directory):
        print("p_d_names " + str(p_d_names))
        for p_file in p_f_names:
            p_f_path = os.path.join(p_root, p_file)
            print("p_f_path " + str(p_f_path))
            if os.path.isfile(p_f_path):
                if p_file == "manifest-sha1.txt":
                    p_f_path = os.path.join(p_root, p_file)
                    package_type = fScanManifest(p_f_path)
                elif p_file == "DC.xml":
                    fGet_Metadata(p_f_path, "DC")
                elif p_file == "MODS.bin":
                    fGet_Metadata(p_f_path, "MODS")
                
                else:
                    for app_no_ext in list_approved_pres_extension:
                        print("app_no_ext " + str(app_no_ext))
                        if p_file == app_no_ext[0]:
                            list_available_pres_extension.append((app_no_ext[0], app_no_ext[1]))
                            
                   
    # determine most suitable pres instance
    selected_p_file = ""
    
    print("list available pres extension : " + str(list_available_pres_extension))
    list_available_pres_extension.sort(key = lambda x: x[1])

    print("list available pres extension sorted : " + str(list_available_pres_extension))
    
    if len(list_available_pres_extension) == 0:
        root_logger.info("fScanSource : there are no valid file formats contained within package " + str(list_available_pres_extension))
        # app_flag = 0
        return False
    else:
        s_p_file = list_available_pres_extension[0]
        
        if s_p_file[0].lower() == "streaming.bin":
            streaming_bin_path = os.path.join(p_root, s_p_file[0])
            resulting_file_name = fGetStreamingBIN(streaming_bin_path)
            if resulting_file_name == "":
                root_logger.error("fScanSource : streaming.bin file could not be acquired for " + str(streaming_bin_path))
                print("ERROR RETRIEVING Streaming.bin FILE. SCRIPT EXITING")
                sys.exit()
            else:
                selected_p_file = resulting_file_name
        else:
            selected_p_file = s_p_file[0]
    
        p_f_path = os.path.join(p_root, selected_p_file)
        
        selected_p_file_no_ext = os.path.splitext(selected_p_file)[0]
        selected_p_file_ext = os.path.splitext(selected_p_file)[1]
        
        print("Prefered file is : " + str(p_f_path))
        print("selected_p_file " + str(selected_p_file))
        print("p_file_no_ext " + str(selected_p_file_no_ext))
        print("p_file_ext " + str(selected_p_file_ext))
        
        asset_type = ""

        app_flag = 1
        asset_type = "asset"
        array_p_file  = selected_p_file_no_ext.split("_")
        root_logger.info("fScanSource : p_f_path basename " + str(array_p_file))
        root_logger.info("fScanSource : array_p_file length " + str(len(array_p_file)))

        # Alaska specific processing - covers only the Bags use case currently
        ######################################################################################
        print("list_approved_bitstream_type_t0 " + str(list_approved_bitstream_type_t0))
        file_definition = "*"

        if file_definition.lower() in list_approved_bitstream_type_t0:
            labt_index = list_approved_bitstream_type_t0.index(file_definition.lower())
            list_corresponding_rep = list_approved_bitstream_type[labt_index][1]
        else:
            labt_index = list_approved_bitstream_type_t0.index("other")
            list_corresponding_rep = list_approved_bitstream_type[labt_index][1]
            
        
            
        print("file_definition " + str(file_definition.lower()))
        print("list_corresponding_rep " + str(list_corresponding_rep))
        
        root_logger.info("fScanSource : Found " + str(file_definition.lower()) + " version " + str(selected_p_file))
        dict_individual_file_checksum[p_f_path] = fv6Checksum(p_f_path, "md5")
        
        norm_p_f = selected_p_file_no_ext.rstrip("_" + file_definition.lower())
        dict_PAX_asset[selected_p_file] = (asset_type, norm_p_f, list_corresponding_rep, p_f_path)
        
        if selected_p_file not in dict_asset_name:
            dict_asset_name[selected_p_file] = selected_p_file
        
        if selected_p_file not in dict_asset_parent:
            dict_asset_parent[selected_p_file] = os.path.dirname(p_f_path)
        return True

    # print("dict_asset_name " + str(dict_asset_name.items()))
    # print("dict_asset_parent " + str(dict_asset_parent.items()))

    #if app_flag == 1:
    #    return True
    # elif app_flag == 0:
    #    root_logger.warning("fScanSource : bag does not contain a valid file format " + str(list_available_pres_extension))
    #    return False
            
            
def fScanSource_ApprovedFormats(p_directory):
    sub_r = "fScanSource_ApprovedFormats"
    root_logger.info(sub_r)
    root_logger.info(sub_r + " : p_directory " + str(p_directory))
    app_flag = 0
    bana = 0
    p_f_path = ""
    norm_p_f = ""
    for p_root, p_d_names, p_f_names in os.walk(p_directory):
        print("p_d_names " + str(p_d_names))
        for p_file in p_f_names:
            p_f_path    = os.path.join(p_root, p_file)
            p_f_parent  = os.path.dirname(p_f_path)
            if os.path.isfile(p_f_path):
                file_ext = os.path.splitext(p_file)[1]
                if file_ext in list_non_approved:
                    root_logger.warning(str(sub_r) + " the following file is non approved and has been deleted from the working folder " + str(p_f_path))
                    os.remove(p_f_path)
                    list_excepted_files.append(p_f_path)
                elif p_file in list_non_approved:
                    root_logger.warning(str(sub_r) + " the following file is non approved and has been deleted from the working folder " + str(p_f_path))
                    os.remove(p_f_path)
                else:
                    if "objects" in p_f_parent:
                        list_contents_folder.append(p_f_path)
                    elif "metadata" in p_f_parent:
                        list_metadata_folder.append(p_f_path)
                    
    return True

            
            

def fScanSource_Lite(p_directory):
    sub_r = "fScanSource"
    list_available_pres_extension.clear()
    root_logger.info(sub_r)
    root_logger.info(sub_r + " : p_directory " + str(p_directory))
    app_flag = 0
    bana = 0
    p_f_path = ""
    list_approved_bitstream_type_t0 = [x[0] for x in list_approved_bitstream_type]
    norm_p_f = ""
    for p_root, p_d_names, p_f_names in os.walk(p_directory):
        print("p_d_names " + str(p_d_names))
        for p_file in p_f_names:
            
            p_f_path = os.path.join(p_root, p_file)
            if os.path.isfile(p_f_path):
                selected_p_file = p_f_path
                
                # p_f_path = os.path.join(p_root, selected_p_file)
                selected_p_file_no_ext = os.path.splitext(selected_p_file)[0]
                selected_p_file_ext = os.path.splitext(selected_p_file)[1]
                
                print("Prefered file is : " + str(p_f_path))
                print("selected_p_file " + str(selected_p_file))
                print("p_file_no_ext " + str(selected_p_file_no_ext))
                print("p_file_ext " + str(selected_p_file_ext))
                
                asset_type = ""

                app_flag = 1
                asset_type = "asset"
                array_p_file  = selected_p_file_no_ext.split("_")
                root_logger.info("fScanSource : p_f_path basename " + str(array_p_file))
                root_logger.info("fScanSource : array_p_file length " + str(len(array_p_file)))

                # Alaska specific processing - covers only the Bags use case currently
                ######################################################################################
                print("list_approved_bitstream_type_t0 " + str(list_approved_bitstream_type_t0))
                file_definition = "*"

                if file_definition.lower() in list_approved_bitstream_type_t0:
                    labt_index = list_approved_bitstream_type_t0.index(file_definition.lower())
                    list_corresponding_rep = list_approved_bitstream_type[labt_index][1]
                else:
                    labt_index = list_approved_bitstream_type_t0.index("other")
                    list_corresponding_rep = list_approved_bitstream_type[labt_index][1]
                    
                
                print("file_definition " + str(file_definition.lower()))
                print("list_corresponding_rep " + str(list_corresponding_rep))
                
                root_logger.info("fScanSource : Found " + str(file_definition.lower()) + " version " + str(selected_p_file))
                dict_individual_file_checksum[p_f_path] = fv6Checksum(p_f_path, "md5")
                
                norm_p_f = selected_p_file_no_ext.rstrip("_" + file_definition.lower())
                dict_PAX_asset[selected_p_file] = (asset_type, norm_p_f, list_corresponding_rep, p_f_path)
                
                if selected_p_file not in dict_asset_name:
                    dict_asset_name[selected_p_file] = selected_p_file
                
                if selected_p_file not in dict_asset_parent:
                    dict_asset_parent[selected_p_file] = os.path.dirname(p_f_path)
                return True



def fGetStreamingBIN(fg_streaming_bin_path):
    with open(fg_streaming_bin_path, "rb") as streaming_file:  
        parser = lxml.etree.XMLParser(remove_blank_text=True)
        md_xml = lxml.etree.parse(streaming_file, parser=parser)
        remote_url = md_xml.xpath("//sources/source/url")[0].text
        print("remote_url " + str(remote_url))
        
        array_remote_url = remote_url.split("/")
        local_file_name = array_remote_url[-1]
        print("local_file_name " + str(local_file_name))
        
        # Make http request for remote file data
        streaming_data = requests.get(remote_url, allow_redirects=True, verify=False)
        # Save file data to local copy
        streaming_bin_save_location = os.path.join(os.path.dirname(fg_streaming_bin_path), local_file_name)
        print("streaming_bin_save_location " + str(streaming_bin_save_location))
        with open(streaming_bin_save_location, 'wb')as file:
            file.write(streaming_data.content)
        return local_file_name
                            
                            
def fGet_Metadata(fg_p_f_path, md_type):
    # read, validate metadata based on metadata type and store in dict
    if md_type == "DC":
        print("fGet_Metadata - DC ")
        with open(fg_p_f_path, "r", encoding = 'utf-8') as meta_file:
            meta_fragment = meta_file.read()
            print(type(meta_fragment))
            # validate metadata against appropriate schema            
            
            xmlschema_doc = lxml.etree.parse('oai_dc.xsd')
            # validate xml    
            parser = lxml.etree.XMLParser(remove_blank_text=True)
            md_xml = lxml.etree.fromstring(meta_fragment, parser=parser)
            new_md_xml = lxml.etree.tostring(md_xml, encoding="unicode", pretty_print=True)
            xmlschema = lxml.etree.XMLSchema(xmlschema_doc)
            if xmlschema.validate(md_xml):
                root_logger.info("fGet_Metadata : Metadata is valid for " + str(fg_p_f_path))
            else:
                root_logger.warning("fGet_Metadata : Metadata validation failed for " + str(fg_p_f_path))
            bag_ref = os.path.basename(os.path.dirname(os.path.dirname(fg_p_f_path)))
            print("fGet_Metadata bag ref " + str(bag_ref))
            dict_metadata_dc[str(bag_ref)] = new_md_xml
        
    elif md_type == "MODS":
        print("fGet_Metadata - MODS ")
        with open(fg_p_f_path, "rb") as meta_file:
            # meta_fragment = meta_file.read()
            # print(type(meta_fragment))   
            xmlschema_doc = lxml.etree.parse('MODS v3.4.xsd')
            # validate xml    
            parser = lxml.etree.XMLParser(remove_blank_text=True)
            md_xml = lxml.etree.parse(meta_file, parser=parser)
            new_md_xml = lxml.etree.tostring(md_xml, encoding="unicode", pretty_print=True)
            xmlschema = lxml.etree.XMLSchema(xmlschema_doc)
            if xmlschema.validate(md_xml):
                root_logger.info("fGet_Metadata : Metadata is valid for " + str(fg_p_f_path))
            else:
                root_logger.warning("fGet_Metadata : Metadata validation failed for " + str(fg_p_f_path))
            bag_ref = os.path.basename(os.path.dirname(os.path.dirname(fg_p_f_path)))
            print("fGet_Metadata bag ref " + str(bag_ref))
            dict_metadata_mods[str(bag_ref)] = new_md_xml
            

def fScanJSON(fs_p_f_path):
    json_referenceFilename = ""
    json_barcode = ""
    dict_json_asset = {}
    with open(fs_p_f_path, "r") as json_file:
        json_file_read = json_file.read()
        try:
            list_json_data = json.loads(json_file_read)
            for list_json_data_keys, list_json_data_vals in list_json_data.items():
                if list_json_data_keys == "asset":
                    for list_json_data_vals_keys, list_json_data_vals_vals in list_json_data_vals.items():
                        if list_json_data_vals_keys == "referenceFilename":
                            list_json_key_val.append(["referenceFilename", list_json_data_vals_vals])
                if list_json_data_keys == "bibliographic":
                    for list_json_data_vals_keys, list_json_data_vals_vals in list_json_data_vals.items():
                        # if list_json_data_vals_keys == "barcode":
                        list_json_key_val.append([list_json_data_vals_keys, list_json_data_vals_vals])
        except json.decoder.JSONDecodeError:
            root_logger.info(": fScanJSON : Error " + str(fs_p_f_path) + " could not be loaded as a json file")
            

def fCreatePAX(fc_workingf_wf_package, fc_package):
    sub_r = "fCreatePAX"
    root_logger.info(sub_r)
    
    print("dict_PAX_asset " + str(dict_PAX_asset))
    print("fc_workingf_wf_package " + str(fc_workingf_wf_package))
    
    workingPAXf_container = os.path.join(workingPAXf_wf, sourcef_wf_package_name)
    root_logger.info("fCreatePAX :  workingPAXf_container " + str(workingPAXf_container))
    
    # fCreateFolderStructure(workingPAXf_container)
    
    for dict_asset_name_keys, dict_asset_name_values in dict_asset_name.items():
        PAX_asset = dict_PAX_asset.get(dict_asset_name_keys, "na")
        print("pax asset " + str(PAX_asset))
        print("pax asset 0 " + str(PAX_asset[0]))
        print("pax asset 1 " + str(PAX_asset[1]))
        print("pax asset 2 " + str(PAX_asset[2]))
        print("pax asset 3 " + str(PAX_asset[3]))
        # print("pax asset 4 " + str(PAX_asset[4]))

        root_logger.info("fCreatePAX :  PAX_asset " + str(PAX_asset))
        source_path = os.path.join(fc_workingf_wf_package, PAX_asset[3])
        
        print("dict_asset_name_keys " + str(dict_asset_name_keys))
        root_logger.info("fCreatePAX :  dict_asset_name_keys " + str(dict_asset_name_keys))
        root_logger.info("fCreatePAX :  source_path " + str(source_path))
        print("source_path " + str(source_path))

        # PAX = sourcef_wf_package_name + "_" + PAX_asset[0]
        PAX = sourcef_wf_package_name

        root_logger.info("fCreatePAX :  PAX " + str(PAX))
        
        workingPAXf_wf_package_PAX = os.path.join(workingPAXf_wf_package, PAX)
        root_logger.info("fCreatePAX :  workingPAXf_wf_package_PAX " + str(workingPAXf_wf_package_PAX))
        
        list_pax_zip_folders.append(workingPAXf_wf_package_PAX)
        workingPAXf_wf_package_PAX_COContainer_Rep_P = os.path.join(workingPAXf_wf_package_PAX, PAX_asset[2])
        workingPAXf_wf_package_PAX_COContainer_Rep_P_CO = os.path.join(workingPAXf_wf_package_PAX_COContainer_Rep_P, PAX_asset[1])
        root_logger.info("fCreatePAX : workingPAXf_wf_package_PAX_COContainer_Rep_P1_CO " + str(workingPAXf_wf_package_PAX_COContainer_Rep_P_CO))
        fCreateFolderStructure(workingPAXf_wf_package_PAX_COContainer_Rep_P_CO)
        fCopyData(source_path, workingPAXf_wf_package_PAX_COContainer_Rep_P_CO, sub_r)


def fCreateFolderStructure(fc_folder_path):
    temp_path = workingdirectory
    array_fc_folder_path = fc_folder_path.split(os.sep)
    array_fc_pre_amble_path = workingdirectory.split(os.sep)
    fc_index = len(array_fc_pre_amble_path)
    print(array_fc_folder_path)
    for affp in range(fc_index, len(array_fc_folder_path)): 
        new_path = os.path.join(temp_path, array_fc_folder_path[affp])
        print(new_path)
        if not os.path.isdir(new_path):
            os.mkdir(new_path)
        temp_path = new_path


def fValidateChecksums():
    root_logger.info("fValidateChecksums")
    validation_pass = True
    for dict_individual_file_checksum_keys, dict_individual_file_checksum_vals in dict_individual_file_checksum.items():
        dict_file_checksum_basename = os.path.basename(dict_individual_file_checksum_keys)
        val_dict_checksum_manifest = dict_checksum_manifest.get(dict_file_checksum_basename, "na")
        if val_dict_checksum_manifest == "na":
            root_logger.error("fValidateChecksums : Checksum for file "
                              + str(dict_individual_file_checksum_keys)
                              + ":"
                              + str(dict_file_checksum_basename)
                              + ":isn't present in the checksum manifest")
            validation_pass = False
        elif dict_individual_file_checksum[dict_individual_file_checksum_keys] \
                != dict_checksum_manifest[dict_file_checksum_basename]:
            root_logger.error("fValidateChecksums : Checksum for file "
                              + str(dict_individual_file_checksum_keys)
                              + " does not match manifest checksum for "
                              + str(dict_file_checksum_basename))
            root_logger.error("fValidateChecksums: Calculated checksum = "
                              + str(dict_individual_file_checksum[dict_individual_file_checksum_keys]))
            validation_pass = False
    return validation_pass



def fReadFileSystem(directory):
    root_logger.info("fReadFileSystem")
    #  acquire file full path detail and add to
    root_logger.info("fReadFileSystem : Directory " + str(directory))
    int_prev_file_depth = 0
    rfs_longest_path = ""
    for root, d_names, f_names in os.walk(directory):
        for f in f_names:
            print(root)
            print(f)
            root_logger.info("fReadFileSystem : Files found in " + str(d_names) + " called " + str(f))
            rfs_full_file_path = os.path.join(root, f)
            root_logger.info("fReadFileSystem : full file path found in workingPAX " + str(rfs_full_file_path))
            array_rfs_full_file_path = rfs_full_file_path.split("\\")
            int_rfs_file_depth = len(array_rfs_full_file_path)
            if int_rfs_file_depth > int_prev_file_depth:
                rfs_longest_path = rfs_full_file_path
                int_prev_file_depth = int_rfs_file_depth
            dict_rfs_file_path[rfs_full_file_path] = int_rfs_file_depth
            root_logger.info("fReadFileSystem : file path " \
                         + str(rfs_full_file_path) \
                         + " : length " + str(int_rfs_file_depth))
    list_greatest_file_depth.append(int_prev_file_depth)
    list_longest_path.append(rfs_longest_path)


def fStart_Workflow(fss_container):
    root_logger.info("fStart_Workflow")
    url = "https://" + hostval + "/sdb/rest/workflow/instances"
    querystring = {"WorkflowContextId": " + wfcontextID + "}
    
    
    payload1 = "<StartWorkflowRequest xmlns=\"http://workflow.preservica.com\">\r\n\t<WorkflowContextId>"
    payload2 = wfcontextID
    payload3 = "</WorkflowContextId>\r\n\t"
    payload4 = "<Parameter>\r\n\t"
    payload5 = "<Key>OpexContainerDirectory</Key>\r\n\t"
    payload6 = "<Value>" + "opex/" + fss_container + "</Value>\r\n\t"
    payload7 = "</Parameter>\r\n\t"
    payload8 = "</StartWorkflowRequest>"
    payload = payload1 + payload2 + payload3 + payload4 + payload5 + payload6 + payload7 + payload8

    print(payload)
    
    headers = {
        'Preservica-Access-Token': gettoken(config_input),
        'Content-Type': "application/xml"
    }
    
    wf_start_response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
    
    print(wf_start_response.status_code)
    print(wf_start_response.text)
    
    
    root_logger.info("fStart_Workflow : Workflow Response : " + wf_start_response.text)
    NSMAP = {"xip_wf": "http://workflow.preservica.com"}

    b_wf_start_response = bytes(wf_start_response.text, 'utf-8')
    parser = lxml.etree.XMLParser(remove_blank_text=True, ns_clean=True)
    wf_tree = lxml.etree.fromstring(b_wf_start_response, parser)
    workflow_id = wf_tree.xpath("//xip_wf:WorkflowInstance/xip_wf:Id", namespaces = NSMAP)
    for wfid in range(len(workflow_id)):
        wf_id = workflow_id[wfid].text
        print("workflow id " + str(wf_id))
        # fCheckWorkflowStatus(wf_id)


def fTargetCheckSum():
    root_logger.info("fTargetCheckSum")
    for tfp in range(len(list_target_files)):
        target_file_path = list_target_files[tfp]
        file_name = os.path.basename(target_file_path)
        root_logger.info(":fTargetCheckSum : Basename file name " + str(file_name))
        cstype = dict_export_METS_file_CheckSumType[file_name]
        if not cstype:
            root_logger.warning("fTargetCheckSum : Calculated file name is incorrect")
            return False
        dict_export_physical_file_CheckSumType[file_name] = cstype
        root_logger.info(":fTargetCheckSum : Physical CheckSumType " + str(file_name) + " : " + str(cstype))
        target_checkum_val = fv6Checksum(target_file_path, cstype)
        dict_export_physical_file_CheckSum[file_name] = target_checkum_val
        root_logger.info(":fTargetCheckSum : Physical CheckSum Value " + str(file_name) + " : " + str(target_checkum_val))
    return True


def fCheckandUnzip():
    zip_sourcef_wf = os.path.join(zip_sourcef)
    for zip_package in os.listdir(zip_sourcef_wf):
        print(zip_package)
        zip_sourcef_wf_zip_package = os.path.join(zip_sourcef_wf, zip_package)
        print(zip_sourcef_wf_zip_package)
        if os.path.isfile(zip_sourcef_wf_zip_package):
            if os.path.splitext(zip_sourcef_wf_zip_package)[1] == ".zip":
                print(os.path.splitext(zip_sourcef_wf_zip_package)[1])
                fUnzipData(zip_sourcef_wf_zip_package, sourcef)
            else:
                print("There is a non zip file in the zip_source folder. Script exiting")
                sys.exit()
        elif os.path.isdir(zip_sourcef_wf_zip_package):
            print("There is a non zipped content in the zip_source folder. Script exiting")
            sys.exit()
    

def fUnzipData(ue_package_source, ue_package_working_folder):
    root_logger.info("fUnzipData")
    err_fUnzipData = 0
    try:
        unpack_archive(os.path.join(ue_package_source), os.path.join(ue_package_working_folder))
        print(str(ue_package_source) + " unzipped successully to " + str(ue_package_working_folder))
        root_logger.info(": fUnzipData : " + str(ue_package_source) + " unzipped successully to " + str(ue_package_working_folder))
    except:
        print("There was an error unzipping " + str(ue_package_source) + " to " + str(ue_package_working_folder))
        sys.exit()
    aaa= 0
    if aaa == 1:
        wp_no_ext = ""
        extract_dir = ""
        try:
            wp_head, wp_tail = os.path.split(ue_package_source)
            # print(wp_head, wp_tail)
            extract_dir = wp_head
            wp_no_ext, wp_ext = os.path.splitext(ue_package_source)
            # print(wp_no_ext, wp_ext)

            unpack_archive(os.path.join(ue_package_source), os.path.join(ue_package_working_folder))
            
            list_unpacked_export_zips.append(str(ue_package_source))
        except:
            root_logger.info(": fUnzipData : Unzip failed for " + str(ue_package_source))
            err_fUnzipData = 1
        if os.path.isdir(wp_no_ext):
            zip_files = Path(wp_no_ext).rglob(".zip")
            while True:
                try:
                    path = next(zip_files)
                except StopIteration:
                    break  # no more files
                except:
                    root_logger.info("fUnzipData : Subfolder unzip failed for " + str(extract_dir))
                    err_fUnzipData = 1
                else:
                    extract_dir1 = path.with_name(path.stem)
                    root_logger.info("fUnzipData : Extract Directory " + str(extract_dir1))
                    unpack_archive(str(path), str(extract_dir1), 'zip')
                    list_unpacked_export_zips.append(str(path))
        else:
            root_logger.info(": fUnzipData : Subfolder unzip failed for " + str(package_working))
            err_fUnzipData = 1
    if err_fUnzipData == 0:
        return True
    elif err_fUnzipData == 1:
        return False


def fv6Checksum(file_path, sum_type):
    root_logger.info("fv6Checksum")
    sum_type = sum_type.replace("-", "")
    if sum_type.lower() == "md5":
        with open(file_path, "rb") as f:
            file_hash = hashlib.md5()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)
        root_logger.info("fv6Checksum : file_path " + str(file_path))
        root_logger.info("fv6Checksum : hash " + file_hash.hexdigest())
        return file_hash.hexdigest()
    elif sum_type.lower() == "sha1":
        with open(file_path, "rb") as f:
            file_hash = hashlib.sha1()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)
        root_logger.info("fv6Checksum : file_path " + str(file_path))
        root_logger.info("fv6Checksum : hash " + file_hash.hexdigest())
        return file_hash.hexdigest()
    elif sum_type.lower() == "sha256":
        with open(file_path, "rb") as f:
            file_hash = hashlib.sha256()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)
        root_logger.info("fv6Checksum : file_path " + str(file_path))
        root_logger.info("fv6Checksum : hash " + file_hash.hexdigest())
        return file_hash.hexdigest()
    elif sum_type.lower() == "sha512":
        with open(file_path, "rb") as f:
            file_hash = hashlib.sha512()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)
        root_logger.info("fv6Checksum : file_path " + str(file_path))
        root_logger.info("fv6Checksum : hash " + file_hash.hexdigest())
        return file_hash.hexdigest()


def fZipPAX():
    root_logger.info("fZipPAX")
    root_logger.info("fZipPAX : zip folders " + str(list_pax_zip_folders))
    for lpzf in list_pax_zip_folders:
        lpzf_dirname = os.path.dirname(lpzf)
        lpzf_basename = os.path.basename(lpzf)
        target_zip_file = os.path.join(lpzf_dirname,  str(lpzf_basename) + ".pax" + ".zip")
        fZipContent(lpzf, target_zip_file, lpzf_dirname)


def fZipContent(z_working_parent_sub_folder, z_target_parent_zip_file, z_working_dir):
    root_logger.info("fZipContent")
    err_fZipContent = 0
    try:
        zipf = zipfile.ZipFile(z_target_parent_zip_file, mode='w')
        # lenDirPath = len(z_working_dir)
        for pres_acc_subfolder in os.listdir(z_working_parent_sub_folder):
            pres_acc_subfolder_path = os.path.join(z_working_parent_sub_folder, pres_acc_subfolder)
            lenDirPath = len(z_working_parent_sub_folder)
            root_logger.info("fZipContent : lenDirPath " + str(lenDirPath))
            for root, _, Z_files in os.walk(pres_acc_subfolder_path):
                for z_file in Z_files:
                    filePath = os.path.join(root, z_file)
                    root_logger.info("fZipContent : " + str(filePath[lenDirPath:]))
                    #  try:
                    zipf.write(filePath, filePath[lenDirPath:])
                    root_logger.info("fZipContent : File " + str(z_file) + " sent to "
                                     + str(z_target_parent_zip_file) + " in file path " + str(filePath) + " with len " + str(filePath[lenDirPath:]))

                    list_delete_pax.append(z_working_parent_sub_folder)
    except BadZipfile as zipfail:
        root_logger.error("fZipContent : File " + str(z_file) + " did not zip to "
                      + str(z_target_parent_zip_file) + ": failure " + str(zipfail))
        err_fZipContent = 1
    zipf.close()
    if err_fZipContent == 0:
        return True
    elif err_fZipContent == 1:
        return False


def fDeletePAX():
    root_logger.info("fDeletePAX")
    for ldp in list_delete_pax:
        try:
            shutil.rmtree(ldp)
        except:
            root_logger.warning("fDeletePAX : Could not delete folder " + str(ldp))


def fSelectWorkflowType():
    sub_r = "fSelectWorkflowType"
    global source
    root_logger.info(sub_r)
    for workflow_type in os.listdir(sourcef):
        if workflow_type == "Bags":
            source = workflow_type
            fGetAMIPackages(source)



def fListPackages(fl_wflow_type):
    fl_sourcef_wf = os.path.join(sourcef, fl_wflow_type)
    for fl_package in os.listdir(fl_sourcef_wf):
        list_packages.append(fl_package)


def fGetPackages(fg_workflow_type):
    sub_r = "fGetBagsPackage"
    global sourcef_wf
    global workingf_wf
    global workingPAXf_wf
    global container
    global targetf_container
    global targetf_container_wf
    global targetf_wf
    global sourcef_wf_package
    global sourcef_wf_package_name
    global workingf_wf_package
    global workingPAXf_wf_package
    global targetf_wf_package
    
    global FAComponentIdNo     # Mxxxxx_##_xxxxx
    global SOCategoryContainer # Mxxxxx_##_xxxxx
    global SOCategoryContents  # Mxxxxx_##_xxxxx
    global SOCategoryMetadata  # Mxxxxx_##_xxxxx
    global SOCategoryElement   # Mxxxxx_##_xxxxx
    global IOCategoryElement   # Mxxxxx_##_xxxxx
    global CMSCollectionID     # M#####_xx_xxxxx
    global RecordNumber        # Mxxxxx_xx_#####
    
    global Ident_Biblio_Key   

    global opex_title_content
    global opex_title_metadata
    

    
    Ident_Biblio_Key = "SOCategory"

    sourcef_wf = os.path.join(sourcef, fg_workflow_type)
    
    print(dict_frh_orig_folder_name.items())

    for package in os.listdir(sourcef_wf):
        fReset_Lists_Dicts()
        
        # ExceptionsReport
        ExceptionsFile  = os.path.join(log_folder, "Exceptions_" + str(fTime()) + ".log")
        Exceptions      = open(ExceptionsFile, "w", encoding='utf-8')
        Exceptions.write("Exceptions file for package " + str(package) + "\n")
        Exceptions.write("The following files have been removed from the opex package")
        
        # Sub Folder Titles
        opex_title_content  = package + "_contents"
        opex_title_metadata = package + "_metadata"
        
        wflow_type = package
        
        array_package_breakdown = package.split("_")
        if len(array_package_breakdown) != 3:
            root_logger.info(str(sub_r) + " : package breakdown. The package name is not consistent " + str(array_package_breakdown))
            sys.exit()
        else:
            CMSCollectionID     = array_package_breakdown[0]
            FAComponentIdNo     = package
            SOCategoryContainer = array_package_breakdown[1] + "Container"
            SOCategoryContents  = array_package_breakdown[1] + "Contents"
            SOCategoryMetadata  = array_package_breakdown[1] + "Metadata"
            SOCategoryElement   = array_package_breakdown[1] + "Element"
            IOCategoryElement   = array_package_breakdown[1] + "Element"
            RecordNumber        = array_package_breakdown[2]
    
    
        print("package |" + str(package) + "|")
        #print("wflow_type " + str(wflow_type))
        #if wflow_type == "NA":
        #    print("The source package is not listed in the hierarchy csv " + str(package))
        #    root_logger.error("fGetPackages : The source package is not listed in the hierarchy csv " + str(package))
        #    continue
        
        workingf_wf             = os.path.join(workingf, wflow_type)
        workingPAXf_wf          = os.path.join(workingPAXf, wflow_type)
        extended_path = ""
        workingPAXf_wf_extended = os.path.join(workingPAXf_wf, extended_path)
        ## 
        #extended_path           = dict_frh_subpaths.get(package, "error")
        #if extended_path        == "error":
        #    root_logger.error(sub_r + " : the extended path for this package was not present in the hierarchy manifest " + str(package))
        #    root_logger.error(sub_r + " : skipping package : " + str(package))
        #    continue

        targetf_wf              = os.path.join(targetf, wflow_type)
        
        container               = "Container_" + package + "_" + fTime()
        targetf_container       = os.path.join(targetf, container)

        sourcef_wf_package              = os.path.join(sourcef_wf, package)
        sourcef_wf_package_name         = package
        workingf_wf_package             = os.path.join(workingf_wf, package)
        workingPAXf_wf_package          = os.path.join(workingPAXf_wf_extended)
        targetf_container_wf            = os.path.join(targetf, container, fg_workflow_type)                                                                                  
        targetf_container_wf_package    = os.path.join(targetf_container_wf, package)                                                                             
        
        root_logger.info(sub_r + " : package " + str(package))
        root_logger.info(sub_r + " : sourcef_wf " + str(sourcef_wf))
        root_logger.info(sub_r + " : sourcef_wf_package " + str(sourcef_wf_package))
        root_logger.info(sub_r + " : sourcef_wf_package_name " + str(sourcef_wf_package_name))
        root_logger.info(sub_r + " : workingf_wf " + str(workingf_wf))
        root_logger.info(sub_r + " : workingf_wf_package " + str(workingf_wf_package))
        root_logger.info(sub_r + " : workingPAXf_wf " + str(workingPAXf_wf))
        root_logger.info(sub_r + " : workingPAXf_wf_extended " + str(workingPAXf_wf_extended))
        root_logger.info(sub_r + " : workingPAXf_wf_package " + str(workingPAXf_wf_package))
        root_logger.info(sub_r + " : container " + str(container))
        root_logger.info(sub_r + " : targetf_wf " + str(targetf_wf))
        root_logger.info(sub_r + " : targetf_container " + str(targetf_container))
        root_logger.info(sub_r + " : targetf_container_wf " + str(targetf_container_wf))
        root_logger.info(sub_r + " : targetf_container_wf_package " + str(targetf_container_wf_package))
        
        
        #if os.path.isdir(sourcef_wf_package):
        #    root_logger.info(": fGetPackages : Copy from source (no zips present) : " + str(sourcef_wf_package))
        #    root_logger.info(": fGetPackages : Processing package source : " + str(sourcef_wf_package))
        #    root_logger.info(": fGetPackages : Processing package working : " + str(workingf_wf_package))
        #    # copy source package from "source" folder to "working" folder
        #    if not fCopytreeData(sourcef_wf_package, workingf_wf_package):
        #        root_logger.info(": fProcessPackages : fCopytreeData error " + str(sourcef_wf_package))
        #        continue
        #    # may not require a wait
        #    time.sleep(10)
        #else:
        #    root_logger.warning(": fGetPackages : something other than a folder was found in the source folder")

        #############

        if not fCopytreeData(sourcef_wf_package, workingf_wf_package):
            root_logger.info("fProcessPackages : fCopytreeData error : Skipping package : " + str(sourcef_wf_package))
            continue

        if not fScanSource_ApprovedFormats(workingf_wf_package):
            root_logger.info("fProcessPackages : fScanSource error : Skipping package : " + str(sourcef_wf_package))
            continue
        Exceptions.write("\n".join(list_excepted_files))
        Exceptions.close()
        # fCreatePAX(workingf_wf_package, package)
        # fZipPAX()
        # fDeletePAX()
        # fReadFileSystem(workingf_wf_package)
        
        #indented on 100222
        
        # inspect list_contents_folder and list_metadata_folder entities, construct resulting package (which will exclude extraneous folders)
        
        fConstructTarget(targetf_container_wf_package)
        

        #if not fCopytreeData(workingf_wf_package, targetf_container_wf_package):
        #    root_logger.info("fProcessPackages : fCopytreeData error : Skipping package : " + str(sourcef_wf_package))
        #    continue
        
        fCreateFileOpexFragments(targetf_container, security_tag, package)
        fCreateFolderOpexFragments(targetf_container, security_tag, ref_title, fg_workflow_type, package)
        fCreateContainerFolderOpexFragment(targetf)
        fOutputDictionaries()
        fSanitiseFolders()


def fConstructTarget(fc_targetf_container_wf_package):
    sub_r = "fConstructTarget"
    package_target_folder_content   = os.path.join(fc_targetf_container_wf_package, opex_title_content)
    package_target_folder_metadata  = os.path.join(fc_targetf_container_wf_package, opex_title_metadata)
    list_acpf = []
    for lcf in list_contents_folder:
        list_acpf.clear()
        content_file_name       = os.path.basename(lcf)
        content_parent_folder   = os.path.dirname(lcf)
        print(content_file_name)
        # package_target_folder_content_file = os.path.join(package_target_folder_content, content_file_name)
        ## the target may have a variable path length so construct the target path based on source path
        array_content_parent_folder             = content_parent_folder.split(os.sep)
        array_fc_targetf_container_wf_package   = fc_targetf_container_wf_package.split(os.sep)
        for acpf in range(len(array_fc_targetf_container_wf_package), len(array_content_parent_folder)):
            list_acpf.append(array_content_parent_folder[acpf])
        print(list_acpf)
        package_target_folder_content_file = os.path.join(fc_targetf_container_wf_package, opex_title_content, (os.sep).join(list_acpf), content_file_name)
        print(package_target_folder_content_file)
        fCopyData(lcf, package_target_folder_content_file, sub_r)
        
    if len(list_metadata_folder) == 0:
        fCreateFolderStructure(package_target_folder_metadata)
    else:
        for lmf in list_metadata_folder:
            metadata_file_name = os.path.basename(lmf)
            print(metadata_file_name)
            package_target_folder_metadata_file = os.path.join(package_target_folder_metadata, metadata_file_name)
            fCopyData(lmf, package_target_folder_metadata_file, sub_r)
    

def fCreateDigArchMetadataFragments(mdfrag_type, md_value):
    sub_r = "fCreateDigArchMetadataFragments"
    root_logger.info(str(sub_r))
    
    # mdfrag1 - CMSCollectionID = namespace = https://nypl.org/preservica/mdfrag1
    if mdfrag_type == "mdfrag1":
        mdfrag_header   = "<cmsCollection xmlns=\"https://nypl.org/prsv_schemas/cmsCollection\">"
        mdfrag_element  = "<cmsCollectionId>" + md_value + "</cmsCollectionId>"
        mdfrag_footer   = "</cmsCollection>"
        mdfrag_full     = mdfrag_header + mdfrag_element + mdfrag_footer
        
        
    elif mdfrag_type == "mdfrag2":
        mdfrag_header   = "<catalogCollection xmlns=\"https://nypl.org/prsv_schemas/catalogCollection\">"
        mdfrag_element  = "<collectionCallNumber>" + md_value + "</collectionCallNumber>"
        mdfrag_footer   = "</catalogCollection>"
        mdfrag_full     = mdfrag_header + mdfrag_element + mdfrag_footer
        
        
    elif mdfrag_type == "mdfrag3":
        mdfrag_header   = "<division xmlns=\"http://nypl.org/prsv_schemas/division\">"
        mdfrag_element  = "<divisionCode>" + md_value + "</divisionCode>"
        mdfrag_footer   = "</division>"
        mdfrag_full     = mdfrag_header + mdfrag_element + mdfrag_footer
        

    elif mdfrag_type == "mdfrag4":
        mdfrag_header   = "<findingAid xmlns=\"http://nypl.org/prsv_schemas/findingAid\">"
        mdfrag_element  = "<faComponentId>" + md_value + "</faComponentId>"
        mdfrag_footer   = "</findingAid>"
        mdfrag_full     = mdfrag_header + mdfrag_element + mdfrag_footer
        
        
    parser = lxml.etree.XMLParser(remove_blank_text=True)
    mdfrag_xml = lxml.etree.fromstring(mdfrag_full, parser=parser)
    new_mdfrag_xml = lxml.etree.tostring(mdfrag_xml, encoding="unicode", pretty_print=True)
    
    #add validator when fragments have been finaised
    return new_mdfrag_xml
    

def fUploadContent():
    root_logger.info("fUploadContent")
    select_upload_folder = 1
    if select_upload_folder == 1:
        suf_target_folder = targetf
        if suf_target_folder == "":
            sys.exit()
        else:
            fss_container = fQuery_folder(suf_target_folder, bucket_prefix)
            print("fss_container " + str(fss_container))
            if start_inc_ingest_wf == 1:
                fStart_Workflow(fss_container)


def fUploadContent_Container():
    root_logger.info("fUploadContent_Container")
    select_nominated_upload_folder = 1
    if select_nominated_upload_folder == 1:
        # nominated_target_folder = filedialog.askdirectory(title='Select the Container folder to upload')
        nominated_target_folder = input('Enter the container path you would like to upload :: ')
        if nominated_target_folder == "":
            print("This is not a container folder")
        elif "Container_" not in nominated_target_folder:
            print("This is not a container folder")
        else:
            returned_target_folder = fQuery_container_folder(nominated_target_folder, bucket_prefix)
            print("returned_target_folder " + str(returned_target_folder))
            # if start_inc_ingest_wf == 1:
            #    fStart_Workflow(returned_target_folder)


def mThread(c_list):
    mthreads = []
    rep_count = 0
    total_rep_count = 0
    task_result = ""
    with ThreadPoolExecutor(max_workers=max_worker_count) as executor:
        for c_next in c_list:
            print(c_next)
            mthreads.append(executor.submit(fStart_Workflow, c_next))
            print("mthreads " + str(mthreads))
        for task in as_completed(mthreads):
            task_result = task.result()
    return task_result


def pThread():
    pthreads = []
    with ThreadPoolExecutor(max_workers=max_worker_count) as executor:
        for lp in list_packages:
            print(lp)
            pthreads.append(executor.submit(fActionPackages, lp))
        for task in as_completed(pthreads):
            p_result = task.result()
    return p_result


def fListUploadDirectory():
    sub_r = "fListUploadDirectory"
    c_f_list = []
    cf_counter = 1
    for containerf in os.listdir(targetf):
        print(containerf)
        dict_containerf[cf_counter] = containerf
        cf_counter +=1
    print("****************************************")
    
    for c_f_key, c_f_val in dict_containerf.items():
        print(str(c_f_key) + "  : " + str(c_f_val))
    print("Enter ALL to upload all packages, or enter the number of the package to upload, or QUIT")
    c_f_input = input()
    if c_f_input == "ALL":
        print("send all")
        sel_type = "All"
        # returned_target_folder = fQuery_container_folder(os.path.join(targetf), bucket_prefix, sel_type)
  
        for c_f_key, c_f_val in dict_containerf.items():
            if fCopytreeData(os.path.join(targetf, c_f_val), os.path.join(uploaddirectory, c_f_val)):
                c_f_list.append(c_f_val)
            else:
                print("Copy FAILED for container " + str(c_f_val))
        if start_inc_ingest_wf == 1:
            mThread(c_f_list)
    elif c_f_input == "QUIT":
        sys.exit()
    else:
        c_f_val_from_dict = dict_containerf.get(int(c_f_input),"NA")
        print(os.path.join(targetf, c_f_val_from_dict))
        sel_type = "ind"
        
        if c_f_val_from_dict == "NA":
            print("You have selected a number that isn't in the list")
        else:
            if fCopytreeData(os.path.join(targetf, c_f_val_from_dict), os.path.join(uploaddirectory, c_f_val_from_dict)):
                if start_inc_ingest_wf == 1:
                    fStart_Workflow(c_f_val_from_dict)
            else:
                print("Copy FAILED for container " + str(c_f_val_from_dict))
 

def fProcessOptions():
    root_logger.info("fProcessOptions")
    # next_step = pymsgbox.confirm(text='Select the process you would like to run', title='Process', buttons=['Create New Container', 'Upload Container'])
    next_step = input('Select the process you would like to run. Enter 1 to Create New Container or 2 to Upload Container :: ')
  
    use_hierarchy_csv = 1
    
    if next_step == "1":
        workflow_type = ""
        while workflow_type not in list_workflows:
            workflow_number = input('Select the workflow type. Enter 1 for DigArch, 2 for xxxxxxxxxxx, 3 for xxxxxxxxxxx :: ')
            if workflow_number == "1":
                workflow_type = "DigArch"
            elif workflow_number != "1":
                print("Select a valid number")
        
        
        fSanitiseFolders()
        if use_metadata_csv == 1:
            fReadChecksumManifest()
        fGetPackages(workflow_type)
        if upload_to_bucket == 1:
            # fUploadContent_Container()
            fListUploadDirectory()
    elif next_step == "2":
        # fUploadContent()
        # fUploadContent_Container()
        fListUploadDirectory()
        
        
def fUpload_file(file_name, f_no_ext, f_name, f_size, object_name):
    root_logger.info("fUpload_file")
    global AWS_Key
    global AWS_Secret
    global bucket
    
    if object_name is None:
        object_name = file_name
    s3_client = boto3.client('s3', aws_access_key_id=AWS_Key, aws_secret_access_key=AWS_Secret)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={"Metadata": {"key": f"{f_no_ext}", "name": f"{f_name}", "size": f"{f_size}"}}, Callback=ProgressPercentage(file_name))
    except botocore.exceptions.ClientError as e:
        root_logger.error(e)
        return False
    return True
    

##########################################################################################################
# Variables
##########################################################################################################
config_input = "DA_config.ini"
config = configparser.ConfigParser()
config.sections()
config.read(config_input)


# read folder variables from config.ini file
hostval = config['DEFAULT']['Host']
masterdirectory = config['DEFAULT']['MasterDirectory']
workingdirectory = config['DEFAULT']['WorkingDirectory']
uploaddirectory = config['DEFAULT']['UploadDirectory']
source = config['DEFAULT']['Source']
# zip_source = config['DEFAULT']['ZIP_Source']
working = config['DEFAULT']['Working']
workingPAX = config['DEFAULT']['WorkingPAX']
metadata = config['DEFAULT']['Metadata']
metadata_fragments = config['DEFAULT']['Metadata_Fragments']
metadata_template = config['DEFAULT']['Metadata_Template']
target = config['DEFAULT']['Target']
logs = config['DEFAULT']['Logs']
# manifests = config['DEFAULT']['Manifests']
# hierarchy_manifest = config['DEFAULT']['Hierarchy_Manifest']

use_commandline_sysarg = int(config['VARIABLES']['Use_commandline_sysarg'])
manually_select_source_folder = int(config['VARIABLES']['Manually_select_source_folder'])
read_catalog_manifest = int(config['VARIABLES']['Read_catalog_manifest'])
folder_to_catalog_level = int(config['VARIABLES']['Folder_to_catalog_level'])
use_folder_source_id = int(config['VARIABLES']['Use_folder_source_id'])
use_file_source_id = int(config['VARIABLES']['Use_file_source_id'])
delete_zero_byte_files = int(config['VARIABLES']['Delete_zero_byte_files'])
append_descriptive_metadata = int(config['VARIABLES']['Append_descriptive_metadata'])
security_tag = str(config['VARIABLES']['Security_tag'])
process_zip = int(config['VARIABLES']['Process_zip'])
reference_foldercount_from_file = int(config['VARIABLES']['Reference_foldercount_from_file'])
total_reference_foldercount_from_file = int(config['VARIABLES']['Total_reference_foldercount_from_file'])
bucket_prefix = str(config['VARIABLES']['Bucket_prefix'])
upload_to_bucket = int(config['VARIABLES']['Upload_to_bucket'])
unzip_from_source = int(config['VARIABLES']['Unzip_from_source'])
copy_from_source = int(config['VARIABLES']['Copy_from_source'])
reference_folder_source_ID_override = int(config['VARIABLES']['Reference_folder_source_ID_override'])
do_not_apply_folder_source_ID = int(config['VARIABLES']['Do_not_apply_folder_source_ID'])
reference_folder_source_ID = str(config['VARIABLES']['Reference_folder_source_ID'])
ref_title = str(config['VARIABLES']['Ref_title'])

parent_hierarchy = str(config['VARIABLES']['Parent_hierarchy'])
record_id_prefix = str(config['VARIABLES']['Record_id_prefix'])
start_inc_ingest_wf = int(config['VARIABLES']['Start_inc_ingest_wf'])
multi_manifestation = int(config['VARIABLES']['Multi_manifestation'])
manually_select_metadata_csv = int(config['VARIABLES']['Manually_select_metadata_csv'])
use_metadata_csv = int(config['VARIABLES']['Use_metadata_csv'])
manually_select_checksum_manifest = int(config['VARIABLES']['Manually_Select_Checksum'])
csv_columns = int(config['VARIABLES']['Csv_columns'])
null_keyword = str(config['VARIABLES']['Null_keyword'])

Cloud_vendor_target = str(config['BUCKET']['CV_Target'])
bucket = str(config['BUCKET']['BUCKET'])
AWS_Key = str(config['BUCKET']['KEY'])
AWS_Secret = str(config['BUCKET']['SECRET'])
wfcontextID = str(config['BUCKET']['Workflow_contextID'])

max_worker_count = int(config['BUCKET']['Max_Worker_Count'])


# define working folders
sourcef = os.path.join(workingdirectory, source)
zip_sourcef = os.path.join(workingdirectory, source)
workingf = os.path.join(workingdirectory, working)
workingPAXf = os.path.join(workingdirectory, workingPAX)
metadata_folder = os.path.join(workingdirectory, metadata)
fragment_folder = os.path.join(workingdirectory, metadata_fragments)
targetf = os.path.join(workingdirectory, target)
log_folder = os.path.join(masterdirectory, logs)
# manifest_folder = os.path.join(masterdirectory, manifests)


# list_working_folders = [working_folder, workingPAX_folder, fragment_folder, target_folder]
list_working_folders = [workingf, workingPAXf]


##########################################################################################################
# Logging
##########################################################################################################
LogFile = os.path.join(log_folder, "Log_" + str(fTime()) + ".log")
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(LogFile, 'w', 'utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
root_logger.addHandler(handler)
root_logger.info("log file for " + str(os.path.basename(__file__)))


# Log variables

root_logger.info("hostval " + str(hostval))
root_logger.info("masterdirectory " + str(masterdirectory))
root_logger.info("workingdirectory " + str(workingdirectory))
root_logger.info("source " + str(source))
root_logger.info("working " + str(working))
root_logger.info("workingPAX " + str(workingPAX))
root_logger.info("metadata " + str(metadata))
root_logger.info("metadata_fragments " + str(metadata_fragments))
root_logger.info("metadata_template " + str(metadata_template))
root_logger.info("target " + str(target))
root_logger.info("logs " + str(logs))
# root_logger.info("manifests " + str(manifests))
# root_logger.info("hierarchy_manifest " + str(hierarchy_manifest))


root_logger.info("use_commandline_sysarg " + str(use_commandline_sysarg))
root_logger.info("manually_select_source_folder " + str(manually_select_source_folder))
root_logger.info("read_catalog_manifest " + str(read_catalog_manifest))
root_logger.info("folder_to_catalog_level " + str(folder_to_catalog_level))
root_logger.info("use_folder_source_id " + str(use_folder_source_id))
root_logger.info("use_file_source_id " + str(use_file_source_id))
root_logger.info("delete_zero_byte_files " + str(delete_zero_byte_files))
root_logger.info("append_descriptive_metadata " + str(append_descriptive_metadata))
# root_logger.info("append_descriptive_metadata_IO " + str(append_descriptive_metadata_IO))
# root_logger.info("append_descriptive_metadata_SO " + str(append_descriptive_metadata_SO))
root_logger.info("security_tag " + str(security_tag))
root_logger.info("process_zip " + str(process_zip))
root_logger.info("reference_foldercount_from_file " + str(reference_foldercount_from_file))
root_logger.info("total_reference_foldercount_from_file " + str(total_reference_foldercount_from_file))
root_logger.info("bucket_prefix " + str(bucket_prefix))
root_logger.info("upload_to_bucket " + str(upload_to_bucket))
root_logger.info("unzip_from_source " + str(unzip_from_source))
root_logger.info("copy_from_source " + str(copy_from_source))
root_logger.info("reference_folder_source_ID_override " + str(reference_folder_source_ID_override))
root_logger.info("do_not_apply_folder_source_ID " + str(do_not_apply_folder_source_ID))
root_logger.info("reference_folder_source_ID " + str(reference_folder_source_ID))
root_logger.info("ref_title " + str(ref_title))


root_logger.info("parent_hierarchy " + str(parent_hierarchy))
root_logger.info("record_id_prefix " + str(record_id_prefix))
root_logger.info("start_inc_ingest_wf " + str(start_inc_ingest_wf))
root_logger.info("multi_manifestation " + str(multi_manifestation))
root_logger.info("manually_select_metadata_csv " + str(manually_select_metadata_csv))
root_logger.info("manually_select_checksum_manifest " + str(manually_select_checksum_manifest))
root_logger.info("csv_columns " + str(csv_columns))
root_logger.info("null_keyword " + str(null_keyword))


# user input
file_manifest = ""
catalog_manifest = ""
container_path = ""

##########################################################################################################
# Runtime
##########################################################################################################
if __name__ == '__main__':
    fProcessOptions()


##########################################################################################################
# End
##########################################################################################################