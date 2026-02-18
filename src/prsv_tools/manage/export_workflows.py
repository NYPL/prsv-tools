import csv
import logging
import argparse
import time
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
import prsv_tools.utility.api as prsvapi

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--startdate",
        "-sd",
        required=False,
        help="""WF from date eg. 2025-11-19""",
        )
    parser.add_argument(
        "--enddate",
        "-ed",
        required=False,
        help="""WF to date, eg. 2025-11-19""",
        )
    parser.add_argument(
        "--wfstate",
        type=str,
        default="Aborted,Active,Completed,Finished_Mixed_Outcome,Pending,Suspended,Unknown,Failed",
        help="Options: Aborted, Active, Completed, Finished_Mixed_Outcome, Pending, Suspended, Unknown, or Failed. Input as: Completed,Failed,Active [no spaces]",
        )
    parser.add_argument(
        "--credentials",
        type=str,
        required=True,
        help="which set of credentials to use",
        )
    parser.add_argument(
        "--saveto",
        type=Path,
        required=True,
        help="path to export csv file to",
        )

    return parser.parse_args()

def get_wf_instances(credential_set, state, from_date=None, to_date=None):
    workflow_url = "https://nypl.preservica.com/sdb/rest/workflow/instances"
    ns = {'wf': 'http://workflow.preservica.com'}
    start = 0
    max_results = 100 
    total_count = None
    
    token = prsvapi.get_token(credential_set)

    while total_count is None or start < total_count:
        params = {
            "state": state,
            "type": "Ingest",
            "start": str(start),
            "max": str(max_results),
            "includeActiveSteps": "false",
            "includeStepInputs": "false",
            "includeStartInputs": "false",
            "includeOutputs": "false",
            "latestFirst": "false"
        }

        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date

        response = None
        
        for attempt in range(3):
            headers = {
                "Preservica-Access-Token": token,
                "Content-Type": "application/xml;charset=UTF-8",
            }
            try:
                res = requests.get(workflow_url, headers=headers, params=params, timeout=30)
                
                if res.status_code == 401:
                    logger.warning("Token expired. Refreshing...")
                    token = prsvapi.get_token(credential_set)
                    continue 
                
                res.raise_for_status()
                response = res
                break
            except requests.RequestException as e:
                logger.warning(f"Workflow request failed (attempt {attempt+1}): {e}")
                time.sleep(2)
        
        if not response:
            logger.error("Failed to retrieve workflows after retries.")
            break

        try:
            root = ET.fromstring(response.content)
            
            if total_count is None:
                total_node = root.find('wf:TotalCount', ns)
                total_count = int(total_node.text) if total_node is not None else 0
                logger.info(f"Total workflows found: {total_count}")

            instances = root.findall('wf:WorkflowInstance', ns)
            if not instances:
                break

            for inst in instances:
                row_data = {}
                for child in inst:
                    tag_name = child.tag.split('}')[-1]
                    text_content = ""
                    
                    if len(child) > 0:
                        sub_data = []
                        for sub in child:
                            sub_tag = sub.tag.split('}')[-1]
                            sub_val = sub.text if sub.text else ""
                            sub_data.append(f"{sub_tag}: {sub_val}")
                        text_content = " | ".join(sub_data)
                    else:
                        text_content = child.text if child.text else ""

                    if tag_name in row_data:
                        row_data[tag_name] += f"; {text_content}"
                    else:
                        row_data[tag_name] = text_content
                
                yield row_data
            
            start += max_results
            time.sleep(0.5)
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {e}")
            break

def main():
    args = parse_args()
    
    # "2025-11-18T00:00:00.000Z"
    if args.startdate and args.enddate:
        from_date = f"{args.startdate}T00:00:00.000Z"
        to_date = f"{args.enddate}T00:00:00.000Z"
        output_file = Path(args.saveto / f"workflow_report_{args.startdate}_{args.enddate}.csv")
    elif args.startdate and not args.enddate:
        from_date = f"{args.startdate}T00:00:00.000Z"
        to_date = None
        output_file = Path(args.saveto / f"workflow_report_{args.startdate}_current.csv")
    else:
        from_date = None
        to_date = None
        output_file = Path(args.saveto / f"workflow_report_ALL.csv")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        'Id', 
        'CorrelationToken', 
        'Started', 
        'Finished', 
        'State', 
        'DisplayState', 
        'ArchivalProcessId', 
        'WorkflowGroupId', 
        'ProcessMonitorApiId', 
        'WorkflowContextId', 
        'WorkflowContextName', 
        'WorkflowDefinitionTextId', 
        'WorkflowDefinitionName', 
        'Creator', 
        'TopLevelDURecord'
    ]

    logger.info(f"Fetching workflows to {output_file}...")

    try:
        with open(output_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            
            count = 0
            
            wf_generator = get_wf_instances(args.credentials, state=args.wfstate, from_date=from_date, to_date=to_date)
            
            for workflow_instance in wf_generator:
                writer.writerow(workflow_instance)
                count += 1
                
                if count % 100 == 0:
                    print(f"Written {count} rows...", end='\r')

        logger.info(f"Successfully wrote {count} workflow instances to {output_file}")

    except Exception as e:
        logger.error(f"Error during execution: {e}")

if __name__ == "__main__":
    main()