#!/usr/local/bin/python
from swiftclient.service import SwiftService, SwiftUploadObject, Connection
from swiftclient.utils import generate_temp_url
from datetime import datetime, timedelta
from urllib.parse import urlparse
from time import sleep, time
import argparse
import urllib3
import logging
import requests
import json
import pytz
import yaml
import os

urllib3.disable_warnings()

logger = logging.getLogger("DocReportsLogger")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

WORK_DIR = "."
ES_HOST = "http://10.6.4.227/elasticsearch"
LIMIT = 1000
WAIT_SEC = 10
FIELDS = ["USER", "REMOTE_ADDR", "DOC_ID", "DOC_HASH", "TIMESTAMP", "@timestamp", "HOSTNAME"]
SWIFT_HOST = "https://swift-dev-gc.prozorro.gov.ua"

TEMP_URL_KEY = "aPS4pSLej9g9EmYxvL5pFMGR4rVAzDRQJ"


def get_doc_logs(start, end):
    total = 1
    offset = 0

    while offset < total:
        total = 0
        request_body = (
            {
                "index": ["hpi-sandbox-*"],
            },
            {
                "query": {
                    "bool": {
                        "must": [
                            {"match_all": {}},
                            {"match_phrase": {"MESSAGE_ID": {"query":"uploaded_document"}}},
                            {"range": {"@timestamp": {
                                "gte": int(start.timestamp() * 1000),
                                "lte": int(end.timestamp() * 1000),
                                "format": "epoch_millis"
                            }}}
                        ],
                    }
                },
                "from": offset,
                "size": LIMIT,
                "sort": [{"@timestamp": {"order": "asc", "unmapped_type": "boolean"}}],
                "_source": {"includes": FIELDS},
            }
        )
        headers = {
            "kbn-version": "5.6.2",
        }
        response = requests.post("{}/_msearch".format(ES_HOST),
                                 data="\n".join(json.dumps(e) for e in request_body) + "\n",
                                 headers=headers)
        if response.status_code != 200:
            logger.error("Unexpected response {}:{}".format(response.status_code, response.text))
            sleep(WAIT_SEC)
            continue
        else:
            resp_json = response.json()
            response = resp_json["responses"][0]
            hits = response["hits"]
            total = hits["total"]
            logger.info(
                "Got {} hits from total {} with offset {}: from {} to {}".format(
                    len(hits["hits"]), total, offset,
                    hits["hits"][0]["_source"]["TIMESTAMP"],
                    hits["hits"][-1]["_source"]["TIMESTAMP"]
                )
            )
            offset += LIMIT
            yield from hits["hits"]


class ReportFilesManager:

    def __init__(self, directory, fields=None):
        self.directory = directory
        self.descriptors = {}
        self.fields = fields or ("TIMESTAMP", "DOC_ID", "DOC_HASH", "REMOTE_ADDR")

        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        logger.info("Closing files..")
        for d in self.descriptors.values():
            try:
                d.close()
            except IOError as e:
                logger.exception(e)

    def write(self, data):
        file_name = "{}.csv".format(data["USER"])
        if file_name not in self.descriptors:
            full_name = os.path.join(self.directory, file_name)
            logger.info("New report file {}".format(full_name))
            if os.path.exists(full_name):
                logger.info("Removing stale data from {}".format(full_name))

            report_file = open(full_name, "a")
            report_file.write(",".join(k for k in self.fields) + "\n")

            self.descriptors[file_name] = report_file
        else:
            report_file = self.descriptors[file_name]

        report_file.write(
            ",".join(data[k] for k in self.fields) + "\n"
        )


def generate_temp_report_url(account, container, key, expires=60*60*48):
    full_path = "/v1/{}/{}/{}".format(account, container, key)
    url = generate_temp_url(
        full_path,
        int(time() + int(expires)),
        TEMP_URL_KEY,
        'GET',
        absolute=False
    )
    return url


def get_swift_details(options):
    connection = Connection(
        authurl=options["os_auth_url"],
        auth_version=options["auth_version"],
        user=options["os_username"],
        key=options["os_password"],
        os_options={
            'user_domain_name': options["os_user_domain_name"],
            'project_domain_name': options["os_project_domain_name"],
            'project_name': options["os_project_name"]
        },
        insecure=options["insecure"]
    )
    storage_url, _ = connection.get_auth()
    parsed_url = urlparse(storage_url)
    storage_host = "{}://{}".format(parsed_url.scheme, parsed_url.netloc)
    account = urlparse(storage_url).path.split("/")[-1]

    return storage_host, account


def main():
    parser = argparse.ArgumentParser(description="Openprocurement Billing")
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.load(f)

    now = datetime.now(tz=pytz.timezone("Europe/Kiev"))
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=1)
    end = today - timedelta(seconds=1)

    logger.info("Report time range: {} - {}".format(start, end))

    directory = os.path.join(WORK_DIR, str(start.date()))

    # fill the directory with report files
    with ReportFilesManager(directory) as rf_manager:
        for hit in get_doc_logs(start, end):
            rf_manager.write(hit["_source"])

    # upload to swift
    upload_objects = []
    for name in os.listdir(directory):
        full_name = os.path.join(directory, name)
        if os.path.isfile(full_name):
            upload_objects.append(
                SwiftUploadObject(
                    full_name,
                    object_name=full_name.replace(
                        directory + '/', '', 1
                    )
                )
            )

    swift_config = config["swift"]
    storage_host, account = get_swift_details(swift_config)

    links = []
    with SwiftService(options=swift_config) as swift:
        for r in swift.upload(swift_config["container"], upload_objects[:2]):
            if r['success']:
                if 'object' in r:
                    links.append(
                        generate_temp_report_url(account, swift_config["container"], r['object'])
                    )
            else:
                logger.error(r)

    print("Reports uploaded:")
    for link in links:
        print("{}{}".format(storage_host, link))


if __name__ == "__main__":
    main()
