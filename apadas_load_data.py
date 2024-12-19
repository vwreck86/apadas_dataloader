#!/usr/bin/env python3

import argparse
import traceback
from datetime import datetime, timedelta
import time
import json
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
import random
from getpass import getpass
from sqlite3 import Error
import csv
import ssl

from typing import Any

# the follow parameters can be set here or overwritten with command line flags
# this is an api key used local testing. You will be promter for the api key
# on program start if the --use-mock-api-key flag is not present

DEFAULT_BASE_URL = "https://midas-telemetry.sec.usace.army.mil"
DEFAULT_BASE_PATH = "xxxxxxxxxx"  # add path to the .dat files

DEFAULT_MODEL = "CR6"


# after setting up the datalogger file in MIDAS, add the api key and sn for each one below
data_keys = [
    {'name': 'mcu1', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU1CALC.dat', 'sn': '*** PUT SN HERE ***'},
    {'name': 'mcu2', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU2CALC.dat', 'sn': '*** PUT SN HERE ***'},
    {'name': 'mcu3', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU3CALC.dat', 'sn': '*** PUT SN HERE ***'},
    {'name': 'mcu4', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU4CALC.dat', 'sn': '*** PUT SN HERE ***'},
    {'name': 'mcu5', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU5CALC.dat', 'sn': '*** PUT SN HERE ***'},
    {'name': 'mcu6', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU6CALC.dat', 'sn': '*** PUT SN HERE ***'},
    {'name': 'mcu7', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU7CALC.dat', 'sn': '*** PUT SN HERE ***'},
    {'name': 'mcu8', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU8CALC.dat', 'sn': '*** PUT SN HERE ***'},
    {'name': 'mcu9', 'key': '******* PUT API KEY HERE *********',
        'file': 'MCU9CALC.dat', 'sn': '*** PUT SN HERE ***'}
]


def post_data(url: str, data: str, api_key: str) -> Any | None:
    global log_output, log_filename

    json_data = json.dumps(data).encode("utf-8")
    context_ssl = ssl._create_unverified_context()

    request = Request(
        url,
        headers={"Content-Type": "application/json", "X-Api-Key": api_key},
        data=json_data,
    )

    try:
        with urlopen(request, timeout=10, context=context_ssl) as response:
            print("response status:", response.status)
            log_file("\n\t\tresponse status:" + str(response.status))
            return response

    except HTTPError as error:
        print("HTTPError", error.status, error.reason, error.read())
        log_file("\n\t\tHTTPError"+str(error.status))
    except URLError as error:
        print("URLError", error.reason)
        log_file("\n\t\tURLError" + str(error.reason))
    except TimeoutError:
        print("Request timed out")
        log_file("\n\t\tRequest timed out")

    return None  # exception was thrown


def parse_data(station_name, data, sn):
    global log_output, log_filename

    fields_list = []
    data_list = []
    i = 0
    entry_counter = 0

    main_dict = {
        "transaction": 0,
        "signature": 99999,
        "environment": {
            "station_name": station_name,
            "table_name": "piezo",
            "model": DEFAULT_MODEL,
            "serial_no": sn,
            "os_version": "CR800.Std.27",
            "prog_name": "from Server:cr800 Template",
        }
    }

    for row in data:

        # set data template
        data = {
            "time": '',
            "no": entry_counter,
            "vals": [],
        }

        entry_counter += 1

        for ea in row:
            # builds the fields list of sensors
            if i < len(row):
                i += 1
                print(ea[0])

                field = {
                    "name": ea[0],
                    "type": "xsd:float",
                    "units": "ft",
                    "process": "Min",
                    "settable": False,
                }
                fields_list.append(field)

            # builds the data list of sensor values
            data["time"] = ea[1].replace(" 00:00:00", "T00:00:00")

            if ea[2].lower() == "nan":
                data["vals"].append(None)
            else:
                data["vals"].append(float(ea[2]))

        data_list.append(data)
        print(data)
        log_file("\n\t\t" + str(data))
    main_dict["fields"] = fields_list

    return {"head": main_dict, "data": data_list}


def read_file(file_name):
    global log_output, log_filename

    # map sensor names from the .dat file to MIDAS instrument name:  ("dat file_instrument_name": "Midas_name")
    translate_list = {'RESLEVELE': 'RESERVOIR POOL', 'P1E': 'P-1', 'P2E': 'P-2', 'P3E': 'P-3', 'P4E': 'P-4',
                      'P5E': 'P-5', 'P6E': 'P-6', 'P7E': 'P-7', 'P8E': 'P-8', 'P9E': 'P-9', 'P10E': 'P-10',
                      'P11E': 'P-11', 'P11AE': 'P-11A', 'P12E': 'P-12', 'P13E': 'P-13', 'P13AE': 'P-13A',
                      'P14AE': 'P-14A', 'P15E': 'P-15', 'P16E': 'P-16', 'P17E': 'P-17', 'P18E': 'P-18',
                      'P19E': 'P-19', 'P20AE': 'P-20A', 'P20E': 'P-20', 'P21E': 'P-21', 'P22E': 'P-22',
                      'P23E': 'P-23', 'P24E': 'P-24', 'P25E': 'P-25', 'P26E': 'P-26', 'P27E': 'P-27', 'P28E': 'P-28',
                      'P29E': 'P-29', 'P30E': 'P-30', 'P31E': 'P-31', 'P32E': 'P-32', 'P34E': 'P-34', 'P35E': 'P-35',
                      'P36E': 'P-36', 'P37E': 'P-37', 'P39AE': 'P-39A', 'P40E': 'P-40', 'P43E': 'P-43', 'P45E': 'P-45',
                      'P46E': 'P-46', 'P46AE': 'P-46A', 'P47E': 'P-47', 'P48E': 'P-48', 'P49E': 'P-49', 'P50E': 'P-50',
                      'P51E': 'P-51', 'P52E': 'P-52', 'P53E': 'P-53', 'ABC1E': 'P-AB-C-1', 'ABC1AE': 'P-AB-C-1A',
                      'ABC2AE': 'P-AB-C-2A', 'ABC2E': 'P-AB-C-2', 'ABC3E': 'P-AB-C-3', 'ABC7E': 'P-AB-C-7', 'ABC8E': 'P-AB-C-8'}

    try:
        open(file_name)

        with open(file_name, 'r') as csv_file:
            file_reader = csv.reader(csv_file, delimiter=',')
            count = 0
            index_list = []
            gage_list = []
            for row in file_reader:
                if len(row) > 0 and len(row[0]) > 0 and not row[0].startswith('TOA5') and not row[0].startswith('TS') and not row[0].startswith('TOACI1'):
                    if row[0].startswith('TIMESTAMP') or row[0].startswith('TMSTAMP'):
                        for x in row:
                            if x.strip('"') in translate_list.keys():
                                name = translate_list.get(x.strip('"'))
                            else:
                                name = x
                            index_list.append(name)
                    else:
                        index_counter = 0
                        temp_list = []
                        for ea in row:
                            if index_counter > 0:
                                if index_list[index_counter] in translate_list.values():
                                    info = (index_list[index_counter],
                                            row[0], row[index_counter])

                                    temp_list.append(info)
                            index_counter += 1
                        gage_list.append(temp_list)

    except Error as e:
        print(e)
        log_file("\n\t\tread file error: file not found")

    return (gage_list)


def get_timestamp_logfile():
    current_time = datetime.now()
    return (current_time.strftime("%b-%d-%Y_%H-%M"))


def log_file(message):
    global log_output, log_filename

    if not log_output:
        try:
            log_output = open(log_filename, "a")
        except Exception as e:
            traceback.print_exc()

    else:
        log_output.write(message)
        log_output.flush()


def main() -> None:

    global log_output, log_filename

    log_filename = (r"./log_mcu_%s.out" % get_timestamp_logfile())
    log_output = ''

    # --------------------#
    #    start log file  #
    # --------------------#
    log_file("Start appending run: " + str(get_timestamp_logfile()))

    # run script
    for ea in data_keys:
        sn = ea["sn"]
        url = f"{DEFAULT_BASE_URL}/telemetry/datalogger/{DEFAULT_MODEL}/{sn}"
        api_key = ea["key"]
        print(api_key)

        data = read_file(DEFAULT_BASE_PATH + ea["file"])
        msg = parse_data(ea["name"], data, sn)
        post_data(url, msg, api_key)


if __name__ == "__main__":
    main()
