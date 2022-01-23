# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Reads the input configuration file, validates them and enables logging."""

import argparse
import datetime
import json
import logging
import re
from google.api_core import exceptions
from toml import decoder
from hive_to_bigquery import custom_exceptions

TIME_FORMAT = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
LOG_FILE_NAME = "logs/hive_bq_migration_{}.log".format(TIME_FORMAT)


def configure_logger():
    """Configures logging properties such as logging level, filename and format."""

    logger = logging.getLogger("Hive2BigQuery")
    logger.setLevel(logging.DEBUG)
    # Sets log formatter with time,log level,filename,line no and function
    # name which produced that log statement.
    formatter = logging.Formatter(
        "[%(asctime)s - %(levelname)s - %(filename)25s:%(lineno)s - %("
        "funcName)20s() ] %(message)s"
    )
    log_handler = logging.FileHandler(filename=LOG_FILE_NAME)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter("%(asctime)s - %(message)s")
    console_handler.setFormatter(formatter)

    # Adds the above defined handlers.
    logger.addHandler(log_handler)
    logger.addHandler(console_handler)

    print("Check the log file {} for detailed logs".format(LOG_FILE_NAME))
    return logger


logger = configure_logger()


def parse_args_for_hive_to_bq():
    """Argument Parser.

    Returns:
        dict: dictionary of arguments.
    """

    parser = argparse.ArgumentParser(
        description="Framework to migrate Hive tables to BigQuery using "
        "Cloud SQL to keep track of the migration progress."
    )
    parser.add_argument(
        "--config-file", required=True, help="Input configurations JSON file."
    )

    return parser.parse_args()


def validate_bq_table_name(table_name):
    """Validates provided BigQuery table name for maximum character limit and
    permitted characters."""

    max_characters = 1024
    patterns = "^[a-zA-Z0-9_]*$"
    error_msg = (
        "Invalid table name {}. Table name must be alphanumeric"
        "(plus underscores) and must be at most 1024 characters"
        " long.".format(table_name)
    )

    if len(table_name) > max_characters:
        raise exceptions.BadRequest(error_msg)

    if not re.search(patterns, table_name):
        raise exceptions.BadRequest(error_msg)


def validate_toml_config(config_data: dict):
    if "source" not in config_data:
        raise ValueError("Please configure the source.")

    source = config_data["source"]

    if "port" not in source:
        raise ValueError(
            "Please ensure the source Hive server port number is configured."
        )

    if "host" not in source:
        raise ValueError("Please ensure the source Hive server host is configured.")

    if "tables" not in source:
        raise ValueError("Please configure at least one source table to migrate.")

    source_tables = source["tables"]
    for source_table in source_tables:
        if "database" not in source_table:
            raise ValueError(
                "Ensure that each source table to migrate has its database configured."
            )
        if "name" not in source_table:
            raise ValueError("Ensure that each source table to migrate has a name.")

    if "target" not in config_data:
        raise ValueError("Please configure the target to migrate to.")

    target = config_data["target"]

    if "project_id" not in target:
        raise ValueError(
            "Please configure the target project ID containing the BigQuery dataset to migrate to."
        )

    if "dataset" not in target:
        raise ValueError(
            "Please configure the target BigQuery dataset ID to migrate to."
        )

    if "bucket" not in target:
        raise ValueError("Please configure the target staging bucket to use.")

    if "tracking_database" not in config_data:
        raise ValueError("Please configure the tracking database to use for migration.")

    tracking_database = config_data["tracking_database"]
    if not all(
        map(
            lambda e: e in tracking_database,
            ["host", "port", "user", "database", "table"],
        )
    ):
        raise ValueError(
            "The configuration keys host, port, user, database, password_secret_id and password_secret_location are all required in tracking_database."
        )

    return config_data


def validate_config_parameters(data: dict):
    """Checks for all the parameters in the input configuration file and
    validates them."""

    # tracking_metatable_name = "tracking_table_info"

    try:
        tracking_db_password_path = data["tracking_database"]["password_file_path"]
    except KeyError:
        tracking_db_password_path = None
        try:
            tracking_db_password_secret = data["tracking_database"][
                "password_secret_id"
            ]
            tracking_db_password_secret_location = data["tracking_database"][
                "password_secret_location"
            ]
        except KeyError:
            raise
    else:
        if not tracking_db_password_path.startswith("gs://"):
            raise ValueError("Tracking database password path must start with gs://")

    hive_tables = data["source"]["tables"]
    bq_tables = data["target"].get("tables", [])

    hive_to_bigquery_table_names = {}
    if isinstance(hive_tables, list) and (
        isinstance(bq_tables, list) or bq_tables is None
    ):
        if not bq_tables:
            bq_tables = hive_tables.copy()
        logger.debug(f"Got the following tables: {hive_tables}")
        # if len(bq_tables) > 1:
        #    raise ValueError(f"Sadly, the tool does not support multiple tables yet. Hopefully soon!")
    else:
        raise ValueError(
            f"Types of the hive table object and the BigQuery table objects must match, or the BigQuery table must be None. They do not. I got a {type(hive_tables)} and a {type(bq_tables)} for hive and BigQuery, respectively."
        )

    hive_port = data["source"]["port"]
    if not isinstance(hive_port, int):
        raise TypeError("Hive port must be an integer")

    tracking_db_port = data["tracking_database"]["port"]
    if not isinstance(tracking_db_port, int):
        raise TypeError("Tracking database port must be an integer")

    gcs_bucket_name = data["target"]["bucket"]
    if gcs_bucket_name.startswith("gs://"):
        gcs_bucket_name = gcs_bucket_name.split("gs://")[1]
    if gcs_bucket_name[-1] == "/":
        gcs_bucket_name = gcs_bucket_name[:-1]

    config = {
        "project_id": data["target"]["project_id"],
        "gcs_bucket_name": data["target"]["bucket"],
        "hive_server_host": data["source"]["host"],
        "hive_server_port": data["source"]["port"],
        "hive_server_username": data["source"].get("user", None),
        "hive_database": data["source"]["tables"][0]["database"],
        "hive_table_name": data["source"]["tables"],
        # "incremental_col": incremental_col,
        "dataset_id": data["target"]["dataset"],
        "bq_table": data["target"]["tables"][0]["name"],
        "bq_table_write_mode": data["target"]["tables"][0]["write_mode"],
        # "use_clustering": use_clustering,
        "tracking_database_host": data["tracking_database"]["host"],
        "tracking_database_port": data["tracking_database"]["port"],
        "tracking_database_user": data["tracking_database"]["user"],
        "tracking_database_db_name": data["tracking_database"]["database"],
        # "tracking_db_password_path": tracking_db_password_path,
        "tracking_db_password_secret": data["tracking_database"]["password_secret_id"],
        "tracking_db_password_secret_location": data["tracking_database"][
            "password_secret_location"
        ],
        "tracking_metatable_name": data["tracking_database"]["table"],
        # "location_id": kms_location,
        # "key_ring_id": kms_key_ring_id,
        # "crypto_key_id": kms_crypto_key_id,
        # "create_validation_table": create_validation_table,
        "log_file_name": LOG_FILE_NAME,
    }

    # merge the old and new for interim compatibility
    config.update(data)

    return config


def read_json_config(json_config: str):
    logger.warn(
        f"This configuration format is deprected. Please use the toml format instead."
    )
    try:
        config = json.loads(json_config)
    except ValueError as ex:
        logger.error(f"Unable to parse configuration JSON: {ex}")
        raise

    return config


def read_toml_config(toml_config: str):
    try:
        config = decoder.loads(toml_config)
    except Exception as ex:
        logger.error(f"Unable to parse configuration TOML: {ex}")
        raise

    return config


def initialize_variables():
    """Initializes variables from the input configuration file.

    Returns:
        dict: A dictionary of validated input arguments.
    """

    args = parse_args_for_hive_to_bq()

    if args.config_file.endswith(".json"):
        try:
            with open(args.config_file) as config_json:
                try:
                    config_str = config_json.read()
                    config = read_json_config(config_str)
                except ValueError:
                    raise
            validated_config = validate_config_parameters(config)
        except (exceptions.BadRequest, TypeError, ValueError, KeyError) as error:
            raise custom_exceptions.ArgumentInitializationError from error

    elif args.config_file.endswith(".toml"):
        try:
            with open(args.config_file) as config_toml:
                try:
                    config_str = config_toml.read()
                    config = read_toml_config(config_str)
                except ValueError:
                    raise
            validated_config = validate_toml_config(config)
        except (exceptions.BadRequest, TypeError, ValueError, KeyError) as error:
            raise custom_exceptions.ArgumentInitializationError from error

    else:
        raise ValueError(
            "The configuration file passed should have an extension of .toml or .json."
        )

    return validated_config
