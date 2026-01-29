import argparse
import json
import os
import subprocess
import requests
import boto3
import hvac
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.util.retry import Retry

# Redpanda schema registry url access credentials
# These are loaded from environment variables
redpanda_connect_username = ""
redpanda_connect_password = ""

def log_changed_properties(old_props, new_props, old_config_dict, new_config_dict):
    overlapping_props = old_props.intersection(new_props)
    changed_props = set()

    for prop in overlapping_props:
        old_value = old_config_dict[prop]
        new_value = new_config_dict[prop]
        if old_value != new_value:
            changed_props.add(prop)

    if len(changed_props) > 0:
        print('The properties below will be changed.')
        for prop in changed_props:
            old_value = old_config_dict[prop]
            new_value = new_config_dict[prop]
            print('    ' + prop + ': ' + str(old_value) + ' -> ' + str(new_value))
        print()


def log_removed_properties(old_props, new_props, old_config_dict):
    removed_props = old_props - new_props

    if len(removed_props) > 0:
        print('The properties below will be removed.')
        for prop in removed_props:
            old_value = old_config_dict[prop]
            print('    ' + prop + ': ' + str(old_value))
        print()


def log_added_properties(old_props, new_props, new_config_dict):
    added_props = new_props - old_props

    if len(added_props) > 0:
        print('The properties below will be added.')
        for prop in added_props:
            new_value = new_config_dict[prop]
            print('    ' + prop + ': ' + str(new_value))
        print()


def log_property_differences(old_config_dict, new_config_dict):
    old_props = set(old_config_dict.keys())
    new_props = set(new_config_dict.keys())

    log_changed_properties(old_props, new_props, old_config_dict, new_config_dict)
    log_removed_properties(old_props, new_props, old_config_dict)
    log_added_properties(old_props, new_props, new_config_dict)


def update_required(old_config_dict, new_config_dict):
    if old_config_dict == new_config_dict:
        print('No update required.\n')
        return False
    else:
        print('Update required.\n')
        return True


class RedpandaConnectClient():
    def __init__(self, env, region, dryrun, prefix, username, password):
        self.env = env
        self.region = region
        self.dryrun = dryrun
        self.prefix = prefix
        self.requests_session = None
        self.existing_connectors = None

        if self.env == 'prod':
            if self.region == 'us-east-1':
                self._configure_requests('https://redpanda-connect.prod.ssa.triplelift.net', username, password)
            elif self.region == 'eu-central-1':
                self._configure_requests('https://redpanda-connect.eu-central-1.prod.ssa.triplelift.net', username, password)
        elif self.env == 'staging':
            self._configure_requests('https://redpanda-connect.staging.triplelift.net', username, password)
        elif self.env == 'dev' or self.env == 'dev':
            self._configure_requests('https://redpanda-connect.dev.ssa.triplelift.net', username, password)
        else:
            raise ValueError('Invalid environment: ' + self.env)


    def _configure_requests(self, base, username, password):
        session = requests.Session()
        retry = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=0.3,
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "DELETE"]
        )
        session.mount(base, HTTPAdapter(max_retries=retry))
        session.auth = HTTPBasicAuth(username, password)
        self.base_url = base
        self.requests_session = session

    def _get_requests_session(self):
        return self.requests_session

    def _get_base_url(self):
        return self.base_url

    def _prefix_match(self, connector_name):
        if self.prefix is None:
            return True
        else:
            return connector_name[:len(self.prefix)] == self.prefix

    def _perform_action(self, connector_name, msg=None):
        if not self._prefix_match(connector_name):
            return False
        else:
            if msg is not None:
                print(msg)

            if self.dryrun:
                print('dryrun: no action performed')
                print()
                print('-' * 80)
                print()
                return False
            else:
                return True

    def get_names_of_existing_connectors(self):
        session = self._get_requests_session()
        connectors_url = self._get_base_url() + '/connectors'
        return session.get(connectors_url).json()

    def get_status_of_existing_connectors(self):
        session = self._get_requests_session()
        connectors_url = self._get_base_url() + '/connectors?expand=status'
        return session.get(connectors_url).json()

    def get_connector_status(self, connector_name):
        get_url = self._get_base_url() + '/connectors/' + connector_name + '/status'
        session = self._get_requests_session()
        return session.get(get_url).json()

    def get_old_connector_config_dict(self, connector_name):
        get_url = self._get_base_url() + '/connectors/' + connector_name + '/config'
        session = self._get_requests_session()
        return session.get(get_url).json()

    def connector_already_exists(self, connector_config):
        if self.existing_connectors is None:
            self.existing_connectors = set(self.get_names_of_existing_connectors())

        already_exists = connector_config.get_connector_name() in self.existing_connectors
        if already_exists:
            print(connector_config.get_connector_name() + ' already exists.\n')
        return already_exists

    def update_connector(self, connector_config):
        connector_name = connector_config.get_connector_name()
        if self._perform_action(connector_name):
            update_url = self._get_base_url() + '/connectors/' + connector_name + '/config'
            session = self._get_requests_session()
            response = session.put(update_url, json=connector_config.as_dict())
            print(response.status_code)

    def create_connector(self, connector_config):
        connector_name = connector_config.get_connector_name()

        new_connector = {
            'name': connector_name,
            'config': connector_config.as_dict()
        }

        msg = connector_name + ' does not already exist.\n'
        msg += 'Creating a new connector with the config below:\n'
        msg += json.dumps(new_connector, indent=4)

        if self._perform_action(connector_name, msg):
            create_url = self._get_base_url() + '/connectors'
            session = self._get_requests_session()
            response = session.post(create_url, json=new_connector)
            print(response.status_code)

    def delete_connector(self, connector_name):
        # Check if connector exists before attempting to stop and delete
        if not self._prefix_match(connector_name):
            return
        
        session = self._get_requests_session()
        existing_connectors = self.get_names_of_existing_connectors()
        
        if connector_name not in existing_connectors:
            print('Connector {} does not exist. Skipping stop and delete.'.format(connector_name))
            print('-' * 80)
            print()
            return
        
        # Stop the connector first before deleting
        msg = 'Stopping the connector named ' + connector_name + ' before deletion'
        print(msg)
        
        if self.dryrun:
            print('dryrun: no action performed')
            print()
            print('-' * 80)
            print()
            return
        
        try:
            # Attempt to stop the connector
            stop_url = self._get_base_url() + '/connectors/' + connector_name + '/stop'
            stop_response = session.put(stop_url)
            print('Stop status: ' + str(stop_response.status_code))
            
            if stop_response.status_code not in [200, 202, 204]:
                print('Warning: Stop command returned unexpected status code. Proceeding with deletion anyway.')
        except Exception as e:
            print('Warning: Failed to stop connector {}. Error: {}. Proceeding with deletion anyway.'.format(connector_name, str(e)))
        
        try:
            # Now delete the connector
            print('Deleting the connector named ' + connector_name)
            delete_url = self._get_base_url() + '/connectors/' + connector_name
            delete_response = session.delete(delete_url)
            print('Delete status: ' + str(delete_response.status_code))
            
            if delete_response.status_code in [200, 204]:
                print('Successfully deleted connector {}'.format(connector_name))
            else:
                print('Warning: Delete command returned unexpected status code for connector {}'.format(connector_name))
        except Exception as e:
            print('Error: Failed to delete connector {}. Error: {}'.format(connector_name, str(e)))
        
        print('-' * 80)
        print()

    def delete_connectors(self, topic_names):
        existing = self.get_names_of_existing_connectors()
        for topic_name in topic_names:
            connector_name = 's3-sink-' + topic_name
            self.delete_connector(connector_name)

    def pause_connector(self, connector_name):
        msg = 'Pausing the connector named ' + connector_name
        if self._perform_action(connector_name, msg):
            pause_url = self._get_base_url() + '/connectors/' + connector_name + '/pause'
            session = self._get_requests_session()
            response = session.put(pause_url)
            print(response.status_code)

    def stop_connector(self, connector_name):
        msg = 'Stopping the connector named ' + connector_name
        if self._perform_action(connector_name, msg):
            stop_url = self._get_base_url() + '/connectors/' + connector_name + '/stop'
            session = self._get_requests_session()
            response = session.put(stop_url)
            print(response.status_code)

    def resume_connector(self, connector_name):
        msg = 'Resuming the connector named ' + connector_name
        if self._perform_action(connector_name, msg):
            resume_url = self._get_base_url() + '/connectors/' + connector_name + '/resume'
            session = self._get_requests_session()
            response = session.put(resume_url)
            print(response.status_code)


class SinkConfigBase():
    def __init__(self, topic, tasks_max=1, dev_only=False):
        self.topic = topic
        self.tasks_max = tasks_max
        self.dev_only = dev_only

    def get_connector_name(self):
        base_name = 'sink-'
        return base_name + self.topic

    def as_dict(self):
        return {
            'name': self.get_connector_name(),
            'topics': self.topic,
            'tasks.max': str(self.tasks_max),
        }

class S3SinkConfigBase(SinkConfigBase):
    def __init__(self, topic, region, tasks_max=2, flush_size=2000000, s3_part_size=10485760,
                 rotate_schedule_interval_ms=60000, consumer_config_overrides={}, dev_only=False):
        super().__init__(topic, tasks_max, dev_only)
        self.s3_part_size = s3_part_size
        self.region = region
        self.flush_size = flush_size
        self.rotate_schedule_interval_ms = rotate_schedule_interval_ms
        self.consumer_config_overrides = consumer_config_overrides

    def as_dict(self):
        config = super().as_dict()
        config.update({
            's3.part.size': str(self.s3_part_size),
            'flush.size': str(self.flush_size),
            'rotate.schedule.interval.ms': str(self.rotate_schedule_interval_ms)
        })
        config.update(self.consumer_config_overrides)
        return config

    def get_connector_name(self):
        base_name = 's3-sink-'
        return base_name + self.topic

    # flush.size, rotate.schedule.interval.ms, and s3.part.size has a direct impact on
    # java memory usage. Tune these values carefully to prevent a OOM condition.
    # More details are available at this StackOverflow:
    # https://stackoverflow.com/questions/50971065/kafka-connect-s3-connector-outofmemory-errors-with-timebasedpartitioner
    # consumer.max.partition.fetch.bytes = 5MB
    def _get_common_base_config(self):
        return {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "s3.region": "us-east-1",
            "filename.offset.zero.pad.width": "20",
            "timezone": "UTC",
            "locale": "en-US",
            "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
            "s3.part.retries": "14",
            "s3.retry.backoff.ms": "100",
            "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
            "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
            "partition.duration.ms": "3600000",
            "schema.compatibility": "BACKWARD",
            "parquet.codec": "zstd",
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "errors.tolerance": "all",
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "timestamp.extractor": "Record",
            "consumer.override.auto.offset.reset": "latest",
            "consumer.override.fetch.max.wait.ms": "100"
        }

    def get_env_specific_config(self):
        pass

    def as_dict(self):
        config = self._get_common_base_config()

        config.update(self.get_env_specific_config())

        config['topics'] = self.topic
        config['name'] = self.get_connector_name()
        config['tasks.max'] = str(self.tasks_max)
        config['flush.size'] = str(self.flush_size)
        config['s3.part.size'] = str(self.s3_part_size)
        config['rotate.schedule.interval.ms'] = str(self.rotate_schedule_interval_ms)
        return config


class ProdS3SinkConfig(S3SinkConfigBase):
    def get_env_specific_config(self):
        if self.region == 'us-east-1':
            return {
                    "topics.dir": "production",
                    "s3.bucket.name": "data-lake.prod.triplelift.net",
                    "path.format": "'dt'=YYYY'-'MM'-'dd/'hr'=HH'/us-east-1'",
                    "value.converter.schema.registry.url": "https://kafka-schema-registry-internal.prod.triplelift.net"
            }
        elif self.region == 'eu-central-1':
            return {
                    "topics.dir": "production",
                    "s3.bucket.name": "data-lake.prod.triplelift.net",
                    "path.format": "'dt'=YYYY'-'MM'-'dd/'hr'=HH'/eu-central-1'",
                    "value.converter.schema.registry.url": "https://kafka-schema-registry-internal.eu-central-1.prod.ssa.triplelift.net"
            }

class DevS3SinkConfig(S3SinkConfigBase):
    def get_env_specific_config(self):
        return {
                "topics.dir": "dev",
                "path.format": "'dt'=YYYY'-'MM'-'dd/'hr'=HH'/us-east-1'",
                "s3.bucket.name": "data-lake.sand.triplelift.net",
                "value.converter.schema.registry.url": "https://kafka-schema-registry.dev.triplelift.net"
        }


class OpensearchSinkConfig(SinkConfigBase):
    def __init__(self, topic, region, tasks_max=1, dev_only=False):
        super().__init__(topic, tasks_max, dev_only)
        self.region = region

    def get_connector_name(self):
        base_name = 'opensearch-sink-'
        return base_name + self.topic

    def as_dict(self):
        config = super().as_dict()
        config.update({
            'connector.class': 'io.aiven.kafka.connect.opensearch.OpensearchSinkConnector',
            'topics': self.topic,
            'tasks.max': str(self.tasks_max),
            'key.converter': 'org.apache.kafka.connect.converters.ByteArrayConverter',
            'key.ignore': 'true',
            'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
            'value.converter.schemas.enable': 'false',
            'schema.ignore': 'true',
            'drop.invalid.message': 'false',
            'behavior.on.malformed.documents': 'warn',
            'errors.tolerance': 'all',
            'errors.log.enable': 'true',
            'errors.log.include.messages': 'true',
            'consumer.override.auto.offset.reset': 'earliest',
            'consumer.override.fetch.max.wait.ms': '100'
        })
        config.update(self.get_env_specific_config())
        return config

    def get_env_specific_config(self):
        pass


class ProdOpensearchSinkConfig(OpensearchSinkConfig):
    def get_env_specific_config(self):
        return {
                "connection.url": "https://platform-changelogs.prod.ssa.triplelift.net",
                "connection.username": "${env:PLATFORM_CHANGELOG_OPENSEARCH_USERNAME}",
                "connection.password": "${env:PLATFORM_CHANGELOG_OPENSEARCH_PASSWORD}",
        }

class DevOpensearchSinkConfig(OpensearchSinkConfig):
    def get_env_specific_config(self):
        return {
                "connection.url": "https://platform-changelogs.dev.ssa.triplelift.net",
                "connection.username": "${env:PLATFORM_CHANGELOG_OPENSEARCH_USERNAME}",
                "connection.password": "${env:PLATFORM_CHANGELOG_OPENSEARCH_PASSWORD}",
        }

def get_prod_opensearch_sink_configs(prod_opensearch_sink_configs):
    return [sink_config for sink_config in prod_opensearch_sink_configs if not sink_config.dev_only]

def get_dev_opensearch_sink_configs(prod_opensearch_sink_configs):
    return [DevOpensearchSinkConfig(sink_config.topic, 'us-east-1', tasks_max=sink_config.tasks_max) 
            for sink_config in prod_opensearch_sink_configs]


def get_prod_s3_sink_configs(prod_s3_sink_configs):
    return [sink_config for sink_config in prod_s3_sink_configs if not sink_config.dev_only]


def get_dev_s3_sink_configs(prod_s3_sink_configs):
    # For dev environment, scale down tasks.max based on production values
    # This maintains proper ordering while reducing resource consumption
    def get_dev_tasks_max(prod_tasks_max):
        if prod_tasks_max > 1000:
            return 501
        elif prod_tasks_max > 500:
            return 157
        elif prod_tasks_max > 100:
            return 57
        else:
            return 1
    
    return [DevS3SinkConfig(sink_config.topic, 'us-east-1', 
                           tasks_max=get_dev_tasks_max(sink_config.tasks_max),
                           flush_size=sink_config.flush_size, 
                           s3_part_size=sink_config.s3_part_size,
                           rotate_schedule_interval_ms=sink_config.rotate_schedule_interval_ms) 
            for sink_config in prod_s3_sink_configs]


# Sorting the Topic List by Partition Count
# Connect has no built-in concept of heavy tasks vs non-heavy tasks.
# The reasoning for flipping it is so that the heaviest topics,
# (those with the most partitions) are among the first ones created
# and equally distributed among all the active worker nodes and then
# all the smaller topics are scheduled after. The goal is to prevent
# multiple heavy tasks to be scheduled on the same node during creation.
# Additionally, we wait for each new connector to reach RUNNING state
# before creating the next one, ensuring proper workload distribution
# across all workers before adding more load.
def get_sorted_connector_configs(connector_configs):
    return sorted(connector_configs, key=lambda config: config.tasks_max, reverse=True)


def wait_for_connector_running(client, connector_name, max_attempts=30, delay_seconds=10):
    """
    Wait for a connector to reach RUNNING state on all tasks.
    
    Args:
        client: The RedpandaConnectClient instance
        connector_name: Name of the connector to check
        max_attempts: Maximum number of status check attempts (default 30)
        delay_seconds: Delay between status checks in seconds (default 10)
    
    Returns:
        bool: True if connector is running, False if timeout or error
    """
    import time
    
    print('Waiting for connector {} to reach RUNNING state...'.format(connector_name))
    
    for attempt in range(max_attempts):
        try:
            status = client.get_connector_status(connector_name)
            
            # Check connector state
            connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
            
            # Check all tasks states
            tasks = status.get('tasks', [])
            if not tasks:
                print('  Attempt {}/{}: No tasks found yet, waiting...'.format(attempt + 1, max_attempts))
                time.sleep(delay_seconds)
                continue
            
            all_tasks_running = all(task.get('state') == 'RUNNING' for task in tasks)
            
            if connector_state == 'RUNNING' and all_tasks_running:
                print('  Connector {} is RUNNING with all {} tasks RUNNING'.format(connector_name, len(tasks)))
                return True
            else:
                task_states = [task.get('state', 'UNKNOWN') for task in tasks]
                print('  Attempt {}/{}: Connector state: {}, Task states: {}'.format(
                    attempt + 1, max_attempts, connector_state, task_states))
                time.sleep(delay_seconds)
        except Exception as e:
            print('  Attempt {}/{}: Error checking status: {}'.format(attempt + 1, max_attempts, str(e)))
            time.sleep(delay_seconds)
    
    print('  WARNING: Connector {} did not reach RUNNING state within timeout'.format(connector_name))
    return False


def process_connector_config(client, connector_config, wait_for_running=False):
    connector_name = connector_config.get_connector_name()
    is_new_connector = False
    
    if client.connector_already_exists(connector_config):
        old_config_dict = client.get_old_connector_config_dict(connector_name)
        new_config_dict = connector_config.as_dict()

        if update_required(old_config_dict, new_config_dict):
            log_property_differences(old_config_dict, new_config_dict)
            client.update_connector(connector_config)
    else:
        is_new_connector = True
        client.create_connector(connector_config)
        # Invalidate the cache so the next connector_already_exists call fetches fresh data
        client.existing_connectors = None
    
    # Wait for connector to reach RUNNING state if requested and it's a new connector
    if wait_for_running and is_new_connector and not client.dryrun:
        wait_for_connector_running(client, connector_name)
        print()  # Add spacing after wait completes


def debug(client, connector):
    print('Debugging connector named: ' + connector)

    connector_names = client.get_names_of_existing_connectors()

    if connector in connector_names:
        print('"{}" is the name of an existing connector.'.format(connector))

        response = client.get_connector_status(connector)
        print('Connector status:')
        print(json.dumps(response, indent=4))

        response = client.get_old_connector_config_dict(connector)
        print('Connector config:')
        print(json.dumps(response, indent=4))
    else:
        print('"{}" is not the name of an existing connector.'.format(connector))
        connector_names.sort()
        print('The names of all the existing connectors are listed below: ')
        print('\n'.join(connector_names))
    exit()

def delete_connectors(redpanda_connect_client):
    existing_connectors = redpanda_connect_client.get_names_of_existing_connectors()
    for existing_connector in existing_connectors:
        redpanda_connect_client.delete_connector(existing_connector)

def pause_connectors(redpanda_connect_client, connector_configs, topics_filter=None):
    """
    Pause connectors based on the provided configurations and optional topic filter.
    
    Args:
        redpanda_connect_client: The client to use for pausing connectors
        connector_configs: List of connector configurations
        topics_filter: Optional list of topic names to filter which connectors to pause
    """
    existing_connectors = set(redpanda_connect_client.get_names_of_existing_connectors())
    
    for connector_config in connector_configs:
        connector_name = connector_config.get_connector_name()
        
        # Check if we should pause this connector
        if topics_filter is not None and connector_config.topic not in topics_filter:
            continue
            
        # Only pause if connector exists
        if connector_name in existing_connectors:
            redpanda_connect_client.pause_connector(connector_name)
        else:
            print('Connector {} does not exist. Skipping pause.'.format(connector_name))

def stop_connectors(redpanda_connect_client, connector_configs, topics_filter=None):
    """
    Stop connectors based on the provided configurations and optional topic filter.
    
    Args:
        redpanda_connect_client: The client to use for stopping connectors
        connector_configs: List of connector configurations
        topics_filter: Optional list of topic names to filter which connectors to stop
    """
    existing_connectors = set(redpanda_connect_client.get_names_of_existing_connectors())
    
    for connector_config in connector_configs:
        connector_name = connector_config.get_connector_name()
        
        # Check if we should stop this connector
        if topics_filter is not None and connector_config.topic not in topics_filter:
            continue
            
        # Only stop if connector exists
        if connector_name in existing_connectors:
            redpanda_connect_client.stop_connector(connector_name)
        else:
            print('Connector {} does not exist. Skipping stop.'.format(connector_name))

def resume_connectors(redpanda_connect_client, connector_configs, topics_filter=None):
    """
    Resume connectors based on the provided configurations and optional topic filter.
    
    Args:
        redpanda_connect_client: The client to use for resuming connectors
        connector_configs: List of connector configurations
        topics_filter: Optional list of topic names to filter which connectors to resume
    """
    existing_connectors = set(redpanda_connect_client.get_names_of_existing_connectors())
    
    for connector_config in connector_configs:
        connector_name = connector_config.get_connector_name()
        
        # Check if we should resume this connector
        if topics_filter is not None and connector_config.topic not in topics_filter:
            continue
            
        # Only resume if connector exists
        if connector_name in existing_connectors:
            redpanda_connect_client.resume_connector(connector_name)
        else:
            print('Connector {} does not exist. Skipping resume.'.format(connector_name))

us_east_1_prod_s3_sink_configs = [
    ProdS3SinkConfig('ad_on_page', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('assertive_yield_log', 'us-east-1',tasks_max=1),
    ProdS3SinkConfig('asset_cardinality_info_v1', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('auction_bid_request_v1', 'us-east-1', tasks_max=84),
    ProdS3SinkConfig('auction_bid_response_info_v1', 'us-east-1', tasks_max=84, flush_size=10000000),
    ProdS3SinkConfig('auction_deal_info_v1', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('auction_consent_info', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('bid_connection_stats', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('bills', 'us-east-1', tasks_max=2),
    ProdS3SinkConfig('budget_spend_deals', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('budget_spend_windows', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('buyer_impression', 'us-east-1', tasks_max=84),
    ProdS3SinkConfig('buyer_impression_adapter', 'us-east-1', tasks_max=24),
    ProdS3SinkConfig('buyer_item', 'us-east-1', tasks_max=84),
    ProdS3SinkConfig('buyer_transaction', 'us-east-1', tasks_max=84),
    ProdS3SinkConfig('click_v1', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('consortia_ids', 'us-east-1', tasks_max=12),
    ProdS3SinkConfig('content_dial_page_ping', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('content_dial_page_view', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('creative_info', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('creative_quality', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('creative_scan', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('cta_rendered_result', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('ctv_bills', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('ctv_impressions', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('ctv_rendered_ads', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('ctv_usage', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('ctv_wins', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('deletion_request', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('deletion_request_unhandled', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('disclosure_rendered_result', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('discrepancy_events', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('dmp_click', 'us-east-1', tasks_max=6),
    ProdS3SinkConfig('dmp_data_export', 'us-east-1', tasks_max=192, flush_size=20000000, rotate_schedule_interval_ms=120000),
    ProdS3SinkConfig('dmp_privacy_requests', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('dmp_user_segment_info', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('dmp_video_depth', 'us-east-1', tasks_max=6, flush_size=10000000),
    ProdS3SinkConfig('dmp_viewability', 'us-east-1', tasks_max=6),
    ProdS3SinkConfig('downstream_bid_feedback_v1', 'us-east-1', tasks_max=4),
    ProdS3SinkConfig('drawbridge', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('dsp_cookie_observation', 'us-east-1', tasks_max=12),
    ProdS3SinkConfig('dynamic_data', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('element_viewability', 'us-east-1', tasks_max=4),
    ProdS3SinkConfig('engagement_events', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('estimates', 'us-east-1', tasks_max=84),
    ProdS3SinkConfig('estimates_v2', 'us-east-1', tasks_max=24),
    ProdS3SinkConfig('event_bus_trans_union_user_ingest', 'us-east-1', tasks_max=12),
    ProdS3SinkConfig('id_bridging_export', 'us-east-1', tasks_max=36),
    ProdS3SinkConfig('impression_segment', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('gpp_data_export', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig(
            'impressions',
            'us-east-1',
            tasks_max=852,
            flush_size=2000000,
            rotate_schedule_interval_ms=60000,
            consumer_config_overrides={
                "consumer.override.fetch.max.bytes": "52428800",
                "consumer.override.fetch.min.bytes": "5242880",
                "consumer.override.max.partition.fetch.bytes": "10485760",
                "consumer.override.max.poll.records": "1000"
            }
    ),
    ProdS3SinkConfig('impressions_pii', 'us-east-1', tasks_max=192, flush_size=30000000),
    ProdS3SinkConfig('instream_video_errors', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('instream_video_metrics_v1', 'us-east-1', tasks_max=63),
    ProdS3SinkConfig('internal_wins', 'us-east-1', tasks_max=56, flush_size=10000000),
    ProdS3SinkConfig('mapped_conversions_v2', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('measurement_impressions', 'us-east-1', tasks_max=21),
    ProdS3SinkConfig('missing_integrated_video_content', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('mouseover_v1', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('openrtb_content', 'us-east-1', tasks_max=63),
    ProdS3SinkConfig('pays', 'us-east-1', tasks_max=16),
    ProdS3SinkConfig('placement_metadata', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('publisher_segment_permission_change', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('privacy_sandbox_auction_wins', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('privacy_sandbox_bid_response', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('privacy_sandbox_impression', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('product_click', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('product_render', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('product_view', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('qr_scan', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('render_error', 'us-east-1', tasks_max=2),
    ProdS3SinkConfig('render_v1', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('rule_experiments', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('segment_fee_usage', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig(
            'segment_seen',
            'us-east-1',
            tasks_max=1026,
            flush_size=2000000,
            rotate_schedule_interval_ms=60000,
            s3_part_size=20971520,
            consumer_config_overrides={
                "consumer.override.fetch.max.bytes": "52428800",
                "consumer.override.fetch.min.bytes": "5242880",
                "consumer.override.max.partition.fetch.bytes": "10485760",
                "consumer.override.max.poll.records": "1000"
            }
    ),
    ProdS3SinkConfig('segment_used', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('segment_publish', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('supplier_contextual_signals', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('supplier_loss_reason', 'us-east-1', tasks_max=1, flush_size=10000000),
    ProdS3SinkConfig('tluid_spoofed', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('tluid_audit', 'us-east-1', tasks_max=4, flush_size=15000000, rotate_schedule_interval_ms=120000),
    ProdS3SinkConfig('tluid_lifecycle_auctions_log', 'us-east-1', tasks_max=4),
    ProdS3SinkConfig('tluid_lifecycle_syncs_log', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('total_capture', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('tracking_pixel', 'us-east-1', tasks_max=4),
    ProdS3SinkConfig('unmapped_supply_requests', 'us-east-1', tasks_max=3),
    ProdS3SinkConfig('user_auction_info', 'us-east-1', tasks_max=8),
    ProdS3SinkConfig('video_depth_v1', 'us-east-1', tasks_max=4, flush_size=10000000),
    ProdS3SinkConfig("video_metrics_v1", 'us-east-1', tasks_max=96, flush_size=10000000, rotate_schedule_interval_ms=120000),
    ProdS3SinkConfig('viewability_2sec_50pct', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('viewability_3p_v1', 'us-east-1', tasks_max=4),
    ProdS3SinkConfig('viewability_50pct', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('viewability_onepx_v1', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('viewability_pixel_3p_measurable_v1', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('viewability_pixel_3p_mrc_v1', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('viewability_pixel_3p_v1_group_m_v1', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('viewability_v1', 'us-east-1', tasks_max=1),
    ProdS3SinkConfig('wins', 'us-east-1', tasks_max=8, flush_size=10000000)
]

eu_central_1_prod_s3_sink_configs = [
    ProdS3SinkConfig('ad_on_page', 'eu-central-1', tasks_max=21),
    ProdS3SinkConfig('assertive_yield_log', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('asset_cardinality_info_v1', 'eu-central-1', tasks_max=16),
    ProdS3SinkConfig('auction_bid_request_v1', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('auction_bid_response_info_v1', 'eu-central-1', tasks_max=84, flush_size=10000000),
    ProdS3SinkConfig('auction_deal_info_v1', 'eu-central-1', tasks_max=21),
    ProdS3SinkConfig('auction_consent_info', 'eu-central-1', tasks_max=3),
    ProdS3SinkConfig('bid_connection_stats', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('bills', 'eu-central-1', tasks_max=63),
    ProdS3SinkConfig('budget_spend_deals', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('budget_spend_windows', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('buyer_impression', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('buyer_impression_adapter', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('buyer_item', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('buyer_transaction', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('click_v1', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('consortia_ids', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('content_dial_page_ping', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('content_dial_page_view', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('creative_info', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('creative_quality', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('creative_scan', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('cta_rendered_result', 'eu-central-1', tasks_max=21),
    ProdS3SinkConfig('ctv_bills', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('ctv_impressions', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('ctv_rendered_ads', 'eu-central-1', tasks_max=10),
    ProdS3SinkConfig('ctv_usage', 'eu-central-1', tasks_max=10),
    ProdS3SinkConfig('ctv_wins', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('disclosure_rendered_result', 'eu-central-1', tasks_max=21),
    ProdS3SinkConfig('discrepancy_events', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('dmp_click', 'eu-central-1', tasks_max=6),
    ProdS3SinkConfig('dmp_data_export', 'eu-central-1', tasks_max=84, flush_size=20000000),
    ProdS3SinkConfig('dmp_privacy_requests', 'eu-central-1', tasks_max=3),
    ProdS3SinkConfig('dmp_user_segment_info', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('dmp_video_depth', 'eu-central-1', tasks_max=6, flush_size=10000000),
    ProdS3SinkConfig('dmp_viewability', 'eu-central-1', tasks_max=6),
    ProdS3SinkConfig('downstream_bid_feedback_v1', 'eu-central-1', tasks_max=63),
    ProdS3SinkConfig('drawbridge', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('dsp_cookie_observation', 'eu-central-1', tasks_max=3),
    ProdS3SinkConfig('dynamic_data', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('element_viewability', 'eu-central-1', tasks_max=16),
    ProdS3SinkConfig('engagement_events', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('estimates', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('estimates_v2', 'eu-central-1', tasks_max=24),
    ProdS3SinkConfig('event_bus_trans_union_user_ingest', 'eu-central-1', tasks_max=6),
    ProdS3SinkConfig('id_bridging_export', 'eu-central-1', tasks_max=6),
    ProdS3SinkConfig('impression_segment', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('gpp_data_export', 'eu-central-1', tasks_max=12),
    ProdS3SinkConfig('impressions', 'eu-central-1', tasks_max=432, flush_size=30000000),
    ProdS3SinkConfig('impressions_pii', 'eu-central-1', tasks_max=144, flush_size=30000000),
    ProdS3SinkConfig('instream_video_errors', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('instream_video_metrics_v1', 'eu-central-1', tasks_max=63),
    ProdS3SinkConfig('internal_wins', 'eu-central-1', tasks_max=168, flush_size=10000000),
    ProdS3SinkConfig('mapped_conversions_v2', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('measurement_impressions', 'eu-central-1', tasks_max=21),
    ProdS3SinkConfig('missing_integrated_video_content', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('mouseover_v1', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('openrtb_content', 'eu-central-1', tasks_max=63),
    ProdS3SinkConfig('pays', 'eu-central-1', tasks_max=16),
    ProdS3SinkConfig('placement_metadata', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('publisher_segment_permission_change', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('privacy_sandbox_auction_wins', 'eu-central-1', tasks_max=10),
    ProdS3SinkConfig('privacy_sandbox_bid_response', 'eu-central-1', tasks_max=43),
    ProdS3SinkConfig('privacy_sandbox_impression', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('product_click', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('product_render', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('product_view', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('qr_scan', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('render_error', 'eu-central-1', tasks_max=63),
    ProdS3SinkConfig('render_v1', 'eu-central-1', tasks_max=21),
    ProdS3SinkConfig('rule_experiments', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('segment_fee_usage', 'eu-central-1', tasks_max=84),
    ProdS3SinkConfig('segment_seen', 'eu-central-1', tasks_max=144, flush_size=15000000),
    ProdS3SinkConfig('segment_used', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('segment_publish', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('supplier_contextual_signals', 'eu-central-1', tasks_max=20),
    ProdS3SinkConfig('supplier_loss_reason', 'eu-central-1', tasks_max=27, flush_size=10000000),
    ProdS3SinkConfig('tluid_spoofed', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('tluid_audit', 'eu-central-1', tasks_max=1, flush_size=15000000, rotate_schedule_interval_ms=120000),
    ProdS3SinkConfig('tluid_lifecycle_auctions_log', 'eu-central-1', tasks_max=4),
    ProdS3SinkConfig('tluid_lifecycle_syncs_log', 'eu-central-1', tasks_max=1),
    ProdS3SinkConfig('total_capture', 'eu-central-1', tasks_max=3),
    ProdS3SinkConfig('tracking_pixel', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('unmapped_supply_requests', 'eu-central-1', tasks_max=3),
    ProdS3SinkConfig('user_auction_info', 'eu-central-1', tasks_max=64),
    ProdS3SinkConfig('video_depth_v1', 'eu-central-1', tasks_max=84, flush_size=10000000),
    ProdS3SinkConfig("video_metrics_v1", 'eu-central-1', tasks_max=96),
    ProdS3SinkConfig('viewability_2sec_50pct', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('viewability_3p_v1', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('viewability_50pct', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('viewability_onepx_v1', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('viewability_pixel_3p_measurable_v1', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('viewability_pixel_3p_mrc_v1', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('viewability_pixel_3p_v1_group_m_v1', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('viewability_v1', 'eu-central-1', tasks_max=8),
    ProdS3SinkConfig('wins', 'eu-central-1', tasks_max=63, flush_size=10000000)
]

prod_opensearch_sink_configs = [
    ProdOpensearchSinkConfig('deal_audit', 'us-east-1'),
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', choices=['prod', 'staging','dev'], required=True, help='the environment')
    parser.add_argument('--dryrun', help='preview potential updates', action='store_true')
    parser.add_argument('--delete', action='store_true', help='delete existing connectors')
    parser.add_argument('--pause', action='store_true', help='pause existing connectors')
    parser.add_argument('--stop', action='store_true', help='stop existing connectors')
    parser.add_argument('--resume', action='store_true', help='resume existing connectors')
    parser.add_argument('--debug', help='log debug info')
    parser.add_argument('--skip-vault', help='Skips fetching of the secrets from vault and looks for OS environment variables', action='store_true')
    parser.add_argument('--aws-auth-vault-mount-point', help='Vault mount point', default='ghrunners')
    parser.add_argument('--prefix', help='prefix of connector names that will be affected')
    parser.add_argument("--topics", help='List of topics names separated by comma to deploy or delete only those topics. If a list is passed, rest of the topics will not be deployed')
    parser.add_argument("--region", choices=['us-east-1', 'eu-central-1', 'all'], default= 'all', help='Region to deploy the connectors. Option only valid for prod env')
    parser.add_argument('--opensearch', help='Deploy OpenSearch Connectors', action='store_true')
    args = parser.parse_args()


    connector_sink_configs = {}
    if args.env == 'prod':
        vault_url = "https://vault-production.triplelift.net"
        if args.region == 'all':
            if args.opensearch:
                connector_sink_configs['us-east-1'] = get_prod_opensearch_sink_configs(prod_opensearch_sink_configs)
            else:
                connector_sink_configs['us-east-1'] = get_prod_s3_sink_configs(us_east_1_prod_s3_sink_configs)
                connector_sink_configs['eu-central-1'] = get_prod_s3_sink_configs(eu_central_1_prod_s3_sink_configs)
        elif args.region == 'us-east-1':
            connector_sink_configs['us-east-1'] = get_prod_s3_sink_configs(us_east_1_prod_s3_sink_configs)
        elif args.region == 'eu-central-1':
            connector_sink_configs['eu-central-1'] = get_prod_s3_sink_configs(eu_central_1_prod_s3_sink_configs)
    elif args.env == 'dev' or args.env == 'staging':
        vault_url = "https://vault-development.triplelift.net"
        if args.opensearch:
            connector_sink_configs['us-east-1'] = get_dev_opensearch_sink_configs(prod_opensearch_sink_configs)
        else:
            connector_sink_configs['us-east-1'] = get_dev_s3_sink_configs(us_east_1_prod_s3_sink_configs)


    if args.dryrun:
        print('=' * 80)
        print('Dryrun')
        print('=' * 80)

    try :
        if args.skip_vault:
            username = os.environ['REDPANDA_CONNECT_SERVER_USER']
            password = os.environ['REDPANDA_CONNECT_SERVER_PASSWORD']
        else:
            session = boto3.session.Session()
            stsClient = session.client('sts')
            print('Detected AWS Profile: ', json.dumps(stsClient.get_caller_identity()))

            awsCredentials = session.get_credentials()

            vaultClient = hvac.Client(url=vault_url)
            vaultClient.auth.aws.iam_login(
                role='ghrunners',
                access_key=awsCredentials.access_key,
                secret_key=awsCredentials.secret_key,
                session_token=awsCredentials.token,
                mount_point='ghrunners',
                use_token=True
        )
            redpand_connect_server_credentials = vaultClient.secrets.kv.v2.read_secret('infra/redpanda-connect/cluster')
            username = redpand_connect_server_credentials["CONNECT_BASIC_AUTH_USERNAME"]
            password = redpand_connect_server_credentials["CONNECT_BASIC_AUTH_PASSWORD"]
    except:
        raise("REDPANDA_CONNECT_SERVER_USER and REDPANDA_CONNECT_SERVER_PASSWORD environment variables not found. Assign redpanda connect server username and password as environment variables")

    usEast1Client = RedpandaConnectClient(args.env, 'us-east-1', args.dryrun, args.prefix, username, password)
    euCentral1Client = RedpandaConnectClient(args.env, 'eu-central-1', args.dryrun, args.prefix, username, password)

    if args.debug is not None:
        if args.region == 'all':
            debug(usEast1Client, args.debug)
            debug(euCentral1Client, args.debug)
        elif args.region == 'us-east-1':
            debug(usEast1Client, args.debug)
        elif args.region == 'eu-central-1':
            debug(euCentral1Client, args.debug)
        exit()

    if args.delete:
        if args.topics:
            if args.region == 'all':
                usEast1Client.delete_connectors(args.topics.split(','))
                euCentral1Client.delete_connectors(args.topics.split(','))
            elif args.region == 'us-east-1':
                usEast1Client.delete_connectors(args.topics.split(','))
            elif args.region == 'eu-central-1':
                euCentral1Client.delete_connectors(args.topics.split(','))
        else:
            if args.region == 'all':
                delete_connectors(usEast1Client)
                delete_connectors(euCentral1Client)
            elif args.region == 'us-east-1':
                delete_connectors(usEast1Client)
            elif 'eu-central-1':
                delete_connectors(euCentral1Client)
        exit()

    if args.pause:
        topics_list = args.topics.split(',') if args.topics else None
        
        for region, sink_configs in connector_sink_configs.items():
            print('-' * 80)
            print('Pausing {} Region Connectors.'.format(region))
            print('-' * 80)
            print()
            
            if topics_list:
                print('Only pausing the following topics: {}'.format(args.topics))
            else:
                print('Pausing all connectors.')
            print()
            
            connectors_configs = get_sorted_connector_configs(sink_configs)
            
            if region == 'us-east-1':
                pause_connectors(usEast1Client, connectors_configs, topics_list)
            elif region == 'eu-central-1':
                pause_connectors(euCentral1Client, connectors_configs, topics_list)
        exit()

    if args.stop:
        topics_list = args.topics.split(',') if args.topics else None
        
        for region, sink_configs in connector_sink_configs.items():
            print('-' * 80)
            print('Stopping {} Region Connectors.'.format(region))
            print('-' * 80)
            print()
            
            if topics_list:
                print('Only stopping the following topics: {}'.format(args.topics))
            else:
                print('Stopping all connectors.')
            print()
            
            connectors_configs = get_sorted_connector_configs(sink_configs)
            
            if region == 'us-east-1':
                stop_connectors(usEast1Client, connectors_configs, topics_list)
            elif region == 'eu-central-1':
                stop_connectors(euCentral1Client, connectors_configs, topics_list)
        exit()

    if args.resume:
        topics_list = args.topics.split(',') if args.topics else None
        
        for region, sink_configs in connector_sink_configs.items():
            print('-' * 80)
            print('Resuming {} Region Connectors.'.format(region))
            print('-' * 80)
            print()
            
            if topics_list:
                print('Only resuming the following topics: {}'.format(args.topics))
            else:
                print('Resuming all connectors.')
            print()
            
            connectors_configs = get_sorted_connector_configs(sink_configs)
            
            if region == 'us-east-1':
                resume_connectors(usEast1Client, connectors_configs, topics_list)
            elif region == 'eu-central-1':
                resume_connectors(euCentral1Client, connectors_configs, topics_list)
        exit()


    for region, sink_configs in connector_sink_configs.items():
        print('-' * 80)
        print('Updating {} Region Connectors.'.format(region))
        print('-' * 80)
        print()
        connectors_configs = get_sorted_connector_configs(sink_configs)
        
        # Display the order of creation (top 10 connectors by tasks.max)
        print('Connectors will be created/updated in the following order (by tasks.max):')
        for i, config in enumerate(connectors_configs[:10]):
            print('  {}. {} (tasks.max={})'.format(i+1, config.get_connector_name(), config.tasks_max))
        if len(connectors_configs) > 10:
            print('  ... and {} more connectors'.format(len(connectors_configs) - 10))
        print()
        
        if args.topics:
            print('Only updating the following topics: {}'.format(args.topics))
            for connector_config in connectors_configs:
                if connector_config.topic in args.topics:
                    if region == 'us-east-1':
                        process_connector_config(usEast1Client, connector_config, wait_for_running=True)
                    elif region == 'eu-central-1':
                        process_connector_config(euCentral1Client, connector_config, wait_for_running=True)
        else:
            print('-' * 80)
            print('Updating all topics.')
            print('-' * 80)
            print()
            for connector_config in connectors_configs:
                if region == 'us-east-1':
                    process_connector_config(usEast1Client, connector_config, wait_for_running=True)
                elif region == 'eu-central-1':
                    process_connector_config(euCentral1Client, connector_config, wait_for_running=True)


if __name__ == '__main__':
    main()
