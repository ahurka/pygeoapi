
import logging
from elasticsearch import Elasticsearch

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


LOGGER = logging.getLogger(__name__)

PROCESS_SETTINGS = {
    'id': 'woudc-data-registry-metrics',
    'title': 'WOUDC Data Registry Metrics Provider',
    'description': 'An extension of the WOUDC Data Registry search index,' \
                   ' providing an API for metrics queries that assess' \
                   ' file submission and/or usage statistics.',
    'keywords': [],
    'links': [],
    'inputs': [
        {
            'id': 'domain',
            'title': 'Metric Domain',
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': False,
                    'options': [
                        'dataset',
                        'contributor',
                    ]
                }
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        {
            'id': 'timescale',
            'title': 'Time Scale',
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': False,
                    'options': [
                        'year',
                        'month',
                    ]
                }
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        {
            'id': 'dataset',
            'title': 'Dataset Filter',
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True,
                }
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        {
            'id': 'country',
            'title': 'Country Filter',
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True,
                }
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        {
            'id': 'station',
            'title': 'Station Filter',
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True,
                }
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        {
            'id': 'network',
            'title': 'Instrument Filter',
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True,
                }
            },
            'minOccurs': 0,
            'maxOccurs': 1
        }
    ],
    'outputs': [{
        'id': 'woudc-data-registry-metrics-response',
        'title': 'WOUDC Data Registry Metrics Output',
        'output': {
            'formats': [{
                'mimeType': 'application/json'
            }]
        }
    }],
    'example': {
        'inputs': [
            {
                'id': 'domain',
                'type': 'text/plain',
                'value': 'dataset'
            },
            {
                'id': 'timescale',
                'type': 'text/plain',
                'value': 'year'
            },
            {
                'id': 'network',
                'type': 'text/plain',
                'value': 'Brewer'
            },
            {
                'id': 'country',
                'type': 'text/plain',
                'value': 'CAN'
            }
        ]
    }
}


class MetricsProcessor(BaseProcessor):
    """
    WOUDC Data Registry metrics API extension.

    Provides an endpoint for a number of metrics queries, which
    return statistics about the contents or usage of the WOUDC data registry.

    Different kinds of metrics are distinguished by their "domain", which
    acts like a string identifier passed to this process as an input.
    This way, multiple types of queries can be served from one endpoint.

    The metrics query system takes the place of the metrics tables in
    WOUDC BPS's web-db database.
    """

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        """

        BaseProcessor.__init__(self, provider_def, PROCESS_SETTINGS)

        url_tokens = provider_def['elastic_path'].split('/')

        LOGGER.debug('Setting Elasticsearch properties')
        self.index = url_tokens[-1]
        host = url_tokens[2]

        LOGGER.debug('Host: {}'.format(host))
        LOGGER.debug('Index name: {}'.format(self.index))

        LOGGER.debug('Connecting to Elasticsearch')
        self.es = Elasticsearch(host)

        if not self.es.ping():
            msg = 'Cannot connect to Elasticsearch'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        LOGGER.debug('Checking Elasticsearch version')
        version = float(self.es.info()['version']['number'][:3])
        if version < 7:
            msg = 'Elasticsearch version below 7 not supported ({})' \
                  .format(version)
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

    def execute(self, inputs):
        domain = inputs['domain']
        timescale = inputs['timescale']

        if domain == 'dataset':
            country = inputs.get('country', None)
            station = inputs.get('station', None)
            network = inputs.get('network', None)

            return self.metrics_dataset(timescale, country, station, network)
        elif domain == 'contributor':
            dataset = inputs.get('dataset', None)
            station = inputs.get('station', None)
            network = inputs.get('network', None)

            return self.metrics_contributor(timescale, dataset, station,
                                            network)

    def metrics_dataset(self, timescale, country=None, station=None,
                        network=None):
        """
        Returns submission metrics from the WOUDC Data Registry, describing
        number of files and observations submitted for each dataset over
        periods of time.

        Optionally filters for matching value of country, station, and network,
        if specified.

        :param timescale: Either 'year' or 'month', describing time range size.
        :param country: Optional country code to filter by.
        :param station: Optional station ID to filter by.
        :param network: Optional instrument name to filter by. 
        """

        return {}

    def metrics_contributor(self, timescale, dataset=None, station=None,
                            network=None):
        """
        Returns submission metrics from the WOUDC Data Registry, describing
        number of files and observations submitted from each agency over
        periods of time.

        Optionally filters for matching value of dataset, station, and network,
        if specified.

        :param timescale: Either 'year' or 'month', describing time range size.
        :param country: Optional dataset name to filter by.
        :param station: Optional station ID to filter by.
        :param network: Optional instrument name to filter by. 
        """

        return {}

    def __repr__(self):
        return '<MetricsProcessor> {}'.format(self.name)
