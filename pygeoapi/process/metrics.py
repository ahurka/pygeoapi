
import logging

from pygeoapi.process.base import BaseProcessor


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

    def execute(self, inputs):
        return {}

    def __repr__(self):
        return '<MetricsProcessor> {}'.format(self.name)
