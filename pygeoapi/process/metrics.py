
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


def wrap_filter(query, prop, value):
    """
    Returns a copy of the query argumnt, wrapped in an additional
    filter requiring the property named by <prop> have a certain value.

    :param query: An Elasticsearch aggregation query dictionary.
    :param prop: The name of a filterable property.
    :param value: Value for the property that the query will match.
    :returns: Query dictionary wrapping the original query in an extra filter.
    """

    property_to_field = {
        'dataset': 'properties.content_category',
        'country': 'properties.platform_country',
        'station': 'properties.platform_id',
        'network': 'properties.instrument_name'
    }
    field = property_to_field[prop]

    wrapper = {
        'size': 0,
        'aggregations': {
            prop: {
                'filter': {
                    'match': {
                        field: value
                    }
                },
                'aggregations': query['aggregations']
            }
        }
    }

    return wrapper


def unwrap_filter(response, category):
    """
    Strips one layer of aggregations (named by <category>) from
    a ElasticSearch query response, leaving it still in proper ES
    response format.

    :param response: An Elasticsearch aggregation response dictionary.
    :param category: Name of the topmost aggregation in the response.
    :returns: The same response, with one level of aggregation removed.
    """

    unwrapped = response.copy()
    unwrapped['aggregations'] = response['aggregations'][category]

    return unwrapped


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
        domain = inputs.pop('domain')
        timescale = inputs.pop('timescale')

        if domain == 'dataset':
            return self.metrics_dataset(timescale, **inputs)
        elif domain == 'contributor':
            dataset = inputs.get('dataset', None)
            station = inputs.get('station', None)
            network = inputs.get('network', None)

            return self.metrics_contributor(timescale, dataset, station,
                                            network)

    def metrics_dataset(self, timescale, **kwargs):
        """
        Returns submission metrics from the WOUDC Data Registry, describing
        number of files and observations submitted for each dataset over
        periods of time.

        Optionally filters for matching value of country, station, and network,
        if present in kwargs.

        :param timescale: Either 'year' or 'month', describing time range size.
        :param kwargs: Optional property values to filter by.
        """

        if timescale == 'year':
            date_interval = '1y'
            date_format = 'yyyy'
        elif timescale == 'month':
            date_interval = '1M'
            date_format = 'yyyy-MM'
        date_aggregation_name = '{}ly'.format(timescale)

        query_core = {
            date_aggregation_name: {
                'date_histogram': {
                    'field': 'properties.timestamp_date',
                    'calendar_interval': date_interval,
                    'format': date_format
                },
                'aggregations': {
                    'total_obs': {
                        'sum': {
                            'field': 'properties.number_of_observations'
                        }
                    }
                }
            }
        }

        query = {
            'size': 0,
            'aggregations': {
                'total_files': {
                    'terms': {
                        'field': 'properties.content_category.keyword'
                    },
                    'aggregations': {
                        'levels': {
                            'terms': {
                                'field': 'properties.content_level'
                            },
                            'aggregations': query_core
                        }
                    }
                }
            }
        }

        filterables = ['country', 'station', 'network']

        for category in filterables:
            if category in kwargs:
                query = wrap_filter(query, category, kwargs[category])

        response = self.es.search(index=self.index, body=query)

        filterables.reverse()
        for category in filterables:
            if category in kwargs:
                response = unwrap_filter(response, category)

        return response

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
