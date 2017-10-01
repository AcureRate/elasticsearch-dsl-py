# -*- coding: utf-8 -*-

import os

from elasticsearch.helpers.test import SkipTest
# from elasticsearch.helpers import bulk
from elasticsearch_dsl.helpers import bulk
from elasticsearch_async import AsyncElasticsearch
from elasticsearch import Elasticsearch

from pytest import fixture, yield_fixture, skip, mark
from async_mock import MagicMock
from .test_integration.test_data import DATA, create_git_index


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


_client_loaded = False


# the following method was async.
# meaning that calling it:   x= get_test_client(...)  returns a coroutine that should be awaited.
#

async def get_test_client(nowait=False, **kwargs):
    from time import sleep

    # construct kwargs from the environment
    kw = {'timeout': 30}
    if 'TEST_ES_CONNECTION' in os.environ:
        from elasticsearch_async import connection

        kw['connection_class'] = getattr(connection, os.environ['TEST_ES_CONNECTION'])

    kw.update(kwargs)
    client = AsyncElasticsearch([os.environ.get('TEST_ES_SERVER', {})], **kw)

    # wait for yellow status
    for _ in range(1 if nowait else 100):
        try:
            await client.cluster.health(wait_for_status='yellow')
            return client
        except ConnectionError:
            sleep(.1)
    else:
        # timeout
        raise SkipTest("Elasticsearch failed to start.")


@yield_fixture(scope='session')
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@fixture(scope='session')
async def client(request):
    # inner import to avoid throwing off coverage
    from elasticsearch_dsl.connections import connections
    # hack to workaround pytest not caching skip on fixtures (#467)
    global _client_loaded
    if _client_loaded:
        skip()

    _client_loaded = True
    try:
        client = await get_test_client(nowait='WAIT_FOR_ES' not in os.environ)
        connections.add_connection('default', client)
        return client
    except SkipTest:
        skip()


@yield_fixture
async def write_client(request, client):
    yield client
    await client.indices.delete('test-*', ignore=404)
    await client.indices.delete_template('test-template', ignore=404)


@yield_fixture
def mock_client(request):
    # inner import to avoid throwing off coverage
    from elasticsearch_dsl.connections import connections
    client = AsyncMock()
    client.search.return_value = dummy_response()
    connections.add_connection('mock', client)
    yield client
    connections._conn = {}
    connections._kwargs = {}


@fixture(scope='session')
async def data_client(request, client, event_loop):
    # create mappings
    await create_git_index(client, 'git')
    # load data
    await bulk(client, DATA, raise_on_error=True, refresh=True)

    # make sure we clean up after ourselves
    def finalizer():
        async def afin():
            await client.indices.delete('git')

        event_loop.run_until_complete(afin())

    request.addfinalizer(finalizer)
    return client


@fixture
def dummy_response():
    return {
        "_shards": {
            "failed": 0,
            "successful": 10,
            "total": 10
        },
        "hits": {
            "hits": [
                {
                    "_index": "test-index",
                    "_type": "company",
                    "_id": "elasticsearch",
                    "_score": 12.0,

                    "_source": {
                        "city": "Amsterdam",
                        "name": "Elasticsearch",
                    },
                },
                {
                    "_index": "test-index",
                    "_type": "employee",
                    "_id": "42",
                    "_score": 11.123,
                    "_parent": "elasticsearch",

                    "_source": {
                        "name": {
                            "first": "Shay",
                            "last": "Bannon"
                        },
                        "lang": "java",
                        "twitter": "kimchy",
                    },
                },
                {
                    "_index": "test-index",
                    "_type": "employee",
                    "_id": "47",
                    "_score": 1,
                    "_parent": "elasticsearch",

                    "_source": {
                        "name": {
                            "first": "Honza",
                            "last": "Král"
                        },
                        "lang": "python",
                        "twitter": "honzakral",
                    },
                },
                {
                    "_index": "test-index",
                    "_type": "employee",
                    "_id": "53",
                    "_score": 16.0,
                    "_parent": "elasticsearch",
                },
            ],
            "max_score": 12.0,
            "total": 123
        },
        "timed_out": False,
        "took": 123
    }


@fixture
def aggs_search():
    from elasticsearch_dsl import Search
    s = Search(index='git', doc_type='commits')
    s.aggs \
        .bucket('popular_files', 'terms', field='files', size=2) \
        .metric('line_stats', 'stats', field='stats.lines') \
        .metric('top_commits', 'top_hits', size=2, _source=["stats.*", "committed_date"])
    s.aggs.bucket('per_month', 'date_histogram', interval='month', field='info.committed_date')
    s.aggs.metric('sum_lines', 'sum', field='stats.lines')
    return s


@fixture
def aggs_data():
    return {
        'took': 4,
        'timed_out': False,
        '_shards': {'total': 1, 'successful': 1, 'failed': 0},
        'hits': {'total': 52, 'hits': [], 'max_score': 0.0},
        'aggregations': {
            'sum_lines': {'value': 25052.0},
            'per_month': {
                'buckets': [
                    {'doc_count': 38, 'key': 1393632000000, 'key_as_string': '2014-03-01T00:00:00.000Z'},
                    {'doc_count': 11, 'key': 1396310400000, 'key_as_string': '2014-04-01T00:00:00.000Z'},
                    {'doc_count': 3, 'key': 1398902400000, 'key_as_string': '2014-05-01T00:00:00.000Z'},
                ]
            },
            'popular_files': {
                'buckets': [
                    {
                        'key': 'elasticsearch_dsl',
                        'line_stats': {'count': 40, 'max': 228.0, 'min': 2.0, 'sum': 2151.0, 'avg': 53.775},
                        'doc_count': 40,
                        'top_commits': {
                            'hits': {
                                'total': 40,
                                'hits': [
                                    {
                                        '_id': '3ca6e1e73a071a705b4babd2f581c91a2a3e5037',
                                        '_type': 'commits',
                                        '_source': {
                                            'stats': {'files': 4, 'deletions': 7, 'lines': 30, 'insertions': 23},
                                            'committed_date': '2014-05-02T13:47:19'
                                        },
                                        '_score': 1.0,
                                        '_parent': 'elasticsearch-dsl-py',
                                        '_routing': 'elasticsearch-dsl-py',
                                        '_index': 'git'
                                    },
                                    {
                                        '_id': 'eb3e543323f189fd7b698e66295427204fff5755',
                                        '_type': 'commits',
                                        '_source': {
                                            'stats': {'files': 1, 'deletions': 0, 'lines': 18, 'insertions': 18},
                                            'committed_date': '2014-05-01T13:32:14'
                                        },
                                        '_score': 1.0,
                                        '_parent': 'elasticsearch-dsl-py',
                                        '_routing': 'elasticsearch-dsl-py',
                                        '_index': 'git'
                                    }
                                ],
                                'max_score': 1.0
                            }
                        }
                    },
                    {
                        'key': 'test_elasticsearch_dsl',
                        'line_stats': {'count': 35, 'max': 228.0, 'min': 2.0, 'sum': 1939.0, 'avg': 55.4},
                        'doc_count': 35,
                        'top_commits': {
                            'hits': {
                                'total': 35,
                                'hits': [
                                    {
                                        '_id': '3ca6e1e73a071a705b4babd2f581c91a2a3e5037',
                                        '_type': 'commits',
                                        '_source': {
                                            'stats': {'files': 4, 'deletions': 7, 'lines': 30, 'insertions': 23},
                                            'committed_date': '2014-05-02T13:47:19'
                                        },
                                        '_score': 1.0,
                                        '_parent': 'elasticsearch-dsl-py',
                                        '_routing': 'elasticsearch-dsl-py',
                                        '_index': 'git'
                                    }, {
                                        '_id': 'dd15b6ba17dd9ba16363a51f85b31f66f1fb1157',
                                        '_type': 'commits',
                                        '_source': {
                                            'stats': {'files': 3, 'deletions': 18, 'lines': 62, 'insertions': 44},
                                            'committed_date': '2014-05-01T13:30:44'
                                        },
                                        '_score': 1.0,
                                        '_parent': 'elasticsearch-dsl-py',
                                        '_routing': 'elasticsearch-dsl-py',
                                        '_index': 'git'
                                    }
                                ],
                                'max_score': 1.0
                            }
                        }
                    }
                ],
                'doc_count_error_upper_bound': 0,
                'sum_other_doc_count': 120
            }
        }
    }
