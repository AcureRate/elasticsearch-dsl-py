from elasticsearch_dsl import DocType, Index, Text, Keyword, Date, analysis, IndexTemplate
import pytest
pytestmark = pytest.mark.asyncio

class Post(DocType):
    title = Text(analyzer=analysis.analyzer('my_analyzer', tokenizer='keyword'))
    published_from = Date()

class User(DocType):
    username = Keyword()
    joined_date = Date()

async def test_index_template_works(write_client):
    it = IndexTemplate('test-template', 'test-*')
    it.doc_type(Post)
    it.doc_type(User)
    it.settings(number_of_replicas=0, number_of_shards=1)
    await it.save()

    i = Index('test-blog')
    await i.create()

    assert {
        'test-blog': {
            'mappings': {
                'post': {
                    'properties': {
                        'title': {'type': 'text', 'analyzer': 'my_analyzer'},
                        'published_from': {'type': 'date'}
                    }
                },
                'user': {
                    'properties': {
                        'username': {'type': 'keyword'},
                        'joined_date': {'type': 'date'}
                    }
                },
            }
        }
    } == await write_client.indices.get_mapping(index='test-blog')


async def test_index_exists(write_client):
    assert await Index('git').exists()
    assert not await Index('not-there').exists()

async def test_index_can_be_created_with_settings_and_mappings(write_client):
    i = Index('test-blog', using=write_client)
    i.doc_type(Post)
    i.doc_type(User)
    i.settings(number_of_replicas=0, number_of_shards=1)
    await i.create()

    assert {
        'test-blog': {
            'mappings': {
                'post': {
                    'properties': {
                        'title': {'type': 'text', 'analyzer': 'my_analyzer'},
                        'published_from': {'type': 'date'}
                    }
                },
                'user': {
                    'properties': {
                        'username': {'type': 'keyword'},
                        'joined_date': {'type': 'date'}
                    }
                },
            }
        }
    } == await write_client.indices.get_mapping(index='test-blog')

    settings = await write_client.indices.get_settings(index='test-blog')
    assert settings['test-blog']['settings']['index']['number_of_replicas'] == '0'
    assert settings['test-blog']['settings']['index']['number_of_shards'] == '1'
    assert settings['test-blog']['settings']['index']['analysis'] == {
        'analyzer': {
            'my_analyzer': {
                'type': 'custom',
                'tokenizer': 'keyword'
            }
        }
    }

async def test_delete(write_client):
    await write_client.indices.create(
        index='test-index',
        body={'settings': {'number_of_replicas': 0, 'number_of_shards': 1}}
    )

    i = Index('test-index', using=write_client)
    await i.delete()
    assert not await write_client.indices.exists(index='test-index')

async def test_multiple_indices_with_same_doc_type_work(write_client):
    i1 = Index('test-index-1', using=write_client)
    i2 = Index('test-index-2', using=write_client)

    for i in (i1, i2):
        i.doc_type(Post)
        i.create()

    for i in ('test-index-1', 'test-index-2'):
        settings = await write_client.indices.get_settings(index=i)
        assert settings[i]['settings']['index']['analysis'] == {
            'analyzer': {
                'my_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'keyword'
                }
            }
        }
