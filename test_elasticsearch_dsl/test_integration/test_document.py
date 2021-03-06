from datetime import datetime
from pytz import timezone

import pytest
pytestmark = pytest.mark.asyncio

from elasticsearch import ConflictError, NotFoundError, RequestError

from elasticsearch_dsl import DocType, Date, Text, Keyword, construct_field, Mapping
from elasticsearch_dsl.utils import AttrList

from pytest import raises

user_field = construct_field('object')
user_field.field('name', 'text', fields={'raw': construct_field('keyword')})

class Repository(DocType):
    owner = user_field
    created_at = Date()
    description = Text(analyzer='snowball')
    tags = Keyword()

    class Meta:
        index = 'git'
        doc_type = 'repos'

class Commit(DocType):
    committed_date = Date()
    authored_date = Date()
    description = Text(analyzer='snowball')

    class Meta:
        index = 'git'
        mapping = Mapping('commits')
        mapping.meta('_parent', type='repos')

async def test_parent_type_is_exposed():
    assert Commit._doc_type.parent == 'repos'
    assert Repository._doc_type.parent is None

async def test_init(write_client):
    await Repository.init(index='test-git')

    assert await write_client.indices.exists_type(index='test-git', doc_type='repos')

async def test_get_raises_404_on_non_existent_id(data_client):
    with raises(NotFoundError):
        await Repository.get('elasticsearch-dsl-php')

async def test_get_returns_none_if_404_ignored(data_client):
    assert None is await Repository.get('elasticsearch-dsl-php', ignore=404)

async def test_get(data_client):
    elasticsearch_repo = await Repository.get('elasticsearch-dsl-py')

    assert isinstance(elasticsearch_repo, Repository)
    assert elasticsearch_repo.owner.name == 'elasticsearch'
    assert datetime(2014, 3, 3) == elasticsearch_repo.created_at

async def test_get_with_tz_date(data_client):
    first_commit = await Commit.get(id='3ca6e1e73a071a705b4babd2f581c91a2a3e5037', parent='elasticsearch-dsl-py')

    tzinfo = timezone('Europe/Prague')
    assert tzinfo.localize(datetime(2014, 5, 2, 13, 47, 19, 123000)) == first_commit.authored_date

async def test_save_with_tz_date(data_client):
    tzinfo = timezone('Europe/Prague')
    first_commit = await Commit.get(id='3ca6e1e73a071a705b4babd2f581c91a2a3e5037', parent='elasticsearch-dsl-py')
    first_commit.committed_date = tzinfo.localize(datetime(2014, 5, 2, 13, 47, 19, 123456))
    await first_commit.save()

    first_commit = await Commit.get(id='3ca6e1e73a071a705b4babd2f581c91a2a3e5037', parent='elasticsearch-dsl-py')
    assert tzinfo.localize(datetime(2014, 5, 2, 13, 47, 19, 123456)) == first_commit.committed_date

COMMIT_DOCS_WITH_MISSING = [
    {'parent': 'elasticsearch-dsl-py', '_id': '0'},                                         # Missing
    {'parent': 'elasticsearch-dsl-py', '_id': '3ca6e1e73a071a705b4babd2f581c91a2a3e5037'},  # Existing
    {'parent': 'elasticsearch-dsl-py', '_id': 'f'},                                         # Missing
    {'parent': 'elasticsearch-dsl-py', '_id': 'eb3e543323f189fd7b698e66295427204fff5755'},  # Existing
]

COMMIT_DOCS_WITH_ERRORS = [
    '0',                                                                                    # Error
    {'parent': 'elasticsearch-dsl-py', '_id': '3ca6e1e73a071a705b4babd2f581c91a2a3e5037'},  # Existing
    'f',                                                                                    # Error
    {'parent': 'elasticsearch-dsl-py', '_id': 'eb3e543323f189fd7b698e66295427204fff5755'},  # Existing
]

async def test_mget(data_client):
    commits = await Commit.mget(COMMIT_DOCS_WITH_MISSING)
    assert commits[0] is None
    assert commits[1]._id == '3ca6e1e73a071a705b4babd2f581c91a2a3e5037'
    assert commits[2] is None
    assert commits[3]._id == 'eb3e543323f189fd7b698e66295427204fff5755'

async def test_mget_raises_exception_when_missing_param_is_invalid(data_client):
    with raises(ValueError):
        await Commit.mget(COMMIT_DOCS_WITH_MISSING, missing='raj')

async def test_mget_raises_404_when_missing_param_is_raise(data_client):
    with raises(NotFoundError):
        await Commit.mget(COMMIT_DOCS_WITH_MISSING, missing='raise')

async def test_mget_ignores_missing_docs_when_missing_param_is_skip(data_client):
    commits = await Commit.mget(COMMIT_DOCS_WITH_MISSING, missing='skip')
    assert commits[0]._id == '3ca6e1e73a071a705b4babd2f581c91a2a3e5037'
    assert commits[1]._id == 'eb3e543323f189fd7b698e66295427204fff5755'

async def test_mget_raises_404_when_error_param_is_true(data_client):
    with raises(RequestError):
        commits = await Commit.mget(COMMIT_DOCS_WITH_ERRORS)

async def test_mget_returns_none_for_error_docs_when_error_param_is_false(data_client):
    commits = await Commit.mget(COMMIT_DOCS_WITH_ERRORS, raise_on_error=False)
    assert commits[0] is None
    assert commits[1]._id == '3ca6e1e73a071a705b4babd2f581c91a2a3e5037'
    assert commits[2] is None
    assert commits[3]._id == 'eb3e543323f189fd7b698e66295427204fff5755'

async def test_mget_error_and_missing_params_together(data_client):
    commits = await Commit.mget(COMMIT_DOCS_WITH_ERRORS, raise_on_error=False, missing='skip')
    assert commits[0]._id == '3ca6e1e73a071a705b4babd2f581c91a2a3e5037'
    assert commits[1]._id == 'eb3e543323f189fd7b698e66295427204fff5755'

async def test_update_works_from_search_response(data_client):
    elasticsearch_repo = (await Repository.search().execute())[0]

    er = await elasticsearch_repo
    await er.update(owner={'other_name': 'elastic'})
    assert 'elastic' == er.owner.other_name

    new_version = await Repository.get('elasticsearch-dsl-py')
    assert 'elastic' == new_version.owner.other_name
    assert 'elasticsearch' == new_version.owner.name

async def test_update(data_client):
    elasticsearch_repo = await Repository.get('elasticsearch-dsl-py')
    v = elasticsearch_repo.meta.version

    await elasticsearch_repo.update(owner={'new_name': 'elastic'}, new_field='testing-update')

    assert 'elastic' == elasticsearch_repo.owner.new_name
    assert 'testing-update' == elasticsearch_repo.new_field

    # assert version has been updated
    assert elasticsearch_repo.meta.version == v + 1

    new_version = await Repository.get('elasticsearch-dsl-py')
    assert 'testing-update' == new_version.new_field
    assert 'elastic' == new_version.owner.new_name
    assert 'elasticsearch' == new_version.owner.name


async def test_save_updates_existing_doc(data_client):
    elasticsearch_repo = await Repository.get('elasticsearch-dsl-py')

    elasticsearch_repo.new_field = 'testing-save'
    v = elasticsearch_repo.meta.version
    assert not await elasticsearch_repo.save()

    # assert version has been updated
    assert elasticsearch_repo.meta.version == v + 1

    new_repo = await data_client.get(index='git', doc_type='repos', id='elasticsearch-dsl-py')
    assert 'testing-save' == new_repo['_source']['new_field']

async def test_save_automatically_uses_versions(data_client):
    elasticsearch_repo = await Repository.get('elasticsearch-dsl-py')
    elasticsearch_repo.meta.version += 1

    with raises(ConflictError):
        await elasticsearch_repo.save()

async def test_can_save_to_different_index(write_client):
    test_repo = Repository(description='testing', meta={'id': 42})
    test_repo.meta.version_type = 'external'
    test_repo.meta.version = 3
    assert await test_repo.save(index='test-document')

    assert {
        'found': True,
        '_index': 'test-document',
        '_type': 'repos',
        '_id': '42',
        '_version': 3,
        '_source': {'description': 'testing'},
    } == await write_client.get(index='test-document', doc_type='repos', id=42)

async def test_delete(write_client):
    await write_client.create(
        index='test-document',
        doc_type='repos',
        id='elasticsearch-dsl-py',
        body={'organization': 'elasticsearch', 'created_at': '2014-03-03', 'owner': {'name': 'elasticsearch'}}
    )

    test_repo = Repository(meta={'id': 'elasticsearch-dsl-py'})
    test_repo.meta.index = 'test-document'
    await test_repo.delete()

    assert not await write_client.exists(
        index='test-document',
        doc_type='repos',
        id='elasticsearch-dsl-py',
    )

async def test_delete_ignores_ttl_and_timestamp_meta(write_client):
    await write_client.create(
        index='test-document',
        doc_type='repos',
        id='elasticsearch-dsl-py',
        body={'organization': 'elasticsearch', 'created_at': '2014-03-03', 'owner': {'name': 'elasticsearch'}},
        ttl='1d',
        timestamp=datetime.now()
    )

    test_repo = Repository(meta={'id': 'elasticsearch-dsl-py'})
    test_repo.meta.index = 'test-document'
    test_repo.meta.ttl = '1d'
    test_repo.meta.timestamp = datetime.now()
    await test_repo.delete()


async def test_search(data_client):
    assert 1 == await Repository.search().count()

async def test_search_returns_proper_doc_classes(data_client):
    result = await Repository.search().execute()

    elasticsearch_repo = await result.hits[0]

    assert isinstance(elasticsearch_repo, Repository)
    assert elasticsearch_repo.owner.name == 'elasticsearch'

async def test_parent_value(data_client):
    s = Commit.search()
    s = s.filter('term', _id='3ca6e1e73a071a705b4babd2f581c91a2a3e5037')
    results = await s.execute()

    commit = await results.hits[0]
    assert 'elasticsearch-dsl-py' == commit.meta.parent
    assert ['eb3e543323f189fd7b698e66295427204fff5755'] == commit.parent_shas


async def test_refresh_mapping(data_client):
    class Commit(DocType):
        class Meta:
            doc_type = 'commits'
            index = 'git'

    await Commit._doc_type.refresh()

    assert 'stats' in Commit._doc_type.mapping
    assert 'committer' in Commit._doc_type.mapping
    assert 'description' in Commit._doc_type.mapping
    assert 'committed_date' in Commit._doc_type.mapping
    assert isinstance(Commit._doc_type.mapping['committed_date'], Date)

async def test_highlight_in_meta(data_client):
    res = await Commit.search().query('match', description='inverting').highlight('description').execute()
    commit = await res[0]

    assert isinstance(commit, Commit)
    assert 'description' in commit.meta.highlight
    assert isinstance(commit.meta.highlight['description'], AttrList)
    assert len(commit.meta.highlight['description']) > 0
