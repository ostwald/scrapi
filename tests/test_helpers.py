import vcr

from scrapi.base import helpers


class TestHelpers(object):

    def test_format_one_tag(self):
        single_tag = ' A single tag '
        single_output = helpers.format_tags(single_tag)
        assert single_output == ['a single tag']
        assert isinstance(single_output, list)

    def test_format_many_tags(self):
        many_tags = [' A', 'Bunch', ' oftags ']
        many_output = helpers.format_tags(many_tags)
        assert set(many_output) == set(['a', 'bunch', 'oftags'])

    def test_format_sep_tags(self):
        sep_tags = ['These, we know', 'should be many']
        sep_output = helpers.format_tags(sep_tags, sep=',')
        assert set(sep_output) == set(['these', 'we know', 'should be many'])

    def test_extract_dois(self):
        identifiers = ['doi: THIS_IS_A_DOI!', 'http://dx.doi.org/andalsothis', 'doi:doi:thistoook']
        valid_dois = helpers.oai_extract_dois(identifiers)
        assert valid_dois == [
            'http://dx.doi.org/THIS_IS_A_DOI!',
            'http://dx.doi.org/andalsothis',
            'http://dx.doi.org/thistoook'
        ]

    def test_oai_extract_url(self):
        identifiers = 'I might be a url but rly I am naaaahhttt'
        extraction_attempt = helpers.oai_extract_url(identifiers)
        extraction_attempt

    def test_process_contributors(self):
        args = ['Stardust Rhodes', 'Golddust Rhodes', 'Dusty Rhodes']
        response = helpers.oai_process_contributors(args)
        assert isinstance(response, list)

    @vcr.use_cassette('tests/vcr/asu.yaml')
    def test_oai_get_records_and_token(self):
        url = 'http://repository.asu.edu/oai-pmh?verb=ListRecords&metadataPrefix=oai_dc&from=2015-03-10&until=2015-03-11'
        force = False
        verify = True
        throttle = 0.5
        namespaces = {
            'dc': 'http://purl.org/dc/elements/1.1/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/',
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
        }
        records, token = helpers.oai_get_records_and_token(url, throttle, force, namespaces, verify)
        assert records
        assert token
        assert len(records) == 50
