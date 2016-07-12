"""
Harvester for OpenSky v2 (http://opensky.ucar.edu/oai2)

- grabs records in "mods" framework
- extracts info to populate customized SHARE schema

Example API call: http://opensky.ucar.edu/oai2?verb=ListRecords&metadataPrefix=mods&from=2016-07-12T00:00:00Z&until=2016-07-13T00:00:00Z

"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester
from datetime import date, timedelta
from furl import furl
from lxml import etree

from scrapi import util
from scrapi.base.helpers import updated_schema, single_result, compose, build_properties
from nameparser import HumanName

from scrapi.linter.document import RawDocument

class OpenskyHarvester (OAIHarvester):
	short_name = 'opensky'
	long_name = 'Opensky Institutional Repository for UCAR/NCAR'
	url = 'http://opensky.ucar.edu'
	timezone_granularity = True
	metadata_prefix = 'mods'

	namespaces = {
		'ns0': 'http://www.openarchives.org/OAI/2.0/',
		'mods' : "http://www.loc.gov/mods/v3",
		'xlink' : "http://www.w3.org/1999/xlink",
		'dc': 'http://purl.org/dc/elements/1.1/'
	}

	base_url = 'http://opensky.ucar.edu/oai2'

	# these are other properties in schema
	property_list = [
		'type', 'date', 'setSpec'
		# 'source', 'format', 'identifier'
	]

	@property
	def schema(self):
		"""
		Custom function to define schema values in terms of OSM records.
		Required fields (see https://osf.io/wur56/wiki/Schema/) are:
		- title
		- contributors
		- uris
		- providerUpdatedDateTime

		NOTE: contributors
		"""

		xlink_desc = 'A collection of RelatedIdentifiers as specified by DataCite.'
		xlink_uri = 'https://schema.datacite.org/meta/kernel-3/index.html'

		# NOTE, the values of the dict are interpreted by the transformer - see base.transformer.py
		return updated_schema(self._schema, {
			'title': ('//mods:mods/mods:titleInfo/mods:title/node()', lambda x: x[0] if x else ''),
			'description': ('//mods:mods/mods:abstract/node()', lambda x: x[0] if x else ''),
			'setSpec': ('//ns0:setSpec/node()', lambda x: x[0] if x else ''),
			'publisher': {'name' : ('//mods:mods/mods:recordInfo/mods:recordContentSource/node()', lambda x: x[0] if x else '')},
			# scrapi.base.helpers.compose takes a list of functions
			# not sure how/why, but below xpath only selects name_els with a family name:)
			'contributors': ('//mods:mods/mods:name[mods:namePart[@type="family"]]',
				 compose(lambda x: [
					 process_contributor(entry)
						for entry in x
					 ], lambda x: x or [])),
			'uris': {
				'canonicalUri': ('//mods:mods/mods:identifier[@displayLabel="Persistent URI"]/node()', lambda x: x[0])
			},

			# use scrapi.base.helpers.build_properties  with relatedIdentifiers function
			# - pass description and uri values into build_properties to describe the
			#   relatedIdentifier property properties (they describe the property itself)
			'otherProperties': build_properties(
				('relatedIdentifiers', relatedIdentifers,
					{'description' : xlink_desc, 'uri':xlink_uri}
				)
			)
		})

	def harvest(self, start_date=None, end_date=None):
		"""
		Override OAIHarvester.harvest for two reasons

		1 - need to get metadata in osm format.
			thought i could simply override META_PREFIX_DATE (see above) but
			apparently this var is never used!

			instead we use:
				url.args['metadataPrefix'] = 'osm'

		2 - I wanted to understand how to use a custom date range for testing (so I can ensure we'll get something from
			the provider).


		"""

		start_date = (start_date or date.today() - timedelta(settings.DAYS_BACK)).isoformat()

		# start_date = (date.today() - timedelta(3)).isoformat() # DEVEL
		end_date = (end_date or date.today()).isoformat()

		if self.timezone_granularity:
			start_date += 'T00:00:00Z'
			end_date += 'T00:00:00Z'

		# DEVEL - to fetch ONLY cross-link records
		start_date = '2016-07-12T13:47:00Z'
		end_date = '2016-07-12T13:48:00Z'

		url = furl(self.base_url)
		url.args['verb'] = 'ListRecords'
		url.args['metadataPrefix'] = self.metadata_prefix
		url.args['from'] = start_date
		url.args['until'] = end_date

		records = self.get_records(url.url, start_date, end_date)
		# print '%d records harvested' % len(records)

		rawdoc_list = []
		for record in records:
			doc_id = record.xpath(
				'ns0:header/ns0:identifier', namespaces=self.namespaces)[0].text
			record = etree.tostring(record, encoding=self.record_encoding)
			rawdoc_list.append(RawDocument({
				'doc': record,
				'source': util.copy_to_unicode(self.short_name),
				'docID': util.copy_to_unicode(doc_id),
				'filetype': 'xml'
			}))

		return rawdoc_list

def valueOf (base_el, xpath, namespaces=None):
	namespaces = namespaces or OpenskyHarvester.namespaces
	try:
		return base_el.xpath(xpath,
							   namespaces=namespaces)[0].text
	except IndexError, msg:
		# nothing found at path
		return ''

def process_contributor(name_el):
	"""
	populate schema element by plucking info from supplied name_el

	sample name_el:
		<name type="personal">
			<namePart type="given">Kevin</namePart>
			<namePart type="given">E.</namePart>
			<namePart type="family">Trenberth</namePart>
			<displayForm>Kevin E. Trenberth</displayForm>
			<role>
				<roleTerm type="text" authority="marcrelator">author</roleTerm>
				<roleTerm type="code" authority="marcrelator">aut</roleTerm>
			</role>
			<affiliation>University Corporation For Atmospheric Research (UCAR):National Center
				for Atmospheric Research (NCAR):Earth-Sun Systems Laboratory
				(ESSL)</affiliation>
		</name>


	NOTE: see schema (https://osf.io/wur56/wiki/Schema/)
	"""

	role = valueOf(name_el, "mods:role/mods:roleTerm[@type='text']")
	last = valueOf(name_el, "mods:namePart[@type='family']")
	first = valueOf(name_el, "mods:namePart[@type='given']")
	name = valueOf(name_el, "mods:displayForm")
	affiliation = valueOf(name_el, "mods:affiliation")

	if 0:
		print etree.tostring(name_el);
		print ' - first: %s' % first
		print ' - last: %s' % last
		print ' - name: %s' % name
		print ' - role: %s' % role
		print ' - affiliation: %s\n' % affiliation


	return {
		'name': '%s, %s' % (last, first),
		'givenName': first,
		'familyName': last,
		'role': role,
		'affiliation':affiliation
	}


def relatedIdentifers(record):
	"""
	pluck information for cross-links from metadata record for
	the custom "relatedIdentifiers" property

	"""

	relationType = 'UNKOWN'
	relatedIdentifier = 'UNKOWN'
	relatedIdentifierType = 'UNKOWN'

	ns=OpenskyHarvester.namespaces

	relatedItem_xpath = '//mods:mods/mods:relatedItem[@otherTypeAuthURI="http://dx.doi.org/10.5438/0010"]'
	try:
		relatedItem = record.xpath(relatedItem_xpath, namespaces=ns)[0]
		if relatedItem is None:
			raise Exception, 'relatedItem not found'
		relationType = relatedItem.get('otherType')
		title = valueOf(relatedItem, 'mods:titleInfo/mods:title')
		publisher = valueOf(relatedItem, 'mods:originInfo/mods:publisher')
		genre = valueOf(relatedItem, 'mods:genre[@authorityURI="http://dx.doi.org/10.5438/0010"]')

		# try to get a DOI, if we can't get doi look for uri
		identityEls = relatedItem.xpath('mods:identifier', namespaces=ns)
		if identityEls == None or len(identityEls) == 0:
			raise Exception, 'identity element not found'
		identityEl = identityEls[0]
		relatedIdentifierType = identityEl.get('type')
		relatedIdentifier = identityEl.text

	except Exception, msg:
		# maybe not really an error - we just didn't find any relation stuff'
		# print 'WARNING: %s' % msg
		return {}

	return {
		'relatedIdentifierType' : relatedIdentifierType,
		'relatedIdentifier' : relatedIdentifier,
		'relationType' : relationType,
		'title': title,
		'publisher' : publisher,
		'resourceType' : genre
	}

