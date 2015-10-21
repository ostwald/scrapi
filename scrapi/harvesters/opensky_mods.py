"""
Harvester for OpenSky v2 (http://opensky.ucar.edu/oai2)

- grabs records in "osm" framework (http://nldr.library.ucar.edu/metadata/osm/1.1/schemas/osm.xsd)
- extracts info to populate customized SHARE schema

Example API call: http://nldr.library.ucar.edu/dds/services/oai2-0?verb=ListRecords&metadataPrefix=osm&from=2015-10-06T00:00:00Z&until=2015-10-09T00:00:00Z

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
	property_listOFF = [
		'type', 'date', 'setSpec',
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
			
			# use scrapi.base.helpers.build_properties  with relatedIdentifiers function
			# - pass description and uri values into build_properties to describe the
			#   relatedIdentifier property properties (they describe the property itself)
			'otherProperties': build_properties(
				('relatedIdentifiers', relatedIdentifers, 
					{'description' : xlink_desc, 'uri':xlink_uri}
				)
			),
			
			# 'publisher': build_properties (
				 # ('name', publisher)
			 # )
			
		})
	
	def harvest(self, start_date=None, end_date=None):
		"""
		Override OAIHarvester.harvest for two reasons
		
		1 - need to get metadata in osm format.
			thought i could simply override META_PREFIX_DATE (see above) but
			apparently this var is never used!
			
			instead we use: 
				url.args['metadataPrefix'] = 'osm'
		
		2 - I wanted to understand how to use a custom date range (so I know we'll get something from
			the provider). 
		

		NOTE: if harvester is using oai_dc it is probably not necessary to override harvest()!
		This was done mainly as a learning exercise
		
		"""
		
		# start_date = (start_date or date.today() - timedelta(settings.DAYS_BACK)).isoformat()
		
		start_date = (date.today() - timedelta(3)).isoformat()
		end_date = (end_date or date.today()).isoformat()

		if self.timezone_granularity:
			start_date += 'T00:00:00Z'
			end_date += 'T00:00:00Z'

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

def getXpath (base_el, xpath, namespaces=None):
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

	
	NOTE: see schema (https://osf.io/wur56/wiki/Schema/) - I "think" we are producing the right json
	structure for "person" but mostly this is just a DEMO of how to get info from provided metadata
	and create a json structure
	"""
	
	role = getXpath(name_el, "mods:role/mods:roleTerm[@type='text']")
	last = getXpath(name_el, "mods:namePart[@type='family']")
	first = getXpath(name_el, "mods:namePart[@type='given']")
	name = getXpath(name_el, "mods:displayForm")
	affiliation = getXpath(name_el, "mods:affiliation")
			
	if 0:
		print etree.tostring(name_el);
		print ' - first: %s' % first
		print ' - last: %s' % last
		print ' - name: %s' % name
		print ' - role: %s' % role
		print ' - affiliation: %s\n' % affiliation
			
			
	return {
		# 'name': name,
		'name': '%s, %s' % (last, first),
		'givenName': first,
		'familyName': last,
		'role': role,
		'affiliation':affiliation
	}

def relatedIdentifers(record):
	"""
	DEMO of how we would pluck the following attributes from metadate record for
	the "relatedIdentifiers" property
	
	- relationIdentifier
	- relationIdentifierType (e.g., DOI)
	- relationType (e.g., hasDataset - but something in datacite vocab?)
	
	and put them in the return object.
	NOTE: the required metadata for relatedIdentifiers is not yet in osm records,
	so I am looking for any old "doi" identifier instead ...
	"""

	
	relatedIdentifier_xpath = '//mods:mods/mods:identifier[@type="doi"]'
	try:
		relatedIdentifier = getXpath(record, relatedIdentifier_xpath)
		relationType = "Documents" # eventually get this from mods
		relatedIdentifierType = "DOI"
	except Exception, msg:
		print 'ERROR: %s' % msg 
		return {}
		
	if not relatedIdentifier:
		return {}
		
	return {
		'relatedIdentifierType' : relatedIdentifierType,
		'relatedIdentifier' : relatedIdentifier,
		'relationType' : relationType
	}

def publisher (record):
	"""
	'//mods:mods/mods:recordInfo/mods:recordContentSource
	"""
	# return {'name':'fooberry'}
	return "fooberry"

