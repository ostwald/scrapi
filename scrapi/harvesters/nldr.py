"""
Harvester for OpenSky v1 (nldr)

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

def process_contributor(person_el):
	"""
	populate schema element by plucking info from supplied person_el
	
	sample person_el:
		<person role="Author" order="1" UCARid="5838">
		  <lastName>Lu</lastName>
		  <firstName>Gang</firstName>
		  <affiliation>
			<instName>University Corporation for Atmospheric Research</instName>
			<instDivision>University Corporation For Atmospheric Research (UCAR):National Center for Atmospheric Research (NCAR):High Altitude Observatory (HAO)</instDivision>
		  </affiliation>
		</person>	
	
	NOTE: see schema (https://osf.io/wur56/wiki/Schema/) - I "think" we are producing the right json
	structure for "person" but mostly this is just a DEMO of how to get info from provided metadata
	and create a json structure
	"""
	first = last = inst = None
	# print etree.tostring(person_el);
	role = person_el.get('role')
	for child in person_el:
		
		# - One way of getting information from child elenent of person_el (based on tagName of child)
		# hide namespace details
		# -- child.tag returns "{http://nldr.library.ucar.edu/metadata/osm}middleName"
		# -- qtag.localname returns "middleName"
		qtag = etree.QName(child)
		# print '  -- %s' % qtag.localname
		
		if qtag.localname == 'firstName':
			first = child.text
		if qtag.localname == 'lastName':
			last = child.text
			
		# - Another way of getting info from child (using xpath)	
		instName_els = person_el.xpath('//osm:affiliation/osm:instName', 
			namespaces={'osm': 'http://nldr.library.ucar.edu/metadata/osm'})
		if len(instName_els):
			inst = instName_els[0].text
		else:
			# debugging - deterime why instName wasn't found
			print etree.tostring(person_el);
			print 'INSTNAME_NOT FOUND'
			
	# return fields must match schema for "person"
	return {
		'name': '%s, %s' % (last, first),
		'givenName': first,
		'familyName': last,
		'role': role or '',
		'affiliation': inst or ''
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
	so I am just using hardcoded values here ...
	"""
	relatedIdentifier = None
	relationType = None
	try:
		relatedIdentifier = "not computed"
		relationType = "Documents"
		relatedIdentifierType = "DOI"
	except Exception, msg:
		print 'ERROR: %s' % msg 
		
	return {
		'relatedIdentifierType' : relatedIdentifierType,
		'relatedIdentifier' : relatedIdentifier,
		'relationType' : relationType
	}

class OpenskyHarvester (OAIHarvester):
	short_name = 'nldr'
	long_name = 'Opensky Institution Repository for NCAR'
	url = 'http://opensky.library.ucar.edu/'
	timezone_granularity = True
	
	# no effect
	##META_PREFIX_DATE = '&metadataPrefix=osm&from={}&until={}'	
	
	# Override these variable is required
	namespaces = {
		'ns1': 'http://nldr.library.ucar.edu/metadata/osm',
		'ns0': 'http://www.openarchives.org/OAI/2.0/',
		'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
		'dc': 'http://purl.org/dc/elements/1.1/',
	}
	
	base_url = 'http://nldr.library.ucar.edu/dds/services/oai2-0'
	
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

		
		return updated_schema(self._schema, {
			'title': ('//ns1:title/node()', lambda x: x[0] if x else ''),
			'description': ('//ns1:abstract/node()', lambda x: x[0] if x else ''),
			'setSpec': ('//ns0:setSpec/node()', lambda x: x[0] if x else ''),
			
			# scrapi.base.helpers.compose takes a list of functions 
			'contributors': ('//ns1:contributors/ns1:person', 
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
		# url.args['metadataPrefix'] = 'oai_dc'
		url.args['metadataPrefix'] = 'osm'
		url.args['from'] = start_date
		url.args['until'] = end_date

		records = self.get_records(url.url, start_date, end_date)

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
	

