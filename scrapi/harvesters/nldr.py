"""
Harvester for the CU Scholar University of Colorado Boulder for the SHARE project

Example API call: http://scholar.colorado.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2015-04-22T00:00:00Z&to=2015-04-23
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
	populate schema element by plucking from supplied person_el
	
	NOTE: I have no idea how this element should really look.
	this is just a DEMO 
	"""
	first = last = inst = None
	# print etree.tostring(person_el);
	role = person_el.get('role')
	for child in person_el:
		
		# hide namespace details
		# -- child.tag returns "{http://nldr.library.ucar.edu/metadata/osm}middleName"
		# -- qtag.localname returns "middleName"
		qtag = etree.QName(child)
		# print '  -- %s' % qtag.localname
		
		if qtag.localname == 'firstName':
			first = child.text
		if qtag.localname == 'lastName':
			last = child.text
			
		instName_els = person_el.xpath('//osm:affiliation/osm:instName', 
			namespaces={'osm': 'http://nldr.library.ucar.edu/metadata/osm'})
		if len(instName_els):
			inst = instName_els[0].text
		else:
			print etree.tostring(person_el);
			print 'INSTNAME_NOT FOUND'
			
			
	return {
		'name': '%s, %s' % (last, first),
		'givenName': first,
		'familyName': last,
		'role': role or '',
		'instName': inst or ''
	}

def relatedIdentifers(record):
	"""
	Here we would pluck
	- relationIdentifier
	- relationIdentifierType (e.g., DOI)
	- relationType (e.g., hasDataset - but something in datacite vocab?)
	
	and put them in the return object
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
		"""
		
		xlink_desc = 'A collection of RelatedIdentifiers as specified by DataCite.'
		xlink_uri = 'https://schema.datacite.org/meta/kernel-3/index.html'

		
		return updated_schema(self._schema, {
			'title': ('//ns1:title/node()', lambda x: x[0] if x else ''),
			'description': ('//ns1:abstract/node()', lambda x: x[0] if x else ''),
			'setSpec': ('//ns0:setSpec/node()', lambda x: x[0] if x else ''),
			
			#compose takes a list of functions
			'contributors': ('//ns1:contributors/ns1:person', 
				compose(lambda x: [
					process_contributor(entry) 
						for entry in x
					], lambda x: x or [])),
			
			'otherProperties': build_properties(
				('relatedIdentifiers', relatedIdentifers, 
					{'description' : xlink_desc, 'uri':xlink_uri}
				)
			)
		})

	
	
	def harvest(self, start_date=None, end_date=None):
		"""
		need to get osm format.
		thought i could simply override META_PREFIX_DATE (see above) but
		apparently this var is never used!
		
		So I'm copying the entire harvest method and modifying:
		start_date - one year ago ..
		url.args['metadataPrefix'] = 'osm'
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
	
	def normalizeOFF(self, raw_doc):
		print "normalizing"
		str_result = raw_doc.get('doc')
		result = etree.XML(str_result)
		set_spec = []
		if self.approved_sets:
			set_spec = result.xpath(
				'ns0:header/ns0:setSpec/node()',
				namespaces=self.namespaces
			)
			# check if there's an intersection between the approved sets and the
			# setSpec list provided in the record. If there isn't, don't normalize.
			if not {x.replace('publication:', '') for x in set_spec}.intersection(self.approved_sets):
				logger.info('Series {} not in approved list'.format(set_spec))
				return None
		
		status = result.xpath('ns0:header/@status', namespaces=self.namespaces)
		if status and status[0] == 'deleted':
			logger.info('Deleted record, not normalizing {}'.format(raw_doc['docID']))
			return None
		
		return super(OAIHarvester, self).normalize(raw_doc)

