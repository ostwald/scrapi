"""
Harvester for the CU Scholar University of Colorado Boulder for the SHARE project

Example API call: http://scholar.colorado.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2015-04-22T00:00:00Z&to=2015-04-23
"""

from __future__ import unicode_literals

import sys

from scrapi.base import OAIHarvester, helpers
from datetime import date, timedelta
from furl import furl
from lxml import etree

from scrapi import util
from scrapi.base.helpers import updated_schema, build_properties, single_result
from scrapi.linter.document import RawDocument

class Opensky_2Harvester (OAIHarvester):
	"""
	harvest oai_dc format from opensky_v2
	"""
	short_name = 'opensky_v2'
	long_name = 'Opensky Institution Repository for NCAR'
	url = 'http://opensky.library.ucar.edu/'
	timezone_granularity = True
	
	# no effect
	##META_PREFIX_DATE = '&metadataPrefix=osm&from={}&until={}'	
	
	base_url = 'http://osstage2.ucar.edu:8080/fedora/oai'
	
	@property
	def schema(self):
		desc = 'A collection of RelatedIdentifiers as specified by DataCite.'
		uri = 'https://schema.datacite.org/meta/kernel-3/index.html'
		return helpers.updated_schema(self._schema, {
			"otherProperties": build_properties(
				('relatedIdentifiers', relatedIdentifers, 
					{'description' : desc, 'uri':uri}
				)
			)
		})
		
def relatedIdentifers(record):
	"""
	use title and format to demonstrate how we can pluck values from record
	"""
	title = None
	format = None
	try:
		
		title_els = record.xpath('//dc:title', namespaces = {'dc': 'http://purl.org/dc/elements/1.1/'})
		if len(title_els):
			title = title_els[0].text
		
		format_els = record.xpath('//dc:format', namespaces = {'dc': 'http://purl.org/dc/elements/1.1/'})
		if len(format_els):
			format = format_els[0].text
		
		
	except Exception, msg:
		print 'ERROR: %s' % msg 
		
	return {
		'relatedIdentifierType' : 'DOI',
		'relatedIdentifier' : title,
		'relationType' : format
	}
