"""
Harvester for the CU Scholar University of Colorado Boulder for the SHARE project

Example API call: http://scholar.colorado.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2015-04-22T00:00:00Z&to=2015-04-23
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class EAGERTESTHarvester(OAIHarvester):
    short_name = 'eagertest'
    long_name = 'EAGER TEST harvester pulling from NLDR OAI'
    # url = 'http://scholar.colorado.edu'
    url = 'https://nldr.library.ucar.edu'
    timezone_granularity = True

    base_url = 'http://nldr.library.ucar.edu/dds/services/oai2-0/'
    # property_list = [
        # 'type', 'source', 'format',
        # 'date', 'setSpec', 'identifier'
    # ]
