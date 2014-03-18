import requests
from xml.etree import ElementTree
from dateutil import parser
from datetime import datetime
DRYAD_BASE = 'http://datadryad.org'
DRYAD_RESOURCE_BASE = DRYAD_BASE + '/resource'
DRI_SUFFIX = '/DRI'
DRI_XML_PREFIX = '{http://di.tamu.edu/DRI/1.0/}'
METS_XML_PREFIX = '{http://www.loc.gov/METS/}'
DIM_XML_PREFIX = '{http://www.dspace.org/xmlns/dspace/dim}'
XLINK_XML_PREFIX = '{http://www.w3.org/TR/xlink/}'

# XPaths for mets metadata
DIM_PATH = './' + METS_XML_PREFIX + 'dmdSec/' + METS_XML_PREFIX + 'mdWrap/' + METS_XML_PREFIX + 'xmlData/' + DIM_XML_PREFIX + 'dim/'
EMBARGO_PATH = DIM_PATH + DIM_XML_PREFIX + 'field[@mdschema="dc"][@element="date"][@qualifier="embargoedUntil"]'
IDENTIFIER_PATH = DIM_PATH + DIM_XML_PREFIX + 'field[@mdschema="dc"][@element="identifier"]'
FILE_PATH = './' + METS_XML_PREFIX + 'fileSec/' + METS_XML_PREFIX + 'fileGrp/' + METS_XML_PREFIX + 'file'
SOLR_QUERY_URL = DRYAD_BASE + '/solr/search/select/?q=dc.date.embargoedUntil_dt:%5BNOW%20TO%20NOW/DAY%2B10000DAY%5D&rows=1000000'


# TODO: identify ourselves as the DryadEmbargoValidator

class DryadObject(object):
    def __init__(self,doi=None,mets_url_relative=None):
        self.doi = doi
        self.mets_url_relative = mets_url_relative
        self.dri_xml = None
        self.dri_tree = None
        self.mets_xml = None
        self.mets_tree = None
    def html_url(self):
        # only valid if doi
        return DRYAD_RESOURCE_BASE + '/' +  self.doi
    def dri_url(self):
        return DRYAD_RESOURCE_BASE + '/' + self.doi + DRI_SUFFIX
    def load_html(self):
        if self.html is not None:
            return
        # For download links
        r = requests.get(self.html_url())
        self.html = r.text
    def load_dri(self):
        if self.dri_xml is not None:
            return
        r = requests.get(self.dri_url())
        self.dri_xml = r.text
    def parse_dri(self):
        if self.dri_tree is not None:
            return
        # From requests documentation we should use r.content to determine the encoding
        self.dri_tree = ElementTree.fromstring(self.dri_xml.encode('utf-8'))
    def extract_mets_url(self):
        if self.mets_url_relative is not None:
            return
        # document/body/div/referenceSet/reference
        # <reference repositoryID="10255" type="DSpace Item" url="/metadata/handle/10255/dryad.12/mets.xml">
        # self.dri_xml is document
        references = self.dri_tree.findall('./' + DRI_XML_PREFIX + 'body/' + DRI_XML_PREFIX + 'div/' + DRI_XML_PREFIX + 'referenceSet/' + DRI_XML_PREFIX + 'reference' )
        if len(references) == 1:
            self.mets_url_relative = references[0].get('url')
    def load_mets(self):
        if self.mets_xml is not None:
            return
        r = requests.get(DRYAD_BASE + self.mets_url_relative)
        self.mets_xml = r.text
    def parse_mets(self):
        if self.mets_tree is not None:
            return
        self.mets_tree = ElementTree.fromstring(self.mets_xml.encode('utf-8'))
        self.read_doi()
    def read_doi(self):
        if self.doi is not None:
            return
        identifier_fields = self.mets_tree.findall(IDENTIFIER_PATH)
        if len(identifier_fields) > 0:
            doi_fields = [f.text for f in identifier_fields if 'doi' in f.text]
            doi_fields = list(set(doi_fields)) #uniquify
            if len(doi_fields) == 1:
                self.doi = doi_fields[0]
            else:
                raise ValueError("More than 1 doi found in mets %s" % doi_fields)

class DataFile(DryadObject):
    # Files have mets but probably no DRI
    def __init__(self,**kwargs):
        DryadObject.__init__(self,**kwargs)
        self.embargoed_until_dates = None
        self.bitstream_links = None
    def read(self):
        self.load_mets()
        self.parse_mets()
    def read_embargoed_until_dates(self):
        if self.embargoed_until_dates is not None:
            return
        # only from the mets
        # <dim:field element="date" qualifier="embargoedUntil" mdschema="dc">2014-09-25</dim:field>
        embargo_fields = self.mets_tree.findall(EMBARGO_PATH)
        if len(embargo_fields) > 0:
            self.embargoed_until_dates = [f.text for f in embargo_fields]
        else:
            self.embargoed_until_dates = []
    def read_bitstream_links(self):
        # Read the file links out of the mets if they exist
        if self.bitstream_links is not None:
            return
        self.bitstream_links = []
        file_elements = self.mets_tree.findall(FILE_PATH)
        for el in file_elements:
            # <mets:file CHECKSUMTYPE="MD5" GROUPID="group_file_128474" ID="file_128474" MIMETYPE="text/plain" SIZE="137110" CHECKSUM="093be9c10e510e1e1de55f0e2b664a13">
            #   <mets:FLocat LOCTYPE="URL" xlink:title="Banding_data_AmNat53974.txt" xlink:label="dataset-file" xlink:type="locator" xlink:href="/bitstream/handle/10255/dryad.45529/Banding_data_AmNat53974.txt?sequence=1"/>
            # </mets:file>
            file_dict = {
                'checksum_type': el.get('CHECKSUMTYPE'),
                'checksum' : el.get('CHECKSUM'),
                'mime_type': el.get('MIMETYPE'),
                'size' : el.get('SIZE'),
                'id' : el.get('ID'),
                'urls': list()
            }
            for fel in el:
                url_dict = {
                    'title': fel.get(XLINK_XML_PREFIX + 'title'),
                    'label': fel.get(XLINK_XML_PREFIX + 'label'),
                    'href' : fel.get(XLINK_XML_PREFIX + 'href'),
                }
                file_dict['urls'].append(url_dict)
            self.bitstream_links.append(file_dict)

class DataPackage(DryadObject):
    def __init__(self,**kwargs):
        DryadObject.__init__(self,**kwargs)
        self.files = list()
        self.relative_file_mets_urls = list()
    def extract_file_mets_urls(self):
        # self.dri_tree must be loaded
        reference_sets = self.dri_tree.findall('./' + DRI_XML_PREFIX + 'body/' + DRI_XML_PREFIX + 'div/' + DRI_XML_PREFIX + 'referenceSet/' + DRI_XML_PREFIX + 'reference/' )
        if len(reference_sets) == 1:
            for file_reference in reference_sets[0]:
                # <reference repositoryID="10255" type="DSpace Item" url="/metadata/handle/10255/dryad.53354/mets.xml"/>
                if file_reference.tag == DRI_XML_PREFIX + 'reference':
                    self.relative_file_mets_urls.append(file_reference.get('url'))
    def load_files(self):
        self.load_dri()
        self.parse_dri()
        self.extract_file_mets_urls()
        self.create_file_objects()
        self.read_file_metadata()
    def create_file_objects(self):
        for relative_file_url in self.relative_file_mets_urls:
            self.files.append(DataFile(mets_url_relative=relative_file_url))
    def read_file_metadata(self):
        for data_file in self.files:
            data_file.read()
    def print_embargo_dates(self):
        print "Embargo dates for package %s with %d files:" % (self.doi, len(self.files))
        for data_file in self.files:
            data_file.read_embargoed_until_dates()
            dates = data_file.embargoed_until_dates
            print "File %s has %d embargo dates: %s" % (data_file.doi, len(dates), (',').join(dates))
    def print_bitstream_links(self):
        for data_file in self.files:
            data_file.read_bitstream_links()
            for bitstream_link in data_file.bitstream_links:
                for url_dict in bitstream_link['urls']:
                    print "title: %s\tlabel: %s\thref: %s" % (url_dict['title'], url_dict['label'], url_dict['href'])
    def check_embargo_links(self):
        results = []
        # Any file with an embargo should not have a link
        now = datetime.now()
        for data_file in self.files:
            data_file.read_embargoed_until_dates()
            data_file.read_bitstream_links()
            result_dict = dict()
            result_dict['file'] = data_file.doi
            # Pick up here
            # Does the file have an embargo date?
            # if the file has an embargo date, it should not have a link
            embargo_active_now = False
            result_dict['embargo_dates'] = ','.join(data_file.embargoed_until_dates)
            for embargo_date in data_file.embargoed_until_dates:
                try:
                    parsed_embargo_date = parser.parse(embargo_date)
                    if parsed_embargo_date > now:
                        embargo_active_now = True
                        break
                except Exception as e:
                    print "Exception parsing embargo date: %s", e
            result_dict['embargo_active'] = embargo_active_now
            has_bitstream_links = len(data_file.bitstream_links) > 0
            result_dict['has_bitstream_links'] = has_bitstream_links
            result_dict['download_results'] = []
            if embargo_active_now:
                # Embargo is active, make sure no links are present
                if has_bitstream_links:
                    print "Found %d bitstream links for embargoed data file %s" % (len(data_file.bitstream_links), data_file.doi)
                    # attempt head
                    for bitstream_link in data_file.bitstream_links:
                        for url_dict in bitstream_link['urls']:
                            absolute_url = DRYAD_BASE + url_dict['href']
                            r = requests.head(absolute_url)
                            result_dict['download_results'].append({ 'url': absolute_url, 'status_code': r.status_code})
            results.append(result_dict)
        return results

# 1. get metadata for the files in a data package
# 2. get download links for the files in a data package
# 3. get embargo status
# 4. check if embargoed items can be downloaded
# 5. report

class SolrDocument(object):
    def __init__(self, url=None):
        self.url = url
        self.solr_xml = None
        self.solr_tree = None
    def load_solr(self):
        if self.solr_xml is not None:
            return
        r = requests.get(self.url)
        self.solr_xml = r.text
    def parse_solr(self):
        if self.solr_tree is not None:
            return
        self.solr_tree = ElementTree.fromstring(self.solr_xml.encode('utf-8'))
    def get_embargoed_file_dois(self):
        # find all the file dois in the solr tree
        # will include package dois.
        dois = self.solr_tree.findall('./result/doc/arr[@name="dc.identifier"]/str')
        # file dois have two slashes
        embargoed_file_dois = [d.text for d in dois if d.text.count('/') >= 2]
        # uniqify
        embargoed_file_dois = list(set(embargoed_file_dois))
        return embargoed_file_dois

def main():
    #  Simple - feed in package DOIs
    package_dois = [
        'doi:10.5061/dryad.s8g15',
        'doi:10.5061/dryad.ct40s'
    ]
    if False:
        for package_doi in package_dois:
            package = DataPackage(doi=package_doi)
            package.load_files()
            results = package.check_embargo_links()
            print results
    solr = SolrDocument(SOLR_QUERY_URL)
    solr.load_solr()
    solr.parse_solr()
    file_dois = solr.get_embargoed_file_dois()
    print "According to solr, there are %d embargoed files" % len(file_dois)
    for file_doi in file_dois:
        # Leaving off here.  will this work?
        print "Checking %s" % file_doi
        data_file = DataFile(doi=file_doi)
        data_file.load_dri()
        data_file.parse_dri()





if __name__ == '__main__':
    main()
