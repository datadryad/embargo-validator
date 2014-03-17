import requests
from xml.etree import ElementTree

DRYAD_BASE = 'http://datadryad.org'
DRYAD_RESOURCE_BASE = DRYAD_BASE + '/resource'
DRI_SUFFIX = '/DRI'
DRI_XML_PREFIX = '{http://di.tamu.edu/DRI/1.0/}'
METS_XML_PREFIX = '{http://www.loc.gov/METS/}'
DIM_XML_PREFIX = '{http://www.dspace.org/xmlns/dspace/dim}'

DIM_PATH = './' + METS_XML_PREFIX + 'dmdSec/' + METS_XML_PREFIX + 'mdWrap/' + METS_XML_PREFIX + 'xmlData/' + DIM_XML_PREFIX + 'dim/'
EMBARGO_PATH = DIM_PATH + DIM_XML_PREFIX + 'field[@mdschema="dc"][@element="date"][@qualifier="embargoedUntil"]'
IDENTIFIER_PATH = DIM_PATH + DIM_XML_PREFIX + 'field[@mdschema="dc"][@element="identifier"]'

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
    def read(self):
        self.load_mets()
        self.parse_mets()
        self.embargoed_until_dates = None

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

# 1. get metadata for the files in a data package
# 2. get download links for the files in a data package
# 3. get embargo status
# 4. check if embargoed items can be downloaded
# 5. report

def main():
    package_dois = [
        'doi:10.5061/dryad.s8g15',
        'doi:10.5061/dryad.ct40s'
    ]
    for package_doi in package_dois:
        package = DataPackage(doi=package_doi)
        package.load_files()
        package.print_embargo_dates()

if __name__ == '__main__':
    main()
