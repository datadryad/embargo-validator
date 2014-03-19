from time import sleep
import requests
from xml.etree import ElementTree
from dateutil import parser
from datetime import datetime
import unicodecsv

DRYAD_BASE = 'http://datadryad.org'
DRYAD_RESOURCE_BASE = DRYAD_BASE + '/resource'
DRI_SUFFIX = '/DRI'
DRI_XML_PREFIX = '{http://di.tamu.edu/DRI/1.0/}'
METS_XML_PREFIX = '{http://www.loc.gov/METS/}'
DIM_XML_PREFIX = '{http://www.dspace.org/xmlns/dspace/dim}'
XLINK_XML_PREFIX = '{http://www.w3.org/TR/xlink/}'
ATOM_XML_PREFIX = '{http://www.w3.org/2005/Atom}'

# XPaths for mets metadata
DIM_PATH = './' + METS_XML_PREFIX + 'dmdSec/' + METS_XML_PREFIX + 'mdWrap/' + METS_XML_PREFIX + 'xmlData/' + DIM_XML_PREFIX + 'dim/'
EMBARGO_PATH = DIM_PATH + DIM_XML_PREFIX + 'field[@mdschema="dc"][@element="date"][@qualifier="embargoedUntil"]'
IDENTIFIER_PATH = DIM_PATH + DIM_XML_PREFIX + 'field[@mdschema="dc"][@element="identifier"]'
FILE_PATH = './' + METS_XML_PREFIX + 'fileSec/' + METS_XML_PREFIX + 'fileGrp/' + METS_XML_PREFIX + 'file'

s = requests.Session()
s.headers = {'User-Agent' : 'DryadEmbargoValidator'}
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
        # Only valid if doi present
        return DRYAD_RESOURCE_BASE + '/' + self.doi + DRI_SUFFIX
    def load_html(self):
        if self.html is not None:
            return
        # For download links
        r = s.get(self.html_url())
        self.html = r.text
    def load_dri(self):
        if self.dri_xml is not None:
            return
        r = s.get(self.dri_url())
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
        r = s.get(DRYAD_BASE + self.mets_url_relative)
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
        # goal is to read METS metadata.  Need mets_relative_url to do that
        if self.mets_url_relative is None and self.doi is not None:
            # Have a DOI, can get mets via DRI
            self.load_dri()
            self.parse_dri()
            self.extract_mets_url()
        if self.mets_url_relative is not None:
            self.load_mets()
            self.parse_mets()
        else:
            raise Exception("Error, no METS url and no DOI. can't read data for DataFile")
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
    def check_embargo_link(self, current_date):
        self.read()
        self.read_embargoed_until_dates()
        self.read_bitstream_links()
        result_dict = dict()
        result_dict['file'] = self.doi
        embargo_active_now = False
        result_dict['embargo_dates'] = ','.join(self.embargoed_until_dates)
        for embargo_date in self.embargoed_until_dates:
            try:
                parsed_embargo_date = parser.parse(embargo_date)
                if parsed_embargo_date > current_date:
                    embargo_active_now = True
                    break
            except Exception as e:
                print "Exception parsing embargo date: %s", e
        result_dict['embargo_active'] = embargo_active_now
        has_bitstream_links = len(self.bitstream_links) > 0
        result_dict['has_bitstream_links'] = has_bitstream_links
        result_dict['download_results'] = []
        if embargo_active_now:
            # Embargo is active, make sure no links are present
            if has_bitstream_links:
                # links are present, they shouldn't be.  Make sure they're not downloadable
                print "Found %d bitstream links for embargoed data file %s" % (len(self.bitstream_links), self.doi)
                # attempt head
                for bitstream_link in self.bitstream_links:
                    for url_dict in bitstream_link['urls']:
                        absolute_url = DRYAD_BASE + url_dict['href']
                        r = s.head(absolute_url)
                        result_dict['download_results'].append({ 'url': absolute_url, 'status_code': r.status_code})
        return result_dict

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
        now = datetime.now()
        results = [f.check_embargo_link(now) for f in self.files]
        return results

# 1. get metadata for the files in a data package
# 2. get download links for the files in a data package
# 3. get embargo status
# 4. check if embargoed items can be downloaded
# 5. report

class DryadXMLDocument(object):
    def __init__(self, url=None):
        self.url = url
        self.xml = None
        self.tree= None
    def read(self):
        self.load()
        self.parse()
    def load(self):
        if self.xml is not None:
            return
        r = s.get(self.url)
        self.xml = r.text
    def parse(self):
        if self.tree is not None:
            return
        self.tree= ElementTree.fromstring(self.xml.encode('utf-8'))

class SolrDocument(DryadXMLDocument):
    def get_file_dois(self):
        self.read()
        # find all the file dois in the solr tree
        # will include package dois.
        dois = self.tree.findall('./result/doc/arr[@name="dc.identifier"]/str')
        # file dois have two slashes
        embargoed_file_dois = [d.text for d in dois if d.text.count('/') >= 2]
        # uniqify
        embargoed_file_dois = list(set(embargoed_file_dois))
        return embargoed_file_dois

class DryadRSSFeed(DryadXMLDocument):
    def get_package_dois(self):
        self.read()
        # DOIs are buried in <id> tags
        # find all the file dois in the solr tree
        # will include package dois.
        resource_ids = self.tree.findall('./' + ATOM_XML_PREFIX + 'entry/' + ATOM_XML_PREFIX + 'id')
        package_dois = []
        for resource_id in resource_ids:
            # get everything after 'resource/'
            doi = resource_id.text.split('resource/')[-1]
            package_dois.append(doi)
        package_dois = list(set(package_dois))
        return package_dois

def check_packages_example():
    package_dois = [
        'doi:10.5061/dryad.s8g15',
        'doi:10.5061/dryad.ct40s'
    ]
    for package_doi in package_dois:
        package_check_results = check_package(package_doi)
        print package_check_results

def check_package(package_doi):
    package = DataPackage(doi=package_doi)
    package.load_files()
    results = package.check_embargo_links()
    return results


# Use of this query assumes that the solr index is up-to-date with embargoedUntil metadata
SOLR_QUERY_URL = DRYAD_BASE + '/solr/search/select/?q=dc.date.embargoedUntil_dt:%5BNOW%20TO%20NOW/DAY%2B10000DAY%5D&rows=1000000&fl=dc.identifier'

def check_solr_index():
    solr = SolrDocument(SOLR_QUERY_URL)
    file_dois = solr.get_file_dois()
    print "Checking %d items in solr with an embargoedUntil date in the future..." % len(file_dois)
    now = datetime.now()
    results = []
    while len(file_dois) > 0:
        file_doi = file_dois.pop(0)
        data_file = DataFile(doi=file_doi)
        try:
            embargo_check_result = data_file.check_embargo_link(now)
            results.append(embargo_check_result)
            num_checked = len(results)
            if num_checked % 25 == 0:
                print "Checked %d files" % num_checked
        except Exception as e:
            # Might be a Treebase URL
            print "Exception checking file doi %s, skipping: %s" % (file_doi, e)
            sleep(1)
    write_embargo_check_csv('embargo_check_solr_index.csv',results)
    leaks = check_for_leaks(results)
    if len(leaks) > 0:
        print "Embargo leak detected in solr indexed data"
        write_embargo_check_csv('embargo_leaks_solr_index.csv', leaks)

RECENTLY_PUBLISHED_RSS_FEED_URL = DRYAD_BASE + '/feed/atom_1.0/10255/3'
def check_rss_feed():
    rss_feed = DryadRSSFeed(url=RECENTLY_PUBLISHED_RSS_FEED_URL)
    data_package_dois = rss_feed.get_package_dois()
    print "Checking files in %d recently published data packages..." % len(data_package_dois)
    results = []
    for doi in data_package_dois:
        results = results + check_package(doi)
    write_embargo_check_csv('embargo_check_rss_feed.csv', results)
    leaks = check_for_leaks(results)
    if len(leaks) > 0:
        print "Embargo leak detected in recently published data"
        write_embargo_check_csv('embargo_leaks_rss_feed.csv', leaks)


def write_embargo_check_csv(filename, results):
    with open(filename, 'wb') as f:
        headers = ['file','embargo_dates','embargo_active','has_bitstream_links','download_results']
        writer = unicodecsv.DictWriter(f,headers)
        writer.writeheader()
        writer.writerows(results)

def check_for_leaks(results):
    leaks = []
    for result in results:
        if result['embargo_active'] is True and result['has_bitstream_links'] is True:
            leaks.append(result)
    return leaks

def main():
    check_solr_index()
    check_rss_feed()

if __name__ == '__main__':
    main()
