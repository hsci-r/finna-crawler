import csv
import logging
import os
from contextlib import nullcontext
from os.path import exists
from typing import Optional
from xml.etree import ElementTree

import click
from lxml import etree
from requests import HTTPError
from sickle import Sickle, OAIResponse
from sickle.models import ResumptionToken
from tqdm import tqdm
from xopen import xopen

_huge_tree_xml_parser = etree.XMLParser(remove_blank_text=True, huge_tree=True, recover=True, resolve_entities=False)


class HugeTreeOAIResponse(OAIResponse):

    def __init__(self, http_response, params):
        super().__init__(http_response, params)

    @property
    def xml(self):
        """The server's response as parsed XML."""
        return etree.XML(self.http_response.content,
                         parser=_huge_tree_xml_parser)


class HugeTreeSickle(Sickle):
    def __init__(self, endpoint, **kwargs):
        super().__init__(endpoint, **kwargs)

    def harvest(self, **kwargs):
        response = super().harvest(**kwargs)
        return HugeTreeOAIResponse(response.http_response, response.params)


def strip_namespaces(doc):
    """Remove all namespaces"""
    for el in doc.getiterator():
        if el.tag.startswith('{'):
            el.tag = el.tag.split('}', 1)[1]
        # loop on element attributes also
        for an in el.attrib.keys():
            if an.startswith('{'):
                el.attrib[an.split('}', 1)[1]] = el.attrib.pop(an)
    # remove namespace declarations
    etree.cleanup_namespaces(doc)


@click.command()
@click.option('-p', '--metadata-prefix', help="metadata prefix to query")
@click.option('-s', '--set', help="set to query")
@click.option('-sf', '--status-file', help="status file for recovering an aborted crawl", required=True)
@click.option('-sx/-nsx', '--strip-xml/--no-strip-xml', default=True,
              help="whether to strip XML namespaces from XML output (default is to strip)")
@click.option('-fr/-nfr', '--full-record/--no-full-record', default=False,
              help="whether to output the record in full or only the main content of it without the OAI/PMH metadata (default is to output only the main content)")
@click.option('-mo', '--metadata-output', help="output TSV (gz/bz2/xz/zst) file in which to write metadata")
@click.option('-ro', '--record-output', help="output (gz/bz2/xz/zst) file in which to write records")
def crawl_finna(metadata_prefix: Optional[str], status_file: str, metadata_output: Optional[str],
                record_output: Optional[str], set: Optional[str], strip_xml: bool, full_record: bool):
    """Download metadata and records from Finna from the desired metadata prefix"""
    sickle = HugeTreeSickle("https://api.finna.fi/OAI/Server", retry_status_codes=[429, 500, 502, 503], max_retries=10,
                            headers={'User-Agent': 'foo'})
    if metadata_prefix is None:
        print(
            f"Available prefixes (specify with -p or --metadata-prefix): {', '.join([prefix.metadataPrefix for prefix in sickle.ListMetadataFormats()])}")
        print(
            f"Available sets (optionally specify with -s or --set): {', '.join([set.setName for set in sickle.ListSets()])}")
        return
    if metadata_output is None and record_output is None:
        print("Neither metadata nor record output file defined, crawl would write out nothing.")
        return
    if exists(status_file):
        with open(status_file, 'rt') as sf:
            parts = sf.readline().rstrip().split('/')
            if len(parts) == 4:
                resumption_token = ResumptionToken(
                    token=parts[0], cursor=parts[1],
                    complete_list_size=parts[2],
                    expiration_date=parts[3]
                )
            elif len(parts) == 1 and parts[0] == '':
                resumption_token = None
            else:
                logging.error(f"Unknown number of parts in status file: {parts}")
                return

    else:
        resumption_token = None
    if not exists(metadata_output):
        with xopen(metadata_output, 'wt', threads=0) as metadata_output_file:
            cw = csv.writer(metadata_output_file, delimiter='\t')
            cw.writerow(['id', 'timestamp', 'id2', 'creator', 'title', 'subjects'])
    with open(status_file, 'wt') as sf, (xopen(metadata_output, 'at',
                                               threads=0) if metadata_output is not None else nullcontext()) as metadata_output_file, (
    xopen(record_output, 'at', threads=0) if record_output is not None else nullcontext()) as record_output_file:
        try:
            records = sickle.ListRecords(metadataPrefix=metadata_prefix, set=set)
            if resumption_token is not None:
                if records.resumption_token.complete_list_size != resumption_token.complete_list_size:
                    logging.warning(
                        f"Total records changed since earlier run ({resumption_token.complete_list_size}!={records.resumption_token.complete_list_size}). ")
                records.resumption_token = resumption_token
                records._next_response()
            if metadata_output is not None:
                cw = csv.writer(metadata_output_file, delimiter='\t')
            else:
                cw = None
            for record in tqdm(records, unit="record", initial=int(records.resumption_token.cursor),
                               total=int(records.resumption_token.complete_list_size), leave=True, dynamic_ncols=True):
                if cw is not None:
                    id2 = filter(lambda id: id is not None, record.metadata.get('identifier', []))
                    creator = filter(lambda creator: creator is not None, record.metadata.get('creator', []))
                    title = filter(lambda title: title is not None, record.metadata.get('title', []))
                    subjects = filter(lambda subject: subject is not None, record.metadata.get('subject', []))
                    cw.writerow([
                        record.header.identifier,
                        record.header.datestamp,
                        '|'.join(id2),
                        '|'.join(creator),
                        '|'.join(title),
                        '|'.join(subjects)
                    ])
                    metadata_output_file.flush()
                    os.fsync(metadata_output_file.fileno())
                if record_output_file is not None:
                    if not full_record and 'metadata' in record.metadata:
                        record_output_file.write(record.metadata['metadata'][0])
                    else:
                        if full_record:
                            elem = record.xml
                        else:
                            elem = record.xml.find('.//' + record._oai_namespace + 'metadata')[0]
                        if strip_xml:
                            strip_namespaces(elem)
                        record_output_file.write(ElementTree.tostring(elem, encoding='unicode', method='xml'))
                    record_output_file.write('\n')
                    record_output_file.flush()
                    os.fsync(record_output_file.fileno())
                sf.truncate(0)
                sf.seek(0)
                if records.resumption_token is not None:
                    sf.write(records.resumption_token.token)
                    sf.write('/')
                    if records.resumption_token.cursor is not None:
                        sf.write(records.resumption_token.cursor)
                    sf.write('/')
                    if records.resumption_token.complete_list_size is not None:
                        sf.write(records.resumption_token.complete_list_size)
                    sf.write('/')
                    if records.resumption_token.expiration_date is not None:
                        sf.write(records.resumption_token.expiration_date)
                sf.flush()
                os.fsync(sf.fileno())
        except HTTPError as e:
            logging.exception(e)
        sf.truncate(0)


if __name__ == '__main__':
    crawl_finna()
