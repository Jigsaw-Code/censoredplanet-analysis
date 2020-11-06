# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""IP Metadata is a class to add network metadata to IPs."""

import csv
import datetime
import gzip
import io
import logging
import os
from pprint import pprint
import re
from typing import Dict, List, Optional, Tuple, TextIO

import pyasn

import apache_beam.io.filesystems as apache_filesystems

CLOUD_DATA_LOCATION = "gs://censoredplanet_geolocation/caida/"

# The as-org2info.txt file contains two tables
# Comment lines with these headers divide the tables.
ORG_TO_COUNTRY_HEADER = "# format:org_id|changed|org_name|country|source"
AS_TO_ORG_HEADER = "# format:aut|changed|aut_name|org_id|opaque_id|source"


def return_false():
  return False


def _read_compressed_file(filepath: str) -> TextIO:
  """Read in a compressed file as a decompressed, decoded file object.

  We have to read the whole file into memory because some operations
  (removing comments, using only the second half of the file)
  require being able to manipulate particular lines.

  Args:
    filepath: a path to a compressed file. Could be either local like
      '/tmp/text.txt.gz' or a gcs file like
      'gs://censoredplanet_geolocation/caida/as-classifications/as2types.txt.gz'

  Returns:
    A file object
  """
  # Hack to read in gzipped files correctly using Beam's Filesystem
  # https://stackoverflow.com/questions/58508802
  """compressed_file = apache_filesystems.FileSystems.open(

      filepath,
      compression_type=apache_filesystems.CompressionTypes.UNCOMPRESSED)
  #text = io.TextIOWrapper(compressed_file)
  pprint(compressed_file)
  uncompressed_file = gzip.decompress(compressed_file.read())

  return uncompressed_file
  """

  import traceback

  f = apache_filesystems.FileSystems.open(filepath)
  # Beam's compressed file implementation fails to implement this spelling of the method
  f.writable = return_false

  pprint(("compressed file", f, dir(f), f.writeable, f.writeable(), f.writable,
          f.writable()))

  try:
    return io.TextIOWrapper(f)
  except Exception as ex:
    pprint((ex, dir(ex), dir(ex.__traceback__), ex.__traceback__.tb_frame))
    traceback.print_tb(ex.__traceback__)
    raise ex


def parse_asn_db(f: TextIO) -> pyasn.pyasn:
  """Returns a pyasn db from a routeview file.

  Args:
    f: an routeview file object

  Returns:
    pyasn database object
  """
  lines = f.readlines()

  # CAIDA file lines are stored in the format
  # 1.0.0.0\t24\t13335
  # but pyasn wants lines in the format
  # 1.0.0.0/24\t13335
  formatted_lines = [
      re.sub(r"(.*)\t(.*)\t(.*)", r"\1/\2\t\3", line) for line in lines
  ]
  as_str = "".join(formatted_lines)

  print(as_str)

  asn_db = pyasn.pyasn(None, ipasn_string=as_str)
  return asn_db


def parse_org_name_to_country_map(f: TextIO) -> Dict[str, Tuple[str, str]]:
  """Returns a mapping of AS org short names to country info from a file.

  Args:
    f: as2org file object

  Returns:
    Dict {as_name -> ("readable name", country_code)}
    ex: {"8X8INC-ARIN": ("8x8, Inc.","US")}
  """
  lines = f.readlines()

  pprint(("lines", lines))

  data_start_index = lines.index(ORG_TO_COUNTRY_HEADER) + 1
  data_end_index = lines.index(AS_TO_ORG_HEADER)
  org_country_lines = lines[data_start_index:data_end_index]
  org_country_data = list(csv.reader(org_country_lines, delimiter="|"))

  org_name_to_country_map: Dict[str, Tuple[str, str]] = {}
  for line in org_country_data:
    org_id, changed_date, org_name, country, source = line
    org_name_to_country_map[org_id] = (org_name, country)

  return org_name_to_country_map


def parse_as_to_org_map(
    f: TextIO, org_id_to_country_map: Dict[str, Tuple[str, str]]
) -> Dict[int, Tuple[str, Optional[str], Optional[str]]]:
  """Returns a mapping of ASNs to organization info from a file.

  Args:
    f: as2org file object
    org_id_to_country_map: Dict {as_name -> ("readable name", country_code)}

  Returns:
    Dict {asn -> (asn_name, readable_name, country)}
    ex {204867 : ("LIGHTNING-WIRE-LABS", "Lightning Wire Labs GmbH", "DE")}
    The final 2 fields may be None
  """
  lines = f.readlines()

  data_start_index = lines.index(AS_TO_ORG_HEADER) + 1
  as_to_org_lines = lines[data_start_index:]
  org_id_data = list(csv.reader(as_to_org_lines, delimiter="|"))

  asn_to_org_info_map: Dict[int, Tuple[str, Optional[str], Optional[str]]] = {}
  for line in org_id_data:
    asn, changed_date, asn_name, org_id, opaque_id, source = line
    try:
      readable_name, country = org_id_to_country_map[org_id]
      asn_to_org_info_map[int(asn)] = (asn_name, readable_name, country)
    except KeyError as e:
      logging.warning("Missing org country info for asn %s %s", asn, e)
      asn_to_org_info_map[int(asn)] = (asn_name, None, None)

  return asn_to_org_info_map


def parse_as_to_type_map(f: TextIO) -> Dict[int, str]:
  """Returns a mapping of ASNs to org type info from a file.

  Args:
    f: as2type file object

  Returns:
    Dict {asn -> network_type}
    ex {398243 : "Enterprise", 13335: "Content", 4: "Transit/Access"}
  """
  # filter comments
  lines = f.readlines()

  data_lines = [line for line in lines if line[0] != "#"]
  type_data = list(csv.reader(data_lines, delimiter="|"))

  as_to_type_map: Dict[int, str] = {}
  for line in type_data:
    asn, source, org_type = line
    as_to_type_map[int(asn)] = org_type

  return as_to_type_map


class IpMetadata(object):
  """A lookup table which contains network metadata about IPs."""

  def __init__(
      self,
      date: datetime.date,
      allow_previous_day=False,
  ):
    """Create an IP Metadata object by reading/parsing all needed data.

    Args:
      date: a date to initialize the asn database to
      allow_previous_day: If the given date's routeview file doesn't exist,
        allow the one from the previous day instead. This is useful when
        processing very recent data where the newest file may not yet exist.
    """
    as_to_org_map, as_to_type_map = self.get_asn_maps()
    self.as_to_org_map = as_to_org_map
    self.as_to_type_map = as_to_type_map

    self.asn_db = self.find_asn_db(date, allow_previous_day)

  def lookup(
      self, ip: str
  ) -> Tuple[str, int, Optional[str], Optional[str], Optional[str],
             Optional[str]]:
    """Lookup metadata infomation about an IP.

    Args:
      ip: string of the format 1.1.1.1 (ipv4 only)

    Returns:
      Tuple(netblock, asn, as_name, as_full_name, as_type, country)
      ("1.0.0.1/24", 13335, "CLOUDFLARENET", "Cloudflare Inc.", "Content", "US")
      The final 4 fields may be None

    Raises:
      KeyError: when the IP's ASN can't be found
    """
    asn, netblock = self.asn_db.lookup(ip)

    if not asn:
      raise KeyError("Missing IP {} at {}".format(ip, self.date.isoformat()))

    if asn not in self.as_to_org_map:
      logging.warning("Missing asn %s in org name map", asn)
    as_name, as_full_name, country = self.as_to_org_map.get(
        asn, (None, None, None))

    if asn not in self.as_to_type_map:
      logging.warning("Missing asn %s in type map", asn)
    as_type = self.as_to_type_map.get(asn, None)

    return (netblock, asn, as_name, as_full_name, as_type, country)

  def get_asn_maps(self):
    """Initializes all ASN files as map objects.

    Returns:
      Tuple of an as2org Dict and as2type Dict
    """
    org_name_to_country_filename = CLOUD_DATA_LOCATION + "as-organizations/20200701.as-org2info.txt.gz"
    as_to_org_filename = CLOUD_DATA_LOCATION + "as-organizations/20200701.as-org2info.txt.gz"
    as_to_type_filename = CLOUD_DATA_LOCATION + "as-classifications/20200801.as2types.txt.gz"

    org_name_to_country_file = _read_compressed_file(
        org_name_to_country_filename)
    as_to_org_file = _read_compressed_file(as_to_org_filename)
    as_to_type_file = _read_compressed_file(as_to_type_filename)

    org_to_country_map = parse_org_name_to_country_map(org_name_to_country_file)
    as_to_org_map = parse_as_to_org_map(as_to_org_file, org_to_country_map)
    as_to_type_map = parse_as_to_type_map(as_to_type_file)

    return as_to_org_map, as_to_type_map

  def get_asn_db(self, date: datetime.date, allow_previous_day) -> pyasn.pyasn:
    """Return an ASN database object.

    Args:
      date: a date to initialize the asn database to
      allow_previous_day: allow using previous routeview file

    Returns:
      pyasn database

    Raises:
        FileNotFoundError: when no matching routeview file is found
    """
    try:
      self.date = date
      f = self.find_routeview_file(self.date)
      return parse_asn_db(f)
    except FileNotFoundError as ex:
      if allow_previous_day:
        self.date = date - datetime.timedelta(days=1)
        f = self.find_routeview_file(self.date)
        return parse_asn_db(f)
      else:
        raise ex

  def find_routeview_file(self, date: datetime.date) -> TextIO:
    """Finds the right routeview file for a given date and creates a pyasn db.

    Args:
      date: date object to initialize the database to

    Returns:
      a TextIO fileobject for the routeview file

    Raises:
      FileNotFoundError: when no matching routeview file is found
    """
    formatted_date = date.isoformat().replace("-", "")
    file_pattern = "routeviews-rv2-" + formatted_date + "*.pfx2as.gz"
    filepath_pattern = CLOUD_DATA_LOCATION + "routeviews/" + file_pattern
    match = FileSystems.match([filepath_pattern], limits=[1])

    try:
      filepath = match[0].metadata_list[0].path
      return _read_compressed_file(filepath)
    except IndexError:
      raise FileNotFoundError(filepath_pattern)
