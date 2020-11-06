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

import datetime
import io
import textwrap
import unittest

from pipeline.metadata import ip_metadata


class IpMetadataTest(unittest.TestCase):

  def test_init_and_lookup(self):
    # This E2E test requires the user to have get access to the
    # gs://censoredplanet_geolocation bucket.
    ip_metadata_db = ip_metadata.IpMetadata(
        datetime.date.fromisoformat("2018-07-27"))
    metadata = ip_metadata_db.lookup("1.1.1.1")

    self.assertEqual(metadata, ("1.1.1.0/24", 13335, "CLOUDFLARENET",
                                "Cloudflare, Inc.", "Content", "US"))

  def test_read_compressed_file(self):
    filepath = "pipeline/metadata/test_file.txt.gz"
    lines = [line for line in ip_metadata._read_compressed_file(filepath)]
    self.assertListEqual(lines, ["test line 1", "test line 2"])

  def test_parse_asn_db(self):
    # Sample content for a routeviews-rv2-*.pfx2as file
    routeview_string = textwrap.dedent("""\
    1.0.0.0	24	13335
    8.8.8.0	24	15169
    9.9.9.0	24	19281""")
    f = io.StringIO(routeview_string)

    asn_db = ip_metadata.parse_asn_db(f)

    print(asn_db)

    self.assertEqual(asn_db.lookup("1.1.1.1"), (13335, "1.1.1.0/24"))
    self.assertEqual(asn_db.lookup("8.8.8.8"), (15169, "8.8.8.0/24"))

  def test_parse_org_name_to_country_map(self):
    f = self.get_as_to_org_file()

    m = ip_metadata.parse_org_name_to_country_map(f)
    self.assertEqual(m, {"LVLT-ARIN": ("Level 3 Communications, Inc.", "US")})

  def test_parse_as_to_org_map(self):
    f = self.get_as_to_org_file()
    org_id_to_country_map = {
        "LVLT-ARIN": ("Level 3 Communications, Inc.", "US")
    }

    m = ip_metadata.parse_as_to_org_map(f, org_id_to_country_map)
    self.assertEqual(m,
                     {1: ("LVLT-ARIN", "Level 3 Communications, Inc.", "US")})

  def test_parse_as_to_type_map(self):
    # Sample content for a as2types.txt file
    as2type_string = textwrap.dedent("""\
    # format: as|source|type
    # date: 20201001", "# name: as2type
    # exe: type-convert-amogh.pl
    # files: 20201001.merged.class.txt
    # types: Content|Enterprise|Transit/Access
    1|CAIDA_class|Transit/Access
    4|CAIDA_class|Content""")

    print(as2type_string)

    f = io.StringIO(as2type_string)

    m = ip_metadata.parse_as_to_type_map(f)
    self.assertEqual(m, {1: "Transit/Access", 4: "Content"})

  def get_as_to_org_file(self):
    # Sample content for an as-org2info.txt file.
    as2org_string = textwrap.dedent("""\
    # name: AS Org
    # some random
    # comment lines
    # format: aut|changed|aut_name|org_id|opaque_id|source
    1|20120224|LVLT-1|LVLT-ARIN|e5e3b9c13678dfc483fb1f819d70883c_ARIN|ARIN
    # format: org_id|changed|name|country|source
    LVLT-ARIN|20120130|Level 3 Communications, Inc.|US|ARIN""")

    f = io.StringIO(as2org_string)
    return f


if __name__ == "__main__":
  unittest.main()
