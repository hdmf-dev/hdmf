import pandas as pd

from hdmf.common.resources import ExternalResources, Key
from hdmf.testing import TestCase, H5RoundTripMixin


class TestExternalResources(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        er = ExternalResources('terms')
        key1 = er.add_key('key1')
        key2 = er.add_key('key1')
        er.add_ref('uuid1', 'field1', key1, 'resource11', 'resource_id11', 'url11')
        er.add_ref('uuid2', 'field2', key2, 'resource21', 'resource_id21', 'url21')
        er.add_ref('uuid1', 'field1', 'key1', 'resource12', 'resource_id12', 'url12')
        return er

    def test_piecewise_add(self):
        er = ExternalResources('terms')

        # this is the term the user wants to use. They will need to specify this
        key = er.add_key('mouse')

        # the user will have to supply this info as well. This is the information
        # needed to retrieve info about the controled term
        er.add_resource(key, 'NCBI Taxonomy', '10090',
                        'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=10090')

        # The user can also pass in the container or it can be wrapped up under NWBFILE
        obj = er.add_object('ca885753-e8a3-418a-86f4-7748fc2252a8', 'species')

        # This could also be wrapped up under NWBFile
        er.add_external_reference(obj, key)

        self.assertEqual(er.keys.data, [('mouse',)])
        self.assertEqual(er.resources.data,
                         [(0, 'NCBI Taxonomy', '10090',
                           'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=10090')])
        self.assertEqual(er.objects.data, [('ca885753-e8a3-418a-86f4-7748fc2252a8', 'species')])

    def test_add_ref(self):
        er = ExternalResources('terms')
        er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'uri1')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data, [(0, 'resource1', 'resource_id1', 'uri1')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1')])

    def test_add_ref_bad_arg(self):
        er = ExternalResources('terms')
        # The contents of the message are not important. Just make sure an error is raised
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', 'field1', 'key1', resource_name='resource1', entity_id='resource_id1')
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', 'field1', 'key1', resource_name='resource1', entity_uri='uri1')
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', 'field1', 'key1', entity_id='resource_id1', entity_uri='uri1')

    def test_add_ref_two_resources(self):
        er = ExternalResources('terms')
        er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'uri1')
        er.add_ref('uuid1', 'field1', 'key1', 'resource2', 'resource_id2', 'uri2')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data,
                         [(0, 'resource1', 'resource_id1', 'uri1'),
                          (0, 'resource2', 'resource_id2', 'uri2')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1')])

    def test_add_ref_two_keys(self):
        er = ExternalResources('terms')
        er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'uri1')
        er.add_ref('uuid2', 'field2', 'key2', 'resource2', 'resource_id2', 'uri2')

        self.assertEqual(er.keys.data, [('key1',), ('key2',)])
        self.assertEqual(er.resources.data,
                         [(0, 'resource1', 'resource_id1', 'uri1'),
                          (1, 'resource2', 'resource_id2', 'uri2')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1'),
                                           ('uuid2', 'field2')])

    def test_add_ref_same_key_diff_objfield(self):
        er = ExternalResources('terms')
        er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'url1')
        er.add_ref('uuid2', 'field2', 'key1', 'resource2', 'resource_id2', 'url2')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data,
                         [(0, 'resource1', 'resource_id1', 'url1'),
                          (0, 'resource2', 'resource_id2', 'url2')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1'),
                                           ('uuid2', 'field2')])

    def test_add_ref_same_keyname(self):
        er = ExternalResources('terms')
        key1 = er.add_key('key1')
        key2 = er.add_key('key1')
        er.add_ref('uuid1', 'field1', key1, 'resource11', 'resource_id11', 'url11')
        er.add_ref('uuid2', 'field2', key2, 'resource21', 'resource_id21', 'url21')
        er.add_ref('uuid1', 'field1', 'key1', 'resource12', 'resource_id12', 'url12')

        self.assertEqual(er.keys.data, [('key1',), ('key1',)])
        self.assertEqual(er.resources.data,
                         [(0, 'resource11', 'resource_id11', 'url11'),
                          (1, 'resource21', 'resource_id21', 'url21'),
                          (0, 'resource12', 'resource_id12', 'url12')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1'),
                                           ('uuid2', 'field2')])

    def test_get_keys(self):
        er = ExternalResources('terms')
        er.add_ref('uuid1', 'field1', 'key1', 'resource11', 'resource_id11', 'url11')
        er.add_ref('uuid2', 'field2', 'key2', 'resource21', 'resource_id21', 'url21')
        er.add_ref('uuid1', 'field1', 'key1', 'resource12', 'resource_id12', 'url12')
        received = er.get_keys()

        expected = pd.DataFrame(
            data=[['key1', 'resource11', 'resource_id11', 'url11'],
                  ['key1', 'resource12', 'resource_id12', 'url12'],
                  ['key2', 'resource21', 'resource_id21', 'url21']],
            columns=['key_name', 'resource_name', 'resource_entity_id', 'resource_entity_uri']
        )
        pd.testing.assert_frame_equal(received, expected)

    def test_get_keys_subset(self):
        er = ExternalResources('terms')
        er.add_ref('uuid1', 'field1', 'key1', 'resource11', 'resource_id11', 'url11')
        er.add_ref('uuid2', 'field2', 'key2', 'resource21', 'resource_id21', 'url21')
        er.add_ref('uuid1', 'field1', 'key1', 'resource12', 'resource_id12', 'url12')
        key = er.keys.row[0]
        received = er.get_keys(keys=key)

        expected = pd.DataFrame(
            data=[['key1', 'resource11', 'resource_id11', 'url11'],
                  ['key1', 'resource12', 'resource_id12', 'url12']],
            columns=['key_name', 'resource_name', 'resource_entity_id', 'resource_entity_uri']
        )
        pd.testing.assert_frame_equal(received, expected)

    def test_add_keys(self):
        er = ExternalResources('terms')
        keys = pd.DataFrame(
            data=[['key1', 'resource11', 'resource_id11', 'url11'],
                  ['key1', 'resource12', 'resource_id12', 'url12'],
                  ['key2', 'resource21', 'resource_id21', 'url21']],
            columns=['key_name', 'resource_name', 'resource_entity_id', 'resource_entity_uri']
        )
        ret = er.add_keys(keys)

        self.assertEqual({'key1', 'key2'}, set(ret))
        self.assertEqual(er.keys.data, [('key1',), ('key2',)])
        self.assertEqual(er.resources.data,
                         [(0, 'resource11', 'resource_id11', 'url11'),
                          (0, 'resource12', 'resource_id12', 'url12'),
                          (1, 'resource21', 'resource_id21', 'url21')])

    def test_keys_roundtrip(self):
        er = ExternalResources('terms')
        keys = pd.DataFrame(
            data=[['key1', 'resource11', 'resource_id11', 'url11'],
                  ['key1', 'resource12', 'resource_id12', 'url12'],
                  ['key2', 'resource21', 'resource_id21', 'url21']],
            columns=['key_name', 'resource_name', 'resource_entity_id', 'resource_entity_uri']
        )
        er.add_keys(keys)
        received = er.get_keys()

        pd.testing.assert_frame_equal(received, keys)


class TestExternalResourcesGetKey(TestCase):

    def setUp(self):
        self.er = ExternalResources('terms')

    def test_get_key(self):
        self.er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'url1')
        self.er.add_ref('uuid2', 'field2', 'key1', 'resource2', 'resource_id2', 'url2')
        keys = self.er.get_key('key1')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.key_name, 'key1')

    def test_get_key_w_object_info(self):
        self.er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'url1')
        self.er.add_ref('uuid2', 'field2', 'key1', 'resource2', 'resource_id2', 'url2')
        keys = self.er.get_key('key1', 'uuid1', 'field1')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.key_name, 'key1')

    def test_get_key_w_bad_object_info(self):
        self.er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'url1')
        self.er.add_ref('uuid2', 'field2', 'key2', 'resource2', 'resource_id2', 'url2')
        with self.assertRaisesRegex(ValueError, "No key with name 'key2' for container 'uuid1' and field 'field1'"):
            self.er.get_key('key2', 'uuid1', 'field1')

    def test_get_key_doesnt_exist(self):
        self.er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'url1')
        self.er.add_ref('uuid2', 'field2', 'key1', 'resource2', 'resource_id2', 'url2')
        with self.assertRaisesRegex(ValueError, "key 'bad_key' does not exist"):
            self.er.get_key('bad_key')

    def test_get_key_same_keyname_all(self):
        self.er = ExternalResources('terms')
        key1 = self.er.add_key('key1')
        key2 = self.er.add_key('key1')
        self.er.add_ref('uuid1', 'field1', key1, 'resource11', 'resource_id11', 'url11')
        self.er.add_ref('uuid2', 'field2', key2, 'resource21', 'resource_id21', 'url21')
        self.er.add_ref('uuid1', 'field1', 'key1', 'resource12', 'resource_id12', 'url12')

        keys = self.er.get_key('key1')

        self.assertIsInstance(keys, list)
        self.assertEqual(keys[0].key_name, 'key1')
        self.assertEqual(keys[1].key_name, 'key1')

    def test_get_key_same_keyname_specific(self):
        self.er = ExternalResources('terms')
        key1 = self.er.add_key('key1')
        key2 = self.er.add_key('key1')
        self.er.add_ref('uuid1', 'field1', key1, 'resource11', 'resource_id11', 'url11')
        self.er.add_ref('uuid2', 'field2', key2, 'resource21', 'resource_id21', 'url21')
        self.er.add_ref('uuid1', 'field1', 'key1', 'resource12', 'resource_id12', 'url12')

        keys = self.er.get_key('key1', 'uuid1', 'field1')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.key_name, 'key1')
