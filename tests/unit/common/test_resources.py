from hdmf.common.resources import ExternalResources
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
        # this should be automatically created in NWBFile
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
        # this should be automatically created in NWBFile
        er = ExternalResources('terms')
        er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'uri1')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data, [(0, 'resource1', 'resource_id1', 'uri1')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1')])

    def test_add_ref_two_resources(self):
        # this should be automatically created in NWBFile
        er = ExternalResources('terms')
        er.add_ref('uuid1', 'field1', 'key1', 'resource1', 'resource_id1', 'uri1')
        er.add_ref('uuid1', 'field1', 'key1', 'resource2', 'resource_id2', 'uri2')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data,
                         [(0, 'resource1', 'resource_id1', 'uri1'),
                          (0, 'resource2', 'resource_id2', 'uri2')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1')])

    def test_add_ref_two_keys(self):
        # this should be automatically created in NWBFile
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
        # this should be automatically created in NWBFile
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
        # this should be automatically created in NWBFile
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
