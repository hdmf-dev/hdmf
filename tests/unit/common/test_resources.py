import pandas as pd

from hdmf.common.resources import ExternalResources, Key, Resource
from hdmf import Data
from hdmf.testing import TestCase, H5RoundTripMixin


class TestExternalResources(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        er = ExternalResources('terms')
        key1 = er._add_key('key1')
        key2 = er._add_key('key1')
        resource1 = er._add_resource(resource='resource0', uri='resource_uri0')
        er.add_ref(
            container='uuid1', field='field1', key=key1,
            resource_name='resource11', resource_uri='resource_uri11',
            entity_id="id11", entity_uri='url11')

        er.add_ref(
            container='uuid2', field='field2', key=key2,
            resource_name='resource21', resource_uri='resource_uri21', entity_id="id12", entity_uri='url21')
        er.add_ref(
            container='uuid3', field='field1', key='key1',
            resource_name='resource12', resource_uri='resource_uri12', entity_id="id13", entity_uri='url12')
        er.add_ref(
            container='uuid4', field='field2', key=key2, resources_idx=resource1,
            entity_id="id14", entity_uri='url23')
        return er

    def test_piecewise_add(self):
        er = ExternalResources('terms')

        # this is the term the user wants to use. They will need to specify this
        key = er._add_key('mouse')

        resource1 = er._add_resource(resource='resource0', uri='resource_uri0')
        # the user will have to supply this info as well. This is the information
        # needed to retrieve info about the controled term
        er._add_entity(key, resource1, '10090', 'uri')

        # The user can also pass in the container or it can be wrapped up under NWBFILE
        obj = er._add_object('object', 'species')

        # This could also be wrapped up under NWBFile
        er._add_external_reference(obj, key)

        self.assertEqual(er.keys.data, [('mouse',)])
        self.assertEqual(er.entities.data,
                         [(0, 0, '10090', 'uri')])
        self.assertEqual(er.objects.data, [('object', 'species')])

    def test_add_ref(self):
        er = ExternalResources('terms')
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        resource1 = er._add_resource(resource='resource0', uri='resource_uri0')
        er.add_ref(
            container=data, field='', key='key1',
            resources_idx=resource1, entity_id='entity_id1', entity_uri='entity1')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [(0, 0, 'entity_id1', 'entity1')])
        self.assertEqual(er.objects.data, [(data.object_id, '')])

    def test_add_ref_duplicate_resource(self):
        er = ExternalResources('terms')
        resource1 = er._add_resource(resource='resource0', uri='resource_uri0')
        er.add_ref(
            container='uuid1', field='field1', key='key1',
            resources_idx=resource1, entity_id='entity_id1', entity_uri='entity1')
        resource_list = er.resources.which(resource='resource0')
        self.assertEqual(len(resource_list), 1)

    def test_add_ref_bad_arg(self):
        er = ExternalResources('terms')
        resource1 = er._add_resource(resource='resource0', uri='resource_uri0')
        # The contents of the message are not important. Just make sure an error is raised
        with self.assertRaises(ValueError):
            er.add_ref(
                'uuid1', 'field1', 'key1', resource_name='resource1',
                resource_uri='uri1', entity_id='resource_id1')
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', 'field1', 'key1', resource_name='resource1', resource_uri='uri1', entity_uri='uri1')
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', 'field1', 'key1', resource_name='resource1', resource_uri='uri1')
        with self.assertRaises(TypeError):
            er.add_ref('uuid1', 'field1')
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', 'field1', 'key1', resource_name='resource1')
        with self.assertRaises(ValueError):
            er.add_ref(
                'uuid1', 'field1', 'key1', resources_idx=resource1,
                resource_name='resource1', resource_uri='uri1')

    def test_add_ref_two_resources(self):
        er = ExternalResources('terms')
        er.add_ref(
            container='uuid1', field='field1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid1', field='field1', key=er.get_key(key_name='key1'), resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data,
                         [('resource1',  'resource_uri1'),
                          ('resource2', 'resource_uri2')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1')])
        self.assertEqual(er.entities.data, [(0, 0, 'id11', 'url11'), (0, 1, 'id12', 'url21')])

    def test_get_resources(self):
        er = ExternalResources('terms')
        er.add_ref(
            container='uuid1', field='field1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        resource = er.get_resource('resource1')
        self.assertIsInstance(resource, Resource)
        with self.assertRaises(ValueError):
            er.get_resource('unknown_resource')

    def test_add_ref_two_keys(self):
        er = ExternalResources('terms')
        er.add_ref(
            container='uuid1', field='field1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', field='field2', key='key2', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')

        self.assertEqual(er.keys.data, [('key1',), ('key2',)])
        self.assertEqual(er.resources.data,
                         [('resource1',  'resource_uri1'),
                          ('resource2', 'resource_uri2')])
        self.assertEqual(er.entities.data, [(0, 0, 'id11', 'url11'), (1, 1, 'id12', 'url21')])

        self.assertEqual(er.objects.data, [('uuid1', 'field1'),
                                           ('uuid2', 'field2')])

    def test_add_ref_same_key_diff_objfield(self):
        er = ExternalResources('terms')
        er.add_ref(
            container='uuid1', field='field1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', field='field2', key='key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')

        self.assertEqual(er.keys.data, [('key1',), ('key1',)])
        self.assertEqual(er.entities.data, [(0, 0, 'id11', 'url11'), (1, 1, 'id12', 'url21')])
        self.assertEqual(er.resources.data,
                         [('resource1',  'resource_uri1'),
                          ('resource2', 'resource_uri2')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1'),
                                           ('uuid2', 'field2')])

    def test_add_ref_same_keyname(self):
        er = ExternalResources('terms')
        key1 = er._add_key('key1')
        key2 = er._add_key('key1')
        er.add_ref(
            container='uuid1', field='field1', key=key1, resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', field='field2', key=key2, resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        er.add_ref(
            container='uuid3', field='field3', key='key1', resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url31')

        self.assertEqual(er.keys.data, [('key1',), ('key1',), ('key1',)])
        self.assertEqual(er.resources.data,
                         [('resource1',  'resource_uri1'),
                          ('resource2', 'resource_uri2'),
                          ('resource3', 'resource_uri3')])
        self.assertEqual(
            er.entities.data,
            [(0, 0, 'id11', 'url11'),
             (1, 1, 'id12', 'url21'),
             (2, 2, 'id13', 'url31')])
        self.assertEqual(er.objects.data, [('uuid1', 'field1'),
                                           ('uuid2', 'field2'),
                                           ('uuid3', 'field3')])

    def test_get_keys(self):
        er = ExternalResources('terms')

        er.add_ref(
            container='uuid1', field='field1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', field='field2', key='key2', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        er.add_ref(
            container='uuid1', field='field1', key=er.get_key(key_name='key1'), resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url31')
        received = er.get_keys()

        expected = pd.DataFrame(
            data=[['key1', 0, 'id11', 'url11'],
                  ['key1', 2, 'id13', 'url31'],
                  ['key2', 1, 'id12', 'url21']],
            columns=['key_name', 'resources_idx', 'entity_id', 'entity_uri'])
        pd.testing.assert_frame_equal(received, expected)

    def test_get_keys_subset(self):
        er = ExternalResources('terms')
        er.add_ref(
            container='uuid1', field='field1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', field='field2', key='key2', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        er.add_ref(
            container='uuid1', field='field1', key=er.get_key(key_name='key1'), resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url31')
        key = er.keys.row[0]
        received = er.get_keys(keys=key)

        expected = pd.DataFrame(
            data=[['key1', 0, 'id11', 'url11'],
                  ['key1', 2, 'id13', 'url31']],
            columns=['key_name', 'resources_idx', 'entity_id', 'entity_uri'])
        pd.testing.assert_frame_equal(received, expected)

    def test_check_object_field_add(self):
        er = ExternalResources('terms')
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er._check_object_field('uuid1', 'field1')
        er._check_object_field(data, 'field2')

        self.assertEqual(er.objects.data, [('uuid1', 'field1'), (data.object_id, 'field2')])


class TestExternalResourcesGetKey(TestCase):

    def setUp(self):
        self.er = ExternalResources('terms')

    def test_get_key(self):
        self.er.add_ref(
            'uuid1', 'field1', 'key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', 'field2', 'key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')

        keys = self.er.get_key('key1', 'uuid2', 'field2')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.idx, 1)

    def test_get_key_bad_arg(self):
        self.er._add_key('key2')
        self.er.add_ref(
            'uuid1', 'field1', 'key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        with self.assertRaises(ValueError):
            self.er.get_key('key2', 'uuid1', 'field1')

    def test_get_key_without_container(self):
        self.er = ExternalResources('terms')
        self.er._add_key('key1')
        keys = self.er.get_key('key1')
        self.assertIsInstance(keys, Key)

    def test_get_key_w_object_info(self):
        self.er.add_ref(
            'uuid1', 'field1', 'key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', 'field2', 'key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        keys = self.er.get_key('key1', 'uuid1', 'field1')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.key, 'key1')

    def test_get_key_w_bad_object_info(self):
        self.er.add_ref(
            'uuid1', 'field1', 'key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', 'field2', 'key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')

        with self.assertRaisesRegex(ValueError, "No key with name 'key2'"):
            self.er.get_key('key2', 'uuid1', 'field1')

    def test_get_key_doesnt_exist(self):
        self.er.add_ref(
            'uuid1', 'field1', 'key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', 'field2', 'key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        with self.assertRaisesRegex(ValueError, "key 'bad_key' does not exist"):
            self.er.get_key('bad_key')

    def test_get_key_same_keyname_all(self):
        self.er = ExternalResources('terms')
        key1 = self.er._add_key('key1')
        key2 = self.er._add_key('key1')
        self.er.add_ref(
            'uuid1', 'field1', key1, resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', 'field2', key2, resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url12')
        self.er.add_ref(
            'uuid1', 'field1', self.er.get_key('key1', 'uuid1', 'field1'), resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url13')

        keys = self.er.get_key('key1')

        self.assertIsInstance(keys, list)
        self.assertEqual(keys[0].key, 'key1')
        self.assertEqual(keys[1].key, 'key1')

    def test_get_key_same_keyname_specific(self):
        self.er = ExternalResources('terms')
        key1 = self.er._add_key('key1')
        key2 = self.er._add_key('key1')
        self.er.add_ref(
            'uuid1', 'field1', key1, resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', 'field2', key2, resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url12')
        self.er.add_ref(
            'uuid1', 'field1', self.er.get_key('key1', 'uuid1', 'field1'), resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url13')

        keys = self.er.get_key('key1', 'uuid1', 'field1')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.key, 'key1')
        self.assertEqual(self.er.keys.data, [('key1',), ('key1',)])
