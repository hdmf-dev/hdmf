import pandas as pd
from hdmf.common import DynamicTable
from hdmf.common.resources import ExternalResources, Key, Resource
from hdmf import Data
from hdmf.testing import TestCase, H5RoundTripMixin
import numpy as np
import unittest
from tests.unit.build_tests.test_io_map import Bar
from tests.unit.utils import create_test_type_map, CORE_NAMESPACE
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec


class TestExternalResources(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        er = ExternalResources(name='terms')
        er.add_ref(
            container='uuid1', key='key1',
            resource_name='resource11', resource_uri='resource_uri11',
            entity_id="id11", entity_uri='url11')

        er.add_ref(
            container='uuid2', key='key2',
            resource_name='resource21', resource_uri='resource_uri21', entity_id="id12", entity_uri='url21')
        return er

    @unittest.skip('Outdated do to privatization')
    def test_piecewise_add(self):
        er = ExternalResources(name='terms')

        # this is the term the user wants to use. They will need to specify this
        key = er._add_key('mouse')

        resource1 = er._add_resource(resource='resource0', uri='resource_uri0')
        # the user will have to supply this info as well. This is the information
        # needed to retrieve info about the controled term
        er._add_entity(key, resource1, '10090', 'uri')

        # The user can also pass in the container or it can be wrapped up under NWBFILE
        obj = er._add_object('object', 'species')

        # This could also be wrapped up under NWBFile
        er._add_object_key(obj, key)

        self.assertEqual(er.keys.data, [('mouse',)])
        self.assertEqual(er.entities.data,
                         [(0, 0, '10090', 'uri')])
        self.assertEqual(er.objects.data, [('object', 'species')])

    def test_to_dataframe(self):
        # Setup complex external resources with keys reused across objects and
        # multiple resources per key
        er = ExternalResources(name='example')
        # Add a species dataset with 2 keys
        data1 = Data(
            name='data_name',
            data=np.array(
                [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
                dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]
            )
        )
        k1, r1, e1 = er.add_ref(
            container=data1,
            field='species',
            key='Mus musculus',
            resource_name='NCBI_Taxonomy',
            resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
            entity_id='NCBI:txid10090',
            entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
        )
        k2, r2, e2 = er.add_ref(
            container=data1,
            field='species',
            key='Homo sapiens',
            resource_name='NCBI_Taxonomy',
            resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
            entity_id='NCBI:txid9606',
            entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'
        )
        # Add a second species dataset that uses the same keys as the first dataset and add an additional key
        data2 = Data(name="species", data=['Homo sapiens', 'Mus musculus', 'Pongo abelii'])
        o2 = er._add_object(data2, relative_path='', field='')
        er._add_object_key(o2, k1)
        er._add_object_key(o2, k2)
        k2, r2, e2 = er.add_ref(
            container=data2,
            field='',
            key='Pongo abelii',
            resource_name='NCBI_Taxonomy',
            resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
            entity_id='NCBI:txid9601',
            entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9601'
        )
        # Add a third data object, this time with 2 entities for a key
        data3 = Data(name="genotypes", data=['Rorb'])
        k3, r3, e3 = er.add_ref(
            container=data3,
            field='',
            key='Rorb',
            resource_name='MGI Database',
            resource_uri='http://www.informatics.jax.org/',
            entity_id='MGI:1346434',
            entity_uri='http://www.informatics.jax.org/marker/MGI:1343464'
        )
        _ = er.add_ref(
            container=data3,
            field='',
            key=k3,
            resource_name='Ensembl',
            resource_uri='https://uswest.ensembl.org/index.html',
            entity_id='ENSG00000198963',
            entity_uri='https://uswest.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000198963'
        )
        # Convert to dataframe and compare against the expected result
        result_df = er.to_dataframe()
        expected_df_data = \
            {'objects_idx': {0: 0, 1: 0, 2: 1, 3: 1, 4: 1, 5: 2, 6: 2},
             'object_id': {0: data1.object_id, 1: data1.object_id,
                           2: data2.object_id, 3: data2.object_id, 4: data2.object_id,
                           5: data3.object_id, 6: data3.object_id},
             'field': {0: 'species', 1: 'species', 2: '', 3: '', 4: '', 5: '', 6: ''},
             'keys_idx': {0: 0, 1: 1, 2: 0, 3: 1, 4: 2, 5: 3, 6: 3},
             'key': {0: 'Mus musculus', 1: 'Homo sapiens', 2: 'Mus musculus', 3: 'Homo sapiens',
                     4: 'Pongo abelii', 5: 'Rorb', 6: 'Rorb'},
             'resources_idx': {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 1, 6: 2},
             'resource': {0: 'NCBI_Taxonomy', 1: 'NCBI_Taxonomy', 2: 'NCBI_Taxonomy', 3: 'NCBI_Taxonomy',
                          4: 'NCBI_Taxonomy', 5: 'MGI Database', 6: 'Ensembl'},
             'resource_uri': {0: 'https://www.ncbi.nlm.nih.gov/taxonomy', 1: 'https://www.ncbi.nlm.nih.gov/taxonomy',
                              2: 'https://www.ncbi.nlm.nih.gov/taxonomy', 3: 'https://www.ncbi.nlm.nih.gov/taxonomy',
                              4: 'https://www.ncbi.nlm.nih.gov/taxonomy', 5: 'http://www.informatics.jax.org/',
                              6: 'https://uswest.ensembl.org/index.html'},
             'entities_idx': {0: 0, 1: 1, 2: 0, 3: 1, 4: 2, 5: 3, 6: 4},
             'entity_id': {0: 'NCBI:txid10090', 1: 'NCBI:txid9606', 2: 'NCBI:txid10090', 3: 'NCBI:txid9606',
                           4: 'NCBI:txid9601', 5: 'MGI:1346434', 6: 'ENSG00000198963'},
             'entity_uri': {0: 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090',
                            1: 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606',
                            2: 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090',
                            3: 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606',
                            4: 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9601',
                            5: 'http://www.informatics.jax.org/marker/MGI:1343464',
                            6: 'https://uswest.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000198963'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Convert to dataframe with categories and compare against the expected result
        result_df = er.to_dataframe(use_categories=True)
        cols_with_categories = [
            ('objects', 'objects_idx'), ('objects', 'object_id'), ('objects', 'field'),
            ('keys', 'keys_idx'), ('keys', 'key'),
            ('resources', 'resources_idx'), ('resources', 'resource'), ('resources', 'resource_uri'),
            ('entities', 'entities_idx'), ('entities', 'entity_id'), ('entities', 'entity_uri')]
        expected_df_data = {c: expected_df_data[c[1]] for c in cols_with_categories}
        expected_df = pd.DataFrame.from_dict(expected_df_data)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_add_ref(self):
        er = ExternalResources(name='terms')
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(
            container=data, key='key1',
            resource_name='resource1', resource_uri='uri1',
            entity_id='entity_id1', entity_uri='entity1')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data, [('resource1', 'uri1')])
        self.assertEqual(er.entities.data, [(0, 0, 'entity_id1', 'entity1')])
        self.assertEqual(er.objects.data, [(data.object_id, '', '')])

    def test_add_ref_duplicate_resource(self):
        er = ExternalResources(name='terms')
        er.add_ref(
            container='uuid1', key='key1',
            resource_name='resource0', resource_uri='uri0',
            entity_id='entity_id1', entity_uri='entity1')
        er.add_ref(
            container='uuid2', key='key2',
            resource_name='resource0', resource_uri='uri0',
            entity_id='entity_id2', entity_uri='entity2')
        resource_list = er.resources.which(resource='resource0')
        self.assertEqual(len(resource_list), 1)

    def test_add_ref_bad_arg(self):
        er = ExternalResources(name='terms')
        resource1 = er._add_resource(resource='resource0', uri='resource_uri0')
        # The contents of the message are not important. Just make sure an error is raised
        with self.assertRaises(ValueError):
            er.add_ref(
                'uuid1', key='key1', resource_name='resource1',
                resource_uri='uri1', entity_id='resource_id1')
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', key='key1', resource_name='resource1', resource_uri='uri1', entity_uri='uri1')
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', key='key1', resource_name='resource1', resource_uri='uri1')
        with self.assertRaises(TypeError):
            er.add_ref('uuid1')
        with self.assertRaises(ValueError):
            er.add_ref('uuid1', key='key1', resource_name='resource1')
        with self.assertRaises(ValueError):
            er.add_ref(
                'uuid1', key='key1', resources_idx=resource1,
                resource_name='resource1', resource_uri='uri1')

    def test_add_ref_two_resources(self):
        er = ExternalResources(name='terms')
        er.add_ref(
            container='uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid1', key=er.get_key(key_name='key1'), resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data,
                         [('resource1',  'resource_uri1'),
                          ('resource2', 'resource_uri2')])
        self.assertEqual(er.objects.data, [('uuid1', '', '')])
        self.assertEqual(er.entities.data, [(0, 0, 'id11', 'url11'), (0, 1, 'id12', 'url21')])

    def test_get_resources(self):
        er = ExternalResources(name='terms')
        er.add_ref(
            container='uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        resource = er.get_resource('resource1')
        self.assertIsInstance(resource, Resource)
        with self.assertRaises(ValueError):
            er.get_resource('unknown_resource')

    def test_add_ref_two_keys(self):
        er = ExternalResources(name='terms')
        er.add_ref(
            container='uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', key='key2', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')

        self.assertEqual(er.keys.data, [('key1',), ('key2',)])
        self.assertEqual(er.resources.data,
                         [('resource1',  'resource_uri1'),
                          ('resource2', 'resource_uri2')])
        self.assertEqual(er.entities.data, [(0, 0, 'id11', 'url11'), (1, 1, 'id12', 'url21')])

        self.assertEqual(er.objects.data, [('uuid1', '', ''),
                                           ('uuid2', '', '')])

    def test_add_ref_same_key_diff_objfield(self):
        er = ExternalResources(name='terms')
        er.add_ref(
            container='uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', key='key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')

        self.assertEqual(er.keys.data, [('key1',), ('key1',)])
        self.assertEqual(er.entities.data, [(0, 0, 'id11', 'url11'), (1, 1, 'id12', 'url21')])
        self.assertEqual(er.resources.data,
                         [('resource1',  'resource_uri1'),
                          ('resource2', 'resource_uri2')])
        self.assertEqual(er.objects.data, [('uuid1', '', ''),
                                           ('uuid2', '', '')])

    def test_add_ref_same_keyname(self):
        er = ExternalResources(name='terms')
        er.add_ref(
            container='uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', key='key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        er.add_ref(
            container='uuid3', key='key1', resource_name='resource3',
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
        self.assertEqual(er.objects.data, [('uuid1', '', ''),
                                           ('uuid2', '', ''),
                                           ('uuid3', '', '')])

    def test_get_keys(self):
        er = ExternalResources(name='terms')

        er.add_ref(
            container='uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', key='key2', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        er.add_ref(
            container='uuid1', key=er.get_key(key_name='key1'), resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url31')
        received = er.get_keys()

        expected = pd.DataFrame(
            data=[['key1', 0, 'id11', 'url11'],
                  ['key1', 2, 'id13', 'url31'],
                  ['key2', 1, 'id12', 'url21']],
            columns=['key_name', 'resources_idx', 'entity_id', 'entity_uri'])
        pd.testing.assert_frame_equal(received, expected)

    def test_get_keys_subset(self):
        er = ExternalResources(name='terms')
        er.add_ref(
            container='uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        er.add_ref(
            container='uuid2', key='key2', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        er.add_ref(
            container='uuid1', key=er.get_key(key_name='key1'), resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url31')
        key = er.keys.row[0]
        received = er.get_keys(keys=key)

        expected = pd.DataFrame(
            data=[['key1', 0, 'id11', 'url11'],
                  ['key1', 2, 'id13', 'url31']],
            columns=['key_name', 'resources_idx', 'entity_id', 'entity_uri'])
        pd.testing.assert_frame_equal(received, expected)

    def test_get_object_resources(self):
        er = ExternalResources(name='terms')
        data = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(container=data, key='Mus musculus', resource_name='NCBI_Taxonomy',
                   resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        received = er.get_object_resources(data)
        expected = pd.DataFrame(
            data=[[0, 0, 'NCBI:txid10090', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090']],
            columns=['keys_idx', 'resource_idx', 'entity_id', 'entity_uri'])
        pd.testing.assert_frame_equal(received, expected)

    def test_object_key_unqiueness(self):
        er = ExternalResources(name='terms')
        data = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(container=data, key='Mus musculus', resource_name='NCBI_Taxonomy',
                   resource_uri='https://www.ncbi.nlm.nih.gov/taxonomy',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        existing_key = er.get_key('Mus musculus')
        er.add_ref(container=data, key=existing_key, resource_name='resource2',
                   resource_uri='resource_uri2',
                   entity_id='entity2',
                   entity_uri='entity_uri2')

        self.assertEqual(er.object_keys.data, [(0, 0)])

    def test_check_object_field_add(self):
        er = ExternalResources(name='terms')
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er._check_object_field('uuid1', '')
        er._check_object_field(data, '')

        self.assertEqual(er.objects.data, [('uuid1', '', ''), (data.object_id, '', '')])

    def test_check_object_field_error(self):
        er = ExternalResources(name='terms')
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er._check_object_field(data, '')
        er._add_object(data, '', '')
        with self.assertRaises(ValueError):
            er._check_object_field(data, '')

    def test_add_ref_attribute(self):
        # Test to make sure the attribute object is being used for the id
        # for the external reference.
        table = DynamicTable(name='table', description='table')
        table.add_column(name='col1', description="column")
        table.add_row(id=0, col1='data')

        er = ExternalResources(name='example')
        er.add_ref(container=table,
                   attribute='id',
                   key='key1',
                   resource_name='resource0',
                   resource_uri='resource0_uri',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data, [('resource0', 'resource0_uri')])
        self.assertEqual(er.entities.data, [(0, 0, 'entity_0', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(table.id.object_id, '', '')])

    def test_add_ref_column_as_attribute(self):
        # Test to make sure the attribute object is being used for the id
        # for the external reference.
        table = DynamicTable(name='table', description='table')
        table.add_column(name='col1', description="column")
        table.add_row(id=0, col1='data')

        er = ExternalResources(name='example')
        er.add_ref(container=table,
                   attribute='col1',
                   key='key1',
                   resource_name='resource0',
                   resource_uri='resource0_uri',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data, [('resource0', 'resource0_uri')])
        self.assertEqual(er.entities.data, [(0, 0, 'entity_0', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(table['col1'].object_id, '', '')])

    def test_add_ref_compound_data(self):
        er = ExternalResources(name='example')

        data = Data(
            name='data_name',
            data=np.array(
                [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
                dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))
        er.add_ref(
            container=data,
            field='species',
            key='Mus musculus',
            resource_name='NCBI_Taxonomy',
            resource_uri='resource0_uri',
            entity_id='NCBI:txid10090',
            entity_uri='entity_0_uri'
        )
        self.assertEqual(er.keys.data, [('Mus musculus',)])
        self.assertEqual(er.resources.data, [('NCBI_Taxonomy', 'resource0_uri')])
        self.assertEqual(er.entities.data, [(0, 0, 'NCBI:txid10090', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(data.object_id, '', 'species')])


class TestExternalResourcesNestedAttributes(TestCase):

    def setUp(self):
        self.attr1 = AttributeSpec(name='attr1', doc='a string attribute', dtype='text')
        self.attr2 = AttributeSpec(name='attr2', doc='an integer attribute', dtype='int')
        self.attr3 = AttributeSpec(name='attr3', doc='an integer attribute', dtype='int')
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[
                DatasetSpec(
                    doc='a dataset',
                    dtype='int',
                    name='data',
                    attributes=[self.attr2]
                )
            ],
            attributes=[self.attr1])

        specs = [self.bar_spec]
        containers = {'Bar': Bar}
        self.type_map = create_test_type_map(specs, containers)
        self.spec_catalog = self.type_map.namespace_catalog.get_namespace(CORE_NAMESPACE).catalog

        self.cls = self.type_map.get_dt_container_cls(self.bar_spec.data_type)
        self.bar = self.cls(name='bar', data=[1], attr1='attr1', attr2=1)
        obj_mapper_bar = self.type_map.get_map(self.bar)
        obj_mapper_bar.map_spec('attr2', spec=self.attr2)

    def test_add_ref_nested(self):
        table = DynamicTable(name='table', description='table')
        table.add_column(name='col1', description="column")
        table.add_row(id=0, col1='data')

        er = ExternalResources(name='example')
        er.add_ref(container=table,
                   attribute='description',
                   key='key1',
                   resource_name='resource0',
                   resource_uri='resource0_uri',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.resources.data, [('resource0', 'resource0_uri')])
        self.assertEqual(er.entities.data, [(0, 0, 'entity_0', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(table.object_id, 'DynamicTable/description', '')])

    def test_add_ref_deep_nested(self):
        er = ExternalResources(name='example', type_map=self.type_map)
        er.add_ref(container=self.bar,
                   attribute='attr2',
                   key='key1',
                   resource_name='resource0',
                   resource_uri='resource0_uri',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')
        self.assertEqual(er.objects.data[0][1], 'Bar/data/attr2', '')


class TestExternalResourcesGetKey(TestCase):

    def setUp(self):
        self.er = ExternalResources(name='terms')

    def test_get_key(self):
        self.er.add_ref(
            'uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', key='key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')

        keys = self.er.get_key('key1', 'uuid2', '')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.idx, 1)

    def test_get_key_bad_arg(self):
        self.er._add_key('key2')
        self.er.add_ref(
            'uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        with self.assertRaises(ValueError):
            self.er.get_key('key2', 'uuid1', '')

    @unittest.skip('Outdated do to privatization')
    def test_get_key_without_container(self):
        self.er = ExternalResources(name='terms')
        self.er._add_key('key1')
        keys = self.er.get_key('key1')
        self.assertIsInstance(keys, Key)

    def test_get_key_w_object_info(self):
        self.er.add_ref(
            'uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', key='key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        keys = self.er.get_key('key1', 'uuid1', '')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.key, 'key1')

    def test_get_key_w_bad_object_info(self):
        self.er.add_ref(
            'uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', key='key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')

        with self.assertRaisesRegex(ValueError, "No key 'key2'"):
            self.er.get_key('key2', 'uuid1', '')

    def test_get_key_doesnt_exist(self):
        self.er.add_ref(
            'uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', key='key1', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url21')
        with self.assertRaisesRegex(ValueError, "key 'bad_key' does not exist"):
            self.er.get_key('bad_key')

    @unittest.skip('Outdated do to privatization')
    def test_get_key_same_keyname_all(self):
        self.er = ExternalResources(name='terms')
        key1 = self.er._add_key('key1')
        key2 = self.er._add_key('key1')
        self.er.add_ref(
            'uuid1', key=key1, resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', key=key2, resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url12')
        self.er.add_ref(
            'uuid1', key=self.er.get_key('key1', 'uuid1', ''), resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url13')

        keys = self.er.get_key('key1')

        self.assertIsInstance(keys, Key)
        self.assertEqual(keys[0].key, 'key1')
        self.assertEqual(keys[1].key, 'key1')

    def test_get_key_same_keyname_specific(self):
        self.er = ExternalResources(name='terms')

        self.er.add_ref(
            'uuid1', key='key1', resource_name='resource1',
            resource_uri='resource_uri1', entity_id="id11", entity_uri='url11')
        self.er.add_ref(
            'uuid2', key='key2', resource_name='resource2',
            resource_uri='resource_uri2', entity_id="id12", entity_uri='url12')
        self.er.add_ref(
            'uuid1', key=self.er.get_key('key1', 'uuid1', ''), resource_name='resource3',
            resource_uri='resource_uri3', entity_id="id13", entity_uri='url13')

        keys = self.er.get_key('key1', 'uuid1', '')
        self.assertIsInstance(keys, Key)
        self.assertEqual(keys.key, 'key1')
        self.assertEqual(self.er.keys.data, [('key1',), ('key2',)])
