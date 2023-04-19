import pandas as pd
from hdmf.common import DynamicTable
from hdmf.common.resources import ExternalResources, Key
from hdmf import Data, Container
from hdmf.testing import TestCase, H5RoundTripMixin, remove_test_file
import numpy as np
import unittest
from tests.unit.build_tests.test_io_map import Bar
from tests.unit.utils import create_test_type_map, CORE_NAMESPACE
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec


class TestExternalResources(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        er = ExternalResources()
        file=Container(name='file')
        file2=Container(name='file2')
        er.add_ref(file=file,
            container=Container(name='Container'), key='key1',
            entity_id="id11", entity_uri='url11')
        er.add_ref(file=file2,
            container=Container(name='Container2'), key='key2',
            entity_id="id12", entity_uri='url12')

        return er

    def remove_er_files(self):
        remove_test_file('./entities.tsv')
        remove_test_file('./objects.tsv')
        remove_test_file('./object_keys.tsv')
        remove_test_file('./keys.tsv')
        remove_test_file('./files.tsv')
        remove_test_file('./er.tsv')



    def test_to_dataframe(self):
        # Setup complex external resources with keys reused across objects and
        # multiple resources per key
        er = ExternalResources()
        # Add a species dataset with 2 keys
        data1 = Data(
            name='data_name',
            data=np.array(
                [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
                dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]
            )
        )
        file = Container(name='file')
        k1, e1 = er.add_ref(file=file,
            container=data1,
            field='species',
            key='Mus musculus',
            entity_id='NCBI:txid10090',
            entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090'
        )
        k2, e2 = er.add_ref(file=file,
            container=data1,
            field='species',
            key='Homo sapiens',
            entity_id='NCBI:txid9606',
            entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'
        )
        # Convert to dataframe and compare against the expected result
        result_df = er.to_dataframe()
        expected_df_data = \
            {'files_idx': {0: 0, 1: 0},
             'file_id': {0: file.object_id, 1: file.object_id},
             'objects_idx': {0: 0, 1: 0},
             'object_id': {0: data1.object_id, 1: data1.object_id},
             'file_id_idx': {0: 0, 1: 0},
             'object_type': {0: 'Data', 1: 'Data'},
             'relative_path': {0: '', 1: ''},
             'field': {0: 'species', 1: 'species'},
             'keys_idx': {0: 0, 1: 1},
             'key': {0: 'Mus musculus', 1: 'Homo sapiens'},
             'entities_idx': {0: 0, 1: 1},
             'entity_id': {0: 'NCBI:txid10090', 1: 'NCBI:txid9606'},
             'entity_uri': {0: 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090',
                            1: 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_assert_external_resources_equal(self):
        file = Container(name='file')
        ref_container_1 = Container(name='Container_1')
        er_left = ExternalResources()
        er_left.add_ref(file=file,
            container=ref_container_1, key='key1',
            entity_id="id11", entity_uri='url11')

        er_right = ExternalResources()
        er_right.add_ref(file=file,
            container=ref_container_1, key='key1',
            entity_id="id11", entity_uri='url11')

        self.assertTrue(ExternalResources.assert_external_resources_equal(er_left,
                                                                          er_right))

    def test_invalid_keys_assert_external_resources_equal(self):
        er_left = ExternalResources()
        er_left.add_ref(file=Container(name='file'),
            container=Container(name='Container'), key='key1',
            entity_id="id11", entity_uri='url11')

        er_right = ExternalResources()
        er_right.add_ref(file=Container(name='file'),
            container=Container(name='Container'), key='invalid',
            entity_id="id11", entity_uri='url11')

        with self.assertRaises(AssertionError):
            ExternalResources.assert_external_resources_equal(er_left,
                                                              er_right)

    def test_invalid_objects_assert_external_resources_equal(self):
        er_left = ExternalResources()
        er_left.add_ref(file=Container(name='file'),
            container=Container(name='Container'), key='key1',
            entity_id="id11", entity_uri='url11')

        er_right = ExternalResources()
        er_right.add_ref(file=Container(name='file'),
            container=Container(name='Container'), key='key1',
            entity_id="id11", entity_uri='url11')

        with self.assertRaises(AssertionError):
            ExternalResources.assert_external_resources_equal(er_left,
                                                              er_right)

    def test_invalid_entity_assert_external_resources_equal(self):
        er_left = ExternalResources()
        er_left.add_ref(file=Container(name='file'),
            container=Container(name='Container'), key='key1',
            entity_id="invalid", entity_uri='invalid')

        er_right = ExternalResources()
        er_right.add_ref(file=Container(name='file'),
            container=Container(name='Container'), key='key1',
            entity_id="id11", entity_uri='url11')

        with self.assertRaises(AssertionError):
            ExternalResources.assert_external_resources_equal(er_left,
                                                              er_right)

    def test_invalid_object_keys_assert_external_resources_equal(self):
        er_left = ExternalResources()
        er_left.add_ref(file=Container(name='file'),
            container=Container(name='Container'), key='invalid',
            entity_id="id11", entity_uri='url11')

        er_right = ExternalResources()
        er_right._add_key('key')
        er_right.add_ref(file=Container(name='file'),
            container=Container(name='Container'), key='key1',
            entity_id="id11", entity_uri='url11')

        with self.assertRaises(AssertionError):
            ExternalResources.assert_external_resources_equal(er_left,
                                                              er_right)

    def test_add_ref(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=Container(name='file'),
            container=data, key='key1',
            entity_id='entity_id1', entity_uri='entity1')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [(0, 'entity_id1', 'entity1')])
        self.assertEqual(er.objects.data, [(0, data.object_id, 'Data', '', '')])

    def test_to_and_from_norm_tsv(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=Container(name='file'),
            container=data, key='key1',
            entity_id='entity_id1', entity_uri='entity1')
        er.to_norm_tsv(path='./')

        er_read = ExternalResources.from_norm_tsv(path='./')
        ExternalResources.assert_external_resources_equal(er_read, er, check_dtype=False)

        self.remove_er_files()

    def test_to_and_from_norm_tsv_entity_value_error(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=Container(name='file'),
            container=data, key='key1',
            entity_id='entity_id1', entity_uri='entity1')
        er.to_norm_tsv(path='./')

        df = er.entities.to_dataframe()
        df.at[0, ('keys_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./entities.tsv', sep='\t', index=False)

        msg = "Key Index out of range in EntityTable. Please check for alterations."
        with self.assertRaisesWith(ValueError, msg):
            _ = ExternalResources.from_norm_tsv(path='./')

        self.remove_er_files()

    def test_to_and_from_norm_tsv_object_value_error(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=Container(name='file'),
            container=data, key='key1',
            entity_id='entity_id1', entity_uri='entity1')
        er.to_norm_tsv(path='./')

        df = er.objects.to_dataframe()
        df.at[0, ('file_id_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./objects.tsv', sep='\t', index=False)

        msg = "File_ID Index out of range in ObjectTable. Please check for alterations."
        with self.assertRaisesWith(ValueError, msg):
            _ = ExternalResources.from_norm_tsv(path='./')

        self.remove_er_files()

    def test_to_and_from_norm_tsv_object_keys_object_idx_value_error(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=Container(name='file'),
            container=data, key='key1',
            entity_id='entity_id1', entity_uri='entity1')
        er.to_norm_tsv(path='./')

        df = er.object_keys.to_dataframe()
        df.at[0, ('objects_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./object_keys.tsv', sep='\t', index=False)

        msg = "Object Index out of range in ObjectKeyTable. Please check for alterations."
        with self.assertRaisesWith(ValueError, msg):
            _ = ExternalResources.from_norm_tsv(path='./')

        self.remove_er_files()

    def test_to_and_from_norm_tsv_object_keys_key_idx_value_error(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=Container(name='file'),
            container=data, key='key1',
            entity_id='entity_id1', entity_uri='entity1')
        er.to_norm_tsv(path='./')

        df = er.object_keys.to_dataframe()
        df.at[0, ('keys_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./object_keys.tsv', sep='\t', index=False)

        msg = "Key Index out of range in ObjectKeyTable. Please check for alterations."
        with self.assertRaisesWith(ValueError, msg):
            _ = ExternalResources.from_norm_tsv(path='./')

        self.remove_er_files()

    # def test_to_flat_tsv_and_from_flat_tsv(self):
    #     # write er to file
    #     er = ExternalResources()
    #     data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
    #     er.add_ref(file=Container(name='file'),
    #         container=data, key='key1',
    #         entity_id='entity_id1', entity_uri='entity1')
    #     er.to_flat_tsv(path='./er.tsv')
    #     # breakpoint()
    #     # read er back from file and compare
    #     er_obj = ExternalResources.from_flat_tsv(path='./er.tsv')
    #     # Check that the data is correct
    #     ExternalResources.assert_external_resources_equal(er_obj, er, check_dtype=False)
    #     self.remove_er_files()

    # def test_to_flat_tsv_and_from_flat_tsv_missing_keyidx(self):
    #     # write er to file
    #     df = self.container.to_dataframe(use_categories=True)
    #     df.at[0, ('keys', 'keys_idx')] = 10  # Change key_ix 0 to 10
    #     df.to_csv(self.export_filename, sep='\t')
    #     # read er back from file and compare
    #     msg = "Missing keys_idx entries [0, 2, 3, 4, 5, 6, 7, 8, 9]"
    #     with self.assertRaisesWith(ValueError, msg):
    #         _ = ExternalResources.from_flat_tsv(path=self.export_filename)
    #
    # def test_to_flat_tsv_and_from_flat_tsv_missing_objectidx(self):
    #     # write er to file
    #     df = self.container.to_dataframe(use_categories=True)
    #     df.at[0, ('objects', 'objects_idx')] = 10  # Change objects_idx 0 to 10
    #     df.to_csv(self.export_filename, sep='\t')
    #     # read er back from file and compare
    #     msg = "Missing objects_idx entries [0, 2, 3, 4, 5, 6, 7, 8, 9]"
    #     with self.assertRaisesWith(ValueError, msg):
    #         _ = ExternalResources.from_flat_tsv(path=self.export_filename)
    #
    # def test_to_flat_tsv_and_from_flat_tsv_missing_fileidx(self):
    #     # write er to file
    #     df = self.container.to_dataframe(use_categories=True)
    #     df.at[0, ('objects', 'file_idx')] = 10  # Change file_idx 0 to 10
    #     df.to_csv(self.export_filename, sep='\t')
    #     # read er back from file and compare
    #     msg = "Missing file_idx entries [0, 2, 3, 4, 5, 6, 7, 8, 9]"
    #     with self.assertRaisesWith(ValueError, msg):
    #         _ = ExternalResources.from_flat_tsv(path=self.export_filename)

    def test_to_flat_tsv_and_from_flat_tsv_missing_entitiesidx(self):
        # write er to file
        er_df = self.container.to_dataframe(use_categories=True)
        er_df.at[0, ('entities', 'entities_idx')] = 10  # Change entities_idx 0 to 10
        er_df.to_csv('./er.tsv', sep='\t')
        # read er back from file and compare
        msg = "Missing entities_idx entries [0, 2, 3, 4, 5, 6, 7, 8, 9]"
        # breakpoint()
        with self.assertRaisesWith(ValueError, msg):
            _ = ExternalResources.from_flat_tsv(path='./er.tsv')

        self.remove_er_files()

    def test_add_ref_two_keys(self):
        er = ExternalResources()
        ref_container_1 = Container(name='Container_1')
        ref_container_2 = Container(name='Container_2')
        er.add_ref(file=Container(name='file'),
            container=ref_container_1, key='key1', entity_id="id11", entity_uri='url11')
        er.add_ref(file=Container(name='file'),
            container=ref_container_2, key='key2', entity_id="id12", entity_uri='url21')

        self.assertEqual(er.keys.data, [('key1',), ('key2',)])
        self.assertEqual(er.entities.data, [(0, 'id11', 'url11'), (1, 'id12', 'url21')])

        self.assertEqual(er.objects.data, [(0, ref_container_1.object_id, 'Container', '', ''),
                                           (1, ref_container_2.object_id, 'Container', '', '')])

    def test_add_ref_same_key_diff_objfield(self):
        er = ExternalResources()
        ref_container_1 = Container(name='Container_1')
        ref_container_2 = Container(name='Container_2')
        er.add_ref(file=Container(name='file'),
            container=ref_container_1, key='key1', entity_id="id11", entity_uri='url11')
        er.add_ref(file=Container(name='file'),
            container=ref_container_2, key='key1', entity_id="id12", entity_uri='url21')

        self.assertEqual(er.keys.data, [('key1',), ('key1',)])
        self.assertEqual(er.entities.data, [(0, 'id11', 'url11'), (1, 'id12', 'url21')])
        self.assertEqual(er.objects.data, [(0, ref_container_1.object_id, 'Container', '', ''),
                                           (1, ref_container_2.object_id, 'Container', '', '')])

    def test_add_ref_same_keyname(self):
        er = ExternalResources()
        ref_container_1 = Container(name='Container_1')
        ref_container_2 = Container(name='Container_2')
        ref_container_3 = Container(name='Container_2')
        er.add_ref(file=Container(name='file'),
            container=ref_container_1, key='key1', entity_id="id11", entity_uri='url11')
        er.add_ref(file=Container(name='file'),
            container=ref_container_2, key='key1', entity_id="id12", entity_uri='url21')
        er.add_ref(file=Container(name='file'),
            container=ref_container_3, key='key1', entity_id="id13", entity_uri='url31')
        self.assertEqual(er.keys.data, [('key1',), ('key1',), ('key1',)])
        self.assertEqual(
            er.entities.data,
            [(0, 'id11', 'url11'),
             (1, 'id12', 'url21'),
             (2, 'id13', 'url31')])
        self.assertEqual(er.objects.data, [(0, ref_container_1.object_id, 'Container', '', ''),
                                           (1, ref_container_2.object_id, 'Container', '', ''),
                                           (2, ref_container_3.object_id, 'Container', '', '')])

    # def test_get_keys(self):
    #     er = ExternalResources()
    #
    #     er.add_ref(file=Container(name='file'),
    #         container=Container(name='Container'), key='key1', entity_id="id11", entity_uri='url11')
    #     er.add_ref(file=Container(name='file'),
    #         container=Container(name='Container'), key='key2', entity_id="id12", entity_uri='url21')
    #     er.add_ref(file=Container(name='file'),
    #         container=Container(name='Container'), key=er.get_key(key_name='key1'), entity_id="id13", entity_uri='url31')
    #     received = er.get_keys()
    #
    #     expected = pd.DataFrame(
    #         data=[['key1', 'id11', 'url11'],
    #               ['key1', 'id13', 'url31'],
    #               ['key2', 'id12', 'url21']],
    #         columns=['key_name', 'entity_id', 'entity_uri'])
    #     pd.testing.assert_frame_equal(received, expected)
    #
    # def test_get_keys_subset(self):
    #     er = ExternalResources()
    #     er.add_ref(file=Container(name='file'),
    #         container=Container(name='Container'), key='key1', entity_id="id11", entity_uri='url11')
    #     er.add_ref(file=Container(name='file'),
    #         container=Container(name='Container'), key='key2', entity_id="id12", entity_uri='url21')
    #     er.add_ref(file=Container(name='file'),
    #         container=Container(name='Container'), key=er.get_key(key_name='key1'), entity_id="id13",
    #         entity_uri='url31')
    #     key = er.keys.row[0]
    #     received = er.get_keys(keys=key)
    #
    #     expected = pd.DataFrame(
    #         data=[['key1', 'id11', 'url11'],
    #               ['key1', 'id13', 'url31']],
    #         columns=['key_name', 'entity_id', 'entity_uri'])
    #     pd.testing.assert_frame_equal(received, expected)

    def test_object_key_unqiueness(self):
        er = ExternalResources()
        data = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=Container(name='file'),container=data, key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        existing_key = er.get_key('Mus musculus')
        er.add_ref(file=Container(name='file'),container=data, key=existing_key,
                   entity_id='entity2',
                   entity_uri='entity_uri2')

        self.assertEqual(er.object_keys.data, [(0, 0)])

    def test_check_object_field_add(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er._check_object_field(container=data, file=Container(name='file'), relative_path='', field='')

        self.assertEqual(er.objects.data, [(0, data.object_id, 'Data', '', '')])

    def test_check_object_field_multi_error(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er._check_object_field(container=data, file=Container(name='file'), relative_path='', field='')
        er._add_object(file_id_idx=0,container=data, relative_path='', field='')
        with self.assertRaises(ValueError):
            er._check_object_field(container=data, file=Container(name='file'), relative_path='', field='')

    def test_check_object_field_not_in_obj_table(self):
        er = ExternalResources()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        with self.assertRaises(ValueError):
            er._check_object_field(container=data, file=Container(name='file'), relative_path='', field='', create=False)

    def test_add_ref_attribute(self):
        # Test to make sure the attribute object is being used for the id
        # for the external reference.
        table = DynamicTable(name='table', description='table')
        table.add_column(name='col1', description="column")
        table.add_row(id=0, col1='data')

        er = ExternalResources()
        er.add_ref(file=Container(name='file'),container=table,
                   attribute='id',
                   key='key1',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [(0, 'entity_0', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(0, table.id.object_id, 'ElementIdentifiers', '', '')])

    def test_add_ref_column_as_attribute(self):
        # Test to make sure the attribute object is being used for the id
        # for the external reference.
        table = DynamicTable(name='table', description='table')
        table.add_column(name='col1', description="column")
        table.add_row(id=0, col1='data')

        er = ExternalResources()
        er.add_ref(file=Container(name='file'),container=table,
                   attribute='col1',
                   key='key1',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [(0, 'entity_0', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(0, table['col1'].object_id, 'VectorData', '', '')])

    def test_add_ref_compound_data(self):
        er = ExternalResources()

        data = Data(
            name='data_name',
            data=np.array(
                [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
                dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))
        er.add_ref(file=Container(name='file'),
            container=data,
            field='species',
            key='Mus musculus',
            entity_id='NCBI:txid10090',
            entity_uri='entity_0_uri'
        )
        self.assertEqual(er.keys.data, [('Mus musculus',)])
        self.assertEqual(er.entities.data, [(0, 'NCBI:txid10090', 'entity_0_uri')])
        file=Container(name='file'),self.assertEqual(er.objects.data, [(0, data.object_id, 'Data', '', 'species')])

    @unittest.skip('Not needed due to read/write to tsv tests')
    def test_roundtrip(self):
        return

    @unittest.skip('Not needed due to read/write to tsv tests')
    def test_roundtrip_export(self):
        return
#
#
# class TestExternalResourcesNestedAttributes(TestCase):
#
#     def setUp(self):
#         self.attr1 = AttributeSpec(name='attr1', doc='a string attribute', dtype='text')
#         self.attr2 = AttributeSpec(name='attr2', doc='an integer attribute', dtype='int')
#         self.attr3 = AttributeSpec(name='attr3', doc='an integer attribute', dtype='int')
#         self.bar_spec = GroupSpec(
#             doc='A test group specification with a data type',
#             data_type_def='Bar',
#             datasets=[
#                 DatasetSpec(
#                     doc='a dataset',
#                     dtype='int',
#                     name='data',
#                     attributes=[self.attr2]
#                 )
#             ],
#             attributes=[self.attr1])
#
#         specs = [self.bar_spec]
#         containers = {'Bar': Bar}
#         self.type_map = create_test_type_map(specs, containers)
#         self.spec_catalog = self.type_map.namespace_catalog.get_namespace(CORE_NAMESPACE).catalog
#
#         self.cls = self.type_map.get_dt_container_cls(self.bar_spec.data_type)
#         self.bar = self.cls(name='bar', data=[1], attr1='attr1', attr2=1)
#         obj_mapper_bar = self.type_map.get_map(self.bar)
#         obj_mapper_bar.map_spec('attr2', spec=self.attr2)
#
#     def test_add_ref_nested(self):
#         table = DynamicTable(name='table', description='table')
#         table.add_column(name='col1', description="column")
#         table.add_row(id=0, col1='data')
#
#         er = ExternalResources()
#         er.add_ref(file=Container(name='file'),container=table,
#                    attribute='description',
#                    key='key1',
#                    entity_id='entity_0',
#                    entity_uri='entity_0_uri')
#         self.assertEqual(er.keys.data, [('key1',)])
#         self.assertEqual(er.entities.data, [(0, 'entity_0', 'entity_0_uri')])
#         self.assertEqual(er.objects.data, [(0, table.object_id, 'description', '')])
#
#     def test_add_ref_deep_nested(self):
#         er = ExternalResources(type_map=self.type_map)
#         er.add_ref(file=Container(name='file'),container=self.bar,
#                    attribute='attr2',
#                    key='key1',
#                    entity_id='entity_0',
#                    entity_uri='entity_0_uri')
#         self.assertEqual(er.objects.data[0][2], 'data/attr2', '')
#
#
# class TestExternalResourcesGetKey(TestCase):
#
#     def setUp(self):
#         self.er = ExternalResources()
#
#     def test_get_key(self):
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id11", entity_uri='url11')
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id12", entity_uri='url21')
#
#         keys = self.er.get_key('key1', 'uuid2', '')
#         self.assertIsInstance(keys, Key)
#         self.assertEqual(keys.idx, 1)
#
#     def test_get_key_bad_arg(self):
#         self.er._add_key('key2')
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id11", entity_uri='url11')
#         with self.assertRaises(ValueError):
#             self.er.get_key('key2', 'uuid1', '')
#
#     def test_get_key_w_object_info(self):
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id11", entity_uri='url11')
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id12", entity_uri='url21')
#         keys = self.er.get_key('key1', 'uuid1', '')
#         self.assertIsInstance(keys, Key)
#         self.assertEqual(keys.key, 'key1')
#
#     def test_get_key_w_bad_object_info(self):
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id11", entity_uri='url11')
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id12", entity_uri='url21')
#
#         with self.assertRaisesRegex(ValueError, "No key 'key2'"):
#             self.er.get_key('key2', 'uuid1', '')
#
#     def test_get_key_doesnt_exist(self):
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id11", entity_uri='url11')
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id12", entity_uri='url21')
#         with self.assertRaisesRegex(ValueError, "key 'bad_key' does not exist"):
#             self.er.get_key('bad_key')
#
#
#     def test_get_key_same_keyname_specific(self):
#         self.er = ExternalResources()
#
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key1', entity_id="id11", entity_uri='url11')
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key='key2', entity_id="id12", entity_uri='url12')
#         self.er.add_ref(file=Container(name='file'),
#             container=Container(name='Container'), key=self.er.get_key('key1', 'uuid1', ''), entity_id="id13", entity_uri='url13')
#
#         keys = self.er.get_key('key1', 'uuid1', '')
#         self.assertIsInstance(keys, Key)
#         self.assertEqual(keys.key, 'key1')
#         self.assertEqual(self.er.keys.data, [('key1',), ('key2',)])
