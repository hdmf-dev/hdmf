import pandas as pd
import unittest
from hdmf.common import DynamicTable, VectorData
from hdmf import TermSet, TermSetWrapper
from hdmf.common.resources import HERD, Key
from hdmf import Data, Container, HERDManager
from hdmf.testing import TestCase, H5RoundTripMixin, remove_test_file
import numpy as np
from tests.unit.build_tests.test_io_map import Bar
from tests.unit.helpers.utils import create_test_type_map, CORE_NAMESPACE
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec
from glob import glob
import zipfile

try:
    import linkml_runtime  # noqa: F401
    LINKML_INSTALLED = True
except ImportError:
    LINKML_INSTALLED = False


class HERDManagerContainer(Container, HERDManager):
    def __init__(self, **kwargs):
        kwargs['name'] = 'HERDManagerContainer'
        super().__init__(**kwargs)


class TestHERD(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        er = HERD()
        file = HERDManagerContainer(name='file')
        file2 = HERDManagerContainer(name='file2')
        er.add_ref(file=file,
                   container=file,
                   key='special',
                   entity_id="id11",
                   entity_uri='url11')
        er.add_ref(file=file2,
                   container=file2,
                   key='key2',
                   entity_id="id12",
                   entity_uri='url12')

        return er

    def remove_er_files(self):
        remove_test_file('./entities.tsv')
        remove_test_file('./entity_keys.tsv')
        remove_test_file('./objects.tsv')
        remove_test_file('./object_keys.tsv')
        remove_test_file('./keys.tsv')
        remove_test_file('./files.tsv')
        remove_test_file('./HERD.zip')

    def child_tsv(self, external_resources):
        for child in external_resources.children:
            df = child.to_dataframe()
            df.to_csv('./'+child.name+'.tsv', sep='\t', index=False)

    def zip_child(self, zip_file):
        files = glob('*.tsv')
        with zipfile.ZipFile(zip_file, 'w') as zipF:
          for file in files:
              zipF.write(file)

    def test_to_dataframe(self):
        # Setup complex external resources with keys reused across objects and
        # multiple resources per key
        er = HERD()
        # Add a species dataset with 2 keys
        data1 = Data(
            name='data_name',
            data=np.array(
                [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
                dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]
            )
        )

        data2 = Data(
            name='data_name',
            data=np.array(
                [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
                dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]
            )
        )

        file_1 = HERDManagerContainer(name='file_1')
        file_2 = HERDManagerContainer(name='file_2')

        k1, e1 = er.add_ref(file=file_1,
                             container=data1,
                             field='species',
                             key='Mus musculus',
                             entity_id='NCBI:txid10090',
                             entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        k2, e2 = er.add_ref(file=file_2,
                            container=data2,
                            field='species',
                            key='Homo sapiens',
                            entity_id='NCBI:txid9606',
                            entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606')

        # Convert to dataframe and compare against the expected result
        result_df = er.to_dataframe()
        expected_df_data = \
            {'file_object_id': {0: file_1.object_id, 1: file_2.object_id},
             'objects_idx': {0: 0, 1: 1},
             'object_id': {0: data1.object_id, 1: data2.object_id},
             'files_idx': {0: 0, 1: 1},
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
        expected_df = expected_df.astype({'keys_idx': 'uint32',
                                          'objects_idx': 'uint32',
                                          'files_idx': 'uint32',
                                          'entities_idx': 'uint32'})
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_assert_external_resources_equal(self):
        file = HERDManagerContainer(name='file')
        ref_container_1 = Container(name='Container_1')
        er_left = HERD()
        er_left.add_ref(file=file,
                        container=ref_container_1,
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        er_right = HERD()
        er_right.add_ref(file=file,
                         container=ref_container_1,
                         key='key1',
                         entity_id="id11",
                         entity_uri='url11')

        self.assertTrue(HERD.assert_external_resources_equal(er_left,
                                                                          er_right))

    def test_invalid_keys_assert_external_resources_equal(self):
        er_left = HERD()
        er_left.add_ref(file=HERDManagerContainer(name='file'),
                        container=Container(name='Container'),
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        er_right = HERD()
        er_right.add_ref(file=HERDManagerContainer(name='file'),
                         container=Container(name='Container'),
                         key='invalid',
                         entity_id="id11",
                         entity_uri='url11')

        with self.assertRaises(AssertionError):
            HERD.assert_external_resources_equal(er_left,
                                                              er_right)

    def test_invalid_objects_assert_external_resources_equal(self):
        er_left = HERD()
        er_left.add_ref(file=HERDManagerContainer(name='file'),
                        container=Container(name='Container'),
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        er_right = HERD()
        er_right.add_ref(file=HERDManagerContainer(name='file'),
                         container=Container(name='Container'),
                         key='key1',
                         entity_id="id11",
                         entity_uri='url11')

        with self.assertRaises(AssertionError):
            HERD.assert_external_resources_equal(er_left,
                                                              er_right)

    def test_invalid_entity_assert_external_resources_equal(self):
        er_left = HERD()
        er_left.add_ref(file=HERDManagerContainer(name='file'),
                        container=Container(name='Container'),
                        key='key1',
                        entity_id="invalid",
                        entity_uri='invalid')

        er_right = HERD()
        er_right.add_ref(file=HERDManagerContainer(name='file'),
                         container=Container(name='Container'),
                         key='key1',
                         entity_id="id11",
                         entity_uri='url11')

        with self.assertRaises(AssertionError):
            HERD.assert_external_resources_equal(er_left,
                                                              er_right)

    def test_invalid_object_keys_assert_external_resources_equal(self):
        er_left = HERD()
        er_left.add_ref(file=HERDManagerContainer(name='file'),
                        container=Container(name='Container'),
                        key='invalid',
                        entity_id="id11",
                        entity_uri='url11')

        er_right = HERD()
        er_right._add_key('key')
        er_right.add_ref(file=HERDManagerContainer(name='file'),
                         container=Container(name='Container'),
                         key='key1',
                         entity_id="id11",
                         entity_uri='url11')

        with self.assertRaises(AssertionError):
            HERD.assert_external_resources_equal(er_left,
                                                              er_right)

    def test_add_ref_search_for_file(self):
        em = HERDManagerContainer()
        er = HERD()
        er.add_ref(container=em, key='key1',
                   entity_id='entity_id1', entity_uri='entity1')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [('entity_id1', 'entity1')])
        self.assertEqual(er.objects.data, [(0, em.object_id, 'HERDManagerContainer', '', '')])

    def test_add_ref_search_for_file_parent(self):
        em = HERDManagerContainer()

        child = Container(name='child')
        child.parent = em

        er = HERD()
        er.add_ref(container=child, key='key1',
                   entity_id='entity_id1', entity_uri='entity1')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [('entity_id1', 'entity1')])
        self.assertEqual(er.objects.data, [(0, child.object_id, 'Container', '', '')])

    def test_add_ref_search_for_file_nested_parent(self):
        em = HERDManagerContainer()

        nested_child = Container(name='nested_child')
        child = Container(name='child')
        nested_child.parent = child
        child.parent = em

        er = HERD()
        er.add_ref(container=nested_child, key='key1',
                   entity_id='entity_id1', entity_uri='entity1')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [('entity_id1', 'entity1')])
        self.assertEqual(er.objects.data, [(0, nested_child.object_id, 'Container', '', '')])

    def test_add_ref_search_for_file_error(self):
        container = Container(name='container')
        er = HERD()

        with self.assertRaises(ValueError):
            er.add_ref(container=container,
                       key='key1',
                       entity_id='entity_id1',
                       entity_uri='entity1')

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_check_termset_wrapper(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')

        # create children and add parent
        col1 = VectorData(
            name='Species_1',
            description='...',
            data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
        )
        species = DynamicTable(name='species', description='My species', columns=[col1])
        objs = species.all_children()

        er = HERD()
        ret = er._HERD__check_termset_wrapper(objs)

        self.assertTrue(isinstance(ret[0][0], VectorData))
        self.assertEqual(ret[0][1], 'data')
        self.assertTrue(isinstance(ret[0][2], TermSetWrapper))

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_add_ref_termset_data(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        er = HERD()
        em = HERDManagerContainer()
        em.link_resources(er)

        # create children and add parent
        col1 = VectorData(
            name='Species_1',
            description='...',
            data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
        )
        species = DynamicTable(name='species', description='My species', columns=[col1])

        species.parent = em

        er.add_ref_term_set(root_container=em)
        self.assertEqual(er.keys.data, [('Homo sapiens',)])
        self.assertEqual(er.entities.data, [('NCBI_TAXON:9606',
        'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606')])
        self.assertEqual(er.objects.data, [(0, col1.object_id, 'VectorData', '', '')])

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_add_ref_termset_attr(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        er = HERD()
        em = HERDManagerContainer()
        em.link_resources(er)

        # create children and add parent
        col1 = VectorData(
            name='Species_1',
            description=TermSetWrapper(value='Homo sapiens', termset=terms),
            data=['Human']
        )
        species = DynamicTable(name='species', description='My species', columns=[col1])

        species.parent = em

        er.add_ref_term_set(root_container=em)
        self.assertEqual(er.keys.data, [('Homo sapiens',)])
        self.assertEqual(er.entities.data, [('NCBI_TAXON:9606',
        'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606')])
        self.assertEqual(er.objects.data, [(0, col1.object_id, 'VectorData', 'description', '')])

    def test_get_file_from_container(self):
        file = HERDManagerContainer(name='file')
        container = Container(name='name')
        container.parent = file
        er = HERD()
        retrieved = er._get_file_from_container(container)

        self.assertEqual(file.name, retrieved.name)

    def test_get_file_from_container_file_is_container(self):
        file = HERDManagerContainer(name='file')
        er = HERD()
        retrieved = er._get_file_from_container(file)

        self.assertEqual(file.name, retrieved.name)


    def test_get_file_from_container_error(self):
        container = Container(name='name')
        er = HERD()

        with self.assertRaises(ValueError):
            er._get_file_from_container(container)

    def test_add_ref(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [('entity_id1', 'entity1')])
        self.assertEqual(er.objects.data, [(0, data.object_id, 'Data', '', '')])

    def test_get_object_type(self):
        er = HERD()
        file = HERDManagerContainer(name='file')
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=file,
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')

        df = er.get_object_type(object_type='Data')

        expected_df_data = \
            {'file_object_id': {0: file.object_id},
             'objects_idx': {0: 0},
             'object_id': {0: data.object_id},
             'files_idx': {0: 0},
             'object_type': {0: 'Data'},
             'relative_path': {0: ''},
             'field': {0: ''},
             'keys_idx': {0: 0},
             'key': {0: 'key1'},
             'entities_idx': {0: 0},
             'entity_id': {0: 'entity_id1'},
             'entity_uri': {0: 'entity1'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)
        expected_df = expected_df.astype({'keys_idx': 'uint32',
                                          'objects_idx': 'uint32',
                                          'files_idx': 'uint32',
                                          'entities_idx': 'uint32'})
        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_object_type_all_instances(self):
        er = HERD()
        file = HERDManagerContainer(name='file')
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=file,
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')

        df = er.get_object_type(object_type='Data', all_instances=True)

        expected_df_data = \
            {'file_object_id': {0: file.object_id},
             'objects_idx': {0: 0},
             'object_id': {0: data.object_id},
             'files_idx': {0: 0},
             'object_type': {0: 'Data'},
             'relative_path': {0: ''},
             'field': {0: ''},
             'keys_idx': {0: 0},
             'key': {0: 'key1'},
             'entities_idx': {0: 0},
             'entity_id': {0: 'entity_id1'},
             'entity_uri': {0: 'entity1'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)
        expected_df = expected_df.astype({'keys_idx': 'uint32',
                                          'objects_idx': 'uint32',
                                          'files_idx': 'uint32',
                                          'entities_idx': 'uint32'})
        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_entity(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        file = HERDManagerContainer(name='file')
        er.add_ref(file=file,
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        self.assertEqual(er.get_entity(entity_id='entity_id1').idx, 0)
        self.assertEqual(er.get_entity(entity_id='entity_id2'), None)

    def test_get_obj_entities(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        file = HERDManagerContainer(name='file')
        er.add_ref(file=file,
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')

        df = er.get_object_entities(file=file,
                                    container=data)
        expected_df_data = \
            {'entity_id': {0: 'entity_id1'},
             'entity_uri': {0: 'entity1'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)

        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_obj_entities_file_none_container(self):
        er = HERD()
        file = HERDManagerContainer()
        er.add_ref(container=file,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        df = er.get_object_entities(container=file)

        expected_df_data = \
            {'entity_id': {0: 'entity_id1'},
             'entity_uri': {0: 'entity1'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)

        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_obj_entities_file_none_not_container_nested(self):
        er = HERD()
        file = HERDManagerContainer()
        child = Container(name='child')

        child.parent = file

        er.add_ref(container=child,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        df = er.get_object_entities(container=child)

        expected_df_data = \
            {'entity_id': {0: 'entity_id1'},
             'entity_uri': {0: 'entity1'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)

        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_obj_entities_file_none_not_container_deep_nested(self):
        er = HERD()
        file = HERDManagerContainer()
        child = Container(name='child')
        nested_child = Container(name='nested_child')

        child.parent = file
        nested_child.parent = child

        er.add_ref(container=nested_child,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        df = er.get_object_entities(container=nested_child)

        expected_df_data = \
            {'entity_id': {0: 'entity_id1'},
             'entity_uri': {0: 'entity1'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)

        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_obj_entities_file_none_error(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        file = HERDManagerContainer(name='file')
        er.add_ref(file=file,
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        with self.assertRaises(ValueError):
            _ = er.get_object_entities(container=data)

    def test_get_obj_entities_attribute(self):
        table = DynamicTable(name='table', description='table')
        table.add_column(name='col1', description="column")
        table.add_row(id=0, col1='data')

        file = HERDManagerContainer(name='file')

        er = HERD()
        er.add_ref(file=file,
                   container=table,
                   attribute='col1',
                   key='key1',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')
        df = er.get_object_entities(file=file,
                                    container=table,
                                    attribute='col1')

        expected_df_data = \
            {'entity_id': {0: 'entity_0'},
             'entity_uri': {0: 'entity_0_uri'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)

        pd.testing.assert_frame_equal(df, expected_df)

    def test_to_and_from_zip(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        er.to_zip(path='./HERD.zip')

        er_read = HERD.from_zip(path='./HERD.zip')
        HERD.assert_external_resources_equal(er_read, er, check_dtype=False)

        self.remove_er_files()

    def test_to_and_from_zip_entity_value_error(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        er.to_zip(path='./HERD.zip')

        self.child_tsv(external_resources=er)

        df = er.entities.to_dataframe()
        df.at[0, ('keys_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./entities.tsv', sep='\t', index=False)

        self.zip_child(zip_file='HERD.zip')

        with self.assertRaises(ValueError):
            _ = HERD.from_zip(path='./HERD.zip')

        self.remove_er_files()

    def test_to_and_from_zip_entity_key_value_error_key(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        er.to_zip(path='./HERD.zip')

        self.child_tsv(external_resources=er)

        df = er.entity_keys.to_dataframe()
        df.at[0, ('keys_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./entity_keys.tsv', sep='\t', index=False)

        self.zip_child(zip_file='HERD.zip')

        with self.assertRaises(ValueError):
            _ = HERD.from_zip(path='./HERD.zip')

        self.remove_er_files()

    def test_to_and_from_zip_entity_key_value_error_entity(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        er.to_zip(path='./HERD.zip')

        self.child_tsv(external_resources=er)

        df = er.entity_keys.to_dataframe()
        df.at[0, ('entities_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./entity_keys.tsv', sep='\t', index=False)

        self.zip_child(zip_file='HERD.zip')

        with self.assertRaises(ValueError):
            _ = HERD.from_zip(path='./HERD.zip')

        self.remove_er_files()

    def test_to_and_from_zip_object_value_error(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        er.to_zip(path='./HERD.zip')

        self.child_tsv(external_resources=er)

        df = er.objects.to_dataframe()
        df.at[0, ('files_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./objects.tsv', sep='\t', index=False)

        self.zip_child(zip_file='HERD.zip')

        msg = "File_ID Index out of range in ObjectTable. Please check for alterations."
        with self.assertRaisesWith(ValueError, msg):
            _ = HERD.from_zip(path='./HERD.zip')

        self.remove_er_files()

    def test_to_and_from_zip_object_keys_object_idx_value_error(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        er.to_zip(path='./HERD.zip')

        self.child_tsv(external_resources=er)

        df = er.object_keys.to_dataframe()
        df.at[0, ('objects_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./object_keys.tsv', sep='\t', index=False)

        self.zip_child(zip_file='HERD.zip')

        msg = "Object Index out of range in ObjectKeyTable. Please check for alterations."
        with self.assertRaisesWith(ValueError, msg):
            _ = HERD.from_zip(path='./HERD.zip')

        self.remove_er_files()

    def test_to_and_from_zip_object_keys_key_idx_value_error(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='key1',
                   entity_id='entity_id1',
                   entity_uri='entity1')
        er.to_zip(path='./HERD.zip')

        self.child_tsv(external_resources=er)

        df = er.object_keys.to_dataframe()
        df.at[0, ('keys_idx')] = 10  # Change key_ix 0 to 10
        df.to_csv('./object_keys.tsv', sep='\t', index=False)

        self.zip_child(zip_file='HERD.zip')

        msg = "Key Index out of range in ObjectKeyTable. Please check for alterations."
        with self.assertRaisesWith(ValueError, msg):
            _ = HERD.from_zip(path='./HERD.zip')

        self.remove_er_files()

    def test_add_ref_two_keys(self):
        er = HERD()
        ref_container_1 = Container(name='Container_1')
        ref_container_2 = Container(name='Container_2')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=ref_container_1,
                   key='key1',
                   entity_id="id11",
                   entity_uri='url11')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=ref_container_2,
                   key='key2',
                   entity_id="id12",
                   entity_uri='url21')

        self.assertEqual(er.keys.data, [('key1',), ('key2',)])
        self.assertEqual(er.entities.data, [('id11', 'url11'), ('id12', 'url21')])

        self.assertEqual(er.objects.data, [(0, ref_container_1.object_id, 'Container', '', ''),
                                           (1, ref_container_2.object_id, 'Container', '', '')])

    def test_add_ref_same_key_diff_objfield(self):
        er = HERD()
        ref_container_1 = Container(name='Container_1')
        ref_container_2 = Container(name='Container_2')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=ref_container_1,
                   key='key1',
                   entity_id="id11",
                   entity_uri='url11')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=ref_container_2,
                   key='key1',
                   entity_id="id12",
                   entity_uri='url21')

        self.assertEqual(er.keys.data, [('key1',), ('key1',)])
        self.assertEqual(er.entities.data, [('id11', 'url11'), ('id12', 'url21')])
        self.assertEqual(er.objects.data, [(0, ref_container_1.object_id, 'Container', '', ''),
                                           (1, ref_container_2.object_id, 'Container', '', '')])

    def test_add_ref_same_keyname(self):
        er = HERD()
        ref_container_1 = Container(name='Container_1')
        ref_container_2 = Container(name='Container_2')
        ref_container_3 = Container(name='Container_2')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=ref_container_1,
                   key='key1',
                   entity_id="id11",
                   entity_uri='url11')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=ref_container_2,
                   key='key1',
                   entity_id="id12",
                   entity_uri='url21')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=ref_container_3,
                   key='key1',
                   entity_id="id13",
                   entity_uri='url31')
        self.assertEqual(er.keys.data, [('key1',), ('key1',), ('key1',)])
        self.assertEqual(
            er.entities.data,
            [('id11', 'url11'),
             ('id12', 'url21'),
             ('id13', 'url31')])
        self.assertEqual(er.objects.data, [(0, ref_container_1.object_id, 'Container', '', ''),
                                           (1, ref_container_2.object_id, 'Container', '', ''),
                                           (2, ref_container_3.object_id, 'Container', '', '')])

    def test_object_key_unqiueness(self):
        er = HERD()
        data = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        existing_key = er.get_key('Mus musculus')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   key=existing_key,
                   entity_id='entity2',
                   entity_uri='entity_uri2')
        self.assertEqual(er.object_keys.data, [(0, 0)])

    def test_object_key_existing_key_new_object(self):
        er = HERD()
        data_1 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        data_2 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_1,
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        existing_key = er.get_key('Mus musculus')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_2,
                   key=existing_key,
                   entity_id='entity2',
                   entity_uri='entity_uri2')
        self.assertEqual(er.object_keys.data, [(0, 0), (1, 0)])

    def test_object_key_existing_key_new_object_error(self):
        er = HERD()
        data_1 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_1,
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        key = er._add_key('key')
        with self.assertRaises(ValueError):
            er.add_ref(file=HERDManagerContainer(name='file'),
                       container=data_1,
                       key=key,
                       entity_id='entity1',
                       entity_uri='entity_uri1')

    def test_reuse_key_reuse_entity(self):
        # With the key and entity existing, the EntityKeyTable should not have duplicates
        er = HERD()
        data_1 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        data_2 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_1,
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        existing_key = er.get_key('Mus musculus')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_2,
                   key=existing_key,
                   entity_id='NCBI:txid10090')

        self.assertEqual(er.entity_keys.data, [(0, 0)])

    def test_resuse_entity_different_key(self):
        # The EntityKeyTable should have two rows: same entity_idx, but different key_idx
        er = HERD()
        data_1 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        data_2 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_1,
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_2,
                   key='mouse',
                   entity_id='NCBI:txid10090')
        self.assertEqual(er.entity_keys.data, [(0, 0), (0, 1)])

    def test_reuse_key_reuse_entity_new(self):
        er = HERD()
        data_1 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        data_2 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_1,
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_1,
                   key='Mice',
                   entity_id='entity_2',
                   entity_uri='entity_2_uri')
        existing_key = er.get_key('Mus musculus')
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_2,
                   key=existing_key,
                   entity_id='entity_2')

        self.assertEqual(er.entity_keys.data, [(0, 0), (1, 1), (1, 0)])

    def test_entity_uri_error(self):
        er = HERD()
        data_1 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))
        with self.assertRaises(ValueError):
            er.add_ref(file=HERDManagerContainer(name='file'),
                       container=data_1,
                       key='Mus musculus',
                       entity_id='NCBI:txid10090')

    def test_entity_uri_reuse_error(self):
        er = HERD()
        data_1 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        data_2 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_1,
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        existing_key = er.get_key('Mus musculus')
        with self.assertRaises(ValueError):
            er.add_ref(file=HERDManagerContainer(name='file'),
                       container=data_2,
                       key=existing_key,
                       entity_id='NCBI:txid10090',
                       entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')

    def test_key_without_entity_error(self):
        er = HERD()
        data_1 = Data(name='data_name', data=np.array([('Mus musculus', 9, 81.0), ('Homo sapien', 3, 27.0)],
                    dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))

        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data_1,
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10090')
        key = er._add_key('key')
        with self.assertRaises(ValueError):
            er.add_ref(file=HERDManagerContainer(name='file'),
                       container=data_1,
                       key=key,
                       entity_id='entity1')

    def test_check_object_field_add(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er._check_object_field(file=HERDManagerContainer(name='file'),
                               container=data,
                               relative_path='',
                               field='')

        self.assertEqual(er.objects.data, [(0, data.object_id, 'Data', '', '')])

    def test_check_object_field_multi_files(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        file = HERDManagerContainer(name='file')

        er._check_object_field(file=file, container=data, relative_path='', field='')
        er._add_file(file.object_id)

        data2 = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        with self.assertRaises(ValueError):
            er._check_object_field(file=file, container=data2, relative_path='', field='')

    def test_check_object_field_multi_error(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        er._check_object_field(file=HERDManagerContainer(name='file'),
                               container=data,
                               relative_path='',
                               field='')
        er._add_object(files_idx=0, container=data, relative_path='', field='')
        with self.assertRaises(ValueError):
            er._check_object_field(file=HERDManagerContainer(name='file'),
                                   container=data,
                                   relative_path='',
                                   field='')

    def test_check_object_field_not_in_obj_table(self):
        er = HERD()
        data = Data(name="species", data=['Homo sapiens', 'Mus musculus'])
        with self.assertRaises(ValueError):
            er._check_object_field(file=HERDManagerContainer(name='file'),
                                   container=data,
                                   relative_path='',
                                   field='',
                                   create=False)

    def test_add_ref_attribute(self):
        # Test to make sure the attribute object is being used for the id
        # for the external reference.
        table = DynamicTable(name='table', description='table')
        table.add_column(name='col1', description="column")
        table.add_row(id=0, col1='data')

        er = HERD()
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=table,
                   attribute='id',
                   key='key1',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [('entity_0', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(0, table.id.object_id, 'ElementIdentifiers', '', '')])

    def test_add_ref_column_as_attribute(self):
        # Test to make sure the attribute object is being used for the id
        # for the external reference.
        table = DynamicTable(name='table', description='table')
        table.add_column(name='col1', description="column")
        table.add_row(id=0, col1='data')

        er = HERD()
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=table,
                   attribute='col1',
                   key='key1',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')

        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [('entity_0', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(0, table['col1'].object_id, 'VectorData', '', '')])

    def test_add_ref_compound_data(self):
        er = HERD()

        data = Data(
            name='data_name',
            data=np.array(
                [('Mus musculus', 9, 81.0), ('Homo sapiens', 3, 27.0)],
                dtype=[('species', 'U14'), ('age', 'i4'), ('weight', 'f4')]))
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=data,
                   field='species',
                   key='Mus musculus',
                   entity_id='NCBI:txid10090',
                   entity_uri='entity_0_uri')

        self.assertEqual(er.keys.data, [('Mus musculus',)])
        self.assertEqual(er.entities.data, [('NCBI:txid10090', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(0, data.object_id, 'Data', '', 'species')])

    def test_roundtrip(self):
        read_container = self.roundtripContainer()
        pd.testing.assert_frame_equal(read_container.to_dataframe(), self.container.to_dataframe())

    def test_roundtrip_export(self):
        read_container = self.roundtripExportContainer()
        pd.testing.assert_frame_equal(read_container.to_dataframe(), self.container.to_dataframe())


class TestHERDNestedAttributes(TestCase):

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

        er = HERD()
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=table,
                   attribute='description',
                   key='key1',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')
        self.assertEqual(er.keys.data, [('key1',)])
        self.assertEqual(er.entities.data, [('entity_0', 'entity_0_uri')])
        self.assertEqual(er.objects.data, [(0, table.object_id, 'DynamicTable', 'description', '')])

    def test_add_ref_deep_nested(self):
        er = HERD(type_map=self.type_map)
        er.add_ref(file=HERDManagerContainer(name='file'),
                   container=self.bar,
                   attribute='attr2',
                   key='key1',
                   entity_id='entity_0',
                   entity_uri='entity_0_uri')
        self.assertEqual(er.objects.data[0][3], 'data/attr2', '')


class TestHERDGetKey(TestCase):

    def setUp(self):
        self.er = HERD()

    def test_get_key_error_more_info(self):
        self.er.add_ref(file=HERDManagerContainer(name='file'),
                        container=Container(name='Container'),
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')
        self.er.add_ref(file=HERDManagerContainer(name='file'),
                        container=Container(name='Container'),
                        key='key1',
                        entity_id="id12",
                        entity_uri='url21')

        msg = "There are more than one key with that name. Please search with additional information."
        with self.assertRaisesWith(ValueError, msg):
            _ = self.er.get_key(key_name='key1')

    def test_get_key(self):
        self.er.add_ref(file=HERDManagerContainer(name='file'),
                        container=Container(name='Container'),
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        key = self.er.get_key(key_name='key1')
        self.assertIsInstance(key, Key)
        self.assertEqual(key.idx, 0)

    def test_get_key_bad_arg(self):
        self.er.add_ref(file=HERDManagerContainer(name='file'),
                        container=Container(name='Container'),
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        with self.assertRaises(ValueError):
            self.er.get_key(key_name='key2')

    def test_get_key_file_container_provided(self):
        file = HERDManagerContainer()
        container1 = Container(name='Container')
        self.er.add_ref(file=file,
                        container=container1,
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')
        self.er.add_ref(file=file,
                        container=Container(name='Container'),
                        key='key1',
                        entity_id="id12",
                        entity_uri='url21')

        key = self.er.get_key(key_name='key1', container=container1, file=file)
        self.assertIsInstance(key, Key)
        self.assertEqual(key.idx, 0)

    def test_get_key_no_file_container_provided(self):
        file = HERDManagerContainer()
        self.er.add_ref(container=file, key='key1', entity_id="id11", entity_uri='url11')

        key = self.er.get_key(key_name='key1', container=file)
        self.assertIsInstance(key, Key)
        self.assertEqual(key.idx, 0)

    def test_get_key_no_file_nested_container_provided(self):
        file = HERDManagerContainer()
        container1 = Container(name='Container')

        container1.parent = file
        self.er.add_ref(file=file,
                        container=container1,
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        key = self.er.get_key(key_name='key1', container=container1)
        self.assertIsInstance(key, Key)
        self.assertEqual(key.idx, 0)

    def test_get_key_no_file_deep_nested_container_provided(self):
        file = HERDManagerContainer()
        container1 = Container(name='Container1')
        container2 = Container(name='Container2')

        container1.parent = file
        container2.parent = container1

        self.er.add_ref(file=file,
                        container=container2,
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        key = self.er.get_key(key_name='key1', container=container2)
        self.assertIsInstance(key, Key)
        self.assertEqual(key.idx, 0)

    def test_get_key_no_file_error(self):
        file = HERDManagerContainer()
        container1 = Container(name='Container')
        self.er.add_ref(file=file,
                        container=container1,
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        with self.assertRaises(ValueError):
            _ = self.er.get_key(key_name='key1', container=container1)

    def test_get_key_no_key_found(self):
        file = HERDManagerContainer()
        container1 = Container(name='Container')
        self.er.add_ref(file=file,
                        container=container1,
                        key='key1',
                        entity_id="id11",
                        entity_uri='url11')

        msg = "No key found with that container."
        with self.assertRaisesWith(ValueError, msg):
            _ = self.er.get_key(key_name='key2', container=container1, file=file)
