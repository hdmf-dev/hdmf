import copy

from hdmf.spec import GroupSpec, DatasetSpec, AttributeSpec, SpecCatalog
from hdmf.testing import TestCase


class SpecCatalogTest(TestCase):

    def setUp(self):
        self.catalog = SpecCatalog()
        self.attributes = [
            AttributeSpec('attribute1', 'my first attribute', 'text'),
            AttributeSpec('attribute2', 'my second attribute', 'text')
        ]
        self.spec = DatasetSpec('my first dataset',
                                'int',
                                name='dataset1',
                                dims=(None, None),
                                attributes=self.attributes,
                                linkable=False,
                                data_type_def='EphysData')

    def test_register_spec(self):
        self.catalog.register_spec(self.spec, 'test.yaml')
        result = self.catalog.get_spec('EphysData')
        self.assertIs(result, self.spec)

    def test_hierarchy(self):
        spikes_spec = DatasetSpec('my extending dataset', 'int',
                                  data_type_inc='EphysData',
                                  data_type_def='SpikeData')

        lfp_spec = DatasetSpec('my second extending dataset', 'int',
                               data_type_inc='EphysData',
                               data_type_def='LFPData')

        self.catalog.register_spec(self.spec, 'test.yaml')
        self.catalog.register_spec(spikes_spec, 'test.yaml')
        self.catalog.register_spec(lfp_spec, 'test.yaml')

        spike_hierarchy = self.catalog.get_hierarchy('SpikeData')
        lfp_hierarchy = self.catalog.get_hierarchy('LFPData')
        ephys_hierarchy = self.catalog.get_hierarchy('EphysData')
        self.assertTupleEqual(spike_hierarchy, ('SpikeData', 'EphysData'))
        self.assertTupleEqual(lfp_hierarchy, ('LFPData', 'EphysData'))
        self.assertTupleEqual(ephys_hierarchy, ('EphysData',))

    def test_subtypes(self):
        """
         -BaseContainer--+-->AContainer--->ADContainer
                        |
                        +-->BContainer
        """
        base_spec = GroupSpec(doc='Base container',
                              data_type_def='BaseContainer')
        acontainer = GroupSpec(doc='AContainer',
                               data_type_inc='BaseContainer',
                               data_type_def='AContainer')
        adcontainer = GroupSpec(doc='ADContainer',
                                data_type_inc='AContainer',
                                data_type_def='ADContainer')
        bcontainer = GroupSpec(doc='BContainer',
                               data_type_inc='BaseContainer',
                               data_type_def='BContainer')
        self.catalog.register_spec(base_spec, 'test.yaml')
        self.catalog.register_spec(acontainer, 'test.yaml')
        self.catalog.register_spec(adcontainer, 'test.yaml')
        self.catalog.register_spec(bcontainer, 'test.yaml')
        base_spec_subtypes = self.catalog.get_subtypes('BaseContainer')
        base_spec_subtypes = tuple(sorted(base_spec_subtypes))  # Sort so we have a guaranteed order for comparison
        acontainer_subtypes = self.catalog.get_subtypes('AContainer')
        bcontainer_substypes = self.catalog.get_subtypes('BContainer')
        adcontainer_subtypes = self.catalog.get_subtypes('ADContainer')
        self.assertTupleEqual(adcontainer_subtypes, ())
        self.assertTupleEqual(bcontainer_substypes, ())
        self.assertTupleEqual(acontainer_subtypes, ('ADContainer',))
        self.assertTupleEqual(base_spec_subtypes,  ('AContainer', 'ADContainer', 'BContainer'))

    def test_subtypes_norecursion(self):
        """
         -BaseContainer--+-->AContainer--->ADContainer
                        |
                        +-->BContainer
        """
        base_spec = GroupSpec(doc='Base container',
                              data_type_def='BaseContainer')
        acontainer = GroupSpec(doc='AContainer',
                               data_type_inc='BaseContainer',
                               data_type_def='AContainer')
        adcontainer = GroupSpec(doc='ADContainer',
                                data_type_inc='AContainer',
                                data_type_def='ADContainer')
        bcontainer = GroupSpec(doc='BContainer',
                               data_type_inc='BaseContainer',
                               data_type_def='BContainer')
        self.catalog.register_spec(base_spec, 'test.yaml')
        self.catalog.register_spec(acontainer, 'test.yaml')
        self.catalog.register_spec(adcontainer, 'test.yaml')
        self.catalog.register_spec(bcontainer, 'test.yaml')
        base_spec_subtypes = self.catalog.get_subtypes('BaseContainer', recursive=False)
        base_spec_subtypes = tuple(sorted(base_spec_subtypes))  # Sort so we have a guaranteed order for comparison
        acontainer_subtypes = self.catalog.get_subtypes('AContainer', recursive=False)
        bcontainer_substypes = self.catalog.get_subtypes('BContainer', recursive=False)
        adcontainer_subtypes = self.catalog.get_subtypes('ADContainer', recursive=False)
        self.assertTupleEqual(adcontainer_subtypes, ())
        self.assertTupleEqual(bcontainer_substypes, ())
        self.assertTupleEqual(acontainer_subtypes, ('ADContainer',))
        self.assertTupleEqual(base_spec_subtypes,  ('AContainer', 'BContainer'))

    def test_subtypes_unknown_type(self):
        subtypes_of_bad_type = self.catalog.get_subtypes('UnknownType')
        self.assertTupleEqual(subtypes_of_bad_type, ())

    def test_get_spec_source_file(self):
        spikes_spec = GroupSpec('test group',
                                data_type_def='SpikeData')
        source_file_path = '/test/myt/test.yaml'
        self.catalog.auto_register(spikes_spec, source_file_path)
        recorded_source_file_path = self.catalog.get_spec_source_file('SpikeData')
        self.assertEqual(recorded_source_file_path, source_file_path)

    def test_get_full_hierarchy(self):
        """
        BaseContainer--+-->AContainer--->ADContainer
                        |
                        +-->BContainer

        Expected output:
        >> print(json.dumps(full_hierarchy, indent=4))
        >> {
        >>     "BaseContainer": {
        >>         "AContainer": {
        >>             "ADContainer": {}
        >>         },
        >>          "BContainer": {}
        >> }
        """
        base_spec = GroupSpec(doc='Base container',
                              data_type_def='BaseContainer')
        acontainer = GroupSpec(doc='AContainer',
                               data_type_inc='BaseContainer',
                               data_type_def='AContainer')
        adcontainer = GroupSpec(doc='ADContainer',
                                data_type_inc='AContainer',
                                data_type_def='ADContainer')
        bcontainer = GroupSpec(doc='BContainer',
                               data_type_inc='BaseContainer',
                               data_type_def='BContainer')
        self.catalog.register_spec(base_spec, 'test.yaml')
        self.catalog.register_spec(acontainer, 'test.yaml')
        self.catalog.register_spec(adcontainer, 'test.yaml')
        self.catalog.register_spec(bcontainer, 'test.yaml')
        full_hierarchy = self.catalog.get_full_hierarchy()
        expected_hierarchy = {
                                "BaseContainer": {
                                    "AContainer": {
                                        "ADContainer": {}
                                    },
                                    "BContainer": {}
                                }
                             }
        self.assertDictEqual(full_hierarchy, expected_hierarchy)

    def test_copy_spec_catalog(self):
        # Register the spec first
        self.catalog.register_spec(self.spec, 'test.yaml')
        result = self.catalog.get_spec('EphysData')
        self.assertIs(result, self.spec)
        # Now test the copy
        re = copy.copy(self.catalog)
        self.assertTupleEqual(self.catalog.get_registered_types(),
                              re.get_registered_types())

    def test_deepcopy_spec_catalog(self):
        # Register the spec first
        self.catalog.register_spec(self.spec, 'test.yaml')
        result = self.catalog.get_spec('EphysData')
        self.assertIs(result, self.spec)
        # Now test the copy
        re = copy.deepcopy(self.catalog)
        self.assertTupleEqual(self.catalog.get_registered_types(),
                              re.get_registered_types())

    def test_catch_duplicate_spec_nested(self):
        spec1 = GroupSpec(
            data_type_def='Group1',
            doc='This is my new group 1',
        )
        spec2 = GroupSpec(
            data_type_def='Group2',
            doc='This is my new group 2',
            groups=[spec1],  # nested definition
        )
        source = 'test_extension.yaml'
        self.catalog.register_spec(spec1, source)
        self.catalog.register_spec(spec2, source)  # this is OK because Group1 is the same spec
        ret = self.catalog.get_spec('Group1')
        self.assertIs(ret, spec1)

    def test_catch_duplicate_spec_different(self):
        spec1 = GroupSpec(
            data_type_def='Group1',
            doc='This is my new group 1',
        )
        spec2 = GroupSpec(
            data_type_def='Group1',
            doc='This is my other group 1',
        )
        source = 'test_extension.yaml'
        self.catalog.register_spec(spec1, source)
        msg = "'Group1' - cannot overwrite existing specification"
        with self.assertRaisesWith(ValueError, msg):
            self.catalog.register_spec(spec2, source)

    def test_catch_duplicate_spec_different_source(self):
        spec1 = GroupSpec(
            data_type_def='Group1',
            doc='This is my new group 1',
        )
        spec2 = GroupSpec(
            data_type_def='Group1',
            doc='This is my new group 1',
        )
        source1 = 'test_extension1.yaml'
        source2 = 'test_extension2.yaml'
        self.catalog.register_spec(spec1, source1)
        msg = "'Group1' - cannot overwrite existing specification"
        with self.assertRaisesWith(ValueError, msg):
            self.catalog.register_spec(spec2, source2)
