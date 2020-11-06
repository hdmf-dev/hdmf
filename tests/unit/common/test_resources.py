from hdmf.common.resources import ExternalResources, KeyTable, Key, ResourceTable, Resource,
                                 ObjectKeyTable, ObjectKey, ObjectTable, Object
from hdmf.testing import TestCase, H5RoundTripMixin


class TestExternalResources(TestCase):

    def __old_test_add_row(self):
        key_table = KeyTable()
        resource_table = ResourceTable()
        object_table = ObjectTable()
        object_key_table = ObjectKeyTable()

        key = Key('ATP Binding')
        rsc = Resource(key, 'Gene Ontology', 'GO:0005524',  'http://amigo.geneontology.org/amigo/term/GO:0005524')

        obj = Object('ca885753-e8a3-418a-86f4-7748fc2252a8', 'foo')
        objkey = ObjectKey(obj, key)

    def test_ext_reference(self)
        er = ExternalResources()
        key = er.add_key('ATP Binding')
        rsc = er.add_resource(key, 'Gene Ontology', 'GO:0005524',  'http://amigo.geneontology.org/amigo/term/GO:0005524')
        obj = er.add_object('ca885753-e8a3-418a-86f4-7748fc2252a8', 'foo')
        er.add_external_reference(obj, key)


class OldTestResourceReferences(TestCase):

    @classmethod
    def build_tables(cls):
        rrmap = ResourceIdentiferMap()
        resrefs = ResourceReferences()

        rrmap.add_row(0, 'ATP Binding', 'Gene Ontology', 'GO:0005524')
        rrmap.add_row(1, 'Cacna1s', 'Mouse Genome Informatics', 'MGI:88294')
        rrmap.add_row(2, 'recA', 'A Systematic Annotation Package for Community Analysis of Genomes', 'ABE-0008876')

        resrefs.add_row(0, 'ca885753-e8a3-418a-86f4-7748fc2252a8', 'foo', 0)
        resrefs.add_row(1, 'e455bf5a-cbc5-48b1-b686-4b4e31f62a53', 'bar', 2)
        resrefs.add_row(2, 'da85e056-caff-4ddd-838c-5f5463e313e6', 'baz', 1)
        resrefs.add_row(3, '0eae6504-da47-4ee9-a375-bbed2d3d65a4', 'qux', 0)
        return rrmap, resrefs

    def __test_constructor(self):
        self.build_tables()


class OldTestExternalResources(H5RoundTripMixin, TestCase):

    def __test_add_reference(self):
        ExternalResources()

    def __test_get_resource_identifier(self):
        rrmap, resrefs = TestResourceReferences.build_tables()
        er = ExternalResources(rrmap, resrefs)
        result = er.get_resource_identifier('ca885753-e8a3-418a-86f4-7748fc2252a8', 'foo', 'ATP Binding')
        self.assertEqual(result[0][0], 'Gene Ontology')
        self.assertEqual(result[0][1], 'http://amigo.geneontology.org/amigo/term/GO:0005524')

        result = er.get_resource_identifier('da85e056-caff-4ddd-838c-5f5463e313e6', 'baz', 'Cacna1s')
        self.assertEqual(result[0][0], 'Mouse Genome Informatics')
        self.assertEqual(result[0][1], 'http://www.informatics.jax.org/marker/MGI:88294')

    def setUpContainer(self):
        rrmap, resrefs = TestResourceReferences.build_tables()
        er = ExternalResources(rrmap, resrefs)
        return er
