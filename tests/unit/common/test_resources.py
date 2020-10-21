from hdmf.common.resources import ResourceReferences, ResourceIdentiferMap, ExternalResources
from hdmf.testing import TestCase, H5RoundTripMixin


class TestResourceReferences(TestCase):

    @classmethod
    def build_tables(cls):
        rrmap = ResourceIdentiferMap()
        resrefs = ResourceReferences()

        rrmap.add_row(0, 'ATP Binding', 'Gene Ontology', 'http://amigo.geneontology.org/amigo/term/GO:0005524')
        rrmap.add_row(1, 'Cacna1s', 'Mouse Genome Informatics', 'http://www.informatics.jax.org/marker/MGI:88294')
        rrmap.add_row(2, 'recA', 'A Systematic Annotation Package for Community Analysis of Genomes',
                      'https://asap.genetics.wisc.edu/asap/feature_info.php?FeatureID=ABE-0008876')

        resrefs.add_row(0, 'ca885753-e8a3-418a-86f4-7748fc2252a8', 'foo', 0)
        resrefs.add_row(1, 'e455bf5a-cbc5-48b1-b686-4b4e31f62a53', 'bar', 2)
        resrefs.add_row(2, 'da85e056-caff-4ddd-838c-5f5463e313e6', 'baz', 1)
        resrefs.add_row(3, '0eae6504-da47-4ee9-a375-bbed2d3d65a4', 'qux', 0)
        return rrmap, resrefs

    def test_constructor(self):
        self.build_tables()


class TestExternalResources(H5RoundTripMixin, TestCase):

    def test_add_reference(self):
        ExternalResources()

    def test_get_crid(self):
        rrmap, resrefs = TestResourceReferences.build_tables()
        er = ExternalResources(rrmap, resrefs)
        result = er.get_crid('ca885753-e8a3-418a-86f4-7748fc2252a8', 'foo', 'ATP Binding')
        self.assertEqual(result[0][0], 'Gene Ontology')
        self.assertEqual(result[0][1], 'http://amigo.geneontology.org/amigo/term/GO:0005524')

        result = er.get_crid('da85e056-caff-4ddd-838c-5f5463e313e6', 'baz', 'Cacna1s')
        self.assertEqual(result[0][0], 'Mouse Genome Informatics')
        self.assertEqual(result[0][1], 'http://www.informatics.jax.org/marker/MGI:88294')

    def setUpContainer(self):
        rrmap, resrefs = TestResourceReferences.build_tables()
        er = ExternalResources(rrmap, resrefs)
        return er
