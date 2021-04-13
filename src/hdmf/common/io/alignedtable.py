from .. import register_map
from ..alignedtable import AlignedDynamicTable
from .table import DynamicTableMap


@register_map(AlignedDynamicTable)
class AlignedDynamicTableMap(DynamicTableMap):
    """
    Customize the mapping for AlignedDynamicTable
    """
    def __init__(self, spec):
        super().__init__(spec)
        # By default the DynamicTables contained as sub-categories in the AlignedDynamicTable are mapped to
        # the 'dynamic_tables' class attribute. This renames the attribute to 'category_tables'
        self.map_spec('category_tables', spec.get_data_type('DynamicTable'))
