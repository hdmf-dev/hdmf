
class ForeignField(object):
    '''
    An object for resolving data stored in a foreign location i.e. across the web
    '''

    @docval({'name': 'cache', 'type': bool, 'help': 'whether or not to cache result after resolving', 'default': True},
            {'name': 'swap', 'type': bool, 'help': 'whether or not to swap in the returned value to the parent Container', 'default': True},
            returns='the value that was retrieved'})
    def resolve(self):
        '''
        Retrieve the foreign value.

        if *swap* is *True*, the returned value is swapped in place of this object on the Container holding this object

        For example, the following would happen:

        >>> if isinstance(container.foo, ForeignField):
                print('foo is foreign')
        foo is foreign
        >>> container.foo.resolve()
        >>> if not isinstance(container.foo, ForeignField):
                print('foo is no longer foreign')
        foo is no longer foreign
        '''
        pass
