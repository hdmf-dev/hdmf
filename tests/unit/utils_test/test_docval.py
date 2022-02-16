import numpy as np
from hdmf.testing import TestCase
from hdmf.utils import (docval, fmt_docval_args, get_docval, getargs, popargs, AllowPositional, get_docval_macro,
                        docval_macro)


class MyTestClass(object):

    @docval({'name': 'arg1', 'type': str, 'doc': 'argument1 is a str'})
    def basic_add(self, **kwargs):
        return kwargs

    @docval({'name': 'arg1', 'type': str, 'doc': 'argument1 is a str'},
            {'name': 'arg2', 'type': int, 'doc': 'argument2 is a int'})
    def basic_add2(self, **kwargs):
        return kwargs

    @docval({'name': 'arg1', 'type': str, 'doc': 'argument1 is a str'},
            {'name': 'arg2', 'type': 'int', 'doc': 'argument2 is a int'},
            {'name': 'arg3', 'type': bool, 'doc': 'argument3 is a bool. it defaults to False', 'default': False})
    def basic_add2_kw(self, **kwargs):
        return kwargs

    @docval({'name': 'arg1', 'type': str, 'doc': 'argument1 is a str', 'default': 'a'},
            {'name': 'arg2', 'type': int, 'doc': 'argument2 is a int', 'default': 1})
    def basic_only_kw(self, **kwargs):
        return kwargs

    @docval({'name': 'arg1', 'type': str, 'doc': 'argument1 is a str'},
            {'name': 'arg2', 'type': 'int', 'doc': 'argument2 is a int'},
            {'name': 'arg3', 'type': bool, 'doc': 'argument3 is a bool. it defaults to False', 'default': False},
            allow_extra=True)
    def basic_add2_kw_allow_extra(self, **kwargs):
        return kwargs


class MyTestSubclass(MyTestClass):

    @docval({'name': 'arg1', 'type': str, 'doc': 'argument1 is a str'},
            {'name': 'arg2', 'type': int, 'doc': 'argument2 is a int'})
    def basic_add(self, **kwargs):
        return kwargs

    @docval({'name': 'arg1', 'type': str, 'doc': 'argument1 is a str'},
            {'name': 'arg2', 'type': int, 'doc': 'argument2 is a int'},
            {'name': 'arg3', 'type': bool, 'doc': 'argument3 is a bool. it defaults to False', 'default': False},
            {'name': 'arg4', 'type': str, 'doc': 'argument4 is a str'},
            {'name': 'arg5', 'type': 'float', 'doc': 'argument5 is a float'},
            {'name': 'arg6', 'type': bool, 'doc': 'argument6 is a bool. it defaults to None', 'default': None})
    def basic_add2_kw(self, **kwargs):
        return kwargs


class MyChainClass(MyTestClass):

    @docval({'name': 'arg1', 'type': (str, 'MyChainClass'), 'doc': 'arg1 is a string or MyChainClass'},
            {'name': 'arg2', 'type': ('array_data', 'MyChainClass'),
             'doc': 'arg2 is array data or MyChainClass. it defaults to None', 'default': None},
            {'name': 'arg3', 'type': ('array_data', 'MyChainClass'), 'doc': 'arg3 is array data or MyChainClass',
             'shape': (None, 2)},
            {'name': 'arg4', 'type': ('array_data', 'MyChainClass'),
             'doc': 'arg3 is array data or MyChainClass. it defaults to None.', 'shape': (None, 2), 'default': None})
    def __init__(self, **kwargs):
        self._arg1, self._arg2, self._arg3, self._arg4 = popargs('arg1', 'arg2', 'arg3', 'arg4', kwargs)

    @property
    def arg1(self):
        if isinstance(self._arg1, MyChainClass):
            return self._arg1.arg1
        else:
            return self._arg1

    @property
    def arg2(self):
        if isinstance(self._arg2, MyChainClass):
            return self._arg2.arg2
        else:
            return self._arg2

    @property
    def arg3(self):
        if isinstance(self._arg3, MyChainClass):
            return self._arg3.arg3
        else:
            return self._arg3

    @arg3.setter
    def arg3(self, val):
        self._arg3 = val

    @property
    def arg4(self):
        if isinstance(self._arg4, MyChainClass):
            return self._arg4.arg4
        else:
            return self._arg4

    @arg4.setter
    def arg4(self, val):
        self._arg4 = val


class TestDocValidator(TestCase):

    def setUp(self):
        self.test_obj = MyTestClass()
        self.test_obj_sub = MyTestSubclass()

    def test_bad_type(self):
        exp_msg = (r"docval for arg1: error parsing argument type: argtype must be a type, "
                   r"a str, a list, a tuple, or None - got <class|type 'dict'>")
        with self.assertRaisesRegex(Exception, exp_msg):
            @docval({'name': 'arg1', 'type': {'a': 1}, 'doc': 'this is a bad type'})
            def method(self, **kwargs):
                pass
            method(self, arg1=1234560)

    def test_bad_shape(self):
        @docval({'name': 'arg1', 'type': 'array_data', 'doc': 'this is a bad shape', 'shape': (None, 2)})
        def method(self, **kwargs):
            pass
        with self.assertRaises(ValueError):
            method(self, arg1=[[1]])
        with self.assertRaises(ValueError):
            method(self, arg1=[1])
        # this should work
        method(self, arg1=[[1, 1]])

    def test_multi_shape(self):
        @docval({'name': 'arg1', 'type': 'array_data', 'doc': 'this is a bad shape',
                 'shape': ((None,), (None, 2))})
        def method1(self, **kwargs):
            pass

        method1(self, arg1=[[1, 1]])
        method1(self, arg1=[1, 2])
        with self.assertRaises(ValueError):
            method1(self, arg1=[[1, 1, 1]])

    def test_fmt_docval_args(self):
        """ Test that fmt_docval_args works """
        test_kwargs = {
            'arg1': 'a string',
            'arg2': 1,
            'arg3': True,
        }
        rec_args, rec_kwargs = fmt_docval_args(self.test_obj.basic_add2_kw, test_kwargs)
        exp_args = ['a string', 1]
        self.assertListEqual(rec_args, exp_args)
        exp_kwargs = {'arg3': True}
        self.assertDictEqual(rec_kwargs, exp_kwargs)

    def test_fmt_docval_args_no_docval(self):
        """ Test that fmt_docval_args raises an error when run on function without docval """
        def method1(self, **kwargs):
            pass

        with self.assertRaisesRegex(ValueError, r"no docval found on .*method1.*"):
            fmt_docval_args(method1, {})

    def test_fmt_docval_args_allow_extra(self):
        """ Test that fmt_docval_args works """
        test_kwargs = {
            'arg1': 'a string',
            'arg2': 1,
            'arg3': True,
            'hello': 'abc',
            'list': ['abc', 1, 2, 3]
        }
        rec_args, rec_kwargs = fmt_docval_args(self.test_obj.basic_add2_kw_allow_extra, test_kwargs)
        exp_args = ['a string', 1]
        self.assertListEqual(rec_args, exp_args)
        exp_kwargs = {'arg3': True, 'hello': 'abc', 'list': ['abc', 1, 2, 3]}
        self.assertDictEqual(rec_kwargs, exp_kwargs)

    def test_docval_add(self):
        """Test that docval works with a single positional
           argument
        """
        kwargs = self.test_obj.basic_add('a string')
        self.assertDictEqual(kwargs, {'arg1': 'a string'})

    def test_docval_add_kw(self):
        """Test that docval works with a single positional
           argument passed as key-value
        """
        kwargs = self.test_obj.basic_add(arg1='a string')
        self.assertDictEqual(kwargs, {'arg1': 'a string'})

    def test_docval_add_missing_args(self):
        """Test that docval catches missing argument
           with a single positional argument
        """
        with self.assertRaisesWith(TypeError, "MyTestClass.basic_add: missing argument 'arg1'"):
            self.test_obj.basic_add()

    def test_docval_add2(self):
        """Test that docval works with two positional
           arguments
        """
        kwargs = self.test_obj.basic_add2('a string', 100)
        self.assertDictEqual(kwargs, {'arg1': 'a string', 'arg2': 100})

    def test_docval_add2_w_unicode(self):
        """Test that docval works with two positional
           arguments
        """
        kwargs = self.test_obj.basic_add2(u'a string', 100)
        self.assertDictEqual(kwargs, {'arg1': u'a string', 'arg2': 100})

    def test_docval_add2_kw_default(self):
        """Test that docval works with two positional
           arguments and a keyword argument when using
           default keyword argument value
        """
        kwargs = self.test_obj.basic_add2_kw('a string', 100)
        self.assertDictEqual(kwargs, {'arg1': 'a string', 'arg2': 100, 'arg3': False})

    def test_docval_add2_pos_as_kw(self):
        """Test that docval works with two positional
           arguments and a keyword argument when using
           default keyword argument value, but pass
           positional arguments by key-value
        """
        kwargs = self.test_obj.basic_add2_kw(arg1='a string', arg2=100)
        self.assertDictEqual(kwargs, {'arg1': 'a string', 'arg2': 100, 'arg3': False})

    def test_docval_add2_kw_kw_syntax(self):
        """Test that docval works with two positional
           arguments and a keyword argument when specifying
           keyword argument value with keyword syntax
        """
        kwargs = self.test_obj.basic_add2_kw('a string', 100, arg3=True)
        self.assertDictEqual(kwargs, {'arg1': 'a string', 'arg2': 100, 'arg3': True})

    def test_docval_add2_kw_all_kw_syntax(self):
        """Test that docval works with two positional
           arguments and a keyword argument when specifying
           all arguments by key-value
        """
        kwargs = self.test_obj.basic_add2_kw(arg1='a string', arg2=100, arg3=True)
        self.assertDictEqual(kwargs, {'arg1': 'a string', 'arg2': 100, 'arg3': True})

    def test_docval_add2_kw_pos_syntax(self):
        """Test that docval works with two positional
           arguments and a keyword argument when specifying
           keyword argument value with positional syntax
        """
        kwargs = self.test_obj.basic_add2_kw('a string', 100, True)
        self.assertDictEqual(kwargs, {'arg1': 'a string', 'arg2': 100, 'arg3': True})

    def test_docval_add2_kw_pos_syntax_missing_args(self):
        """Test that docval catches incorrect type with two positional
           arguments and a keyword argument when specifying
           keyword argument value with positional syntax
        """
        msg = "MyTestClass.basic_add2_kw: incorrect type for 'arg2' (got 'str', expected 'int')"
        with self.assertRaisesWith(TypeError, msg):
            self.test_obj.basic_add2_kw('a string', 'bad string')

    def test_docval_add_sub(self):
        """Test that docval works with a two positional arguments,
           where the second is specified by the subclass implementation
        """
        kwargs = self.test_obj_sub.basic_add('a string', 100)
        expected = {'arg1': 'a string', 'arg2': 100}
        self.assertDictEqual(kwargs, expected)

    def test_docval_add2_kw_default_sub(self):
        """Test that docval works with a four positional arguments and
           two keyword arguments, where two positional and one keyword
           argument is specified in both the parent and sublcass implementations
        """
        kwargs = self.test_obj_sub.basic_add2_kw('a string', 100, 'another string', 200.0)
        expected = {'arg1': 'a string', 'arg2': 100,
                    'arg4': 'another string', 'arg5': 200.0,
                    'arg3': False, 'arg6': None}
        self.assertDictEqual(kwargs, expected)

    def test_docval_add2_kw_default_sub_missing_args(self):
        """Test that docval catches missing arguments with a four positional arguments
           and two keyword arguments, where two positional and one keyword
           argument is specified in both the parent and sublcass implementations,
           when using default values for keyword arguments
        """
        with self.assertRaisesWith(TypeError, "MyTestSubclass.basic_add2_kw: missing argument 'arg5'"):
            self.test_obj_sub.basic_add2_kw('a string', 100, 'another string')

    def test_docval_add2_kw_kwsyntax_sub(self):
        """Test that docval works when called with a four positional
           arguments and two keyword arguments, where two positional
           and one keyword argument is specified in both the parent
           and sublcass implementations
        """
        kwargs = self.test_obj_sub.basic_add2_kw('a string', 100, 'another string', 200.0, arg6=True)
        expected = {'arg1': 'a string', 'arg2': 100,
                    'arg4': 'another string', 'arg5': 200.0,
                    'arg3': False, 'arg6': True}
        self.assertDictEqual(kwargs, expected)

    def test_docval_add2_kw_kwsyntax_sub_missing_args(self):
        """Test that docval catches missing arguments when called with a four positional
           arguments and two keyword arguments, where two positional and one keyword
           argument is specified in both the parent and sublcass implementations
        """
        with self.assertRaisesWith(TypeError, "MyTestSubclass.basic_add2_kw: missing argument 'arg5'"):
            self.test_obj_sub.basic_add2_kw('a string', 100, 'another string', arg6=True)

    def test_docval_add2_kw_kwsyntax_sub_nonetype_arg(self):
        """Test that docval catches NoneType when called with a four positional
           arguments and two keyword arguments, where two positional and one keyword
           argument is specified in both the parent and sublcass implementations
        """
        msg = "MyTestSubclass.basic_add2_kw: None is not allowed for 'arg5' (expected 'float', not None)"
        with self.assertRaisesWith(TypeError, msg):
            self.test_obj_sub.basic_add2_kw('a string', 100, 'another string', None, arg6=True)

    def test_only_kw_no_args(self):
        """Test that docval parses arguments when only keyword
           arguments exist, and no arguments are specified
        """
        kwargs = self.test_obj.basic_only_kw()
        self.assertDictEqual(kwargs, {'arg1': 'a', 'arg2': 1})

    def test_only_kw_arg1_no_arg2(self):
        """Test that docval parses arguments when only keyword
           arguments exist, and only first argument is specified
           as key-value
        """
        kwargs = self.test_obj.basic_only_kw(arg1='b')
        self.assertDictEqual(kwargs, {'arg1': 'b', 'arg2': 1})

    def test_only_kw_arg1_pos_no_arg2(self):
        """Test that docval parses arguments when only keyword
           arguments exist, and only first argument is specified
           as positional argument
        """
        kwargs = self.test_obj.basic_only_kw('b')
        self.assertDictEqual(kwargs, {'arg1': 'b', 'arg2': 1})

    def test_only_kw_arg2_no_arg1(self):
        """Test that docval parses arguments when only keyword
           arguments exist, and only second argument is specified
           as key-value
        """
        kwargs = self.test_obj.basic_only_kw(arg2=2)
        self.assertDictEqual(kwargs, {'arg1': 'a', 'arg2': 2})

    def test_only_kw_arg1_arg2(self):
        """Test that docval parses arguments when only keyword
           arguments exist, and both arguments are specified
           as key-value
        """
        kwargs = self.test_obj.basic_only_kw(arg1='b', arg2=2)
        self.assertDictEqual(kwargs, {'arg1': 'b', 'arg2': 2})

    def test_only_kw_arg1_arg2_pos(self):
        """Test that docval parses arguments when only keyword
           arguments exist, and both arguments are specified
           as positional arguments
        """
        kwargs = self.test_obj.basic_only_kw('b', 2)
        self.assertDictEqual(kwargs, {'arg1': 'b', 'arg2': 2})

    def test_extra_kwarg(self):
        """Test that docval parses arguments when only keyword
           arguments exist, and both arguments are specified
           as positional arguments
        """
        with self.assertRaises(TypeError):
            self.test_obj.basic_add2_kw('a string', 100, bar=1000)

    def test_extra_args_pos_only(self):
        """Test that docval raises an error if too many positional
           arguments are specified
        """
        msg = ("MyTestClass.basic_add2_kw: Expected at most 3 arguments ['arg1', 'arg2', 'arg3'], got 4: 4 positional "
               "and 0 keyword []")
        with self.assertRaisesWith(TypeError, msg):
            self.test_obj.basic_add2_kw('a string', 100, True, 'extra')

    def test_extra_args_pos_kw(self):
        """Test that docval raises an error if too many positional
           arguments are specified and a keyword arg is specified
        """
        msg = ("MyTestClass.basic_add2_kw: Expected at most 3 arguments ['arg1', 'arg2', 'arg3'], got 4: 3 positional "
               "and 1 keyword ['arg3']")
        with self.assertRaisesWith(TypeError, msg):
            self.test_obj.basic_add2_kw('a string', 'extra', 100, arg3=True)

    def test_extra_kwargs_pos_kw(self):
        """Test that docval raises an error if extra keyword
           arguments are specified
        """
        msg = ("MyTestClass.basic_add2_kw: Expected at most 3 arguments ['arg1', 'arg2', 'arg3'], got 4: 2 positional "
               "and 2 keyword ['arg3', 'extra']")
        with self.assertRaisesWith(TypeError, msg):
            self.test_obj.basic_add2_kw('a string', 100, extra='extra', arg3=True)

    def test_extra_args_pos_only_ok(self):
        """Test that docval raises an error if too many positional
           arguments are specified even if allow_extra is True
        """
        msg = ("MyTestClass.basic_add2_kw_allow_extra: Expected at most 3 arguments ['arg1', 'arg2', 'arg3'], got "
               "4 positional")
        with self.assertRaisesWith(TypeError, msg):
            self.test_obj.basic_add2_kw_allow_extra('a string', 100, True, 'extra', extra='extra')

    def test_extra_args_pos_kw_ok(self):
        """Test that docval does not raise an error if too many
           keyword arguments are specified and allow_extra is True
        """
        kwargs = self.test_obj.basic_add2_kw_allow_extra('a string', 100, True, extra='extra')
        self.assertDictEqual(kwargs, {'arg1': 'a string', 'arg2': 100, 'arg3': True, 'extra': 'extra'})

    def test_dup_kw(self):
        """Test that docval raises an error if a keyword argument
           captures a positional argument before all positional
           arguments have been resolved
        """
        with self.assertRaisesWith(TypeError, "MyTestClass.basic_add2_kw: got multiple values for argument 'arg1'"):
            self.test_obj.basic_add2_kw('a string', 100, arg1='extra')

    def test_extra_args_dup_kw(self):
        """Test that docval raises an error if a keyword argument
           captures a positional argument before all positional
           arguments have been resolved and allow_extra is True
        """
        msg = "MyTestClass.basic_add2_kw_allow_extra: got multiple values for argument 'arg1'"
        with self.assertRaisesWith(TypeError, msg):
            self.test_obj.basic_add2_kw_allow_extra('a string', 100, True, arg1='extra')

    def test_unsupported_docval_term(self):
        """Test that docval does not allow setting of arguments
           marked as unsupported
        """
        msg = "docval for arg1: keys ['unsupported'] are not supported by docval"
        with self.assertRaisesWith(Exception, msg):
            @docval({'name': 'arg1', 'type': 'array_data', 'doc': 'this is a bad shape', 'unsupported': 'hi!'})
            def method(self, **kwargs):
                pass

    def test_catch_dup_names(self):
        """Test that docval does not allow duplicate argument names
        """
        @docval({'name': 'arg1', 'type': 'array_data', 'doc': 'this is a bad shape'},
                {'name': 'arg1', 'type': 'array_data', 'doc': 'this is a bad shape2'})
        def method(self, **kwargs):
            pass
        msg = "TestDocValidator.test_catch_dup_names.<locals>.method: The following names are duplicated: ['arg1']"
        with self.assertRaisesWith(ValueError, msg):
            method(self, arg1=[1])

    def test_get_docval_all(self):
        """Test that get_docval returns a tuple of the docval arguments
        """
        args = get_docval(self.test_obj.basic_add2)
        self.assertTupleEqual(args, ({'name': 'arg1', 'type': str, 'doc': 'argument1 is a str'},
                                     {'name': 'arg2', 'type': int, 'doc': 'argument2 is a int'}))

    def test_get_docval_one_arg(self):
        """Test that get_docval returns the matching docval argument
        """
        arg = get_docval(self.test_obj.basic_add2, 'arg2')
        self.assertTupleEqual(arg, ({'name': 'arg2', 'type': int, 'doc': 'argument2 is a int'},))

    def test_get_docval_two_args(self):
        """Test that get_docval returns the matching docval arguments in order
        """
        args = get_docval(self.test_obj.basic_add2, 'arg2', 'arg1')
        self.assertTupleEqual(args, ({'name': 'arg2', 'type': int, 'doc': 'argument2 is a int'},
                                     {'name': 'arg1', 'type': str, 'doc': 'argument1 is a str'}))

    def test_get_docval_missing_arg(self):
        """Test that get_docval throws error if the matching docval argument is not found
        """
        with self.assertRaisesWith(ValueError, "Function basic_add2 does not have docval argument 'arg3'"):
            get_docval(self.test_obj.basic_add2, 'arg3')

    def test_get_docval_missing_args(self):
        """Test that get_docval throws error if the matching docval arguments is not found
        """
        with self.assertRaisesWith(ValueError, "Function basic_add2 does not have docval argument 'arg3'"):
            get_docval(self.test_obj.basic_add2, 'arg3', 'arg4')

    def test_get_docval_missing_arg_of_many_ok(self):
        """Test that get_docval throws error if the matching docval arguments is not found
        """
        with self.assertRaisesWith(ValueError, "Function basic_add2 does not have docval argument 'arg3'"):
            get_docval(self.test_obj.basic_add2, 'arg2', 'arg3')

    def test_get_docval_none(self):
        """Test that get_docval returns an empty tuple if there is no docval
        """
        args = get_docval(self.test_obj.__init__)
        self.assertTupleEqual(args, tuple())

    def test_get_docval_none_arg(self):
        """Test that get_docval throws error if there is no docval and an argument name is passed
        """
        with self.assertRaisesWith(ValueError, 'Function __init__ has no docval arguments'):
            get_docval(self.test_obj.__init__, 'arg3')

    def test_bool_type(self):
        @docval({'name': 'arg1', 'type': bool, 'doc': 'this is a bool'})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        res = method(self, arg1=True)
        self.assertEqual(res, True)
        self.assertIsInstance(res, bool)

        res = method(self, arg1=np.bool_(True))
        self.assertEqual(res, np.bool_(True))
        self.assertIsInstance(res, np.bool_)

    def test_bool_string_type(self):
        @docval({'name': 'arg1', 'type': 'bool', 'doc': 'this is a bool'})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        res = method(self, arg1=True)
        self.assertEqual(res, True)
        self.assertIsInstance(res, bool)

        res = method(self, arg1=np.bool_(True))
        self.assertEqual(res, np.bool_(True))
        self.assertIsInstance(res, np.bool_)

    def test_uint_type(self):
        """Test that docval type specification of np.uint32 works as expected."""
        @docval({'name': 'arg1', 'type': np.uint32, 'doc': 'this is a uint'})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        res = method(self, arg1=np.uint32(1))
        self.assertEqual(res, np.uint32(1))
        self.assertIsInstance(res, np.uint32)

        msg = ("TestDocValidator.test_uint_type.<locals>.method: incorrect type for 'arg1' (got 'uint8', expected "
               "'uint32')")
        with self.assertRaisesWith(TypeError, msg):
            method(self, arg1=np.uint8(1))

        msg = ("TestDocValidator.test_uint_type.<locals>.method: incorrect type for 'arg1' (got 'uint64', expected "
               "'uint32')")
        with self.assertRaisesWith(TypeError, msg):
            method(self, arg1=np.uint64(1))

    def test_uint_string_type(self):
        """Test that docval type specification of string 'uint' matches np.uint of all available precisions."""
        @docval({'name': 'arg1', 'type': 'uint', 'doc': 'this is a uint'})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        res = method(self, arg1=np.uint(1))
        self.assertEqual(res, np.uint(1))
        self.assertIsInstance(res, np.uint)

        res = method(self, arg1=np.uint8(1))
        self.assertEqual(res, np.uint8(1))
        self.assertIsInstance(res, np.uint8)

        res = method(self, arg1=np.uint16(1))
        self.assertEqual(res, np.uint16(1))
        self.assertIsInstance(res, np.uint16)

        res = method(self, arg1=np.uint32(1))
        self.assertEqual(res, np.uint32(1))
        self.assertIsInstance(res, np.uint32)

        res = method(self, arg1=np.uint64(1))
        self.assertEqual(res, np.uint64(1))
        self.assertIsInstance(res, np.uint64)

    def test_allow_positional_warn(self):
        @docval({'name': 'arg1', 'type': bool, 'doc': 'this is a bool'}, allow_positional=AllowPositional.WARNING)
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        # check that supplying a keyword arg is OK
        res = method(self, arg1=True)
        self.assertEqual(res, True)
        self.assertIsInstance(res, bool)

        # check that supplying a positional arg raises a warning
        msg = ('TestDocValidator.test_allow_positional_warn.<locals>.method: '
               'Positional arguments are discouraged and may be forbidden in a future release.')
        with self.assertWarnsWith(FutureWarning, msg):
            method(self, True)

    def test_allow_positional_error(self):
        @docval({'name': 'arg1', 'type': bool, 'doc': 'this is a bool'}, allow_positional=AllowPositional.ERROR)
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        # check that supplying a keyword arg is OK
        res = method(self, arg1=True)
        self.assertEqual(res, True)
        self.assertIsInstance(res, bool)

        # check that supplying a positional arg raises an error
        msg = ('TestDocValidator.test_allow_positional_error.<locals>.method: '
               'Only keyword arguments (e.g., func(argname=value, ...)) are allowed.')
        with self.assertRaisesWith(SyntaxError, msg):
            method(self, True)

    def test_enum_str(self):
        """Test that the basic usage of an enum check on strings works"""
        @docval({'name': 'arg1', 'type': str, 'doc': 'an arg', 'enum': ['a', 'b']})  # also use enum: list
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        self.assertEqual(method(self, 'a'), 'a')
        self.assertEqual(method(self, 'b'), 'b')

        msg = ("TestDocValidator.test_enum_str.<locals>.method: "
               "forbidden value for 'arg1' (got 'c', expected ['a', 'b'])")
        with self.assertRaisesWith(ValueError, msg):
            method(self, 'c')

    def test_enum_int(self):
        """Test that the basic usage of an enum check on ints works"""
        @docval({'name': 'arg1', 'type': int, 'doc': 'an arg', 'enum': (1, 2)})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        self.assertEqual(method(self, 1), 1)
        self.assertEqual(method(self, 2), 2)

        msg = ("TestDocValidator.test_enum_int.<locals>.method: "
               "forbidden value for 'arg1' (got 3, expected (1, 2))")
        with self.assertRaisesWith(ValueError, msg):
            method(self, 3)

    def test_enum_uint(self):
        """Test that the basic usage of an enum check on uints works"""
        @docval({'name': 'arg1', 'type': np.uint, 'doc': 'an arg', 'enum': (np.uint(1), np.uint(2))})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        self.assertEqual(method(self, np.uint(1)), np.uint(1))
        self.assertEqual(method(self, np.uint(2)), np.uint(2))

        msg = ("TestDocValidator.test_enum_uint.<locals>.method: "
               "forbidden value for 'arg1' (got 3, expected (1, 2))")
        with self.assertRaisesWith(ValueError, msg):
            method(self, np.uint(3))

    def test_enum_float(self):
        """Test that the basic usage of an enum check on floats works"""
        @docval({'name': 'arg1', 'type': float, 'doc': 'an arg', 'enum': (3.14, )})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        self.assertEqual(method(self, 3.14), 3.14)

        msg = ("TestDocValidator.test_enum_float.<locals>.method: "
               "forbidden value for 'arg1' (got 3.0, expected (3.14,))")
        with self.assertRaisesWith(ValueError, msg):
            method(self, 3.)

    def test_enum_bool_mixed(self):
        """Test that the basic usage of an enum check on a tuple of bool, int, float, and string works"""
        @docval({'name': 'arg1', 'type': (bool, int, float, str, np.uint), 'doc': 'an arg',
                 'enum': (True, 1, 1.0, 'true', np.uint(1))})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        self.assertEqual(method(self, True), True)
        self.assertEqual(method(self, 1), 1)
        self.assertEqual(method(self, 1.0), 1.0)
        self.assertEqual(method(self, 'true'), 'true')
        self.assertEqual(method(self, np.uint(1)), np.uint(1))

        msg = ("TestDocValidator.test_enum_bool_mixed.<locals>.method: "
               "forbidden value for 'arg1' (got 0, expected (True, 1, 1.0, 'true', 1))")
        with self.assertRaisesWith(ValueError, msg):
            method(self, 0)

    def test_enum_bad_type(self):
        """Test that docval with an enum check where the arg type includes an invalid enum type fails"""
        msg = ("docval for arg1: enum checking cannot be used with arg type (<class 'bool'>, <class 'int'>, "
               "<class 'str'>, <class 'numpy.float64'>, <class 'object'>)")
        with self.assertRaisesWith(Exception, msg):
            @docval({'name': 'arg1', 'type': (bool, int, str, np.float64, object), 'doc': 'an arg', 'enum': (1, 2)})
            def method(self, **kwargs):
                return popargs('arg1', kwargs)

    def test_enum_none_type(self):
        """Test that the basic usage of an enum check on None works"""
        msg = ("docval for arg1: enum checking cannot be used with arg type None")
        with self.assertRaisesWith(Exception, msg):
            @docval({'name': 'arg1', 'type': None, 'doc': 'an arg', 'enum': (True, 1, 'true')})
            def method(self, **kwargs):
                pass

    def test_enum_single_allowed(self):
        """Test that docval with an enum check on a single value fails"""
        msg = ("docval for arg1: enum value must be a list or tuple (received <class 'str'>)")
        with self.assertRaisesWith(Exception, msg):
            @docval({'name': 'arg1', 'type': str, 'doc': 'an arg', 'enum': 'only one value'})
            def method(self, **kwargs):
                pass

    def test_enum_str_default(self):
        """Test that docval with an enum check on strings and a default value works"""
        @docval({'name': 'arg1', 'type': str, 'doc': 'an arg', 'default': 'a', 'enum': ['a', 'b']})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        self.assertEqual(method(self), 'a')

        msg = ("TestDocValidator.test_enum_str_default.<locals>.method: "
               "forbidden value for 'arg1' (got 'c', expected ['a', 'b'])")
        with self.assertRaisesWith(ValueError, msg):
            method(self, 'c')

    def test_enum_str_none_default(self):
        """Test that docval with an enum check on strings and a None default value works"""
        @docval({'name': 'arg1', 'type': str, 'doc': 'an arg', 'default': None, 'enum': ['a', 'b']})
        def method(self, **kwargs):
            return popargs('arg1', kwargs)

        self.assertIsNone(method(self))

    def test_enum_forbidden_values(self):
        """Test that docval with enum values that include a forbidden type fails"""
        msg = ("docval for arg1: enum values are of types not allowed by arg type "
               "(got [<class 'bool'>, <class 'list'>], expected <class 'bool'>)")
        with self.assertRaisesWith(Exception, msg):
            @docval({'name': 'arg1', 'type': bool, 'doc': 'an arg', 'enum': (True, [])})
            def method(self, **kwargs):
                pass


class TestDocValidatorChain(TestCase):

    def setUp(self):
        self.obj1 = MyChainClass('base', [[1, 2], [3, 4], [5, 6]], [[10, 20]])
        # note that self.obj1.arg3 == [[1, 2], [3, 4], [5, 6]]

    def test_type_arg(self):
        """Test that passing an object for an argument that allows a specific type works"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], [[10, 20]])
        self.assertEqual(obj2.arg1, 'base')

    def test_type_arg_wrong_type(self):
        """Test that passing an object for an argument that does not match a specific type raises an error"""
        err_msg = "MyChainClass.__init__: incorrect type for 'arg1' (got 'object', expected 'str or MyChainClass')"
        with self.assertRaisesWith(TypeError, err_msg):
            MyChainClass(object(), [[10, 20], [30, 40], [50, 60]], [[10, 20]])

    def test_shape_valid_unpack(self):
        """Test that passing an object for an argument with required shape tests the shape of object.argument"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], [[10, 20]])
        obj3 = MyChainClass(self.obj1, obj2, [[100, 200]])
        self.assertListEqual(obj3.arg3, obj2.arg3)

    def test_shape_invalid_unpack(self):
        """Test that passing an object for an argument with required shape and object.argument has an invalid shape
        raises an error"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], [[10, 20]])
        # change arg3 of obj2 to fail the required shape - contrived, but could happen because datasets can change
        # shape after an object is initialized
        obj2.arg3 = [10, 20, 30]

        err_msg = "MyChainClass.__init__: incorrect shape for 'arg3' (got '(3,)', expected '(None, 2)')"
        with self.assertRaisesWith(ValueError, err_msg):
            MyChainClass(self.obj1, obj2, [[100, 200]])

    def test_shape_none_unpack(self):
        """Test that passing an object for an argument with required shape and object.argument is None is OK"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], [[10, 20]])
        obj2.arg3 = None
        obj3 = MyChainClass(self.obj1, obj2, [[100, 200]])
        self.assertIsNone(obj3.arg3)

    def test_shape_other_unpack(self):
        """Test that passing an object for an argument with required shape and object.argument is an object without
        an argument attribute raises an error"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], [[10, 20]])
        obj2.arg3 = object()

        err_msg = (r"cannot check shape of object '<object object at .*>' for argument 'arg3' "
                   r"\(expected shape '\(None, 2\)'\)")
        with self.assertRaisesRegex(ValueError, err_msg):
            MyChainClass(self.obj1, obj2, [[100, 200]])

    def test_shape_valid_unpack_default(self):
        """Test that passing an object for an argument with required shape and a default value tests the shape of
        object.argument"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], arg4=[[10, 20]])
        obj3 = MyChainClass(self.obj1, [[100, 200], [300, 400], [500, 600]], arg4=obj2)
        self.assertListEqual(obj3.arg4, obj2.arg4)

    def test_shape_invalid_unpack_default(self):
        """Test that passing an object for an argument with required shape and a default value and object.argument has
        an invalid shape raises an error"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], arg4=[[10, 20]])
        # change arg3 of obj2 to fail the required shape - contrived, but could happen because datasets can change
        # shape after an object is initialized
        obj2.arg4 = [10, 20, 30]

        err_msg = "MyChainClass.__init__: incorrect shape for 'arg4' (got '(3,)', expected '(None, 2)')"
        with self.assertRaisesWith(ValueError, err_msg):
            MyChainClass(self.obj1, [[100, 200], [300, 400], [500, 600]], arg4=obj2)

    def test_shape_none_unpack_default(self):
        """Test that passing an object for an argument with required shape and a default value and object.argument is
        an object without an argument attribute raises an error"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], arg4=[[10, 20]])
        # change arg3 of obj2 to fail the required shape - contrived, but could happen because datasets can change
        # shape after an object is initialized
        obj2.arg4 = None
        obj3 = MyChainClass(self.obj1, [[100, 200], [300, 400], [500, 600]], arg4=obj2)
        self.assertIsNone(obj3.arg4)

    def test_shape_other_unpack_default(self):
        """Test that passing an object for an argument with required shape and a default value and object.argument is
        None is OK"""
        obj2 = MyChainClass(self.obj1, [[10, 20], [30, 40], [50, 60]], arg4=[[10, 20]])
        # change arg3 of obj2 to fail the required shape - contrived, but could happen because datasets can change
        # shape after an object is initialized
        obj2.arg4 = object()

        err_msg = (r"cannot check shape of object '<object object at .*>' for argument 'arg4' "
                   r"\(expected shape '\(None, 2\)'\)")
        with self.assertRaisesRegex(ValueError, err_msg):
            MyChainClass(self.obj1, [[100, 200], [300, 400], [500, 600]], arg4=obj2)


class TestGetargs(TestCase):
    """Test the getargs function and its error conditions."""

    def test_one_arg_first(self):
        kwargs = {'a': 1, 'b': None}
        expected_kwargs = kwargs.copy()
        res = getargs('a', kwargs)
        self.assertEqual(res, 1)
        self.assertDictEqual(kwargs, expected_kwargs)

    def test_one_arg_second(self):
        kwargs = {'a': 1, 'b': None}
        expected_kwargs = kwargs.copy()
        res = getargs('b', kwargs)
        self.assertEqual(res, None)
        self.assertDictEqual(kwargs, expected_kwargs)

    def test_many_args_get_some(self):
        kwargs = {'a': 1, 'b': None, 'c': 3}
        expected_kwargs = kwargs.copy()

        res = getargs('a', 'c', kwargs)
        self.assertListEqual(res, [1, 3])
        self.assertDictEqual(kwargs, expected_kwargs)

    def test_many_args_get_all(self):
        kwargs = {'a': 1, 'b': None, 'c': 3}
        expected_kwargs = kwargs.copy()

        res = getargs('a', 'b', 'c', kwargs)
        self.assertListEqual(res, [1, None, 3])
        self.assertDictEqual(kwargs, expected_kwargs)

    def test_many_args_reverse(self):
        kwargs = {'a': 1, 'b': None, 'c': 3}
        expected_kwargs = kwargs.copy()
        res = getargs('c', 'b', 'a', kwargs)
        self.assertListEqual(res, [3, None, 1])
        self.assertDictEqual(kwargs, expected_kwargs)

    def test_many_args_unpack(self):
        kwargs = {'a': 1, 'b': None, 'c': 3}
        expected_kwargs = kwargs.copy()
        res1, res2, res3 = getargs('a', 'b', 'c', kwargs)
        self.assertEqual(res1, 1)
        self.assertEqual(res2, None)
        self.assertEqual(res3, 3)
        self.assertDictEqual(kwargs, expected_kwargs)

    def test_too_few_args(self):
        kwargs = {'a': 1, 'b': None}
        msg = 'Must supply at least one key and a dict'
        with self.assertRaisesWith(ValueError, msg):
            getargs(kwargs)

    def test_last_arg_not_dict(self):
        kwargs = {'a': 1, 'b': None}
        msg = 'Last argument must be a dict'
        with self.assertRaisesWith(ValueError, msg):
            getargs(kwargs, 'a')

    def test_arg_not_found_one_arg(self):
        kwargs = {'a': 1, 'b': None}
        msg = "Argument not found in dict: 'c'"
        with self.assertRaisesWith(ValueError, msg):
            getargs('c', kwargs)

    def test_arg_not_found_many_args(self):
        kwargs = {'a': 1, 'b': None}
        msg = "Argument not found in dict: 'c'"
        with self.assertRaisesWith(ValueError, msg):
            getargs('a', 'c', kwargs)


class TestPopargs(TestCase):
    """Test the popargs function and its error conditions."""

    def test_one_arg_first(self):
        kwargs = {'a': 1, 'b': None}
        res = popargs('a', kwargs)
        self.assertEqual(res, 1)
        self.assertDictEqual(kwargs, {'b': None})

    def test_one_arg_second(self):
        kwargs = {'a': 1, 'b': None}
        res = popargs('b', kwargs)
        self.assertEqual(res, None)
        self.assertDictEqual(kwargs, {'a': 1})

    def test_many_args_pop_some(self):
        kwargs = {'a': 1, 'b': None, 'c': 3}
        res = popargs('a', 'c', kwargs)
        self.assertListEqual(res, [1, 3])
        self.assertDictEqual(kwargs, {'b': None})

    def test_many_args_pop_all(self):
        kwargs = {'a': 1, 'b': None, 'c': 3}
        res = popargs('a', 'b', 'c', kwargs)
        self.assertListEqual(res, [1, None, 3])
        self.assertDictEqual(kwargs, {})

    def test_many_args_reverse(self):
        kwargs = {'a': 1, 'b': None, 'c': 3}
        res = popargs('c', 'b', 'a', kwargs)
        self.assertListEqual(res, [3, None, 1])
        self.assertDictEqual(kwargs, {})

    def test_many_args_unpack(self):
        kwargs = {'a': 1, 'b': None, 'c': 3}
        res1, res2, res3 = popargs('a', 'b', 'c', kwargs)
        self.assertEqual(res1, 1)
        self.assertEqual(res2, None)
        self.assertEqual(res3, 3)
        self.assertDictEqual(kwargs, {})

    def test_too_few_args(self):
        kwargs = {'a': 1, 'b': None}
        msg = 'Must supply at least one key and a dict'
        with self.assertRaisesWith(ValueError, msg):
            popargs(kwargs)

    def test_last_arg_not_dict(self):
        kwargs = {'a': 1, 'b': None}
        msg = 'Last argument must be a dict'
        with self.assertRaisesWith(ValueError, msg):
            popargs(kwargs, 'a')

    def test_arg_not_found_one_arg(self):
        kwargs = {'a': 1, 'b': None}
        msg = "Argument not found in dict: 'c'"
        with self.assertRaisesWith(ValueError, msg):
            popargs('c', kwargs)

    def test_arg_not_found_many_args(self):
        kwargs = {'a': 1, 'b': None}
        msg = "Argument not found in dict: 'c'"
        with self.assertRaisesWith(ValueError, msg):
            popargs('a', 'c', kwargs)


class TestMacro(TestCase):

    def test_macro(self):
        self.assertTrue(isinstance(get_docval_macro(), dict))
        self.assertSetEqual(set(get_docval_macro().keys()), {'array_data', 'scalar_data', 'data'})

        self.assertTupleEqual(get_docval_macro('scalar_data'), (str, int, float, bytes, bool))

        @docval_macro('scalar_data')
        class Dummy1:
            pass

        self.assertTupleEqual(get_docval_macro('scalar_data'), (str, int, float, bytes, bool, Dummy1))

        @docval_macro('dummy')
        class Dummy2:
            pass

        self.assertTupleEqual(get_docval_macro('dummy'), (Dummy2, ))
