class ComplexSlice(object):

    @docval({'name': 'target', 'type': HDMFDataset, 'help': 'the Container to perform slices on'})
    def __init__(self, **kwargs):
        self.__target = target   # e.g. TimeSeries.data

    @property
    def target(self):
        self.__target = target

    def __getitem__(self, *args):
        new_args = [None] * len(args)
        new_kwargs = dict()
        for i, arg in enumerate(args):
            axis_name = self.target.axis[i]
            axis = getattr(self.target, axis)
            if isinstance(arg, Mask):
                new_kwargs[axis_name], new_args = arg.resolve(axis)
            else:
                # assumes arg is a bool array of length len(axis)
                new_kwargs[axis_name], new_args[i] = axis[arg], arg
        new_kwargs['dataset'] = self.target.dataset[*new_args]
        return self.target.copy(**new_kwargs)



