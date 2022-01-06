import pickle


class DebugException(Exception):
    """
    DebugException allows a function to pass internals
    after exception is raised, as a keyword argument

    Usage: DebugException('foo', bar='bar')

    In a shell environment, you can access passed internals
    using sys module

    access foo using sys.last_value.args
    access bar using sys.last_value.kwargs

    %debug
    """
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        super().__init__(*args)


def pickle_read(readfile):
    with open(readfile, 'rb') as picklefile:
        thing = pickle.load(picklefile)
    return thing


def pickle_dump(thing, writefile):
    with open(writefile, 'wb') as picklefile:
        pickle.dump(thing, picklefile)
