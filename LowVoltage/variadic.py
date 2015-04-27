# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import inspect
import functools


def variadic(typ):
    def flatten(args):
        flat_args = []
        for arg in args:
            if isinstance(arg, typ):
                flat_args.append(arg)
            else:
                flat_args.extend(arg)
        return flat_args

    def decorator(wrapped):
        assert wrapped.__doc__ is not None
        spec = inspect.getargspec(wrapped)
        assert len(spec.args) >= 1
        assert spec.varargs is None
        assert spec.keywords is None

        def call_wrapped(*args):
            args = list(args)
            args[-1] = flatten(args[-1])
            return wrapped(*args)

        prototype = list(spec.args)
        if spec.defaults is not None:
            # Could we do that outside of the generated code?
            # Reusing the original default objects instead of dumping them as string would work in more cases.
            # But he this is good enough for None, which is our only use case yet.
            assert spec.defaults[-1] == []
            for i in range(len(prototype) - len(spec.defaults), len(prototype) - 1):
                prototype[i] += "={}".format(spec.defaults[i - len(prototype)])
        prototype[-1] = "*" + prototype[-1]
        prototype = ", ".join(prototype)
        call = ", ".join(spec.args)
        wrapper_code = "def wrapper({}): return call_wrapped({})".format(prototype, call)

        exec_globals = {"call_wrapped": call_wrapped}
        exec wrapper_code in exec_globals
        wrapper = exec_globals["wrapper"]

        functools.update_wrapper(wrapper, wrapped)
        wrapper.__doc__ = "Note that this function is variadic. See :ref:`variadic-functions`.\n\n" + wrapper.__doc__
        return wrapper

    return decorator
