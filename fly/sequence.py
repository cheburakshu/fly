# encoding: utf-8
import contextvars
import functools
import logging
import os
import importlib
import functools
import json
from string import Template
from typing import Dict


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "error").upper(),
    format=os.getenv(
        "LOG_FORMAT",
        "%(asctime)s,%(msecs)03.0f [%(name)-17s][%(levelname)-8s:%(lineno)-4d][%(processName)s:%(process)d] %(message)s",
    ),
    datefmt=os.getenv("LOG_DATEFMT", "%Y-%m-%d %H:%M:%S %Z"),
)


log = logging.getLogger(__name__)


def plain(arg):
    if not arg:
        return None
    return functools.reduce(
        lambda x, kv: str(x).replace(*kv),
        (("$", ""), ("{", ""), ("}", "")),
        arg,
    )


def flatten(data, parent="", sep="."):
    ret = {}
    if not isinstance(data, (dict, list)):
        return data
    if isinstance(data, dict):
        iterable = data.items
    elif isinstance(data, list):
        iterable = functools.partial(enumerate, data)
    for k, v in iterable():
        p = (parent and f"{parent}{sep}{k}") or k
        if isinstance(v, (dict, list)):
            ret.update(flatten(v, parent=p, sep=sep))
        else:
            ret[p] = v
    return ret


def get_param_map(params, args, kwargs):
    ret, sep = {}, "/"
    flat_kwargs = flatten(kwargs, sep=sep)
    print("++++", args, kwargs, flat_kwargs)
    for var in params or {}:
        for idx, arg in enumerate(args or []):
            print("---", var, plain(arg))
            if var == plain(arg):
                ret.setdefault(var, {}).setdefault("arg", []).append(idx)
        for k, v in flat_kwargs.items():
            if var in plain(k).split(sep):
                ret.setdefault(var, {}).setdefault("key", []).append(k)
            if var in plain(v).split(sep):
                ret.setdefault(var, {}).setdefault("value", []).append((k, v))
    return ret


def resolve_function(func, module=None):
    ret = None
    if callable(func):
        ret = func
    else:
        mod = module and importlib.import_module(module) or __builtins__
        ret = getattr(mod, func)
    return ret


def transform(args, kwargs, param_map, transform_dict):
    for param, fun in transform_dict.items():
        meta = param_map[param]
        print("==", meta, "== args=", args)
        for idx in meta.get("arg", []):
            args[idx] = fun(args[idx])
        # TODO: need to implement this correctly for dictioniaries and nested dicts
    return args, kwargs


class Param(Template):
    """ """

    idpattern = "(?a:[_a-z][_a-z0-9]*.[_a-z][_a-z0-9]*.[_a-z][_a-z0-9]*)"

    def __init__(self, template: str) -> None:
        super().__init__(template)


class State(Dict):
    """
    Stores a run state
    """

    def __init__(self):
        super().__init__()
        self.data = {}

    def update(self, ident, stage, data):
        Context.current.setdefault(ident, {}).update({stage: data})
        for key in data:
            Context.current.setdefault("__flat__", {}).update(
                {f"{ident}.{stage}.{key}": data[key]}
            )

    def retrieve(self, ident, stage=None):
        ret = Context.current.get(ident, {})
        if stage:
            return ret.get(stage)
        return ret


class Context:
    """
    A context manager that saves some per-thread state.
    """

    _state_var = contextvars.ContextVar("state", default=None)

    def __init__(self, state):
        self._state = state

    @classmethod
    @property
    def current(cls):
        return cls._state_var.get()

    def __enter__(self):
        self._data = self.__class__._state_var.set(self._state)

    def __exit__(self, *exc):
        self.__class__._state_var.reset(self._data)
        del self._data


class Base:
    def __init__(self, params, ident=None):
        self.ident = ident
        self.params = params

    def run(self):
        raise NotImplemented

    def resolve_params(self, params):
        ret = {}
        if params:
            template = json.dumps(params)
            p = Param(template=template)
            ret = json.loads(
                p.safe_substitute(**Context.current.retrieve(ident="__flat__"))
            )
        return ret


class Data:
    def get(self):
        raise NotImplemented

    def put(self):
        raise NotImplemented


class Input(Base):
    def run(self):
        data = self.resolve_params(self.params)
        return data


class Decision(Base):
    def run(self):
        data = {}
        return data


class Output(Base):
    def run(self):
        data = self.params | Context.current.retrieve(
            ident=self.ident, stage="process"
        ).get("ret")
        return data


class Process(Base):
    def run(self):
        ret = {"ret": None, "error": None, "success": False}
        try:
            _args, _kwargs, _mod, _fun, _transforms, param_map = (
                self.params.get("args", []),
                self.params.get("kwargs", {}),
                self.params.get("import"),
                self.params["fun"],
                self.params.get("transforms", {}),
                {},
            )

            if _transforms:
                print("**** TRANSFORMS ***", _transforms, _args, _kwargs)
                param_map = get_param_map(
                    params=list(_transforms), args=_args, kwargs=_kwargs
                )
                print(param_map)

            args = self.resolve_params(_args)
            kwargs = self.resolve_params(_kwargs)

            if _transforms and param_map:
                args, kwargs = transform(
                    args, kwargs, param_map=param_map, transform_dict=_transforms
                )
                print("*** ARGS< KWARGS***", args, kwargs)

            fun = resolve_function(func=_fun, module=_mod)
            print("&&&", fun)
            # if callable(_fun):
            #     fun = _fun
            # else:
            #     mod = _mod and importlib.import_module(_mod) or __builtins__
            #     fun = getattr(mod, _fun)

            ret["ret"] = fun(*args, **kwargs)
            ret["success"] = True
        except Exception as exc:
            log.exception("Error in stage: %s", exc)
            ret["error"] = str(exc)

        data = self.params | {"ret": ret}
        return data


class Stage(Base):
    def run(self):
        ret = {"ret": None, "success": False, "error": None}
        try:
            for stage in [Input, Decision, Process, Output]:
                stage_name = stage.__name__.lower()
                ret = stage(
                    ident=self.ident, params=self.params.get(stage_name, {})
                ).run()
                Context.current.update(stage=stage_name, data=ret, ident=self.ident)
        except Exception as exc:
            log.exception("Error in stage: %s", exc)
            ret = {"ret": None, "success": False, "error": str(exc)}
            Context.current.update(stage="output", data=ret, ident=self.ident)


class Sequence(Base):
    def get_ident(self, name):
        return (self.ident and f"{self.ident}.{name}") or name

    def run(self):
        ret = None
        with Context(Context.current or State()):
            for stage in self.params:
                ident = self.get_ident(stage["name"])
                if "sequence" in stage:
                    s = Sequence(params=stage["sequence"], ident=ident)
                else:
                    s = Stage(params=stage, ident=ident)
                s.run()
                ret = Context.current.retrieve(ident=s.ident, stage="output")
                print(ret, "===", Context.current)
        return ret


if __name__ == "__main__":
    params = [
        {
            "name": "foo",
            "input": {},
            "decision": {},
            "process": {"fun": "print", "args": [1, 2, 3]},
            "output": {},
        }
    ]
    s = Sequence(params=params)
    print(s.run())

    params = [
        {
            "name": "foo",
            "input": {"key": "value"},
            "process": {
                "fun": "print",
            },
        },
        {
            "name": "bar",
            "sequence": [
                {
                    "name": "foo",
                    "input": {"key": "value"},
                    "decision": {},
                    "process": {"fun": "print", "args": ["running a sequence!"]},
                    "output": {},
                }
            ],
        },
        {
            "name": "parent",
            "sequence": [
                {
                    "name": "child",
                    "sequence": [
                        {
                            "name": "foo",
                            "input": {"key": "value"},
                            "decision": {},
                            "process": {
                                "fun": "print",
                                "args": ["running a sequence in a child!"],
                            },
                            "output": {},
                        }
                    ],
                }
            ],
        },
        {
            "name": "print",
            "input": {},
            "decision": {},
            "process": {"fun": "print", "args": [3]},
            "output": {},
        },
    ]
    s = Sequence(params=params)
    print(s.run())

    # Input string
    params = [
        {
            "name": "foo",
            "input": {"key": "value"},
            "process": {
                "fun": "print",
            },
        }
    ]
    s = Sequence(params=params)
    print(s.run())

    # Param
    p = Param(template="$input_foo.between_bar.c_car")
    assert (p.safe_substitute(**{"input_foo.between_bar.c_car": "value"})) == "value"

    params = [
        {
            "name": "foo",
            "input": {},
            "decision": {},
            "process": {"fun": "sum", "args": [[1, 2]]},
            "output": {},
        }
    ]
    s = Sequence(params=params)
    print(s.run())

    params = [
        {"name": "finput", "process": {"fun": print, "args": ["we", "can", "win"]}}
    ]
    s = Sequence(params=params)
    print(s.run())

    # Flatten
    data = {
        "a": {"b": "c"},
        "d": ["e"],
        "e": 1,
        "f": {"g": {"h": {"i": [{"j": 2}, 1]}}},
    }
    print(flatten(data))
    assert flatten(data) == {
        "a.b": "c",
        "d.0": "e",
        "e": 1,
        "f.g.h.i.0.j": 2,
        "f.g.h.i.1": 1,
    }

    transforms = {"a.b.c": int, "i.j.k": bool}
    args = ["$a.b.c", "${a.b.c}"]
    kwargs = {
        "$a.b.c": "foo.bar",
        "val": "${i.j.k}",
        "list_of_dict": [{"$a.b.c": 1, "$i.j.k": 2}],
    }
    param_map = get_param_map(list(transforms), args, kwargs)
    print(param_map, "---")
    assert param_map == {
        "a.b.c": {"arg": [0, 1], "key": ["$a.b.c", "list_of_dict/0/$a.b.c"]},
        "i.j.k": {"value": [("val", "${i.j.k}")], "key": ["list_of_dict/0/$i.j.k"]},
    }
    args = ["1", "1"]
    kwargs = {
        "1": "foo.bar",
        "val": "abc",
        "list_of_dict": [{"1": 1, "abc": 2}],
    }
    args, kwargs = transform(
        args, kwargs, param_map=param_map, transform_dict=transforms
    )
    print("final =", args, kwargs)
    params = [
        {"name": "var", "input": {"hello": "world"}},
        {
            "name": "foo",
            "input": {"n1": 1, "n2": 2},
            "decision": {},
            "process": {
                "fun": "sum",
                "args": [["$foo.input.n1", "$foo.input.n2"]],
                "transforms": {
                    "foo.input.n1": int,
                    "foo.input.n2": int,
                },
            },
            "output": {},
        },
    ]
    s = Sequence(params=params)
    print(s.run())
