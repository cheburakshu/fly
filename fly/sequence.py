# encoding: utf-8
import contextvars
import logging
import os
import importlib
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
        return Context.current.get(ident, {}).get(stage)


class Context:
    """
    A context manager that saves some per-coroutine state globally.
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


class Data:
    def get(self):
        raise NotImplemented

    def put(self):
        raise NotImplemented


class Input(Base):
    def run(self):
        data = self.params
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
        args, kwargs, _mod, _fun, ret = (
            self.params.get("args", []),
            self.params.get("kwargs", {}),
            self.params.get("import"),
            self.params["fun"],
            {"ret": None, "error": None, "success": False},
        )

        try:
            mod = _mod and importlib.import_module(_mod) or __builtins__
            fun = getattr(mod, _fun)
            ret["ret"] = fun(*args, **kwargs)
            ret["success"] = True
        except Exception as exc:
            ret["error"] = str(exc)

        data = self.params | {"ret": ret}
        return data


class Stage(Base):
    def run(self):
        for stage in [Input, Decision, Process, Output]:
            ret = stage(
                ident=self.ident, params=self.params.get(stage.__name__.lower(), {})
            ).run()
            Context.current.update(
                stage=stage.__name__.lower(), data=ret, ident=self.ident
            )


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
        {
            "name": "foo",
            "input": {"n1": 1, "n2": 2},
            "decision": {},
            "process": {"fun": "sum", "args": [[1, 2]]},
            "output": {},
        }
    ]
    s = Sequence(params=params)
    print(s.run())
