import enum
import json as std_json
import logging
from collections import OrderedDict, deque

import numpy as np
from kombu.utils.json import JSONEncoder as kombu_JSONEncoder
from maestrowf.abstracts import enums as maestro_enums
from maestrowf.datastructures.core.executiongraph import ExecutionGraph, _StepRecord
from maestrowf.datastructures.core.study import StudyStep
from maestrowf.datastructures.environment import Variable

from merlin.common.sample_index import SampleIndex
from merlin.study.dag import MerlinDAG
from merlin.study.step import MerlinStep
from merlin.common.tasks import merlin_step


MAESTRO_ENUMS = {
    "State": maestro_enums.State,
    "JobStatusCode": maestro_enums.JobStatusCode,
    "SubmissionCode": maestro_enums.SubmissionCode,
    "CancelCode": maestro_enums.CancelCode,
    "StudyStatus": maestro_enums.StudyStatus,
}


LOG = logging.getLogger(__name__)


class MerlinEncoder(kombu_JSONEncoder):
    """
    Encode Merlin / Maestro objects into a json string.
    """

    def default(self, obj, lvl=0):
        reducer = getattr(obj, "__json__", None)
        if reducer is not None:
            return reducer()
        print(lvl * "  " + str(type(obj)))
        if type(obj) == tuple or "celery" in str(type(obj)):
            print(lvl * "  " + str(obj))
        if "celery" in str(type(obj)):
            print(lvl * "  " + str(obj.__dict__))
        if obj is None:
            return std_json.dumps({"__None__": ""})
        if type(obj) in {str, int, float, bool}:
            return std_json.dumps(obj)
        if type(obj) == list:
            result = []
            for item in obj:
                result.append(self.default(item, lvl + 1))
            return std_json.dumps({"__list__": result})
        if type(obj) == tuple:
            result = []
            for item in obj:
                result.append(self.default(item, lvl + 1))
            return std_json.dumps({"__tuple__": tuple(result)})
        if type(obj) == dict:
            result = {}
            for k, v in obj.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps(result)
        if isinstance(obj, enum.Enum):
            return std_json.dumps({"__enum__": str(obj)})
        if isinstance(obj, np.ndarray):
            return std_json.dumps({"__ndarray__": obj.tolist()})
        if isinstance(obj, Variable):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"__Variable__": result})
        if isinstance(obj, StudyStep):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"__StudyStep__": result})
        if isinstance(obj, set):
            result = []
            for item in obj:
                result.append(self.default(item, lvl + 1))
            return std_json.dumps({"__set__": result})
        if isinstance(obj, deque):
            result = []
            for item in obj:
                result.append(self.default(item, lvl + 1))
            return std_json.dumps({"__deque__": result})
        if isinstance(obj, OrderedDict):
            result = {}
            for k, v in obj.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"__OrderedDict__": result})
        if isinstance(obj, _StepRecord):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"___StepRecord__": result})
        if isinstance(obj, ExecutionGraph):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"__ExecutionGraph__": result})
        if isinstance(obj, MerlinDAG):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"__MerlinDAG__": result})
        if isinstance(obj, MerlinStep):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"__MerlinStep__": result})
        if isinstance(obj, SampleIndex):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"__SampleIndex__": result})
        if isinstance(obj, merlin_step):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = self.default(v, lvl + 1)
            return std_json.dumps({"__merlin_step__": result})
        return kombu_JSONEncoder().default(obj)


class MerlinDecoder(std_json.JSONDecoder):
    """
    Decode Merlin / Maestro objects from json.
    """

    def __init__(self):
        std_json.JSONDecoder.__init__(self, object_hook=self.dict_to_obj)

    def dict_to_obj(self, dct):
        if "__None__" in dct:
            return None
        if "__enum__" in dct:
            name, member = dct["__enum__"].split(".")
            return getattr(MAESTRO_ENUMS[name], member)
        if "__ndarray__" in dct:
            return np.asarray(dct["__ndarray__"])
        if "__list__" in dct:
            return list(dct["__list__"])
        if "__tuple__" in dct:
            return tuple(dct["__tuple__"])
        if "__set__" in dct:
            return set(dct["__set__"])
        if "__deque__" in dct:
            return deque(dct["__deque__"])
        if "__OrderedDict__" in dct:
            return OrderedDict(dct["__OrderedDict__"])
        if "__Variable__" in dct:
            return Variable(**dct["__Variable__"])
        if "__StudyStep__" in dct:
            dct = dct["__StudyStep__"]
            result = StudyStep()
            for k, v in dct.items():
                setattr(result, k, v)
            result.name = dct["name"]
            result.description = dct["description"]
            result.run = dct["run"]
            return result
        if "___StepRecord__" in dct:
            dct = dct["___StepRecord__"]
            dummy_step = StudyStep()
            result = _StepRecord("workspace", dummy_step)
            for k, v in dct.items():
                setattr(result, k, v)
            return result
        if "__ExecutionGraph__" in dct:
            dct = dct["__ExecutionGraph__"]
            result = ExecutionGraph()
            for k, v in dct.items():
                setattr(result, k, v)
            return result
        if "__MerlinStep__" in dct:
            dct = dct["__MerlinStep__"]
            dummy_step = StudyStep()
            dummy_step_record = _StepRecord("workspace", dummy_step)
            result = MerlinStep(dummy_step_record)
            for k, v in dct.items():
                setattr(result, k, v)
            return result
        if "__MerlinDAG__" in dct:
            dct = dct["__MerlinDAG__"]
            result = MerlinDAG(ExecutionGraph(), None)
            for k, v in dct.items():
                setattr(result, k, v)
            return result
        if "__SampleIndex__" in dct:
            dct = dct["__SampleIndex__"]
            result = SampleIndex(1, 10, {}, "")
            for k, v in dct.items():
                setattr(result, k, v)
            return result
        else:
            return dct


def dumps(obj):
    encoder = MerlinEncoder()
    return std_json.dumps(obj, cls=MerlinEncoder)


def loads(json_str):
    decoder = MerlinDecoder()
    return decoder.decode(json_str)
