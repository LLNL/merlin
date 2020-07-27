import enum
import json
from collections import deque, OrderedDict

from maestrowf.abstracts import enums as maestro_enums
from maestrowf.datastructures.core.executiongraph import ExecutionGraph, _StepRecord
from maestrowf.datastructures.core.study import StudyStep
from maestrowf.datastructures.environment import Variable

from merlin.study.dag import MerlinDAG
from merlin.study.step import MerlinStep


MAESTRO_ENUMS = {
    "State": maestro_enums.State,
    "JobStatusCode": maestro_enums.JobStatusCode,
    "SubmissionCode": maestro_enums.SubmissionCode,
    "CancelCode": maestro_enums.CancelCode,
    "StudyStatus": maestro_enums.StudyStatus,
}


class MerlinEncoder(json.JSONEncoder):
    """
    Encode Merlin / Maestro objects into a json string.
    """

    @staticmethod
    def to_dict(obj, lvl=0):
        """
        Makes a Merlin or Maestro object into nested python objects recognized by
        json, such as dict, str, list, int, etc.
        """
        if obj is None or type(obj) in {dict, list, tuple, str, int, float, bool}:
            return obj
        print(lvl*"  " + str(type(obj)))
        if isinstance(obj, enum.Enum):
            return {"__enum__": str(obj)}
        if isinstance(obj, Variable):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = (MerlinEncoder.to_dict(v, lvl+1))
            return {"__Variable__": result}
        if isinstance(obj, StudyStep):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = (MerlinEncoder.to_dict(v, lvl+1))
            return {"__StudyStep__": result}
        if isinstance(obj, set):
            result = []
            for item in obj:
                result.append(MerlinEncoder.to_dict(item, lvl+1))
            return {"__set__": result}
        if isinstance(obj, deque):
            result = []
            for item in obj:
                result.append(MerlinEncoder.to_dict(item, lvl+1))
            return {"__deque__": result}
        if isinstance(obj, OrderedDict):
            result = {}
            for k, v in obj.items():
                result[k] = (MerlinEncoder.to_dict(v, lvl+1))
            return {"__OrderedDict__": result}
        if isinstance(obj, _StepRecord):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = MerlinEncoder.to_dict(v, lvl+1)
            return {"___StepRecord__": result}
        if isinstance(obj, ExecutionGraph):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = MerlinEncoder.to_dict(v, lvl+1)
            return {"__ExecutionGraph__": result}
        if isinstance(obj, MerlinDAG):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = MerlinEncoder.to_dict(v, lvl+1)
            return {"__MerlinDAG__": result}
        if isinstance(obj, MerlinStep):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = MerlinEncoder.to_dict(v, lvl+1)
            return {"__MerlinStep__": result}
        else:
            raise TypeError(f"Type '{type(obj)}' not supported by MerlinEncoder!")

    def encode(self, obj):
        d = MerlinEncoder.to_dict(obj)
        try:
            return json.dumps(d, indent=4)
        except Exception as e:
            print(f"Broke on object of type {type(obj)}: {obj}")
            raise e

class MerlinDecoder(json.JSONDecoder):
    """
    Decode Merlin / Maestro objects from json.
    """

    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_obj)

    def dict_to_obj(self, dct):
        if "__enum__" in dct:
            name, member = dct["__enum__"].split(".")
            return getattr(MAESTRO_ENUMS[name], member)
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
        else:
            return dct
