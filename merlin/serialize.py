from maestrowf.abstracts import enums
from maestrowf.datastructures.environment import Variable
from maestrowf.datastructures.core.study import StudyStep
from maestrowf.datastructures.core.executiongraph import _StepRecord, ExecutionGraph
from merlin.study.step import MerlinStep
from merlin.study.dag import MerlinDAG
from collections import deque

import enum

import json


MAESTRO_ENUMS = {"State": enums.State, "JobStatusCode": enums.JobStatusCode, "SubmissionCode": enums.SubmissionCode, "CancelCode": enums.CancelCode, "StudyStatus": enums.StudyStatus}


class MerlinEncoder(json.JSONEncoder):
    """
    Encode Merlin / Maestro objects into a json string.
    """
    def to_dict(obj):
        """
        Makes a Merlin or Maestro object into nested python objects recognized by
        json, such as dict, str, list, int, etc.
        """
        if isinstance(obj, dict) or isinstance(obj, list):
            return obj
        if isinstance(obj, enum.Enum):
            return {"__enum__": str(obj)}
        if isinstance(obj, Variable):
            return {"__Variable__": obj.__dict__}
        if isinstance(obj, set):
            return {"__set__": list(obj)}
        if isinstance(obj, deque):
            return {"__deque__": list(obj)}
        if isinstance(obj, StudyStep):
            return {"__StudyStep__": obj.__dict__}
        if isinstance(obj, _StepRecord):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = to_dict(v)
            return {"___StepRecord__": result}
        if isinstance(obj, ExecutionGraph):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = to_dict(v)
            return {"__ExecutionGraph__": result}
        if isinstance(obj, MerlinDAG):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = to_dict(v)
            return {"__MerlinDAG__": result}
        if isinstance(obj, MerlinStep):
            result = {}
            for k, v in obj.__dict__.items():
                result[k] = to_dict(v)
            return {"__MerlinStep__": result}
        else:
            print(f"Type '{type(obj)}' not supported!")
            return obj

    def encode(self, obj):
        d = to_dict(obj)
        return json.dumps(d, indent=4)


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
        if "__Variable__" in dct:
            return Variable(**dct["__Variable__"])
        if "__StudyStep__" in dct:
            dct = dct["__StudyStep__"]
            result = StudyStep()
            for k,v in dct.items():
                setattr(result, k, v)
            result.name = dct["name"]
            result.description = dct["description"]
            result.run = dct["run"]
            return result
        if "___StepRecord__" in dct:
            dct = dct["___StepRecord__"]
            dummy_step = StudyStep()
            result = _StepRecord("workspace", dummy_step)
            for k,v in dct.items():
                setattr(result, k, v)
            return result
        if "__ExecutionGraph__" in dct:
            dct = dct["__ExecutionGraph__"]
            result = ExecutionGraph()
            for k,v in dct.items():
                setattr(result, k, v)
            return result
        if "__MerlinStep__" in dct:
            dct = dct["__MerlinStep__"]
            dummy_step = StudyStep()
            dummy_step_record = _StepRecord("workspace", dummy_step)
            result = MerlinStep(dummy_step_record)
            for k,v in dct.items():
                setattr(result, k, v)
            return result
        if "__MerlinDAG__" in dct:
            dct = dct["__MerlinDAG__"]
            result = MerlinDAG(ExecutionGraph(), None)
            for k,v in dct.items():
                setattr(result, k, v)
            return result
        else:
            return dct
