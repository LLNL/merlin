from typing import Dict, List, Union

from maestrowf.datastructures.core import ParameterGenerator


def get_custom_generator(env, **kwargs) -> ParameterGenerator:
    """
    Generates parameters to feed at the CL to hello_world.py.
    """
    p_gen: ParameterGenerator = ParameterGenerator()
    params: Dict[str, Union[List[float], str]] = {
        "X2": {"values": [1 / i for i in range(3, 6)], "label": "X2.%%"},
        "N_NEW": {"values": [2**i for i in range(1, 4)], "label": "N_NEW.%%"},
    }
    key: str
    value: Union[List[float], str]
    for key, value in params.items():
        p_gen.add_parameter(key, value["values"], value["label"])

    return p_gen
