from maestrowf.datastructures.core import ParameterGenerator


def get_custom_generator(env, **kwargs):
    p_gen = ParameterGenerator()
    params = {
        "X2": {"values": [1 / i for i in range(3, 6)], "label": "X2.%%"},
        "N_NEW": {"values": [2**i for i in range(1, 4)], "label": "N_NEW.%%"},
    }

    for key, value in params.items():
        p_gen.add_parameter(key, value["values"], value["label"])

    return p_gen
