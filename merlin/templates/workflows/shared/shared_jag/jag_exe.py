"""
Module for running JAG

note: Because JAG is based in python 2, this workflow requires a
python 2.7 virtual environment. Make a py2 venv in merlin/ with:
    make PYTHON=python all
"""

# This has to be set before the JAG is imported and immediately after matplotib
# is imported or else it causes issues with the JAG.
import argparse
import sys
import importlib
import json
import matplotlib

matplotlib.use("Agg")
import numpy

import jag.implosion.implosion as implosion

from conduit_bundler import pack_conduit_node_from_dict, dump_node
from utils import pack


def munge_jag_results(jag_run):
    """ Return a properly formatted result to be serialized. """
    result = {}
    for out in jag_run.data:
        result[out] = jag_run.data[out]
        try:
            pack(result[out])
        except BaseException:
            print("WARNING: Un-Pickleable Data: " + str(result[out]))
            # Conduit doesn't like unicode, thus the excessive ensurance of use
            # of str type instead.
            result[out] = str(str("Un-Pickleable!: ") + str(result[out]))
        if out == "outputs":
            if "images" in result["outputs"]:
                result["outputs"]["images"] = format_jag_images(
                    result["outputs"]["images"]
                )

            if "scalars" in result["outputs"]:
                result["outputs"]["scalars"] = format_jag_scalars(
                    result["outputs"]["scalars"]
                )

            if "timeseries" in result["outputs"]:
                result["outputs"]["timeseries"] = format_jag_timeseries(
                    result["outputs"]["timeseries"]
                )

    return result


def format_jag_images(jag_images):
    """
    Reformat the JAG Images to a nicer dictionary hierarchy.
    """
    # Re-order hierarchy.
    image_dict = {}
    for i in jag_images:
        view = i["view"]
        time = i["image_time"]
        if view in image_dict:
            image_dict[view][time] = i
        else:
            image_dict[view] = {time: i}
    return image_dict


def format_jag_scalars(jag_scalars):
    """
    Reformat JAG scalars.
    Removes image moments, which are pulled into timeseries
    """
    scalar_dict = {}
    for s in jag_scalars:
        if "image" not in s:
            scalar_dict[s] = jag_scalars[s]
    return scalar_dict


def format_jag_timeseries(jag_timeseries):
    """
    Separates time and value
    Records the value's shape, before conduit flattens it automatically
    Necessary b/c some time series are multi-d (eg image moments)
    """
    timeseries = {}
    for ts in jag_timeseries:
        ts_array = numpy.array(jag_timeseries[ts][1])
        timeseries[ts] = {
            "time": numpy.array(jag_timeseries[ts][0]),
            "value": ts_array,
            "value_shape": numpy.array(ts_array.shape).astype(int),
        }
    return timeseries


def jag_implosion(**kwargs):
    """
    Runs the Implosion with the given arguments, packs the results into a
    Conduit Node and returns them.
    """
    run = implosion.implosion(**kwargs)
    try:
        success, exit = run.solve()
    except Exception as e:  # If JAG dies, don't bring down the house
        run.data["outputs"] = {}
        run.data["performance"] = {"success": False, "error": "Exception: " + repr(e)}
    # replace all JAG inputs with only the varying ones
    run.data["inputs"] = kwargs["varying_params"]
    results = pack_conduit_node_from_dict(munge_jag_results(run))
    return results


# def get_inputs(self, sample, varying_params, fixed_params, record=None,
#               dump=True, address=''):
def get_inputs(fixed_params, overridden):
    """
    Generate a Jag Input dictionary from a a fixed_params dictionary.

    Note: JAG requires some inputs as nested dictionaries, so we need some
    logic to catch that (Defined by [key1:key2]=value. This is further
    complicated by some of the nested keys not being strings, eg tuples,
    such as with: inputs['shape_model_initial_modes']={(2,-2):5}
    """
    jag_kwargs = {}
    jag_kwargs["varying_params"] = overridden

    # Fixed_params default is set by the JagEnsemble fixed_params
    # property.
    if fixed_params is not None:
        jag_kwargs.update(fixed_params)

    # We will delete these from scalars and since jag to put them into
    # timeseries.
    if (
        jag_kwargs["postp_image_decompose"]
        and "image_moments" not in jag_kwargs["postp_timeseries_vars"]
    ):
        jag_kwargs["postp_timeseries_vars"].append("image_moments")
    return jag_kwargs


#


def setup_argparse():
    descript = """
    Run a single instance of JAG with input parameters defined by a parameter file and an
    override at the command line.
    """

    # Arguments
    parser = argparse.ArgumentParser(description=descript)
    parser.add_argument(
        "-p",
        "--param_file",
        type=str,
        default="jag_params.py",
        help="JAG static parameter file. Must be simply the name of a file in your PYTHON_PATH.",
    )

    parser.add_argument(
        "-o",
        "--override",
        type=str,
        default="{}",
        help="JSON string with key value pairs to override / add to the parameters read in from the param file",
    )

    parser.add_argument(
        "-f",
        "--output_file",
        type=str,
        default="results.hdf5",
        help="Output conduit file. Should be a .hdf5, .json, or .conduit_bin file.",
    )

    return parser


def process_args(args):
    module_name = args.param_file.split(".py")[0]
    params = importlib.import_module(module_name)
    override = json.loads(args.override)  # [0]
    params.static_shot_params.update(override)

    jag_kwargs = get_inputs(params.static_shot_params, override)

    result = jag_implosion(**jag_kwargs)

    dump_node(result, args.output_file)


def main():
    parser = setup_argparse()
    args = parser.parse_args()
    process_args(args)


if __name__ == "__main__":
    sys.exit(main())
