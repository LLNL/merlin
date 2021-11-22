import yaml
from jinja2 import Environment, FileSystemLoader, meta


# get all variable in template file
def get_variables(filename):
    env = Environment(loader=FileSystemLoader("./"))
    template_source = env.loader.get_source(env, filename)[0]
    parsed_content = env.parse(template_source)

    return meta.find_undeclared_variables(parsed_content)


def get_dict_from_yaml(filename, get_template=True):
    env = Environment(loader=FileSystemLoader("./"))
    template = env.get_template(filename)
    outputText = template.render()

    if get_template:
        return yaml.safe_load(outputText), template
    return yaml.safe_load(outputText)


def get_bounds_X(test_function):
    return {
        "rosenbrock": str([[-2, 2] for i in range(N_DIMS)]).replace(" ", ""),
        "ackley": str([[-5, 5] for i in range(2)]).replace(" ", ""),
        "rastrigin": str([[-5.12, 5.12] for i in range(N_DIMS)]).replace(" ", ""),
        "griewank": str([[-10, 10] for i in range(N_DIMS)]).replace(" ", ""),
    }[test_function]


filename = "template_optimization.temp"

workflow_dict, template = get_dict_from_yaml(filename)
undefined_variables = get_variables(filename)

TEST_FUNCTION = workflow_dict["env"]["variables"]["TEST_FUNCTION"]
DEBUG = workflow_dict["env"]["variables"]["DEBUG"]
N_DIMS = workflow_dict["env"]["variables"]["N_DIMS"]

if (TEST_FUNCTION == "ackley") and (N_DIMS != 2):
    raise Exception("The ackley function only accepts 2 dims, change the N_DIMS variable")

bounds_x = get_bounds_X(TEST_FUNCTION)
uncerts_x = [0.1 for i in range(N_DIMS)]
column_labels = [f"INPUT_{i + 1}" for i in range(N_DIMS)]

if DEBUG:
    # Reduce the number of iterations and the number of samples per iteration
    max_iter = 3

    n_samples = 2
    n_exploit = 2
    n_samples_line = 2
    n_samples_start = 8

defined_variables = {}
for undefined_variable in undefined_variables:
    if undefined_variable in locals():
        defined_variables[undefined_variable] = globals()[undefined_variable]
    else:
        print(f"Variable '{undefined_variable}' is not defined, using default specified in yaml file")

# to save the results
with open("optimization.yaml", "w") as fh:
    fh.write(template.render(defined_variables))
