# Config variables
## Common
var1 = "value1"
var2 = [1, 2, 3]
var3 = {'k': 'value3'}

## 3 DB connections called
var1 = Variable.get("var1")
var2 = Variable.get("var2")
var3 = Variable.get("var3")

## Recommended way
dag_config = Variable.get("example_variables_config", deserialize_json=True)
var1 = dag_config["var1"]
var2 = dag_config["var2"]
var3 = dag_config["var3"]

# You can directly use a variable from a jinja template
## {{ var.value.<variable_name> }}

t2 = BashOperator(
    task_id="get_variable_value",
    bash_command='echo {{ var.value.var3 }} ',
    dag=dag,
)

## {{ var.json.<variable_name> }}
t3 = BashOperator(
    task_id="get_variable_json",
    bash_command='echo {{ var.json.example_variables_config.var3 }} ',
    dag=dag,

