import re
import os


def validate_variable_values_in_file(file_path):

    variable_pattern = re.compile(
        r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*["\']?(.+?)["\']?\s*$', re.MULTILINE
    )

    illegal_chars = r"[-/:;()$&@.,?!‘“]"

    with open(file_path, "r") as file:
        content = file.read()
    variables = variable_pattern.findall(content)

    for var_name, value in variables:
        if (
            (var_name == "account")
            or (var_name == "local_key_path")
            or (var_name == "password")
        ):
            continue

        if re.search(illegal_chars, value):
            raise ValueError(f"Invalid value '{value}': contains illegal characters.")


def set_application_title(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()

    with open(file_path, "w") as file:
        for line in lines:
            if "title: iaa_streamlit_title" in line:
                file.write("    title: Interactive Assessment Application\n")
            else:
                file.write(line)

    print("Updated: Application Title")


def uppercase_config_values_in_file(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()

    with open(file_path, "w") as file:
        for line in lines:
            if "=" in line:
                key, value = line.split("=", 1)
                if key.strip() in ["account", "password"]:
                    file.write(f"{key}= {value.strip()}\n")
                else:
                    file.write(f"{key}= {value.strip().upper()}\n")
            else:
                file.write(line)

    print("Validated: Config .toml file is valid.")


app_config_file_path = os.path.join("../streamlit/", "snowflake.yml")
app_toml_config_file_name = "iaa_config.toml"

set_application_title(app_config_file_path)
validate_variable_values_in_file(app_toml_config_file_name)
uppercase_config_values_in_file(app_toml_config_file_name)
