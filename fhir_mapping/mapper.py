import yaml
import json

def get_value_from_path(data, path):
    """Retrieve a nested value from JSON using dot notation."""
    keys = path.split(".")
    for k in keys:
        if isinstance(data, dict):
            data = data.get(k)
        else:
            return None
    return data

def set_value_in_path(target_dict, path, value):
    """Set a value in a nested dict using dot notation (creates intermediate dicts)."""
    keys = path.split(".")
    for k in keys[:-1]:
        target_dict = target_dict.setdefault(k, {})
    target_dict[keys[-1]] = value

def transform_to_fhir(vendor_json, mapping_yaml):
    fhir_resources = []

    for field_name, mapping in mapping_yaml["mappings"].items():
        source_path = mapping["source"]
        fhir_path = mapping["target"]
        code = mapping["code"]
        unit = mapping["unit"]

        value = get_value_from_path(vendor_json, source_path)
        if value is None:
            continue

        # Build a minimal FHIR Observation
        fhir_resource = {
            "resourceType": "Observation",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": code, "display": field_name}]},
            "valueQuantity": {"value": value, "unit": unit}
        }

        # Apply mapping target if deeper structure needed
        set_value_in_path(fhir_resource, fhir_path, value)

        fhir_resources.append(fhir_resource)

    return fhir_resources


if __name__ == "__main__":
    with open("test_mapping.yaml", "r") as f:
        mapping_yaml = yaml.safe_load(f)

    with open("test_input.json", "r") as f:
        vendor_json = json.load(f)

    fhir_output = transform_to_fhir(vendor_json, mapping_yaml)

    print(json.dumps(fhir_output, indent=2))
