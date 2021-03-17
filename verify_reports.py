#
# python3 verify_reports.py go sp3testdata_v1.json
#

import json, pathlib, os

import argh

def get_val_from_dict(dict1, path1):
    try:
        for p in path1.split("."):
            dict1 = dict1[p]
        val1 = dict1
    except KeyError:
        return None
    return val1

def compare2(sample_name, sp3_report, truth_data):
    errors = list()
    species1 = list(get_val_from_dict(sp3_report, "mykrobe_speciation.data.species").keys())
    species2 = [get_val_from_dict(truth_data, "mykrobe_speciation.data.species")]
    if species1 != species2:
        errors.append(f"{sample_name}: species: {species1} != {species2}")

    lineages1 = get_val_from_dict(sp3_report, "mykrobe_speciation.data.lineages")
    lineages2 = get_val_from_dict(truth_data, "mykrobe_speciation.data.lineages")
    if lineages1 != lineages2:
        errors.append(f"{sample_name}: lineages: {lineages1} != {lineages2}")

    prediction_ex1 = get_val_from_dict(sp3_report, "resistance.data.prediction_ex")
    prediction_ex2 = get_val_from_dict(truth_data, "resistance.data.prediction_ex")
    for p in set(prediction_ex1.keys()).union(set(prediction_ex2.keys())):
        if p not in prediction_ex1:
            errors.append(f"{sample_name}: {p} not in sp3 report prediction_ex")
            continue
        if p not in prediction_ex2:
            errors.append(f"{sample_name}: {p} not in truth data prediction_ex")
            continue
        if prediction_ex1[p] != prediction_ex2[p]:
            errors.append(f"{sample_name}: prediction_ex key {p}: {prediction_ex1[p]} != {prediction_ex2[p]}")

    effects1 = get_val_from_dict(sp3_report, "resistance.data.effects")
    effects2 = get_val_from_dict(truth_data, "resistance.data.effects")
    for effect1 in effects1:
        del effect1['source']
        if effect1 not in effects2:
            errors.append(f"{sample_name}: missing effect in truth data: {effect1}")
    for effect2 in effects2:
        if effect2 not in effects1:
            errors.append(f"{sample_name}: missing effect in sp3 report: {effect2}")

    return errors

def go(reports_file):
    sp3_reports = json.loads(open(reports_file).read())
    truth_files = list(pathlib.Path(pathlib.Path(reports_file).stem).glob("*.json"))
    truth_files = [x.stem for x in truth_files]

    for sample in sp3_reports.keys():
        if sample not in truth_files:
            print(f"{sample} missing in truth files")

    for sample in truth_files:
        if sample not in sp3_reports.keys():
            print(f"{sample} missing in sp3 reports")

    truth_data = dict()
    for sample_name in sp3_reports.keys():
        if sample_name not in truth_files:
            continue
        truth_file = pathlib.Path(reports_file).stem / pathlib.Path(sample_name + ".json")
        truth_data[sample_name] = json.loads(open(truth_file).read())

    errors = dict()
    for sample_name in set(sp3_reports.keys()).intersection(set(truth_files)):
        errors[sample_name] = compare2(sample_name, sp3_reports[sample_name], truth_data[sample_name])

    for sample_name, errors_ in errors.items():
        print("")
        for error in errors_:
            print(error)


if __name__ == "__main__":
    parser = argh.ArghParser()
    parser.add_commands([go])
    parser.dispatch()
