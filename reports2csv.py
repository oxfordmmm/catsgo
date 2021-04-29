import json

import argh

def get_val_from_dict(dict1, path1):
    try:
        for p in path1.split("."):
            dict1 = dict1[p]
            if type(dict1) == list and dict1:
                dict1 = dict1[0]
        val1 = dict1
        if type(val1) == dict:
            val1 = list(dict1.keys())[0]
        if type(val1) == list:
            val1 = val1[0]
    except KeyError:
        return ""
    return val1

def main(reports_json_file):
    report_data = json.loads(open(reports_json_file).read())

    my_cols = ["resistance.data.prediction_ex.INH",
               "resistance.data.prediction_ex.RIF",
               "resistance.data.prediction_ex.PZA",
               "resistance.data.prediction_ex.EMB",
               "resistance.data.prediction_ex.AMI",
               "resistance.data.prediction_ex.KAN",
               "resistance.data.prediction_ex.LEV",
               "resistance.data.prediction_ex.STM",
               "mykrobe_speciation.data.phylo_group",
               "mykrobe_speciation.data.sub_complex",
               "mykrobe_speciation.data.species",
               "mykrobe_speciation.data.lineages"
               ]
    
    out = list()

    header = list()
    header.append("sample_name")
    for col in my_cols:
        parts = col.split(".")
        header.append(parts[-1])
    print(",".join(header))
    
    for k, v in report_data.items():
        row = list()
        row.append(k)
        for col in my_cols:
            row.append(get_val_from_dict(v, col))
        out.append(",".join(row))
    return out


if __name__ == "__main__":
    argh.dispatch_command(main)
