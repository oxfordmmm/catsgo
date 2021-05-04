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

    species_cols = [
                    "phylogenetics.phylo_group",
                    "phylogenetics.sub_complex",
                    "phylogenetics.species",
                    "phylogenetics.lineage.lineage"
                   ]
    resistance_cols = [
        "susceptibility.Ofloxacin.predict",
        "susceptibility.Ofloxacin.called_by",
        "susceptibility.Moxifloxacin.predict",
        "susceptibility.Moxifloxacin.called_by",
        "susceptibility.Isoniazid.predict",
        "susceptibility.Isoniazid.called_by",
        "susceptibility.Kanamycin.predict",
        "susceptibility.Kanamycin.called_by",
        "susceptibility.Ethambutol.predict",
        "susceptibility.Ethambutol.called_by",
        "susceptibility.Streptomycin.predict",
        "susceptibility.Streptomycin.called_by",
        "susceptibility.Ciprofloxacin.predict",
        "susceptibility.Ciprofloxacin.called_by",
        "susceptibility.Pyrazinamide.predict",
        "susceptibility.Pyrazinamide.called_by",
        "susceptibility.Rifampicin.predict",
        "susceptibility.Rifampicin.called_by",
        "susceptibility.Amikacin.predict",
        "susceptibility.Amikacin.called_by",
        "susceptibility.Capreomycin.predict",
        "susceptibility.Capreomycin.called_by"
                   ]
    out = list()

    header = list()
    header.append("sample_name")
    for col in species_cols:
        parts = col.split(".")
        header.append(parts[-1])

    for col in resistance_cols:
        parts = col.split(".")
        header.append(f'{parts[-2]}.{parts[-1]}')
        
    print(",".join(header))   
    
    for k, v in report_data.items():
        row = list()
        row.append(k)
        for col in species_cols:
            row.append(get_val_from_dict(v, col))
        for col in resistance_cols:
            row.append(get_val_from_dict(v, col))
        out.append(",".join(row))
    return out


if __name__ == "__main__":
    argh.dispatch_command(main)
