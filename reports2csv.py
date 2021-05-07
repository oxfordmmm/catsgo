import json

import argh

def get_list_value(dict1,path1):
    '''
    path1 = LEV|gene_name, mutation_name
    result = gyrA_E21Q|gyrA_S95T
    '''
    key = path1.split('|')[0]
    keys = path1.split('|')[1].split(',')

    list1 = dict1[key]

    val1 = dict()
    val1_list = []
    for dic1 in list1:
        val_list = []
        for k in keys:
            if k in dic1.keys():
                val_list.append(dic1[k])
        val_list_string = '_'.join(val_list)
        val1_list.append(val_list_string)
    val1 = '|'.join(val1_list)
    return val1

def get_val_from_dict(dict1, path1):
    try:
        for p in path1.split("."):
            if '|' in p:
                dict1 = get_list_value(dict1,p)
            else:
                dict1 = dict1[p]
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
               "resistance.data.INH|gene_name,mutation_name",
               "resistance.data.prediction_ex.RIF",
               "resistance.data.RIF|gene_name,mutation_name",
               "resistance.data.prediction_ex.PZA",
               "resistance.data.PZA|gene_name,mutation_name",
               "resistance.data.prediction_ex.EMB",
               "resistance.data.EMB|gene_name,mutation_name",
               "resistance.data.prediction_ex.AMI",
               "resistance.data.AMI|gene_name,mutation_name",
               "resistance.data.prediction_ex.KAN",
               "resistance.data.KAN|gene_name,mutation_name",
               "resistance.data.prediction_ex.LEV",
               "resistance.data.LEV|gene_name,mutation_name",
               "resistance.data.prediction_ex.STM",
               "resistance.data.STM|gene_name,mutation_name",
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
