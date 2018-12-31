import subprocess
import glob
import time

def parse_enumeration(output):
    x = output.split('\n')
    embeds = set()
    for line in x:
        if 'Mapping' not in line:
            continue
        comb = line.split('=>')
        comb = comb[1].strip().split()
        emb = []
        for y in comb:
            emb.append(y.split(':')[1])
        embeds.add(','.join(sorted(emb)))
    time.sleep(1)
    print('Number of Embeddings (de-duplicated) => ', len(embeds))


query_folder = './queries_cfl'

#input_graph = 'citeseer'
input_graph = 'mico'
datagraph = '{}.unsafe.graph_cfl'.format(input_graph)

for filepath in glob.glob(query_folder + '/*{}*'.format(input_graph)):
    cmd = '../{} {} {} -f 1 100M'.format('CFLMatch_Enumeration', datagraph, filepath)

    p = subprocess.Popen([cmd], stdout=subprocess.PIPE, shell=True)
    output, errors = p.communicate()
    output = output.decode("utf-8")

    parse_enumeration(output)

# cmd = '../{} {} {} -f 1 1B'.format('CFLMatch', datagraph, query)

# p = subprocess.Popen([cmd], stdout=subprocess.PIPE, shell=True)
# # outChars = p.stderr.read()
# output, errors = p.communicate()
# errors = errors.decode("utf-8")
#
# idx_start = errors.find('Average Search Time Per Query')
# perf_numbers = errors[idx_start:]
# stats = perf_numbers.split('\n')
# stats_dict = {}
# for stat in stats:
#     if '=>' not in stat:
#         continue
#     x = stat.split('=>', 2)
#     stats_dict[x[0].strip()] = x[1].split()[0].strip()
# print(stats_dict)