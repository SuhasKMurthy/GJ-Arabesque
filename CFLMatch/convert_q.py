import os
import glob

folder = './queries/'
for filepath in glob.glob(folder + '/*'):
    q_graph = os.path.basename(filepath)
    count_v = 0
    count_e = 0
    l = []
    with open(filepath, "r") as file:
        for line in file:
            elem = line.split()
            c = len(elem) - 2
            count_e += c
            count_v += 1
            l.append('{} {} {} {}'.format(elem[0], elem[1], str(c), ' '.join(elem[2:])))

    file_write_string = 't 0 {} {}\n'.format(str(count_v), str(count_e))
    file_write_string += '\n'.join(l)
    with open(q_graph+'_cfl', 'w') as file:
        file.write(file_write_string)