import os
import glob

folder = './graphs'
for filepath in glob.glob(folder + '/*'):
    data_graph = os.path.basename(filepath)
    print(data_graph)
    v_str = []
    e_str = []
    with open(filepath, "r") as file:
        for line in file:
            elem = line.split()
            v_str.append('v {} {}'.format(elem[0], elem[1]))
            if len(elem) > 2:
                for x in range(2, len(elem)):
                    e_str.append('e {} {} 1'.format(elem[0], elem[x]))

    file_write_string = 't 1 {}\n'.format(str(len(v_str)))
    file_write_string += '\n'.join(v_str)
    file_write_string += '\n'
    file_write_string += '\n'.join(e_str)

    with open(data_graph+'_cfl', 'w') as file:
        file.write(file_write_string)

