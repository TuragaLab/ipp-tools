""" This module contains GPU related utils

"""

import numpy as np

def assigned_gpu_is_free(gpu_id):
    """ Checks to see if gpu with gpu_id is free, returns True if so
    """
    max_memory = 0.1
    max_usage = 0.2
    gpu_id = int(gpu_id)
    gpu_status = fetch_gpu_status()
    gpu_ids = [gpu['id'] for gpu in gpu_status]
    if gpu_id not in gpu_id:
        print("GPU {} not found".format(gpu_id))
        return False

    gpu_idx = np.where(gpu_ids == gpu_id)[0].squeeze()
    gpu_status = gpu_status[gpu_idx]

    if gpu_status['memory_frac'] > max_memory:
        print("Wired memory frac exceeds max")
        return False

    if gpu_status['usage_frac'] > max_usage:
        print("Volatile usage exceeds max")
        return False



def fetch_gpu_status():
    """ Run nvidia-smi and parse the output
    requires Python 2 only dependency
    """
    import commands
    status_code, output = commands.getstatusoutput('nvidia-smi')
    assert status_code == 0

    gpu_records = []
    titan_line = False
    process_block = False
    valid_ids = []
    for line in output.split('\n'):
        # if the line looks like this
        # |   1  TITAN X (Pascal)    Off  | 0000:03:00.0     Off |                  N/A |
        if 'TITAN X' in line:
            titan_line = True
            titan_id = int(_strip_empty_str(line.split('|')[1].split(' '))[0])
            continue
        # if the line looks like this
        # | 79%   87C    P2   221W / 250W |   2291MiB / 12189MiB |    100%      Default |
        if titan_line:
            _, physicals, memory, usage, _ = line.split('|')

            physicals_tokens = _strip_empty_str(physicals.split(' '))
            memory_tokens = _strip_empty_str(memory.split(' '))
            usage_tokens = _strip_empty_str(usage.split(' '))

            record = {
                'physicals': {
                    'fan': physicals_tokens[0],
                    'temp': physicals_tokens[1],
                    'power': physicals_tokens[3]
                },
                'memory_frac': float(memory_tokens[0][:-3]) / float(memory_tokens[2][:-3]),
                'tot_memory': int(memory_tokens[2][:-3]),
                'usage_frac': float(usage_tokens[0][:-1]) / 100.,
                'id': titan_id,
                'processes': []
            }
            gpu_records.append(record)
            valid_ids.append(titan_id)
            titan_line = False
            continue

        # if the line looks like this
        # | Processes:                                                       GPU Memory |
        if 'Processes:' in line:
            process_block = True
            indexed_gpus = {rec['id']: rec for rec in gpu_records}
            continue

        # if the line looks like this
        # |    1     89398    C   python                                        2289MiB |-
        if process_block:
            disallowed_lines = ['+----', '|  GPU', '|======', '|    0 ']
            invalid_line = np.asarray([line.startswith(illegal_line) for illegal_line in disallowed_lines]).any()
            if invalid_line:
                continue
            gpu_id, pid, ptype, name, mem_usage = _strip_empty_str(_strip_empty_str(line.split('|'))[0].split(' '))
            gpu_id = int(gpu_id)
            pid = int(pid)
            if gpu_id in valid_ids:
                process_record = {
                    'pid': pid,
                    'type': ptype,
                    'name': name,
                    'mem_frac': float(mem_usage[:-3])  / indexed_gpus[gpu_id]['tot_memory']
                }
                indexed_gpus[gpu_id]['processes'].append(process_record)
            continue
    return gpu_records

def _strip_empty_str(tok_list):
    tokens = []
    for tok in tok_list:
        if tok is not '':
            tokens.append(tok)
    return tokens
