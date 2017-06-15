# ipp-tools
Collection of IPyParallel tools and helper scripts

## Installation

`pip install path/to/ipp-tools`

### Requirements

 - ipyparallel

## Slurm Example

`slurm_map` allows for execution of jobs on the `slurm` cluster from within python. You pass `slurm_map` the function you want to run, arguments to run that function on, and a specification of the resources required to run the function, and `slurm_map` does all the dirty work of launching an ipyparallel cluster on `slurm`, connecting to it, running your job, and taking the cluster down once everything is finished. 

The resource specification is a dictionary with the following fields and default values:

  - max_workers: 1
  - min_workers: 1
  - worker_n_cpus: 4
  - worker_n_gpus: 0
  - worker_mem_mbs: 32000
  
Note that you must specify a range of allowable numbers of workers and not an exact number as workers are allocated with the `slurm` scheduler, so depending on usage you may not get the exact number you asked for. `slurm_map` raises a `TimeoutError` if it can't connect to the client or the client doesn't have enough workers, after retrying `n_retries` times. 
  

``` 
from ipp_tools.slurm import slurm_map
import numpy as np 


def my_sq(x):
    return x ** 2
    

# you don't have to specify every field, in this case `min_workers` will be set to it's default value of 1
resource_requirements = {
    'max_workers': 10,
    'worker_n_cpus': 4, 
    'worker_n_gpus': 0,  
    'worker_mem_mbs': 16000  # default: 32000
}

args = np.arange(10)
sqd_args = slurm_map(my_sq, args, resource_requirements, env='virtualenv_to_run_in')

print(sqd_args)
> [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
```

## Caveats
  - don't put huge objects in `args` - it gets serialized and passed around. Instead use `args` as keys and have the workers load large objects from disk. 
