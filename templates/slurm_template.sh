#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --output={output_path}
#SBATCH --ntasks={n_tasks}
#SBATCH --time=0
#SBATCH --mem-per-cpu={mem_mb}
#SBATCH --cpus-per-task={n_cpus}
#SBATCH --gres=gpu:{n_gpus}
#SBATCH --comment="{comment}"

source activate {dev_env}
srun ipengine --profile={profile} --location={controller_hostname} --cluster-id={cluster_id}
