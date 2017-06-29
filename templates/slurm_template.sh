#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --output={output_path}
#SBATCH --ntasks=1
#SBATCH --array=1-{n_tasks}
#SBATCH --time=0
#SBATCH --mem={mem_mb}
#SBATCH --cpus-per-task={n_cpus}
#SBATCH --gres=gpu:{n_gpus}
#SBATCH --comment="{comment}"

srun ~/anaconda3/{engine_path} --profile={profile} --location={controller_hostname} --cluster-id={cluster_id}
