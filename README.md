# A Python API for accessing Forest Inventory and Analysis (FIA) database in parallel

**REPOSITORY CITATION**

Mirzaee, Ashkan. A Python API for accessing Forest Inventory and Analysis (FIA) database in parallel (2021). https://doi.org/10.6084/m9.figshare.14687547

## Access conditions
<a rel="license" href="http://creativecommons.org/licenses/by-sa/3.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/3.0/80x15.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/3.0/">Creative Commons Attribution-ShareAlike 3.0 Unported License</a>.
Sourcecode is available under a [GNU General Public License](https://www.gnu.org/licenses/gpl-3.0.en.html).

## Contact information
- Ashkan Mirzaee: amirzaee@mail.missouri.edu

## System requirement
The workflow in this repository is designed to run in both parallel and serial. To run this application in parallel you need a Linux based cluster with [slurm](https://slurm.schedmd.com/) job scheduling system. As a student, faculty or researcher, you might have access to your institute's cluster by using `ssh username@server.domain` on a Unix Shell (default terminal on Linux and macOS computers) or an SSH Client. If you do not have access to a cluster, the workflow can be run in serial on personal computers' Unix Shell. A basic knowledge about [Unix Shell](https://ashki23.github.io/shell.html) and [HPC](https://ashki23.github.io/hpc.html) can help to follow the workflow easily.

## Abstract
The Forest Inventory and Analysis (FIA) Program of the US Forest Service provides the information needed to assess America's forests. Many researchers rely on forest attribute estimations from the FIA program to evaluate forest conditions. US Forest Service provides multiple methods to use FIA database (FIADB) including EVALIDator and FIA Data Mart. The FIA Data Mart allows users to download raw data files and SQLite databases. Among the available formats, only SQLite database is an option for a large number of queries. To use SQLite database, users require to download entire database (10GB) and establish a local SQLite server. Beside the complexity of the query commands, the local database size growing very fast by implementing more queries. Also, Forest Service update FIADB regularly and for access to new releases users need to update the local server periodically.

On the other hand, EVALIDator relies on the HTML queries and allows users to produce a large variety of population estimates through a web-application with the lowest level of difficulty. However, EVALIDator provides a single query at the time and it makes the web-application not suitable for collecting a large FIA data. The Python API uses the JSON query system to collect large data from FIADB in parallel. It uses EVALIDator to access FIADB, but it can handle large number of queries at a time.

In this project we used Python and Slurm workload manager to generate numerous parallel workers and distribute them across the cluster. The API is designed to scale up the query process such that by increasing processing elements (PE) the process expected to speedup linearly. The API can be set up and configured to be run on a single core computer or in a cluster for any given year, state, coordinate, and forest attribute. It can also search for the closest available FIA survey year to each query and provides a real-time access to most recent releases FIADB. After collecting requested information, outputs are stored on CSV and JSON files and can be used by other scientific software.

## Configurations
The following shows configurable items and template values:
```json
{
    "year" : [2017,2019],
    "state": ["AL","LA"],
    "attribute_cd": [7,8],
    "tolerance": 1,
    "job_number_max": 24,
    "job_time_hr": 8,
    "partition": "General",
    "query_type": ["state","county","coordinate"]
}
```

**Metadata** - `config.json` includes:
- **year:** list of years. For a single year use a singleton list *e.g. [2017]*
- **state:** list of states. Use `["ALL"]` to include all states in the data. For a single state use a singleton list *e.g. ["MO"]*
- **attribute_cd:** list of FIA forest attributes code. For a single code use a singleton list *e.g. [7]*
- **tolerance:** a binary variable of 0 or 1. Set 1 to use the closest available (FIA survey year)[https://apps.fs.usda.gov/fia/datamart/recent_load_history.html] to the listed years, if the listed year is not available in FIA
- **job_number_max:** max number of jobs that can be submitted at the same time. Use 1 for running in serial
- **job_time_hr:** estimated hours that each job might takes (equivalent to `--time` slurm option). Not required to change for running in serial
- **partition:** name of the selected partition in the cluster (equivalent to `--partition` slurm option). Not required to change for running in serial
- **query_type:** type of the FIA query i.e. state, county, and/or coordinate

## Workflow
The workflow includes the following steps:

- Setup the environment
- Download required data by Bash
- Download FIA dataset and generate outputs by Python

You can find the workflow in `batch_file.sh`. After updating `config.json`, run `sbatch batch_file.sh` in a cluster or `source batch_file.sh` in a Unix Shell to submit all jobs and generate outputs. When the database is collected, `python test-db.py` can be used to check the collected information for a coordinate, state or county.
