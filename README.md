# βHΞDI 
## Introduction
βHΞDI (Biomarker-based Heuristic Engine for Dengue Identification) is a computational tool designed for the identification of Dengue virus serotypes in wastewater next-generation sequencing data. It leverages specific genomic fragments, referred to as sankets, to detect sequences associated with the Dengue virus. This repository contains the command-line interface (CLI) and API for processing FASTQ files and identifying Dengue virus serotypes.

## Installation

### Prerequisites
- Go (1.15 or later)
- SeqKit
  
### Installing SeqKit
SeqKit must be installed as a prerequisite. You can install SeqKit by following the instructions on its GitHub repository: [SeqKit GitHub](https://github.com/shenwei356/seqkit).

### Setting Up the BHEDI CLI Tool
1. Clone the repository:
   
```bash
   git clone https://github.com/pranjalpruthi/bhedi.git
   ```

2. Navigate to the cloned directory:
   
```bash
   cd bhedi
   ```

3. Build the CLI tool:
   
```bash
   go build -o bhedi-cli
   ```


## Usage

### CLI Tool
To process a FASTQ file and generate a Parquet file with the analysis results, run:

```bash
./bhedi-cli -i <input_dir> -o <output_dir>
```
11da13e1-f06d-45fe-b757-f426801aac98

Replace `<input_dir>` with the directory containing your FASTQ files and `<output_dir>` with the directory where you want the results to be saved.

### API
To start the API server, run:

```bash
go run api/main.go
```

The API will be available at `http://localhost:3000`.

## Dependencies

### CLI Dependencies
- Standard Library Packages: `bufio`, `encoding/csv`, `flag`, `fmt`, `io`, `log`, `math`, `os`, `os/exec`, `path/filepath`, `strconv`, `strings`, `sync`
- Third-Party Packages: `github.com/shenwei356/seqkit`, `github.com/cheggaaa/pb/v3`, `github.com/shenwei356/bio/seqio/fastx`, `github.com/xitongsys/parquet-go-source/local`, `github.com/xitongsys/parquet-go/writer`

### API Dependencies
- Standard Library Packages: Same as CLI, minus `flag`
- Third-Party Packages: `github.com/gofiber/fiber/v2`, `github.com/gofiber/fiber/v2/middleware/cors`, `github.com/gofiber/fiber/v2/middleware/logger`, plus all third-party packages listed under CLI Dependencies

## Notes
- Ensure `seqkit` is installed and accessible in your system's PATH.
- Manage dependencies using Go modules (`go.mod` and `go.sum`) for reproducible builds.
- The API component requires the Fiber web framework and its middleware for CORS and logging.




## Use SimP to Plot reports from βHΞDI-CLI


## SimP Tool

## Introduction
SimP (Simple Plotter) is a visualization tool designed to plot data processed by the βHΞDI CLI tool. It leverages Python libraries such as Pandas, Dask, HoloViews, and Plotly to generate insightful plots from Parquet files containing analysis results of Dengue virus serotypes in wastewater next-generation sequencing data. SimP supports various plot types including GC percentage box plots, serotype frequency heatmaps, and B score distributions.

## Installation

### Prerequisites
- Python 3.10 or later
- Conda or virtualenv (recommended for managing Python packages)

### Dependencies
SimP requires the following Python packages:
- pandas
- dask
- holoviews
- plotly
- argparse
- numpy

You can install these dependencies using pip:

```bash
pip install pandas dask holoviews plotly argparse numpy
```


Or, if you prefer using Conda or Mamba, you can create a new environment and install the required packages:

```bash
conda create -n simp_env python=3.10 pandas dask holoviews plotly numpy
conda activate simp_env
```


```bash
mamba create -n simp_env python=3.10 pandas dask holoviews plotly numpy
mamba activate simp_env
```

### Installing SimP
Currently, SimP is provided as a Python script (`sim.py`). Ensure you have the required dependencies installed in your environment before running the script.

## Usage

To use SimP for plotting, you need to specify the input directory containing the Parquet files processed by βHΞDI CLI and the output directory where the plots will be saved.


```bash
python sim.py -i <input_dir> -o <output_dir>
```


Replace `<input_dir>` with the directory containing your Parquet files and `<output_dir>` with the directory where you want the plots to be saved.

### Example
Assuming you have Parquet files in `/path/to/parquet_files` and you want to save the plots in `/path/to/plots`, run:


```bash
python sim.py -i /path/to/parquet_files -o /path/to/plots
```


This will generate various plots such as GC percentage box plots, serotype frequency heatmaps, and B score distributions, and save them as HTML files in the specified output directory.

## Running on High-Performance Computing Clusters
SimP can also be run on HPC clusters using SLURM. Here's an example SLURM script:


```bash
#!/bin/bash
#SBATCH --job-name=SimP
#SBATCH --output=./log/SimP%j.out
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --mem=16GB
#SBATCH --partition=short

# Activate your Conda environment or Python virtual environment
conda activate simp_env

# Run SimP
time python sim.py -i /path/to/parquet_files -o /path/to/plots
```
]

Adjust the SLURM parameters according to your cluster's configuration and your job's requirements.




## Contributing
Contributions to the βHΞDI project are welcome. Please refer to the CONTRIBUTING.md file for guidelines on how to contribute.

## License
This project is licensed under the AGPLv3 License - see the LICENSE file for details.

```
