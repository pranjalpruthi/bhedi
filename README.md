# BHEDI PIPELINE
## Introduction
BHEDI (Biomarker-based Heuristic Engine for Dengue Identification) is a computational tool designed for the identification of Dengue virus serotypes in wastewater next-generation sequencing data. It leverages specific genomic fragments, referred to as sankets, to detect sequences associated with the Dengue virus. This repository contains the command-line interface (CLI) and API for processing FASTQ files and identifying Dengue virus serotypes.

## Installation

### Prerequisites
- Go (1.15 or later)
- SeqKit

### Installing SeqKit
SeqKit must be installed as a prerequisite. You can install SeqKit by following the instructions on its GitHub repository: [SeqKit GitHub](https://github.com/shenwei356/seqkit).

### Setting Up the BHEDI CLI Tool
1. Clone the repository:
   
```bash
   git clone https://github.com/pranjalpruthi/BHEDI.git
   ```

2. Navigate to the cloned directory:
   
```bash
   cd BHEDI
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

## Contributing
Contributions to the BHEDI project are welcome. Please refer to the CONTRIBUTING.md file for guidelines on how to contribute.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

```
