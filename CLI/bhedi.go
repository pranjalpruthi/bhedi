package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/xopen"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type SanketInfo struct {
	Serotype string
	Sanket   string
	SLen     int
	SSRCount string
	MLenAvg  string
	MRCAvg   string
	PCount   string
	PLenAvg  string
}

type MatchInfo struct {
	Sanket   string
	Serotype string
	SLen     int
	SSRCount string
	MLenAvg  string
	MRCAvg   string
	PCount   string
	PLenAvg  string
	BScore   float64 // Add this line

}

type ProcessRecordResult struct {
	ReadID        string `parquet:"name=read_id, type=UTF8"`
	Matches       []MatchInfo
	GCPercentage  float64 `parquet:"name=gc_percentage, type=DOUBLE"`
	TotalCoverage int     `parquet:"name=total_coverage, type=INT32"`
	MatchesFound  bool
	BScore        float64 `parquet:"name=b_score, type=DOUBLE"`
}

type ParquetRecord struct {
	ReadID        string  `parquet:"name=read_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	MatchedSanket string  `parquet:"name=matched_sanket, type=BYTE_ARRAY, convertedtype=UTF8"`
	Serotype      string  `parquet:"name=serotype, type=BYTE_ARRAY, convertedtype=UTF8"`
	GCPercentage  float64 `parquet:"name=gc_percentage, type=DOUBLE"`
	TotalCoverage int     `parquet:"name=total_coverage, type=INT32"`
	SLen          int     `parquet:"name=s_len, type=INT32"`
	SSRCount      string  `parquet:"name=ssr_count, type=BYTE_ARRAY, convertedtype=UTF8"`
	MLenAvg       string  `parquet:"name=mlen_avg, type=BYTE_ARRAY, convertedtype=UTF8"`
	MRCAvg        string  `parquet:"name=mrc_avg, type=BYTE_ARRAY, convertedtype=UTF8"`
	PCount        string  `parquet:"name=p_count, type=BYTE_ARRAY, convertedtype=UTF8"`
	PLenAvg       string  `parquet:"name=plen_avg, type=BYTE_ARRAY, convertedtype=UTF8"`
	BScore        float64 `parquet:"name=b_score, type=DOUBLE"`
}

func calculateGCPercentage(seq string) float64 {
	gcCount := strings.Count(seq, "G") + strings.Count(seq, "C")
	return (float64(gcCount) / float64(len(seq))) * 100
}

func calculateBScore(totalCoverage, sLen int, ssrCount, pCount string) float64 {
	// Convert string parameters to integers
	ssrCountInt, err1 := strconv.Atoi(ssrCount)
	if err1 != nil {
		ssrCountInt = 0 // Default to 0 if conversion fails
	}
	pCountInt, err2 := strconv.Atoi(pCount)
	if err2 != nil {
		pCountInt = 0 // Default to 0 if conversion fails
	}

	// Initialize base score components
	var baseScore float64 = 0

	// Check for presence of both ssrCount and pCount and adjust base score
	if ssrCountInt > 0 && pCountInt > 0 {
		baseScore += 0.3 // Assign a higher base score if both are present
	} else if ssrCountInt > 0 || pCountInt > 0 {
		baseScore += 0.15 // Assign a lower base score if only one is present
	}

	// Normalize and weight totalCoverage and sLen
	// Assuming maximum expected values for normalization
	maxTotalCoverage := 1000.0 // Adjust based on expected range
	maxSLen := 100.0           // Adjust based on expected range

	normalizedTotalCoverage := math.Min(float64(totalCoverage)/maxTotalCoverage, 1)
	normalizedSLen := math.Min(float64(sLen)/maxSLen, 1)

	// Weighted contributions (adjust weights as needed)
	totalCoverageWeight := 0.3 // Higher weight for totalCoverage
	sLenWeight := 0.4          // Weight for sLen

	// Calculate weighted contributions
	weightedTotalCoverage := normalizedTotalCoverage * totalCoverageWeight
	weightedSLen := normalizedSLen * sLenWeight

	// Calculate final BScore
	bScore := baseScore + weightedTotalCoverage + weightedSLen

	// Ensure BScore is within the 0-1 range
	bScore = math.Min(math.Max(bScore, 0), 1)

	return bScore
}

func processRecord(seq string, id string, sankets map[string]SanketInfo) ProcessRecordResult {
	gcPercentage := calculateGCPercentage(seq)
	var matches []MatchInfo
	coverageMap := make(map[string]int)
	matchesFound := false
	for _, info := range sankets {
		if strings.Contains(seq, info.Sanket) {
			matchesFound = true
			match := MatchInfo{
				Sanket:   info.Sanket,
				Serotype: info.Serotype,
				SLen:     info.SLen,
				SSRCount: info.SSRCount,
				MLenAvg:  info.MLenAvg,
				MRCAvg:   info.MRCAvg,
				PCount:   info.PCount,
				PLenAvg:  info.PLenAvg,
			}
			matches = append(matches, match)
			coverageMap[info.Serotype]++
		}
	}
	totalCoverage := 0
	for _, count := range coverageMap {
		totalCoverage += count
	}
	for i, match := range matches {
		matches[i].BScore = calculateBScore(totalCoverage, match.SLen, match.SSRCount, match.PCount)

	}
	return ProcessRecordResult{
		ReadID:        id,
		Matches:       matches,
		GCPercentage:  gcPercentage,
		TotalCoverage: totalCoverage,
		MatchesFound:  matchesFound,
	}
}
func getTotalRecords(fastqPath string) (int, error) {
	cmd := exec.Command("seqkit", "stats", fastqPath, "--tabular")
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	lines := strings.Split(string(output), "\n")
	if len(lines) > 2 {
		fields := strings.Fields(lines[1])
		if len(fields) > 3 {
			return strconv.Atoi(fields[3])
		}
	}
	return 0, fmt.Errorf("failed to parse seqkit stats output")
}

func processFastqFile(fastqPath string, sankets map[string]SanketInfo, outputDir string) {
	totalRecords, err := getTotalRecords(fastqPath)
	if err != nil {
		fmt.Printf("Error getting total records for %s: %v\n", fastqPath, err)
		return
	}
	fastqFile, err := xopen.Ropen(fastqPath)
	if err != nil {
		fmt.Printf("Error opening FASTQ file %s: %v\n", fastqPath, err)
		return
	}
	defer fastqFile.Close()
	reader, err := fastx.NewDefaultReader(fastqPath)
	if err != nil {
		fmt.Printf("Error initializing FASTX reader for %s: %v\n", fastqPath, err)
		return
	}
	var wg sync.WaitGroup
	results := make(chan ProcessRecordResult, totalRecords)
	bar := pb.StartNew(totalRecords)
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				fmt.Printf("Error reading FASTQ record from %s: %v\n", fastqPath, err)
				return
			}
		}
		seqCopy := string(record.Seq.Seq)
		idCopy := string(record.ID)
		wg.Add(1)
		go func(seq string, id string) {
			defer wg.Done()
			result := processRecord(seq, id, sankets)
			results <- result
			bar.Increment()
		}(seqCopy, idCopy)
	}
	go func() {
		wg.Wait()
		close(results)
		bar.Finish()
	}()
	outputFilePath := filepath.Join(outputDir, filepath.Base(fastqPath)+".parquet")
	fw, err := local.NewLocalFileWriter(outputFilePath)
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(ParquetRecord), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}
	pw.RowGroupSize = 1024 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for result := range results {
		if result.MatchesFound {
			for _, match := range result.Matches {
				if err = pw.Write(ParquetRecord{
					ReadID:        result.ReadID,
					MatchedSanket: match.Sanket,
					Serotype:      match.Serotype,
					GCPercentage:  result.GCPercentage,
					TotalCoverage: result.TotalCoverage,
					SLen:          match.SLen,
					SSRCount:      match.SSRCount,
					MLenAvg:       match.MLenAvg,
					MRCAvg:        match.MRCAvg,
					PCount:        match.PCount,
					PLenAvg:       match.PLenAvg,
					BScore:        match.BScore, // Use match.BScore instead of result.BScore
				}); err != nil {
					fmt.Println("Write error", err)
				}
			}
		} else {
			if err = pw.Write(ParquetRecord{
				ReadID:        result.ReadID,
				MatchedSanket: "No Match Found",
				Serotype:      "N/A",
				GCPercentage:  result.GCPercentage,
				TotalCoverage: 0,
				SLen:          0,
				SSRCount:      "",
				MLenAvg:       "",
				MRCAvg:        "",
				PCount:        "",
				PLenAvg:       "",
				BScore:        0, // Use 0 as BScore for no match found
			}); err != nil {
				fmt.Println("Write error", err)
			}
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
	}
	fw.Close()
	fmt.Printf("Analysis complete for %s. Results saved to %s.\n", fastqPath, outputFilePath)
}

func main() {
	var inputDir, outputDir string
	flag.StringVar(&inputDir, "i", "", "Input directory containing FASTQ files")
	flag.StringVar(&outputDir, "o", "", "Output directory for result files")
	flag.Parse()
	if inputDir == "" || outputDir == "" {
		fmt.Println("Input and output directories must be specified.")
		return
	}
	dirEntries, err := os.ReadDir(inputDir)
	if err != nil {
		fmt.Printf("Error reading directory %s: %v\n", inputDir, err)
		return
	}
	sankets := make(map[string]SanketInfo)
	csvFile, err := os.Open("sanket.csv")
	if err != nil {
		fmt.Printf("Error opening CSV file: %v\n", err)
		return
	}
	defer csvFile.Close()
	r := csv.NewReader(bufio.NewReader(csvFile))
	r.Read() // Skip header
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading CSV record: %v\n", err)
			return
		}
		sid := record[0]
		sanket := record[1]
		sLen, _ := strconv.Atoi(record[2])
		serotype := record[3]
		ssrCount := record[4]
		mlenAvg := record[5]
		mrcAvg := record[6]
		pCount := record[7]
		plenAvg := record[8]
		sankets[sid] = SanketInfo{
			Serotype: serotype,
			Sanket:   sanket,
			SLen:     sLen,
			SSRCount: ssrCount,
			MLenAvg:  mlenAvg,
			MRCAvg:   mrcAvg,
			PCount:   pCount,
			PLenAvg:  plenAvg,
		}
	}
	for _, entry := range dirEntries {
		if !entry.IsDir() {
			fileName := entry.Name()
			if strings.HasSuffix(fileName, ".fastq") {
				fastqPath := filepath.Join(inputDir, fileName)
				processFastqFile(fastqPath, sankets, outputDir)
			}
		}
	}
	fmt.Println("All analyses are complete.")
}
