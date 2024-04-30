package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// Define your structs here (SanketInfo, MatchInfo, ProcessRecordResult, ParquetRecord)
var coverageMapMutex sync.Mutex
var parquetWriterMutex sync.Mutex // Mutex for the Parquet writer

type SanketInfo struct {
	SID      string // Add this line
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
	SID      string // Add this line
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
	SID           string  `parquet:"name=sid, type=BYTE_ARRAY, convertedtype=UTF8"`
	ReadID        string  `parquet:"name=read_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	MatchedSanket string  `parquet:"name=matched_sanket, type=BYTE_ARRAY, convertedtype=UTF8"`
	Serotype      string  `parquet:"name=serotype, type=BYTE_ARRAY, convertedtype=UTF8"`
	GCPercentage  float64 `parquet:"name=gc_percentage, type=DOUBLE"`
	TotalCoverage int32   `parquet:"name=total_coverage, type=INT32"` // Changed to int32
	SLen          int32   `parquet:"name=s_len, type=INT32"`          // Changed to int32
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
func calculateBScore(totalCoverage, sLen int, ssrCount, pCount string, avgReadLength float64, totalRecords int) float64 {
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
		baseScore += 0.35 // Assign a higher base score if both are present
	} else if ssrCountInt > 0 || pCountInt > 0 {
		baseScore += 0.2 // Assign a lower base score if only one is present
	}

	// Calculate maxTotalCoverage using the Lander/Waterman equation
	genomeSize := 11000.0 // Dengue virus genome size in base pairs
	maxTotalCoverage := (avgReadLength * float64(totalRecords)) / genomeSize

	// Normalize and weight totalCoverage and sLen
	// Adjust normalization based on the actual range of totalCoverage values
	maxExpectedCoverage := maxTotalCoverage // You might want to adjust this based on your dataset
	normalizedTotalCoverage := math.Min(float64(totalCoverage)/maxExpectedCoverage, 1)

	maxSLen := 25.0 // Adjust based on expected range
	normalizedSLen := math.Min(float64(sLen)/maxSLen, 1)

	// Weighted contributions (adjust weights as needed)
	totalCoverageWeight := 0.37 // Higher weight for totalCoverage
	sLenWeight := 0.4           // Weight for sLen

	// Calculate weighted contributions
	weightedTotalCoverage := normalizedTotalCoverage * totalCoverageWeight
	weightedSLen := normalizedSLen * sLenWeight

	// Calculate final BScore
	bScore := baseScore + weightedTotalCoverage + weightedSLen

	// Ensure BScore is within the 0-1 range
	bScore = math.Min(math.Max(bScore, 0), 1)

	return bScore
}

func getTotalRecordsAndAvgReadLength(fastqPath string) (totalRecords int, avgReadLength float64, err error) {
	cmd := exec.Command("seqkit", "stats", fastqPath, "--tabular")
	output, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}
	lines := strings.Split(string(output), "\n")
	if len(lines) > 2 {
		fields := strings.Fields(lines[1])
		if len(fields) > 5 {
			totalRecords, err = strconv.Atoi(fields[3])
			if err != nil {
				return 0, 0, fmt.Errorf("failed to parse total records: %w", err)
			}
			avgReadLength, err = strconv.ParseFloat(fields[5], 64)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to parse average read length: %w", err)
			}
			return totalRecords, avgReadLength, nil
		}
	}
	return 0, 0, fmt.Errorf("failed to parse seqkit stats output")
}

func processRecord(seq string, id string, sankets map[string]SanketInfo, avgReadLength float64, totalRecords int) ProcessRecordResult {
	gcPercentage := calculateGCPercentage(seq)
	var matches []MatchInfo
	coverageMap := make(map[string]int)
	matchesFound := false
	for _, info := range sankets {
		if strings.Contains(seq, info.Sanket) {
			matchesFound = true
			match := MatchInfo{
				SID:      info.SID, // Add this line
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
		// Pass the missing avgReadLength and totalRecords arguments
		matches[i].BScore = calculateBScore(totalCoverage, match.SLen, match.SSRCount, match.PCount, avgReadLength, totalRecords)
	}
	return ProcessRecordResult{
		ReadID:        id,
		Matches:       matches,
		GCPercentage:  gcPercentage,
		TotalCoverage: totalCoverage,
		MatchesFound:  matchesFound,
	}
}

// LoadSankets loads sanket information from a CSV file
func LoadSankets(csvFilePath string) (map[string]SanketInfo, error) {
	sankets := make(map[string]SanketInfo)
	csvFile, err := os.Open(csvFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening CSV file: %w", err)
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
			return nil, fmt.Errorf("error reading CSV record: %w", err)
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
			SID:      sid, // Ensure this line is correct
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
	return sankets, nil
}

func processFastqStream(fastqReader io.Reader, sankets map[string]SanketInfo, parquetFilePath string, totalRecords int, avgReadLength float64) error {
	// Initialize the FASTX reader
	reader, err := fastx.NewReaderFromIO(nil, fastqReader, "")
	if err != nil {
		return fmt.Errorf("error initializing FASTX reader: %w", err)
	}

	// Setup Parquet writer
	fw, err := local.NewLocalFileWriter(parquetFilePath)
	if err != nil {
		return fmt.Errorf("can't create local file: %w", err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(ParquetRecord), 4)
	if err != nil {
		return fmt.Errorf("can't create parquet writer: %w", err)
	}
	defer pw.WriteStop()

	// Initialize progress bar
	bar := pb.StartNew(totalRecords)
	defer bar.Finish()

	// Setup concurrency control
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 30) // Limit the number of concurrent goroutines

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading FASTQ record: %w", err)
		}

		// Make deep copies of the data needed by the goroutine
		seqCopy := string(record.Seq.Seq) // This is already a copy, but included for clarity
		idCopy := string(record.ID)

		wg.Add(1)
		semaphore <- struct{}{} // Acquire a token

		go func(seqCopy string, idCopy string) {
			defer wg.Done()
			result := processRecord(seqCopy, idCopy, sankets, avgReadLength, totalRecords)

			if result.MatchesFound {
				// Write each match as a separate record in the Parquet file
				for _, match := range result.Matches {
					parquetRecord := ParquetRecord{
						SID:           match.SID,
						ReadID:        result.ReadID,
						MatchedSanket: match.Sanket,
						Serotype:      match.Serotype,
						GCPercentage:  result.GCPercentage,
						TotalCoverage: int32(result.TotalCoverage),
						SLen:          int32(match.SLen),
						SSRCount:      match.SSRCount,
						MLenAvg:       match.MLenAvg,
						MRCAvg:        match.MRCAvg,
						PCount:        match.PCount,
						PLenAvg:       match.PLenAvg,
						BScore:        match.BScore,
					}
					parquetWriterMutex.Lock()
					if err := pw.Write(parquetRecord); err != nil {
						log.Printf("error writing to Parquet file: %v", err)
					}
					parquetWriterMutex.Unlock()
				}
			} else {
				// Write a record indicating no match was found
				parquetWriterMutex.Lock()
				if err := pw.Write(ParquetRecord{
					ReadID:        result.ReadID,
					MatchedSanket: "No Match Found",
					Serotype:      "Unassigned",
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
					log.Printf("error writing to Parquet file: %v", err)
				}
				parquetWriterMutex.Unlock()
			}

			bar.Increment() // Update progress bar
			<-semaphore     // Release the token
		}(seqCopy, idCopy) // Pass the copies to the goroutine
	}

	wg.Wait() // Wait for all goroutines to finish
	bar.Finish()

	// Lock the mutex before stopping the Parquet writer
	parquetWriterMutex.Lock()
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("error finalizing Parquet file write: %w", err)
	}
	parquetWriterMutex.Unlock() // Unlock the mutex after stopping the writer

	return nil
}
func main() {
	app := fiber.New(fiber.Config{
		BodyLimit: 11 * 1024 * 1024 * 1024, // Set limit to slightly above 10 GB
	})
	app.Use(cors.New()) // Enable CORS for all routes
	app.Use(logger.New())

	app.Post("/upload", func(c *fiber.Ctx) error {
		file, err := c.FormFile("file")
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Upload failed")
		}

		fastqFile, err := file.Open()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to open uploaded file")
		}
		defer fastqFile.Close()

		// Load sankets from CSV
		sankets, err := LoadSankets("sanket.csv") // Specify the path to your CSV file
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to load sankets: %v", err))
		}

		// Save the uploaded file to a temporary location to use it with getTotalRecordsAndAvgReadLength
		tempFile, err := os.CreateTemp("", "fastq-*.tmp")
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to create a temporary file: %v", err))
		}
		defer tempFile.Close()
		defer os.Remove(tempFile.Name()) // Clean up the temp file afterwards

		_, err = io.Copy(tempFile, fastqFile)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to save the uploaded file: %v", err))
		}

		// Get total records and average read length for progress bar and BScore calculation
		totalRecords, avgReadLength, err := getTotalRecordsAndAvgReadLength(tempFile.Name())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to get total records and average read length: %v", err))
		}

		// Re-open the temp file for reading
		fastqFile, err = os.Open(tempFile.Name())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to re-open the temp file")
		}
		defer fastqFile.Close()

		// Process the FASTQ file
		tempParquetFile := "output.parquet" // Consider generating a unique file name
		if err := processFastqStream(fastqFile, sankets, tempParquetFile, totalRecords, avgReadLength); err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to process FASTQ file: %v", err))
		}

		// Return the Parquet file
		return c.Download(tempParquetFile)
	})

	log.Fatal(app.Listen(":3000"))
}
