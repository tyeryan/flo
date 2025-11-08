package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// NEM12 Record Types
const (
	RecordType100 = "100" // Header
	RecordType200 = "200" // NMI Data Details
	RecordType300 = "300" // Interval Data
	RecordType400 = "400" // Interval Event
	RecordType500 = "500" // B2B Details
	RecordType900 = "900" // End of Data
)

// Header represents a 100 record
type Header struct {
	VersionHeader   string
	DateTime        time.Time
	FromParticipant string
	ToParticipant   string
}

// NMIDataDetails represents a 200 record
type NMIDataDetails struct {
	NMI                   string     // M
	NMIConfiguration      string     // M
	RegisterID            string     // M/N e.g. “1”, “2”, “E1”, “B1”.
	NMISuffix             string     // M e.g. “E1”, “B1”, “Q1”, “K1”.
	MDMDataStreamID       string     // M/N e.g. “N1”, “N2”
	MeterSerialNumber     string     // M/N
	UOM                   string     // M, Unit of Measure
	IntervalLength        int        // M, Time in minutes of each Interval period 5, 15, or 30
	NextScheduledReadDate *time.Time // M/N
}

// IntervalData represents a 300 record
type IntervalData struct {
	IntervalDate      time.Time
	IntervalValues    []float64
	QualityMethod     string
	ReasonCode        int
	ReasonDescription string
	UpdateDateTime    time.Time
	MSATSLoadDateTime *time.Time
}

// IntervalEvent represents a 400 record
type IntervalEvent struct {
	StartInterval     int
	EndInterval       int
	QualityMethod     string
	ReasonCode        int
	ReasonDescription string
}

// B2BDetails represents a 500 record
type B2BDetails struct {
	TransCode       string
	RetServiceOrder string
	ReadDateTime    time.Time
	IndexRead       float64
}

// NMIDataBlock represents a group of records for one NMI
type NMIDataBlock struct {
	DataDetails    NMIDataDetails
	IntervalData   []IntervalData
	IntervalEvents []IntervalEvent
	B2BDetails     []B2BDetails
}

type NEM12File struct {
	Header        Header
	NMIDataBlocks []NMIDataBlock
	TotalRecords  int
}

// File Parser struct with DB to save records
type NEM12FileParser struct {
	DB *sql.DB
}

// Creates new instance of the file parser
func NewNEM12FileParser(db *sql.DB) *NEM12FileParser {
	return &NEM12FileParser{
		DB: db,
	}
}

const schema = `
	CREATE TABLE IF NOT EXISTS meter_readings (
	id UUID default gen_random_uuid() NOT NULL,
	"nmi" VARCHAR(10) NOT NULL,
	"timestamp" TIMESTAMP NOT NULL,
	"consumption" NUMERIC NOT NULL,
	
	CONSTRAINT meter_readings_pk PRIMARY KEY (id),
	CONSTRAINT meter_readings_unique_consumption UNIQUE ("nmi", "timestamp")
	);
`

func (p *NEM12FileParser) InitializeSchema() error {
	_, err := p.DB.Exec(schema)
	return err
}

// according to DateTime (14) specification
// CCYYMMDDhhmmss
func parseNEM12DateTime14(dateTimeStr string) (time.Time, error) {
	return time.Parse("20060102150405", dateTimeStr)
}

// according to DateTime (12) specification
// CCYYMMDDhhmm
func parseNEM12DateTime12(dateTimeStr string) (time.Time, error) {
	return time.Parse("200601021504", dateTimeStr)
}

// according to DateTime (8) specification
// CCYYMMDD
func parseNEM12Date8(dateStr string) (time.Time, error) {
	return time.Parse("20060102", dateStr)
}

func main() {
	fmt.Println("Creating DB connection....")
	ctx := context.Background()
	var err error
	connStr := "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=flo sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	fmt.Println("Successfully connected to database")

	parser := NewNEM12FileParser(db)

	// step 1: initialise database with schema
	fmt.Println("Initializing schema for database...")
	if err := parser.InitializeSchema(); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}
	fmt.Println("Initializing schema for database done")

	// step 2: parse NEM12 file
	filePath := flag.String("f", "test.csv", "path to the NEM12 input file")
	flag.Parse()

	nem12File, err := parser.ParseFile(*filePath)
	if err != nil {
		log.Fatalf("Failed to parse file: %v", err)
	}

	// step 3: save records to database
	err = parser.SaveFileToDB(ctx, nem12File)
	if err != nil {
		log.Fatalf("Failed to save file to DB: %v", err)
	}

	log.Printf("Successfully processed file with %d NMI blocks", len(nem12File.NMIDataBlocks))
}

func (p *NEM12FileParser) SaveFileToDB(ctx context.Context, nem12File *NEM12File) error {
	log.Println("Saving records to DB")
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// update the table with new value on conflict
	smt, err := tx.PrepareContext(ctx, `
		INSERT INTO meter_readings (nmi, "timestamp", consumption)
		VALUES ($1, $2, $3)
		ON CONFLICT (nmi, "timestamp") DO UPDATE SET consumption = EXCLUDED.consumption
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare context for insert statement: %w", err)
	}

	defer smt.Close()

	totalInserted := 0
	for _, block := range nem12File.NMIDataBlocks {
		for _, intervalData := range block.IntervalData {
			// number of intervals per day based on interval length
			intervalLength := block.DataDetails.IntervalLength

			// insert each interval value with its calculated timestamp
			for i, value := range intervalData.IntervalValues {
				// timestamp for this interval
				minutesFromMidnight := i * intervalLength
				timestamp := intervalData.IntervalDate.Add(time.Duration(minutesFromMidnight) * time.Minute)

				_, err = smt.ExecContext(ctx, block.DataDetails.NMI, timestamp, value)
				if err != nil {
					return fmt.Errorf("failed to insert meter reading: %w", err)
				}
				totalInserted++
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error commiting transaction %w", err)
	}
	log.Printf("Successfully inserted %d meter readings", totalInserted)
	return nil
}

func (p *NEM12FileParser) ParseFile(filename string) (*NEM12File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	defer file.Close()

	nem12File := &NEM12File{}
	scanner := bufio.NewScanner(file)
	var currentNMIBlock *NMIDataBlock
	lineNumber := 0

	// Track parsing state for hierarchy validation
	hasHeader := false
	hasNMIBlock := false

	// Handle broken lines - accumulate line fragments
	var currentLine strings.Builder

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lineNumber++

		if line == "" {
			continue
		}

		// need to add comma to ensure that it represent a actual record type and not part of value
		startsWithRecordType := strings.HasPrefix(line, "100,") ||
			strings.HasPrefix(line, "200,") ||
			strings.HasPrefix(line, "300,") ||
			strings.HasPrefix(line, "400,") ||
			strings.HasPrefix(line, "500,") ||
			strings.HasPrefix(line, "900")

		if startsWithRecordType && currentLine.Len() > 0 {
			newBlock, newHasHeader, newHasNMIBlock, err := p.processLine(nem12File, currentNMIBlock, currentLine.String(), lineNumber-1, hasHeader, hasNMIBlock)
			if err != nil {
				return nil, err
			}
			currentNMIBlock = newBlock
			hasHeader = newHasHeader
			hasNMIBlock = newHasNMIBlock
			currentLine.Reset()
		}

		currentLine.WriteString(line)

		// If line doesn't end with comma and starts with record type, it's complete
		if !strings.HasSuffix(line, ",") && startsWithRecordType {
			newBlock, newHasHeader, newHasNMIBlock, err := p.processLine(nem12File, currentNMIBlock, currentLine.String(), lineNumber, hasHeader, hasNMIBlock)
			if err != nil {
				return nil, err
			}
			currentNMIBlock = newBlock
			hasHeader = newHasHeader
			hasNMIBlock = newHasNMIBlock
			currentLine.Reset()
		}
	}

	// Process any remaining line
	if currentLine.Len() > 0 {
		var err error
		currentNMIBlock, _, _, err = p.processLine(nem12File, currentNMIBlock, currentLine.String(), lineNumber, hasHeader, hasNMIBlock)
		if err != nil {
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return nem12File, nil
}

// processLine handles a single complete NEM12 record line
// Returns: (updatedNMIBlock, updatedHasHeader, updatedHasNMIBlock, error)
func (p *NEM12FileParser) processLine(nem12File *NEM12File, currentNMIBlock *NMIDataBlock, line string, lineNumber int, hasHeader bool, hasNMIBlock bool) (*NMIDataBlock, bool, bool, error) {
	fields := strings.Split(line, ",")
	if len(fields) == 0 {
		return currentNMIBlock, hasHeader, hasNMIBlock, nil
	}

	recordType := fields[0]

	switch recordType {
	case RecordType100:
		// Validate: 100 record must be the first record in the file
		if hasHeader {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 100 record must be the first record", lineNumber)
		}
		header, err := parse100Record(fields)
		if err != nil {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		nem12File.Header = *header
		return currentNMIBlock, true, hasNMIBlock, nil

	case RecordType200:
		// Validate: 200 record must come after a 100 record
		if !hasHeader {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 200 record must come after a 100 record", lineNumber)
		}
		// Validate: previous NMI block must have at least one 300 record
		if currentNMIBlock != nil {
			if len(currentNMIBlock.IntervalData) == 0 {
				return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 200 record must be followed by at least one 300 record", lineNumber)
			}
			nem12File.NMIDataBlocks = append(nem12File.NMIDataBlocks, *currentNMIBlock)
		}
		dataDetails, err := parse200Record(fields)
		if err != nil {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		newBlock := &NMIDataBlock{
			DataDetails: *dataDetails,
		}
		return newBlock, hasHeader, true, nil

	case RecordType300:
		// Validate: 300 record must come after a 200 record
		if !hasHeader {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 300 record must come after a 100 record", lineNumber)
		}
		if currentNMIBlock == nil || !hasNMIBlock {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 300 record must come after a 200 record", lineNumber)
		}
		intervalData, err := parse300Record(fields)
		if err != nil {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		currentNMIBlock.IntervalData = append(currentNMIBlock.IntervalData, *intervalData)
		return currentNMIBlock, hasHeader, hasNMIBlock, nil

	case RecordType400:
		// Validate: 400 record must come after a 200 record
		if !hasHeader {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 400 record must come after a 100 record", lineNumber)
		}
		if currentNMIBlock == nil || !hasNMIBlock {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 400 record must come after a 200 record", lineNumber)
		}
		intervalEvent, err := parse400Record(fields)
		if err != nil {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: %w", lineNumber, err)
		}

		currentNMIBlock.IntervalEvents = append(currentNMIBlock.IntervalEvents, *intervalEvent)
		return currentNMIBlock, hasHeader, hasNMIBlock, nil

	case RecordType500:
		// Validate: 500 record must come after a 200 record
		if !hasHeader {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 500 record must come after a 100 record", lineNumber)
		}
		if currentNMIBlock == nil || !hasNMIBlock {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 500 record must come after a 200 record", lineNumber)
		}
		b2bDetail, err := parse500Record(fields)
		if err != nil {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		currentNMIBlock.B2BDetails = append(currentNMIBlock.B2BDetails, *b2bDetail)
		return currentNMIBlock, hasHeader, hasNMIBlock, nil

	case RecordType900:
		// Validate: 900 record should come after at least one 200 record
		if !hasHeader {
			return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 900 record must come after a 100 record", lineNumber)
		}
		// Validate: current NMI block must have at least one 300 record
		if currentNMIBlock != nil {
			if len(currentNMIBlock.IntervalData) == 0 {
				return currentNMIBlock, hasHeader, hasNMIBlock, fmt.Errorf("line %d: invalid file structure - 200 record must be followed by at least one 300 record", lineNumber)
			}
			nem12File.NMIDataBlocks = append(nem12File.NMIDataBlocks, *currentNMIBlock)
		}
		nem12File.TotalRecords = lineNumber
		return currentNMIBlock, hasHeader, hasNMIBlock, nil
	}

	return currentNMIBlock, hasHeader, hasNMIBlock, nil
}

// According to document, Header Record
// Example: RecordIndicator,VersionHeader,DateTime,FromParticipant,ToParticipant
// 100,NEM12,200301011534,MDP1,Retailer1
func parse100Record(fields []string) (*Header, error) {
	if len(fields) < 5 {
		return &Header{}, fmt.Errorf("invalid 100 record: expected at least 5 fields, got %d", len(fields))
	}

	dateTime, err := parseNEM12DateTime12(fields[2])
	if err != nil {
		return &Header{}, fmt.Errorf("invalid datetime in 100 record: %w", err)
	}

	return &Header{
		VersionHeader:   fields[1],
		DateTime:        dateTime,
		FromParticipant: fields[3],
		ToParticipant:   fields[4],
	}, nil
}

// NMI data details record
// Multiple 300-500 record blocks are allowed within a single 200 record.
// Example: RecordIndicator,NMI,NMIConfiguration,RegisterID,NMISuffix,MDMDataStreamIdentifier,
// MeterSerialNumber,UOM,IntervalLength,NextScheduledReadDate
// 200,VABD000163,E1Q1,1,E1,N1,METSER123,kWh,30,20040120
func parse200Record(fields []string) (*NMIDataDetails, error) {
	if len(fields) < 9 {
		return &NMIDataDetails{}, fmt.Errorf("invalid 200 record: expected at least 9 fields, got %d", len(fields))
	}

	intervalLength, err := strconv.Atoi(fields[8])
	if err != nil {
		return &NMIDataDetails{}, fmt.Errorf("invalid interval length: %w", err)
	}

	var nextReadDate *time.Time
	if len(fields) > 9 && fields[9] != "" {
		date, err := parseNEM12Date8(fields[9])
		if err == nil {
			nextReadDate = &date
		}
	}

	return &NMIDataDetails{
		NMI:                   fields[1],
		NMIConfiguration:      fields[2],
		RegisterID:            fields[3],
		NMISuffix:             fields[4],
		MDMDataStreamID:       fields[5],
		MeterSerialNumber:     fields[6],
		UOM:                   fields[7],
		IntervalLength:        intervalLength,
		NextScheduledReadDate: nextReadDate,
	}, nil
}

// Example: RecordIndicator,IntervalDate,IntervalValue1 . . . IntervalValueN,
// QualityMethod,ReasonCode,ReasonDescription,UpdateDateTime,MSATSLoadDateTime
// 300,20030501,50.1, . . . ,21.5,V,,,20030101153445,20030102023012
func parse300Record(fields []string) (*IntervalData, error) {
	if len(fields) < 4 {
		return &IntervalData{}, fmt.Errorf("invalid 300 record: expected at least 4 fields, got %d", len(fields))
	}

	intervalDate, err := parseNEM12Date8(fields[1])
	if err != nil {
		return &IntervalData{}, fmt.Errorf("invalid interval date: %w", err)
	}

	// Parse interval values (from field 2 onwards until quality method)
	var intervalValues []float64
	lastDataField := len(fields) - 5 // Last 5 fields are quality, reason, desc, update time and msats load date time

	for i := 2; i < lastDataField; i++ {
		if fields[i] == "" {
			intervalValues = append(intervalValues, 0)
			continue
		}
		fmt.Println("Field value", fields[i])
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return &IntervalData{}, fmt.Errorf("invalid interval value at position %d: %w", i, err)
		}
		intervalValues = append(intervalValues, value)
	}

	qualityMethod := fields[lastDataField]
	reasonCode := 0
	if fields[lastDataField+1] != "" {
		reasonCode, _ = strconv.Atoi(fields[lastDataField+1])
	}
	reasonDesc := fields[lastDataField+2]

	updateDateTime := time.Now()
	if len(fields) > lastDataField+3 && fields[lastDataField+3] != "" {
		updateDateTime, _ = parseNEM12DateTime14(fields[lastDataField+3])
	}

	var msatsLoadDateTime *time.Time
	if len(fields) > lastDataField+4 && fields[lastDataField+4] != "" {
		dt, err := parseNEM12DateTime14(fields[lastDataField+4])
		if err == nil {
			msatsLoadDateTime = &dt
		}
	}

	return &IntervalData{
		IntervalDate:      intervalDate,
		IntervalValues:    intervalValues,
		QualityMethod:     qualityMethod,
		ReasonCode:        reasonCode,
		ReasonDescription: reasonDesc,
		UpdateDateTime:    updateDateTime,
		MSATSLoadDateTime: msatsLoadDateTime,
	}, nil
}

func parse400Record(fields []string) (*IntervalEvent, error) {
	if len(fields) < 6 {
		return &IntervalEvent{}, fmt.Errorf("invalid 400 record: expected at least 6 fields, got %d", len(fields))
	}

	startInterval, err := strconv.Atoi(fields[1])
	if err != nil {
		return &IntervalEvent{}, fmt.Errorf("invalid start interval: %w", err)
	}

	endInterval, err := strconv.Atoi(fields[2])
	if err != nil {
		return &IntervalEvent{}, fmt.Errorf("invalid end interval: %w", err)
	}

	reasonCode, _ := strconv.Atoi(fields[4])

	return &IntervalEvent{
		StartInterval:     startInterval,
		EndInterval:       endInterval,
		QualityMethod:     fields[3],
		ReasonCode:        reasonCode,
		ReasonDescription: fields[5],
	}, nil
}

func parse500Record(fields []string) (*B2BDetails, error) {
	if len(fields) < 5 {
		return &B2BDetails{}, fmt.Errorf("invalid 500 record: expected at least 5 fields, got %d", len(fields))
	}

	readDateTime, err := parseNEM12DateTime14(fields[3])
	if err != nil {
		return &B2BDetails{}, fmt.Errorf("invalid read datetime: %w", err)
	}

	indexRead := 0.0
	if fields[4] != "" {
		indexRead, err = strconv.ParseFloat(fields[4], 64)
		if err != nil {
			return &B2BDetails{}, fmt.Errorf("invalid index read: %w", err)
		}
	}

	return &B2BDetails{
		TransCode:       fields[1],
		RetServiceOrder: fields[2],
		ReadDateTime:    readDateTime,
		IndexRead:       indexRead,
	}, nil
}
