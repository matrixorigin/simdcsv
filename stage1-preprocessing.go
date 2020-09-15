package simdcsv

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"log"
	"math/bits"
	"reflect"
	"strings"
)

// Substitute values when preprocessing a chunk
// NB 0x0 should be avoided (since trailing bytes
// beyond the end of the buffer are zeroed out)
const preprocessedDelimiter = 0x1
const preprocessedSeparator = 0x2
const preprocessedQuote = 0x3

func preprocessDoubleQuotes(in []byte) (out []byte) {

	// Replace delimiter and separators
	// Remove any surrounding quotes
	// Replace double quotes with single quote

	out = make([]byte, 0, len(in))
	quoted := false

	for i := 0; i < len(in); i++ {
		b := in[i]

		if quoted {
			if b == '"' && i+1 < len(in) && in[i+1] == '"' {
				// replace escaped quote with single quote
				out = append(out, '"')
				// and skip next char
				i += 1
			} else if b == '"' {
				quoted = false
			} else if b == '\r' && i+1 < len(in) && in[i+1] == '\n' {
				out = append(out, '\n') // replace \r\n in quoted fields with single \n (conform to encoding/csv behavior)
				i += 1
			} else {
				out = append(out, b)
			}
		} else {
			if b == '"' {
				quoted = true
			} else if b == '\r' && i+1 < len(in) && in[i+1] == '\n' {
				// replace delimiter with '\1'
				out = append(out, preprocessedDelimiter)
				// and skip next char
				i += 1
			} else if b == '\n' {
				// replace delimiter with '\1'
				out = append(out, preprocessedDelimiter)
			} else if b == ',' {
				// replace separator with '\2'
				out = append(out, preprocessedSeparator)
			} else {
				out = append(out, b)
			}
		}
	}

	return
}

type stage1Input struct {
	quoteMaskIn          uint64
	separatorMaskIn      uint64
	carriageReturnMaskIn uint64
	quoteMaskInNext      uint64
	quoted               uint64
	newlineMaskIn        uint64
	newlineMaskInNext    uint64
}

type stage1Output struct {
	quoteMaskOut          uint64
	separatorMaskOut      uint64
	carriageReturnMaskOut uint64
	needsPostProcessing   uint64
}

func preprocessMasksToMasksInverted(input *stage1Input, output *stage1Output) {

	const clearMask = 0xfffffffffffffffe

	separatorMaskIn := input.separatorMaskIn
	carriageReturnMaskIn := input.carriageReturnMaskIn
	quoteMaskIn := input.quoteMaskIn

	separatorPos := bits.TrailingZeros64(separatorMaskIn)
	carriageReturnPos := bits.TrailingZeros64(carriageReturnMaskIn)
	quotePos := bits.TrailingZeros64(quoteMaskIn)

	output.quoteMaskOut = quoteMaskIn                   // copy quote mask to output
	output.separatorMaskOut = separatorMaskIn           // copy separator mask to output
	output.carriageReturnMaskOut = carriageReturnMaskIn // copy carriage return mask to output
	output.needsPostProcessing = 0                      // flag to indicate whether post-processing is need for these masks

	for {
		if quotePos < separatorPos && quotePos < carriageReturnPos {

			if input.quoted != 0 && quotePos == 63 && input.quoteMaskInNext&1 == 1 { // last bit of quote mask and first bit of next quote mask set?
				// clear out both active bit and ...
				quoteMaskIn &= clearMask << quotePos
				output.quoteMaskOut &= ^(uint64(1) << quotePos) // mask out quote
				output.needsPostProcessing = 1                  // post-processing is required for double quotes
				// first bit of next quote mask
				input.quoteMaskInNext &= ^uint64(1)
			} else if input.quoted != 0 && quoteMaskIn&(1<<(quotePos+1)) != 0 { // next quote bit is also set (so two adjacent bits) ?
				// clear out both active bit and subsequent bit
				quoteMaskIn &= clearMask << (quotePos + 1)
				output.quoteMaskOut &= ^(uint64(3) << quotePos) // mask out two quotes
				output.needsPostProcessing = 1                  // post-processing is required for double quotes
			} else {
				input.quoted = ^input.quoted

				quoteMaskIn &= clearMask << quotePos
			}

			quotePos = bits.TrailingZeros64(quoteMaskIn)

		} else if separatorPos < quotePos && separatorPos < carriageReturnPos {

			if input.quoted != 0 {
				output.separatorMaskOut &= ^(uint64(1) << separatorPos) // mask out separator bit in quoted field
			}

			separatorMaskIn &= clearMask << separatorPos
			separatorPos = bits.TrailingZeros64(separatorMaskIn)

		} else if carriageReturnPos < quotePos && carriageReturnPos < separatorPos {

			if input.quoted != 0 {
				output.carriageReturnMaskOut &= ^(uint64(1) << carriageReturnPos) // mask out carriage return bit in quoted field
				output.needsPostProcessing = 1                                    // post-processing is required for carriage returns in quoted fields
			} else {
				if carriageReturnPos == 63 && input.newlineMaskInNext&1 == 0 {
					output.carriageReturnMaskOut &= ^(uint64(1) << carriageReturnPos) // mask out carriage return for replacement without following newline
				} else if input.newlineMaskIn&(uint64(1)<<(carriageReturnPos+1)) == 0 {
					output.carriageReturnMaskOut &= ^(uint64(1) << carriageReturnPos) // mask out carriage return bit in quoted field
				}
			}

			carriageReturnMaskIn &= clearMask << carriageReturnPos
			carriageReturnPos = bits.TrailingZeros64(carriageReturnMaskIn)

		} else {
			// we must be done
			break
		}
	}

	return
}

func preprocessInPlaceMasks(in []byte, quoted *bool) (quoteMask, separatorMask, carriageReturnMask uint64) {

	for i := 0; i < 64 && i < len(in); i++ {
		b := in[i]

		if *quoted {
			if b == '"' && i+1 < len(in) && in[i+1] == '"' {
				i += 1
			} else if b == '"' {
				quoteMask |= 1 << i // in[i] = preprocessedQuote
				*quoted = false
				//} else if b == '\r' && i+1 < len(in) && in[i+1] == '\n' {
				//	i += 1
				//} else {
			}
		} else {
			if b == '"' {
				quoteMask |= 1 << i // in[i] = preprocessedQuote
				*quoted = true
			} else if b == '\r' { // && i+1 < len(in) && in[i+1] == '\n' {
				carriageReturnMask |= 1 << i // in[i] = '\n'
			} else if b == ',' {
				// replace separator with '\2'
				separatorMask |= 1 << i // in[i] = preprocessedSeparator
			}
		}
	}

	return
}

func clearAndMerge(data []byte, mask, replacement uint64) {

	for i := 0; i < 64 && i < len(data); i++ {
		if mask&(1<<i) == (1 << i) {
			data[i] = byte(replacement)
		}
	}
}

func alternativeStage1Masks(data []byte) {

	buf := make([]byte, len(data))
	copy(buf, data)

	quoted := false
	quoteMask0, separatorMask0, carriageReturnMask0 := preprocessInPlaceMasks(buf, &quoted)
	quoteMask1, separatorMask1, carriageReturnMask1 := preprocessInPlaceMasks(buf[64:], &quoted)

	fmt.Println()
	fmt.Printf("%s", string(bytes.ReplaceAll(bytes.ReplaceAll(buf[:64], []byte{0xd}, []byte{0x20}), []byte{0xa}, []byte{0x20})))
	fmt.Printf("·%s\n", string(bytes.ReplaceAll(bytes.ReplaceAll(buf[64:], []byte{0xd}, []byte{0x20}), []byte{0xa}, []byte{0x20})))

	fmt.Printf("%064b·%064b\n", bits.Reverse64(quoteMask0), bits.Reverse64(quoteMask1))
	fmt.Printf("%064b·%064b\n", bits.Reverse64(separatorMask0), bits.Reverse64(separatorMask1))
	fmt.Printf("%064b·%064b\n", bits.Reverse64(carriageReturnMask0), bits.Reverse64(carriageReturnMask1))

	clearAndMerge(buf, quoteMask0, preprocessedQuote)
	clearAndMerge(buf[64:], quoteMask1, preprocessedQuote)

	clearAndMerge(buf, separatorMask0, preprocessedSeparator)
	clearAndMerge(buf[64:], separatorMask1, preprocessedSeparator)

	clearAndMerge(buf, carriageReturnMask0, '\n')
	clearAndMerge(buf[64:], carriageReturnMask1, '\n')

	fmt.Print(hex.Dump(buf))

	simdrecords := Stage2ParseBuffer(buf, 0xa, preprocessedSeparator, preprocessedQuote, nil)
	fmt.Println(simdrecords)

	//
	// postprocess
	//   replace "" to " in specific columns
	//   replace \r\n to \n in specific columns
	fmt.Printf("double quotes: `%s`\n", simdrecords[3][2])
	simdrecords[3][2] = strings.ReplaceAll(simdrecords[3][2], "\"\"", "\"")
	fmt.Printf(" carriage ret: `%s`\n", simdrecords[2][1])
	simdrecords[2][1] = strings.ReplaceAll(simdrecords[2][1], "\r\n", "\n")

	fmt.Println()

	fmt.Println("[0]:", simdrecords[0])
	fmt.Println("[1]:", simdrecords[1])
	fmt.Println("[2]:", simdrecords[2])
	fmt.Println("[3]:", simdrecords[3])

	r := csv.NewReader(bytes.NewReader(data))
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("encoding/csv: %v", err)
	}

	if !reflect.DeepEqual(simdrecords, records) {
		log.Fatalf("alternativeStage1Masks: got %v, want %v", simdrecords, records)
	}
}

type postProcRow struct {
	start int
	end  int
}

//
// Determine which rows and columns need post processing
// This is  need to replace both "" to " as well as
// \r\n to \n in specific columns
func getPostProcRows(buf []byte, postProc []uint64, simdrecords [][]string) (ppRows []postProcRow) {

	// TODO: Crude implementation, make more refined/granular

	sliceptr := func(slc []byte) uintptr {
		return (*reflect.SliceHeader)(unsafe.Pointer(&slc)).Data
	}
	stringptr := func (s string) uintptr {
		return (*reflect.StringHeader)(unsafe.Pointer(&s)).Data
	}

	ppRows = make([]postProcRow, 0, 128)

	row, pbuf := 0, sliceptr(buf)
	for ipp, pp := range postProc {

		if ipp < len(postProc) - 1 && pp == postProc[ipp+1]  {
			continue // if offset occurs multiple times, process only last one
		}

		// find start row to process
		for row < len(simdrecords) && uint64(stringptr(simdrecords[row][0])-pbuf) < pp {
			row++
		}

		ppr := postProcRow{}
		if row > 0 {
			ppr.start = row-1
		}

		// find end row to process
		for row < len(simdrecords) && uint64(stringptr(simdrecords[row][0])-pbuf) < pp+64 {
			row++
		}
		ppr.end = row

		ppRows = append(ppRows, ppr)
	}
	return
}
