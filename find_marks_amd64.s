//+build !noasm !appengine

// find_marks_in_slice(msg []byte, indexes *[INDEXES_SIZE]uint32, indexes_length, indexes_cap, carried, position *uint64) (pmsg, out uint64)
TEXT ·find_marks_in_slice(SB), 7, $0

	MOVQ         $0x2c, AX
	MOVQ         AX, X6
	VPBROADCASTB X6, Y6
	XORQ         AX, AX
	MOVQ         AX, pmsg+88(FP)
	MOVQ         AX, endofline+96(FP)

loop:
	MOVQ    msg+0(FP), DI
	ADDQ    pmsg+88(FP), DI
	VMOVDQU (DI), Y8      // load low 32-bytes
	VMOVDQU 0x20(DI), Y9  // load high 32-bytes
	ADDQ    $0x40, pmsg+88(FP)

	// CALL  ·__find_separator(SB)
    PUSHQ AX        // save separator mask

	MOVQ $0, DX
	MOVQ prev_iter_inside_quote+64(FP), CX
	MOVQ quote_bits+72(FP), R8
	MOVQ error_mask+80(FP), R9

	// CALL ·__find_quote_mask_and_bits(SB)

	MOVQ    AX, DX  // get quotemask
    CALL ·__find_newline_delimiters(SB)

    XORQ   R10, R10
    TZCNTQ BX, CX
    JCS    skipEOL   // carry is set if nothing found
    INCQ   CX
    INCQ   endofline+96(FP) // set end of line flag

    SUBQ   $0x40, pmsg+88(FP) // correct starting position for next line
    ADDQ   CX, pmsg+88(FP)

    CMPQ   CX, $64   // shlq belows fails, so treat as special case
    JZ     skipEOL
    INCQ   R10
    SHLQ   CX, R10   // one greater than the mask
skipEOL:
    DECQ   R10       // mask up to and including end-of-line marker

    POPQ  CX         // separator mask
	ORQ   BX, CX     // merge in end-of-line marker (if set)
	ANDNQ CX, AX, AX
    ANDQ  R10, AX    // clear out bits beyond end-of-line marker

	XORQ    R15, R15
	MOVQ    $1, R14
	CMPB    0x40(DI), $0x22                         // Is first char of next 64-byte a quote?
	CMOVQNE R15, R14
	MOVQ    quote_bits+72(FP), R15; MOVQ (R15), R15
	SHRQ    $1, R14, R15                            // Merge in bit-status of next 64-byte chunk

	MOVQ indexes+24(FP), DI
	MOVQ indexes_length+32(FP), SI; MOVQ (SI), BX // INDEX
	MOVQ carried+48(FP), R11; MOVQ (R11), DX      // CARRIED
	MOVQ position+56(FP), R12; MOVQ (R12), R10    // POSITION
	CALL ·__flatten_bits_incremental(SB)
	MOVQ R10, (R12)                               // POSITION
	MOVQ DX, (R11)                                // CARRIED
	MOVQ BX, (SI)                                 // INDEX

	MOVQ pmsg+88(FP), AX
    CMPQ endofline+96(FP), $0 // test for end of line
    JZ   skipReset

    MOVQ $0, endofline+96(FP) // reset end of line flag
    MOVQ $0, (R11)            // reset state for flatten_bits_incr
    MOVQ AX, (R12)

skipReset:
    MOVQ indexes_length+32(FP), SI; MOVQ (SI), BX // INDEX
    ADDQ $64, BX
    CMPQ BX, indexes_cap+40(FP)
    JGE  done

	CMPQ AX, msg_len+8(FP)
	JLT  loop

done:
	RET
