// Copyright 2014 Quoc-Viet Nguyen. All rights reserved.
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

package modbus

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestTCPEncoding(t *testing.T) {
	var transactionID uint32
	packager := tcpPackager{transactionID: &transactionID}
	pdu := ProtocolDataUnit{}
	pdu.FunctionCode = 3
	pdu.Data = []byte{0, 4, 0, 3}

	adu, err := packager.Encode(&pdu)
	if err != nil {
		t.Fatal(err)
	}

	expected := []byte{0, 1, 0, 0, 0, 6, 0, 3, 0, 4, 0, 3}
	if !bytes.Equal(expected, adu) {
		t.Fatalf("Expected %v, actual %v", expected, adu)
	}
}

func TestTCPDecoding(t *testing.T) {
	var transactionID uint32 = 1
	packager := tcpPackager{}
	packager.transactionID = &transactionID
	packager.SlaveID = 17
	adu := []byte{0, 1, 0, 0, 0, 6, 17, 3, 0, 120, 0, 3}

	pdu, err := packager.Decode(adu)
	if err != nil {
		t.Fatal(err)
	}

	if pdu.FunctionCode != 3 {
		t.Fatalf("Function code: expected %v, actual %v", 3, pdu.FunctionCode)
	}
	expected := []byte{0, 120, 0, 3}
	if !bytes.Equal(expected, pdu.Data) {
		t.Fatalf("Data: expected %v, actual %v", expected, adu)
	}
}

func TestTCPTransporter(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, err = io.Copy(conn, conn)
		if err != nil {
			t.Error(err)
			return
		}
	}()
	client := &tcpTransporter{
		Address:     ln.Addr().String(),
		Timeout:     1 * time.Second,
		IdleTimeout: 100 * time.Millisecond,
	}
	req := []byte{0, 1, 0, 2, 0, 2, 1, 2}
	rsp, err := client.Send(req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(req, rsp) {
		t.Fatalf("unexpected response: %x", rsp)
	}
	time.Sleep(150 * time.Millisecond)
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.conn != nil {
		t.Fatalf("connection is not closed: %+v", client.conn)
	}
}

func TestErrTCPHeaderLength_Error(t *testing.T) {
	// should not explode
	_ = ErrTCPHeaderLength(1000).Error()
}

// readInputRegistersResponse encodes a response to ReadInputRegisters for the given transaction id
func readInputRegistersResponse(t *testing.T, transactionID uint16, data []byte) []byte {
	t.Helper()
	tid := uint32(transactionID) - 1
	packager := &tcpPackager{transactionID: &tid}
	adu, err := packager.Encode(&ProtocolDataUnit{
		FunctionCode: FuncCodeReadInputRegisters,
		Data:         append([]byte{byte(len(data))}, data...),
	})
	if err != nil {
		t.Fatal(err)
	}
	return adu
}

// readRequest reads a single modbus tcp request and returns its transaction id
func readRequest(conn net.Conn) (uint16, error) {
	header := make([]byte, tcpHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, err
	}
	body := make([]byte, binary.BigEndian.Uint16(header[4:])-1)
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(header), nil
}

// A gateway that answers a timed out request late, on the next connection,
// must not fail the following request.
func TestTCPLateResponseIsDiscarded(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	data := []byte{0xCA, 0xFE}
	done := make(chan struct{})
	defer close(done)

	go func() {
		// first connection: swallow request until the client gives up
		conn, err := ln.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		late, err := readRequest(conn)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = io.Copy(io.Discard, conn) // wait for client to close
		conn.Close()

		// second connection: answer the old request first, then the current one
		conn, err = ln.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		current, err := readRequest(conn)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = conn.Write(readInputRegistersResponse(t, late, []byte{0xDE, 0xAD}))
		_, _ = conn.Write(readInputRegistersResponse(t, current, data))
		<-done
	}()

	handler := NewTCPClientHandler(ln.Addr().String())
	handler.Timeout = 200 * time.Millisecond
	client := NewClient(handler)

	if _, err := client.ReadInputRegisters(0, 1); err == nil {
		t.Fatal("expected first request to time out")
	}

	resp, err := client.ReadInputRegisters(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resp, data) {
		t.Fatalf("got wrong response: got %x wanted %x", resp, data)
	}
}

// A response with a transaction id that was never outstanding is still an error.
func TestTCPUnexpectedTransactionIDFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan struct{})
	defer close(done)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		current, err := readRequest(conn)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = conn.Write(readInputRegistersResponse(t, current+5, []byte{0xCA, 0xFE}))
		<-done
	}()

	handler := NewTCPClientHandler(ln.Addr().String())
	handler.Timeout = 200 * time.Millisecond
	client := NewClient(handler)

	_, err = client.ReadInputRegisters(0, 1)
	if _, ok := err.(errTransactionIDMismatch); !ok {
		t.Fatalf("expected transaction id mismatch, got %v", err)
	}
}

func TestTCPIsLateResponse(t *testing.T) {
	tc := []struct {
		lastSuccessful, got, expected uint16
		late                          bool
	}{
		{0, 1, 2, true},
		{0, 0, 1, false},        // nothing outstanding before the first request
		{5, 5, 8, false},        // already answered
		{5, 6, 8, true},         // timed out earlier
		{5, 8, 8, false},        // the current request is no mismatch
		{5, 9, 8, false},        // never sent
		{65534, 65535, 1, true}, // wrap-around
		{65534, 0, 1, true},
		{65534, 2, 1, false},
	}

	for _, tc := range tc {
		mb := &tcpTransporter{lastSuccessfulTransactionID: tc.lastSuccessful}
		if late := mb.isLateResponse(tc.got, tc.expected); late != tc.late {
			t.Errorf("lastSuccessful=%d got=%d expected=%d: late=%v, want %v", tc.lastSuccessful, tc.got, tc.expected, late, tc.late)
		}
	}
}

func BenchmarkTCPEncoder(b *testing.B) {
	var transactionID uint32
	encoder := tcpPackager{
		transactionID: &transactionID,
		SlaveID:       10,
	}
	pdu := ProtocolDataUnit{
		FunctionCode: 1,
		Data:         []byte{2, 3, 4, 5, 6, 7, 8, 9},
	}
	for i := 0; i < b.N; i++ {
		_, err := encoder.Encode(&pdu)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTCPDecoder(b *testing.B) {
	decoder := tcpPackager{
		SlaveID: 10,
	}
	adu := []byte{0, 1, 0, 0, 0, 6, 17, 3, 0, 120, 0, 3}
	for i := 0; i < b.N; i++ {
		_, err := decoder.Decode(adu)
		if err != nil {
			b.Fatal(err)
		}
	}
}
