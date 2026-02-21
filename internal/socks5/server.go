package socks5

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
)

type ConnectRequest struct {
	Target string
}

type Server struct {
	addr    string
	logger  *slog.Logger
	handler func(conn net.Conn, req ConnectRequest)
}

func New(addr string, logger *slog.Logger, handler func(net.Conn, ConnectRequest)) *Server {
	return &Server{addr: addr, logger: logger, handler: handler}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.logger.Info("SOCKS5 listening", "addr", s.addr)
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return fmt.Errorf("accept: %w", err)
			}
		}
		go s.serve(conn)
	}
}

func (s *Server) serve(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in SOCKS5 handler", "err", r)
			conn.Close()
		}
	}()

	req, err := Handshake(conn)
	if err != nil {
		s.logger.Warn("SOCKS5 handshake failed", "err", err, "remote", conn.RemoteAddr())
		conn.Close()
		return
	}
	s.handler(conn, *req)
}

func Handshake(conn net.Conn) (*ConnectRequest, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("read auth header: %w", err)
	}
	if header[0] != 0x05 {
		return nil, fmt.Errorf("unsupported SOCKS version %d", header[0])
	}
	nMethods := int(header[1])
	methods := make([]byte, nMethods)
	if _, err := io.ReadFull(conn, methods); err != nil {
		return nil, fmt.Errorf("read methods: %w", err)
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return nil, fmt.Errorf("write auth reply: %w", err)
	}

	reqHeader := make([]byte, 4)
	if _, err := io.ReadFull(conn, reqHeader); err != nil {
		return nil, fmt.Errorf("read request header: %w", err)
	}
	if reqHeader[0] != 0x05 {
		return nil, fmt.Errorf("unsupported SOCKS version in request %d", reqHeader[0])
	}
	if reqHeader[1] != 0x01 {
		return nil, fmt.Errorf("unsupported command %d (only CONNECT supported)", reqHeader[1])
	}

	atyp := reqHeader[3]
	var host string
	switch atyp {
	case 0x01:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return nil, fmt.Errorf("read IPv4: %w", err)
		}
		host = net.IP(addr).String()
	case 0x03:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return nil, fmt.Errorf("read domain length: %w", err)
		}
		domain := make([]byte, int(lenBuf[0]))
		if _, err := io.ReadFull(conn, domain); err != nil {
			return nil, fmt.Errorf("read domain: %w", err)
		}
		host = string(domain)
	case 0x04:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return nil, fmt.Errorf("read IPv6: %w", err)
		}
		host = net.IP(addr).String()
	default:
		return nil, fmt.Errorf("unknown address type %d", atyp)
	}

	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return nil, fmt.Errorf("read port: %w", err)
	}
	port := binary.BigEndian.Uint16(portBuf)

	return &ConnectRequest{Target: fmt.Sprintf("%s:%d", host, port)}, nil
}

func SendSuccess(conn net.Conn) error {
	_, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	return err
}

func SendFailure(conn net.Conn) {
	conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	conn.Close()
}
