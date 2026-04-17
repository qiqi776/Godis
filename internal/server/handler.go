package server

import (
    "bufio"
    "errors"
    "io"
    "net"

    "godis/internal/resp"
)

func (s *Server) handleConn(conn net.Conn) {
    s.registerConn(conn)
    defer s.unregisterConn(conn)

    session := NewSession(conn)
    defer conn.Close()

    s.logger.Infof("client connected: %s", session.RemoteAddr)
    defer s.logger.Infof("client disconnected: %s", session.RemoteAddr)

    reader := bufio.NewReader(conn)
    for {
        tokens, err := resp.ReadCommand(reader)
        if err != nil {
            switch {
            case errors.Is(err, io.EOF):
                return
            case errors.Is(err, net.ErrClosed):
                return
            case errors.Is(err, resp.ErrEmptyCommand):
                continue
            default:
                if _, writeErr := conn.Write(resp.Error(err.Error())); writeErr != nil {
                    s.logger.Errorf("write error to %s: %v", session.RemoteAddr, writeErr)
                    return
                }
                continue
            }
        }

        reply := s.executor.Execute(session, tokens)
        if _, err := conn.Write(reply); err != nil {
            s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
            return
        }
    }
}