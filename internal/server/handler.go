package server

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strings"

	"godis/internal/resp"
)

func (s *Server) handleConn(conn net.Conn) {
    s.registerConn(conn)
    defer s.unregisterConn(conn)

    session := NewSession(conn)
    defer s.ps.UnsubscribeAll(conn)
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

        name := ""
        if len(tokens) > 0 {
            name = strings.ToUpper(string(tokens[0]))
        }
        switch name {
        case "SUBSCRIBE":
            if len(tokens) < 2 {
                if _, err := conn.Write(resp.Error("ERR wrong number of arguments for 'subscribe' command")); err != nil {
                    s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
                    return
                }
                continue
            }
            channels := make([]string, 0, len(tokens)-1)
            for _, token := range tokens[1:] {
                channels = append(channels, string(token))
            }
            for _, channel := range channels {
                s.ps.Subscribe(conn, channel)
                session.Subscribe(channel)
                reply := pubsubSubscribeReply(channel, session.SubCount())
                if _, err := conn.Write(reply); err != nil {
                    s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
                    return
                }
            }
            continue

        case "UNSUBSCRIBE":
			var channels []string
			if len(tokens) == 1 {
				channels = session.UnsubscribeAll()
				s.ps.UnsubscribeAll(conn)
			} else {
				channels = make([]string, 0, len(tokens)-1)
				for _, token := range tokens[1:] {
					channels = append(channels, string(token))
				}
			}

			if len(channels) == 0 {
				reply := pubsubUnsubscribeReply("", 0)
				if _, err := conn.Write(reply); err != nil {
					s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
					return
				}
				continue
			}

            if len(tokens) > 1 {
                for _, channel := range channels {
                    session.Unsubscribe(channel)
			        s.ps.Unsubscribe(conn, channel)
                    reply := pubsubUnsubscribeReply(channel, session.SubCount())
                    if _, err := conn.Write(reply); err != nil {
                        s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
                        return
                    }
                }
                continue
            }
            count := len(channels)
            for _, channel := range channels {
                count--
                reply := pubsubUnsubscribeReply(channel, count)
                if _, err := conn.Write(reply); err != nil {
                    s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
                    return
                }
            }
            continue

		case "PUBLISH":
			if len(tokens) != 3 {
				if _, err := conn.Write(resp.Error("ERR wrong number of arguments for 'publish' command")); err != nil {
					s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
					return
				}
				continue
			}

			delivered := s.ps.Publish(string(tokens[1]), tokens[2])
			if _, err := conn.Write(resp.Integer(delivered)); err != nil {
				s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
				return
			}
			continue
		}
        

        reply := s.executor.Execute(session, tokens)
        if _, err := conn.Write(reply); err != nil {
            s.logger.Errorf("write error to %s: %v", session.RemoteAddr, err)
            return
        }
    }
}