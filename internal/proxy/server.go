package proxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"pgrollback/pkg/protocol"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	// ConnectionTimeout é o timeout para operações de leitura/escrita nas conexões
	ConnectionTimeout = 3600 * time.Second
	// DefaultSessionTimeout é o timeout padrão para sessões se não especificado
	DefaultSessionTimeout = 24 * time.Hour
	// DefaultListenPort é a porta padrão de escuta se não especificada
	DefaultListenPort = 5433
	// portCheckTimeout é o timeout para verificar se uma porta está em uso
	portCheckTimeout = 100 * time.Millisecond
	// serverStartupCheckAttempts é o número de tentativas para verificar se o servidor está escutando
	serverStartupCheckAttempts = 20
	// serverStartupCheckInterval é o intervalo entre tentativas de verificação
	serverStartupCheckInterval = 100 * time.Millisecond
)

type Server struct {
	PgRollback *PgRollback
	listener   net.Listener
	listenHost string // actual bind host (set after Listen)
	listenPort int    // actual bind port (set after Listen; when 0 was requested, kernel assigns)
	wg         sync.WaitGroup
	startErr   error
	mu         sync.RWMutex
	// activeConns holds all accepted client connections so Stop() can close them and unblock handlers
	activeConns map[net.Conn]struct{}
	// GUI on same port; non-nil when NewServer(..., withGUI=true). Owns inject listener + HTTP server.
	gui *samePortGUIServer
}

// ListenHost returns the host the server is bound to (e.g. "127.0.0.1").
func (s *Server) ListenHost() string { s.mu.RLock(); defer s.mu.RUnlock(); return s.listenHost }

// ListenPort returns the port the server is bound to. Useful when NewServer was called with port 0 (dynamic port).
func (s *Server) ListenPort() int { s.mu.RLock(); defer s.mu.RUnlock(); return s.listenPort }

// bindAndListenTCP binds the TCP listener from bindPort: 0 = kernel-assigned port (updates listenHost/listenPort);
// >0 = fixed port (optional isPortInUse check); <0 returns error.
// After Listen succeeds, waits until the port accepts TCP connections (same probe as waitUntilListening).
func (s *Server) bindAndListenTCP(bindPort int) error {
	if bindPort < 0 {
		return fmt.Errorf("invalid proxy listen port %d (must be >= 0)", bindPort)
	}
	host := s.listenHost
	if bindPort > 0 && isPortInUse(host, bindPort) {
		return fmt.Errorf("port %s:%d is already in use. Cannot start server. Please stop any service using this port", host, bindPort)
	}
	listenAddr := fmt.Sprintf("%s:%d", host, bindPort)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}
	s.listener = listener
	if bindPort == 0 {
		if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok {
			s.mu.Lock()
			s.listenPort = tcpAddr.Port
			if tcpAddr.IP != nil && !tcpAddr.IP.IsUnspecified() {
				s.listenHost = tcpAddr.IP.String()
			}
			s.mu.Unlock()
		}
	}
	readyHost := s.ListenHost()
	readyPort := s.ListenPort()
	if !s.waitUntilListening(readyHost, readyPort) {
		_ = s.listener.Close()
		s.listener = nil
		return fmt.Errorf("server failed to start listening on %s:%d after %d attempts", readyHost, readyPort, serverStartupCheckAttempts)
	}
	return nil
}

// isPortInUse verifica se uma porta está em uso tentando conectar a ela
func isPortInUse(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), portCheckTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// NewServer cria uma nova instância do Server e inicia o servidor automaticamente
// Retorna sempre o Server, mesmo se houver erro ao iniciar
//
// Proxy listen address (proxyListenHost, proxyListenPort):
//   - proxyListenPort > 0: bind to that fixed port (fails if already in use).
//   - proxyListenPort == 0: bind to host:0 and use the kernel-assigned port; call ListenPort() after start.
//
// Se proxyListenHost estiver vazio, usa "localhost". Se sessionTimeout for 0, usa DefaultSessionTimeout.
// Se houver erro ao iniciar, o erro é armazenado no Server e pode ser verificado com StartError()
func NewServer(postgresHost string, postgresPort int, postgresDB, postgresUser, postgresPass string, timeout time.Duration, sessionTimeout time.Duration, keepaliveInterval time.Duration, proxyListenHost string, proxyListenPort int, withGUI bool) *Server {
	if sessionTimeout <= 0 {
		sessionTimeout = DefaultSessionTimeout
	}

	pgrollback := NewPgRollback(postgresHost, postgresPort, postgresDB, postgresUser, postgresPass, timeout, sessionTimeout, keepaliveInterval)
	server := &Server{
		PgRollback:  pgrollback,
		listenHost:  proxyListenHost,
		listenPort:  proxyListenPort,
		activeConns: make(map[net.Conn]struct{}),
	}

	if err := server.bindAndListenTCP(proxyListenPort); err != nil {
		server.mu.Lock()
		server.startErr = err
		server.mu.Unlock()
		return server
	}

	if withGUI {
		server.gui = newSamePortGUIServer(server)
	}

	go server.acceptConnections()

	logIfVerbose("PgRollback server listening on %s:%d", server.ListenHost(), server.ListenPort())
	return server
}

// waitUntilListening aguarda até que o servidor esteja realmente escutando na porta
func (s *Server) waitUntilListening(host string, port int) bool {
	for i := 0; i < serverStartupCheckAttempts; i++ {
		if isPortInUse(host, port) {
			return true
		}
		time.Sleep(serverStartupCheckInterval)
	}
	return false
}

// acceptConnections aceita conexões de clientes em loop (método privado)
//
// IMPORTANTE: Comportamento de Conexão e Reutilização
//
// O pgrollback é um servidor intermediário que fica entre o cliente e o PostgreSQL real.
// Todas as conexões são feitas ao pgrollback, não diretamente ao PostgreSQL.
//
// Reutilização de Conexões PostgreSQL por application_name:
// - Cada cliente se conecta ao pgrollback usando application_name (via parâmetro de conexão)
// - O application_name é extraído e convertido em testID (via protocol.ParseApplicationIdentity)
// - O mesmo testID sempre reutiliza a mesma conexão física ao PostgreSQL real
// - Isso é gerenciado por SessionsByTestID: cada sessão guarda sua DB (conn+tx) sob o testID
//
// Simulação de Autenticação para o Cliente:
//   - Cada cliente sempre passa pelo handshake completo de autenticação com o pgrollback
//   - O pgrollback simula a autenticação PostgreSQL (pede senha, responde AuthenticationOK)
//   - O cliente nunca sabe se está usando uma conexão PostgreSQL nova ou reutilizada
//   - Isso permite que aplicações como PHP (que abrem e fecham conexões rapidamente)
//     continuem usando a mesma transação PostgreSQL, mantendo as alterações
//
// Benefícios:
// - Transações não são perdidas quando o cliente fecha a conexão (PHP, scripts, etc.)
// - Alterações persistem entre reconexões do mesmo application_name
// - Controle total sobre quando fazer rollback (via comandos pgrollback especiais)
// - Isolamento entre diferentes testIDs (cada um tem sua própria transação)
func (s *Server) acceptConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			if s.listener == nil {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			s.mu.Lock()
			s.startErr = err
			s.mu.Unlock()
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		s.debugLogIncomingConn(conn)
		if s.gui != nil {
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			peeked := make([]byte, peekSize)
			n, peekErr := conn.Read(peeked)
			conn.SetReadDeadline(time.Time{})
			if peekErr == nil && n > 0 {
				peeked = peeked[:n]
				wrapped := newPeekedConn(conn, peeked)
				if isHTTPPeek(peeked) {
					s.gui.pushConn(wrapped)
					continue
				}
				s.wg.Add(1)
				go s.handleConnection(wrapped)
				continue
			}
			// Peek failed or no data: treat as PostgreSQL (replay nothing would be wrong, so use peeked if any)
			if n > 0 {
				wrapped := newPeekedConn(conn, peeked[:n])
				s.wg.Add(1)
				go s.handleConnection(wrapped)
			} else {
				conn.Close()
			}
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() error {
	s.PgRollback.StopLockStatusPoller()
	s.mu.Lock()
	if s.listener != nil {
		listener := s.listener
		s.listener = nil
		// Copy active connections so we can close them without holding mu (closing unblocks handlers)
		conns := make([]net.Conn, 0, len(s.activeConns))
		for c := range s.activeConns {
			conns = append(conns, c)
		}
		s.mu.Unlock()
		if err := listener.Close(); err != nil {
			return err
		}
		if s.gui != nil {
			s.gui.shutdown()
		}
		for _, c := range conns {
			_ = c.Close()
		}
	} else {
		s.mu.Unlock()
	}
	s.wg.Wait()
	return nil
}

// addActiveConn records a client connection so Stop() can close it to unblock handlers.
func (s *Server) addActiveConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeConns != nil {
		s.activeConns[c] = struct{}{}
	}
}

// removeActiveConn removes a client connection from the active set (e.g. when handler returns).
func (s *Server) removeActiveConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeConns, c)
}

// StartError retorna o erro de inicialização, se houver
func (s *Server) StartError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startErr
}

func (s *Server) handleConnection(clientConn net.Conn) {
	defer s.wg.Done()
	defer clientConn.Close()
	s.addActiveConn(clientConn)
	defer s.removeActiveConn(clientConn)

	// Log para rastrear conexões TCP ao pgrollback
	remoteAddr := clientConn.RemoteAddr().String()
	// Adiciona stack trace para identificar qual código está causando a conexão
	logIfVerbose("[SERVER] Nova conexão TCP recebida de %s", remoteAddr)
	defer logIfVerbose("[SERVER] Conexão TCP fechada de %s", remoteAddr)

	clientConn.SetDeadline(time.Now().Add(ConnectionTimeout))

	length, err := ReadFrontendMessageLength(clientConn)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Printf("Error reading message length: %v", err)
		}
		return
	}

	if !IsSSLRequestLength(length) {
		s.resumeStartupMessageAfterLengthPrefix(clientConn, length)
		return
	}
	s.handleEightByteSpecialFrame(clientConn, length)
}

// handleEightByteSpecialFrame runs after we read length==8 and must read the following request code.
func (s *Server) handleEightByteSpecialFrame(clientConn net.Conn, length int32) {
	code, err := ReadSpecialRequestCode(clientConn)
	if err != nil {
		log.Printf("Error reading special request code: %v", err)
		return
	}

	switch {
	case IsPostgresSSLRequestCode(code):
		s.replySSLNotSupportedThenStartup(clientConn)
	default:
		s.processStartupWithReplayedSpecialFrame(clientConn, length, code)
	}
}

// resumeStartupMessageAfterLengthPrefix re-injects the 4-byte StartupMessage length we already read.
func (s *Server) resumeStartupMessageAfterLengthPrefix(clientConn net.Conn, length int32) {
	backend := s.createBackendWithPreRead(clientConn, 4, length, 0)
	s.processConnectionStartupMessage(backend, clientConn)
}

// replySSLNotSupportedThenStartup implements the PostgreSQL SSL negotiation: respond with 'N' (SSL not available),
// then proceed as if the next bytes are a normal StartupMessage (same TCP connection).
func (s *Server) replySSLNotSupportedThenStartup(clientConn net.Conn) {
	if err := WriteSSLResponse(clientConn, false); err != nil {
		log.Printf("Error writing SSL response: %v", err)
		return
	}
	backend := pgproto3.NewBackend(clientConn, clientConn)
	s.processConnectionStartupMessage(backend, clientConn)
}

// processStartupWithReplayedSpecialFrame replays an 8-byte special request (length+code) then runs startup.
func (s *Server) processStartupWithReplayedSpecialFrame(clientConn net.Conn, length int32, code int32) {
	backend := s.createBackendWithPreRead(clientConn, 8, length, code)
	s.processConnectionStartupMessage(backend, clientConn)
}

// createBackendWithPreRead cria um backend reconstruindo bytes já lidos
func (s *Server) createBackendWithPreRead(clientConn net.Conn, dataSize int, length int32, code int32) *pgproto3.Backend {
	preReadData := make([]byte, dataSize)
	binary.BigEndian.PutUint32(preReadData[0:4], uint32(length))
	if dataSize == 8 {
		binary.BigEndian.PutUint32(preReadData[4:8], uint32(code))
	}
	multiReader := io.MultiReader(bytes.NewReader(preReadData), clientConn)
	return pgproto3.NewBackend(multiReader, clientConn)
}

// isConnClosedBeforePasswordErr reports common client-side teardown while waiting for PasswordMessage.
func isConnClosedBeforePasswordErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "wsarecv") ||
		strings.Contains(s, "use of closed") ||
		strings.Contains(s, "forcibly closed") ||
		strings.Contains(s, "aborted")
}

// processConnectionStartupMessage processa a mensagem de startup do cliente e estabelece a sessão
//
// Fluxo de Autenticação:
// 1. Recebe StartupMessage do cliente (contém application_name e outros parâmetros)
// 2. Extrai o testID do application_name (via protocol.ParseApplicationIdentity)
// 3. Simula autenticação PostgreSQL para o cliente:
//   - Solicita senha (AuthenticationCleartextPassword)
//   - Recebe senha do cliente
//   - Responde AuthenticationOK
//
// 4. Obtém ou cria sessão para o testID:
//   - Se já existe sessão para este testID: reutiliza a conexão PostgreSQL existente
//   - Se não existe: cria nova conexão PostgreSQL e nova transação
//
// 5. Inicia proxy para encaminhar comandos entre cliente e PostgreSQL
//
// IMPORTANTE: O cliente sempre passa por autenticação completa, mesmo quando
// reutilizamos uma conexão PostgreSQL existente. Isso garante que o cliente
// não percebe diferença entre uma conexão nova e uma reutilizada.
func (s *Server) processConnectionStartupMessage(backend *pgproto3.Backend, clientConn net.Conn) {
	clientConn.SetDeadline(time.Now().Add(ConnectionTimeout))

	params, err := getConnectionStartupParameters(backend)
	if err != nil {
		return
	}
	// testID + nome para log a partir de application_name (ver protocol.ParseApplicationIdentity)
	testID, appName := protocol.ParseApplicationIdentity(params)

	// Log para identificar qual teste/código está fazendo a conexão
	remoteAddr := clientConn.RemoteAddr().String()
	logIfVerbose("[SERVER] Conexão estabelecida - testID=%s, application_name=%s, origem=%s", testID, appName, remoteAddr)

	// Simula autenticação PostgreSQL: sempre solicita senha do cliente
	// Isso garante que o cliente sempre passa pelo mesmo fluxo, independente
	// de estarmos reutilizando uma conexão PostgreSQL ou criando nova
	if err := WriteAuthenticationCleartextPassword(clientConn); err != nil {
		log.Printf("Error writing authentication request: %v", err)
		return
	}

	passwordMsg, err := backend.Receive()
	if err != nil {
		// Client often closes abandoned or raced TCP connects (e.g. database/sql pool churn); not worth ERROR spam.
		if isConnClosedBeforePasswordErr(err) {
			logIfVerbose("client closed connection before password message: %v", err)
		} else {
			log.Printf("Error receiving password message: %v", err)
		}
		return
	}

	if _, ok := passwordMsg.(*pgproto3.PasswordMessage); !ok {
		log.Printf("Expected password message, got: %T", passwordMsg)
		return
	}

	// Obtém ou cria sessão para este testID
	// - Se já existe: reutiliza conexão PostgreSQL e transação existentes
	// - Se não existe: cria nova conexão PostgreSQL e nova transação
	// A conexão PostgreSQL é persistente e reutilizada para o mesmo testID
	_, err = s.PgRollback.GetOrCreateSession(testID)
	if err != nil {
		errorBackend := pgproto3.NewBackend(clientConn, clientConn)
		sendErrorToClient(errorBackend, err.Error())
		return
	}

	// Responde AuthenticationOK ao cliente
	// O cliente agora está "autenticado" e não sabe se estamos usando
	// uma conexão PostgreSQL nova ou reutilizada
	if err := WriteAuthenticationOK(clientConn); err != nil {
		log.Printf("Error writing authentication OK: %v", err)
		return
	}

	// Inicia proxy para encaminhar comandos entre cliente e PostgreSQL
	s.startProxy(testID, clientConn, backend)
}

func getConnectionStartupParameters(backend *pgproto3.Backend) (map[string]string, error) {
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("Error receiving startup message from client: %v", err)
	}

	params := make(map[string]string)
	if sm, ok := startupMsg.(*pgproto3.StartupMessage); ok {
		for k, v := range sm.Parameters {
			params[k] = v
		}
	}
	return params, nil
}
