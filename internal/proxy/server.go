package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"pgtest-transient/pkg/protocol"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	// ConnectionTimeout é o timeout para operações de leitura/escrita nas conexões
	ConnectionTimeout = 60 * time.Second
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
	Pgtest   *PGTest
	listener net.Listener
	wg       sync.WaitGroup
	startErr error
	mu       sync.RWMutex
}

// isPortInUse verifica se uma porta está em uso tentando conectar a ela
func isPortInUse(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), portCheckTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// NewServer cria uma nova instância do Server e inicia o servidor automaticamente
// Retorna sempre o Server, mesmo se houver erro ao iniciar
// Se listenHost for vazio, usa "localhost" como padrão
// Se listenPort for 0, usa DefaultListenPort (5433) como padrão
// Se sessionTimeout for 0, usa DefaultSessionTimeout (24h) como padrão
// Verifica se a porta está disponível antes de tentar iniciar o servidor
// Se houver erro ao iniciar, o erro é armazenado no Server e pode ser verificado com StartError()
func NewServer(postgresHost string, postgresPort int, postgresDB, postgresUser, postgresPass string, timeout time.Duration, sessionTimeout time.Duration, keepaliveInterval time.Duration, listenHost string, listenPort int) *Server {
	// Usa valores padrão se não especificados
	if sessionTimeout <= 0 {
		sessionTimeout = DefaultSessionTimeout
	}
	if listenPort == 0 {
		listenPort = DefaultListenPort
	}
	if listenHost == "" {
		listenHost = "localhost"
	}

	pgtest := NewPGTest(postgresHost, postgresPort, postgresDB, postgresUser, postgresPass, timeout, sessionTimeout, keepaliveInterval)
	server := &Server{
		Pgtest: pgtest,
	}

	// Verifica se a porta já está em uso antes de tentar criar o listener
	addr := fmt.Sprintf("%s:%d", listenHost, listenPort)
	if isPortInUse(listenHost, listenPort) {
		server.mu.Lock()
		server.startErr = fmt.Errorf("port %s:%d is already in use. Cannot start server. Please stop any service using this port", listenHost, listenPort)
		server.mu.Unlock()
		return server
	}

	// Tenta criar o listener para verificar erros imediatos
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		server.mu.Lock()
		server.startErr = fmt.Errorf("failed to listen on %s: %w", addr, err)
		server.mu.Unlock()
		return server
	}

	server.listener = listener

	// Inicia o loop de aceitação de conexões em uma goroutine
	go server.acceptConnections()

	// Aguarda o servidor estar realmente escutando antes de retornar
	if !server.waitUntilListening(listenHost, listenPort) {
		server.mu.Lock()
		server.startErr = fmt.Errorf("server failed to start listening on %s after %d attempts", addr, serverStartupCheckAttempts)
		server.mu.Unlock()
		return server
	}

	logIfVerbose("PGTest server listening on %s", addr)
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
// O pgtest é um servidor intermediário que fica entre o cliente e o PostgreSQL real.
// Todas as conexões são feitas ao pgtest, não diretamente ao PostgreSQL.
//
// Reutilização de Conexões PostgreSQL por application_name:
// - Cada cliente se conecta ao pgtest usando application_name (via parâmetro de conexão)
// - O application_name é extraído e convertido em testID (via protocol.ExtractTestID)
// - O mesmo testID sempre reutiliza a mesma conexão física ao PostgreSQL real
// - Isso é gerenciado por SessionsByTestID: cada sessão guarda sua DB (conn+tx) sob o testID
//
// Simulação de Autenticação para o Cliente:
//   - Cada cliente sempre passa pelo handshake completo de autenticação com o pgtest
//   - O pgtest simula a autenticação PostgreSQL (pede senha, responde AuthenticationOK)
//   - O cliente nunca sabe se está usando uma conexão PostgreSQL nova ou reutilizada
//   - Isso permite que aplicações como PHP (que abrem e fecham conexões rapidamente)
//     continuem usando a mesma transação PostgreSQL, mantendo as alterações
//
// Benefícios:
// - Transações não são perdidas quando o cliente fecha a conexão (PHP, scripts, etc.)
// - Alterações persistem entre reconexões do mesmo application_name
// - Controle total sobre quando fazer rollback (via comandos pgtest especiais)
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

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() error {
	s.mu.Lock()
	if s.listener != nil {
		listener := s.listener
		s.listener = nil
		s.mu.Unlock()
		if err := listener.Close(); err != nil {
			return err
		}
	} else {
		s.mu.Unlock()
	}
	s.wg.Wait()
	return nil
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

	// Log para rastrear conexões TCP ao pgtest
	remoteAddr := clientConn.RemoteAddr().String()
	// Adiciona stack trace para identificar qual código está causando a conexão
	logIfVerbose("[SERVER] Nova conexão TCP recebida de %s", remoteAddr)
	defer logIfVerbose("[SERVER] Conexão TCP fechada de %s", remoteAddr)

	clientConn.SetDeadline(time.Now().Add(ConnectionTimeout))

	var length int32
	if err := binary.Read(clientConn, binary.BigEndian, &length); err != nil {
		if err != io.EOF {
			log.Printf("Error reading message length: %v", err)
		}
		return
	}

	// Verifica se é SSLRequest (length = 8)
	if length == 8 {
		var code int32
		if err := binary.Read(clientConn, binary.BigEndian, &code); err != nil {
			log.Printf("Error reading SSL request code: %v", err)
			return
		}

		if code == SSLRequestCode {
			if err := WriteSSLResponse(clientConn, false); err != nil {
				log.Printf("Error writing SSL response: %v", err)
				return
			}
		} else {
			// Reconstruir bytes lidos
			backend := s.createBackendWithPreRead(clientConn, 8, length, code)
			s.processConnectionStartupMessage(backend, clientConn)
			return
		}
	} else {
		// Reconstruir bytes do length
		backend := s.createBackendWithPreRead(clientConn, 4, length, 0)
		s.processConnectionStartupMessage(backend, clientConn)
		return
	}

	// Backend normal após tratar SSLRequest
	backend := pgproto3.NewBackend(clientConn, clientConn)
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

// processConnectionStartupMessage processa a mensagem de startup do cliente e estabelece a sessão
//
// Fluxo de Autenticação:
// 1. Recebe StartupMessage do cliente (contém application_name e outros parâmetros)
// 2. Extrai o testID do application_name (via protocol.ExtractTestID)
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
	// Extrai testID do application_name do cliente
	// O mesmo application_name sempre resulta no mesmo testID
	testID, err := protocol.ExtractTestID(params)
	if err != nil {
		errorBackend := pgproto3.NewBackend(clientConn, clientConn)
		sendErrorToClient(errorBackend, err.Error())
		return
	}

	// Log para identificar qual teste/código está fazendo a conexão
	// O testID identifica qual teste está conectando (ex: "test_commit_protection" = TestProtectionAgainstAccidentalCommit)
	appName := protocol.ExtractAppname(params)
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
		log.Printf("Error receiving password message: %v", err)
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
	_, err = s.Pgtest.GetOrCreateSession(testID)
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
