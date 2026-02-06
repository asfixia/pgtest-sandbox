// Funções utilitárias para testes de integração do pgtest.
// Este arquivo contém todas as funções auxiliares usadas pelos testes,
// separadas do código de teste propriamente dito para melhor organização.
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pgtest-transient/internal/config"
	"pgtest-transient/internal/testutil"
	"pgtest-transient/pkg/logger"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var pgtestConfig *config.Config

func init() {
	configPath := os.Getenv("PGTEST_CONFIG")
	if configPath == "" {
		// Tenta encontrar o arquivo de config relativo à raiz do projeto
		workDir, _ := os.Getwd()
		// Procura por go.mod para encontrar a raiz do projeto
		projectRoot := workDir
		for {
			if _, err := os.Stat(filepath.Join(projectRoot, "go.mod")); err == nil {
				break
			}
			parent := filepath.Dir(projectRoot)
			if parent == projectRoot {
				// Não encontrou go.mod, usa diretório atual
				projectRoot = workDir
				break
			}
			projectRoot = parent
		}
		configPath = filepath.Join(projectRoot, "config", "pgtest-transient.yaml")
	}
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	pgtestConfig = cfg
}

func getConfig() *config.Config {
	return pgtestConfig
}

// --- Configuração e DSN ---

// getTestSchema retorna o schema configurado para testes, ou "public" como padrão
func getTestSchema() string {
	if pgtestConfig != nil && pgtestConfig.Test.Schema != "" {
		return pgtestConfig.Test.Schema
	}
	return "public"
}

// getPGTestProxyDSN retorna o DSN para conectar ao servidor intermediário pgtest (proxy)
func getPGTestProxyDSN(testID string) string {
	// Usa o host do proxy pgtest, não do PostgreSQL real
	host := pgtestConfig.Proxy.ListenHost
	if host == "" {
		host = "localhost"
	}
	// As credenciais e database vêm do PostgreSQL real (o proxy repassa)
	user := pgtestConfig.Postgres.User
	if user == "" {
		user = "postgres"
	}
	password := pgtestConfig.Postgres.Password
	dbname := pgtestConfig.Postgres.Database
	if dbname == "" {
		dbname = "postgres"
	}
	pgtestListenPort := pgtestConfig.Proxy.ListenPort
	// Conecta ao proxy pgtest na porta configurada
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable application_name=pgtest_%s",
		host, pgtestListenPort, user, password, dbname, testID)
}

// getRealPostgresDSN retorna o DSN para conectar diretamente ao servidor PostgreSQL real
// (sem passar pelo proxy pgtest)
// Usa variáveis de ambiente se disponíveis, caso contrário usa a configuração do pgtest.yaml
func getRealPostgresDSN(t *testing.T) string {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	pass := os.Getenv("POSTGRES_PASSWORD")
	db := os.Getenv("POSTGRES_DB")

	// Se variáveis de ambiente não estão configuradas, usa a configuração do pgtest.yaml
	if host == "" || port == "" || user == "" || db == "" {
		if pgtestConfig == nil {
			t.Fatalf("POSTGRES_* environment variables or pgtest.yaml config required for direct PostgreSQL connection")
		}
		if host == "" {
			host = pgtestConfig.Postgres.Host
			if host == "" {
				host = "localhost"
			}
		}
		if port == "" {
			port = fmt.Sprintf("%d", pgtestConfig.Postgres.Port)
			if port == "0" {
				port = "5432"
			}
		}
		if user == "" {
			user = pgtestConfig.Postgres.User
			if user == "" {
				user = "postgres"
			}
		}
		if pass == "" {
			pass = pgtestConfig.Postgres.Password
		}
		if db == "" {
			db = pgtestConfig.Postgres.Database
			if db == "" {
				db = "postgres"
			}
		}
	}

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, db)
}

// --- Conexão ---

// pingConnection executa um ping na conexão com timeout padrão de 5 segundos.
// Falha o teste se o ping não for bem-sucedido.
// Útil para estabelecer conexões TCP imediatamente após sql.Open() (que é lazy).
func pingConnection(t *testing.T, db *sql.DB) {
	t.Helper()
	pingWithTimeout(t, db, 120*time.Second, false) //Danilo
}

// connectToRealPostgres cria uma conexão direta ao servidor PostgreSQL real (sem passar pelo proxy).
// Esta conexão está fora da transação gerenciada pelo pgtest e permite verificar o estado real do banco.
// Estabelece a conexão imediatamente e retorna o *sql.DB configurado.
func connectToRealPostgres(t *testing.T) *sql.DB {
	t.Helper()
	realPostgresDSN := getRealPostgresDSN(t)
	postgresDBDirect, err := sql.Open("pgx", realPostgresDSN)
	if err != nil {
		t.Fatalf("Failed to open direct PostgreSQL connection: %v", err)
	}

	// Estabelece a conexão TCP imediatamente (sql.Open é lazy)
	pingConnection(t, postgresDBDirect)

	return postgresDBDirect
}

// connectToPGTestProxy cria uma conexão ao servidor intermediário pgtest (proxy)
// Retorna a conexão e um erro, se houver
// IMPORTANTE: Configura o pool para usar apenas 1 conexão para garantir que todas
// as operações usem a mesma conexão TCP ao pgtest, evitando problemas de sincronização
func connectToPGTestProxy(t *testing.T, testID string) *sql.DB {
	t.Helper()
	pgtestProxyDSN := getPGTestProxyDSN(testID)
	pgtestDB, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}

	// Estabelece a conexão TCP imediatamente (sql.Open é lazy)
	// Isso faz com que o servidor receba a conexão e logue "[SERVER] Nova conexão TCP recebida de ..."
	pingConnection(t, pgtestDB)

	//// Garante que usamos apenas 1 conexão TCP ao pgtest
	//// Isso evita que o database/sql crie múltiplas conexões TCP, cada uma com seu
	//// próprio proxyConnection, o que pode causar problemas de sincronização
	//pgtestDB.SetMaxOpenConns(1)
	//pgtestDB.SetMaxIdleConns(1)
	//pgtestDB.SetConnMaxLifetime(0) // Conexões não expiram

	return pgtestDB
}

// connectToPGTestProxySingleConn is like connectToPGTestProxy but forces a single connection
// (MaxOpenConns(1)). Used by repro tests to rule out pool reordering.
func connectToPGTestProxySingleConn(t *testing.T, testID string) *sql.DB {
	t.Helper()
	pgtestProxyDSN := getPGTestProxyDSN(testID)
	pgtestDB, err := sql.Open("pgx", pgtestProxyDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PGTest proxy: %v", err)
	}
	pingConnection(t, pgtestDB)
	pgtestDB.SetMaxOpenConns(1)
	pgtestDB.SetMaxIdleConns(1)
	pgtestDB.SetConnMaxLifetime(0)
	return pgtestDB
}

// pingWithTimeout executa um ping na conexão com timeout separado.
// Se contextMessage for fornecido e o ping falhar, faz t.Fatalf com a mensagem.
// Se retryOnFailure for true, tenta uma query SELECT 1 antes de falhar.
// Retorna true se o ping foi bem-sucedido, false caso contrário (apenas se contextMessage não foi fornecido).
func pingWithTimeout(t *testing.T, db *sql.DB, timeout time.Duration, retryOnFailure bool, contextMessage ...string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		if retryOnFailure {
			logger.TestDebug(t, "Connection ping failed, attempting reconnect: %v", err)
			_, _ = db.Exec("SELECT 1")
			// Tenta novamente com novo timeout
			retryCtx, retryCancel := context.WithTimeout(context.Background(), timeout)
			defer retryCancel()
			if err := db.PingContext(retryCtx); err != nil {
				if len(contextMessage) > 0 && contextMessage[0] != "" {
					t.Fatalf("%s: %v", contextMessage[0], err)
				}
				return false
			}
			return true
		}
		if len(contextMessage) > 0 && contextMessage[0] != "" {
			t.Fatalf("%s: %v", contextMessage[0], err)
		} else {
			t.Fatalf("Connection ping failed: %v", err)
		}
		return false
	}
	return true
}

// --- Asserções (verificam estado de tabelas e queries) ---

// assertTableExistence verifica se uma tabela existe ou não existe (sem contar linhas).
// shouldExist indica o resultado esperado: true = tabela deve existir, false = tabela não deve existir.
// Usa t.Fatalf se o estado real não corresponder ao esperado.
func assertTableExistence(t *testing.T, db *sql.DB, tableName string, shouldExist bool, successMessage string) {
	t.Helper()
	expectedQuantity := 0
	if shouldExist {
		expectedQuantity = 1
	}
	// Scalar query with no FROM: always returns exactly one row (0 or 1). Avoids "sql: no rows in result set"
	// when the proxy returns empty result for COUNT(*) FROM to_regclass(...) WHERE ... when table does not exist.
	query := fmt.Sprintf("SELECT CASE WHEN to_regclass('%s') IS NOT NULL THEN 1 ELSE 0 END", tableName)
	assertQueryCount(t, db, query, expectedQuantity, "Error: Table existence assertion failed: "+successMessage)
}

// assertTableExists verifica que uma tabela existe (sem contar linhas).
// Wrapper conveniente para assertTableExistence com shouldExist = true.
func assertTableExists(t *testing.T, db *sql.DB, tableName string, successMessage string) {
	t.Helper()
	assertTableExistence(t, db, tableName, true, successMessage)
}

// assertTableDoesNotExist verifica que uma tabela não existe (sem contar linhas).
// Wrapper conveniente para assertTableExistence com shouldExist = false.
func assertTableDoesNotExist(t *testing.T, db *sql.DB, tableName string, successMessage string) {
	t.Helper()
	assertTableExistence(t, db, tableName, false, successMessage)
}

// assertCount verifica que uma query SQL retorna um count específico.
// Usa t.Fatalf se o count não corresponder ao esperado ou se houver erro na query.
// Usa Query em vez de QueryRow para que, quando o proxy retorna 0 linhas (ex.: tabela inexistente),
// tratemos como count 0 em vez de "sql: no rows in result set".
func assertCount(t *testing.T, db *sql.DB, query string, expectedCount int, successMessage string) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v\nQuery: %s", err, query)
	}
	defer rows.Close()

	count := -1 //-1 means "not found"
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan count: %v\nQuery: %s", err, query)
		}
	}
	// 0 rows from proxy -> count stays 0; 1 row -> count from result

	if count != expectedCount {
		t.Fatalf("Query count = %d, expected %d\nQuery: %s", count, expectedCount, query)
	}

	if successMessage != "" {
		t.Logf("SUCCESS: %s", successMessage)
	}
}

// assertQueryCount verifica que uma query retorna um count específico.
// Útil para queries com WHERE ou outras condições.
func assertQueryCount(t *testing.T, db *sql.DB, query string, expectedCount int, successMessage string) {
	t.Helper()
	assertCount(t, db, query, expectedCount, successMessage)
}

// assertTableRowCount é um alias para assertTableCount mantido para compatibilidade.
func assertTableRowCount(t *testing.T, db *sql.DB, tableName string, expectedCount int, successMessage string) {
	t.Helper()
	testutil.AssertTableCount(t, db, tableName, expectedCount, successMessage)
}

// --- Helpers de fluxo (usados em vários testes) ---

// execBegin executa BEGIN e garante que a resposta foi completamente processada
// Encapsula a lógica de verificação de erro e sincronização
// Retorna true se o BEGIN foi executado com sucesso, false caso contrário
func execBegin(t *testing.T, db *sql.DB, contextMsg string) bool {
	t.Helper()

	// Executa BEGIN - o pgtest converterá em SAVEPOINT
	// O Exec() do database/sql já aguarda a resposta completa (CommandComplete + ReadyForQuery)
	// antes de retornar, então não precisamos de sincronização adicional
	result, err := db.Exec("BEGIN")
	if err != nil {
		// Se falhar, tenta verificar o estado da conexão
		logger.TestError(t, "Failed to execute BEGIN: %v (type: %T)", err, err)
		if !pingWithTimeout(t, db, 5*time.Second, false) {
			logger.TestError(t, "Connection was reset. BEGIN error: %v", err)
			t.Fatalf("Connection was reset. BEGIN error: %v", err)
		}
		logger.TestError(t, "Failed to execute BEGIN (connection seems valid): %v", err)
		t.Fatalf("Failed to execute BEGIN (connection seems valid): %v", err)
		return false
	}

	// Força a leitura completa da resposta para garantir sincronização
	// (Exec() já aguarda ReadyForQuery; RowsAffected() garante consumo completo)
	if result != nil {
		_, _ = result.RowsAffected()
	}

	if contextMsg != "" {
		logger.TestDebug(t, "BEGIN executed successfully: %s", contextMsg)
	} else {
		logger.TestDebug(t, "BEGIN executed successfully")
	}

	return true
}

// createTableWithValueColumn cria uma tabela com colunas (id SERIAL PRIMARY KEY, value TEXT).
// Falha o teste se houver erro.
// Usa a função unificada do pacote testutil.
func createTableWithValueColumn(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	testutil.CreateTableWithValueColumn(t, db, tableName)
}

// insertOneRow insere uma linha na tabela com coluna value (id SERIAL PRIMARY KEY, value TEXT).
// Executa INSERT INTO tableName (value) VALUES (value), trata erro e exige RowsAffected == 1.
// Usa interpolação na string (não $1) porque o proxy pgtest encaminha em modo simple query, que não suporta parâmetros bound.
// contextMessage é opcional e será incluído nas mensagens de erro para identificar onde no teste falhou.
// Usa a função unificada do pacote testutil.
func insertOneRow(t *testing.T, db *sql.DB, tableName string, value string, contextMessage string) {
	t.Helper()
	testutil.InsertOneRow(t, db, tableName, value, contextMessage)
}

// insertDuplicateRow tenta inserir uma linha duplicada (mesmo id) e espera que dê erro.
// Falha o teste se a inserção não der erro (duplicata deveria ser detectada).
func insertDuplicateRow(t *testing.T, db *sql.DB, tableName string, id int, value string) {
	t.Helper()
	escapedValue := strings.ReplaceAll(value, "'", "''")
	_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES (%d, '%s')", tableName, id, escapedValue))
	if err == nil {
		t.Fatalf("Expected error for duplicate key (id=%d), got nil", id)
	}
	t.Logf("SUCCESS: Duplicate key error correctly detected: %v", err)
}

// execCommit executa COMMIT. No pgtest isso vira RELEASE SAVEPOINT.
// Falha se houver erro ou se RowsAffected != 0 (comando de controle não altera linhas).
func execCommit(t *testing.T, db *sql.DB) {
	t.Helper()
	result, err := db.Exec("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}
	n, _ := result.RowsAffected()
	if n != 0 {
		t.Fatalf("COMMIT (RELEASE SAVEPOINT) should report 0 rows affected, got: %d", n)
	}
}

// execRollback envia ROLLBACK ao pgtest (que pode bloqueá-lo).
// Apenas loga o resultado; usa quando se espera que ROLLBACK seja bloqueado pelo pgtest.
func execRollback(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("ROLLBACK")
	if err != nil {
		t.Logf("ROLLBACK was blocked (expected): %v", err)
	} else {
		t.Log("ROLLBACK executed (should be blocked)")
	}
}

// execRollbackOrFail executa ROLLBACK e falha o teste se der erro.
// Use quando o ROLLBACK deve ser executado com sucesso.
func execRollbackOrFail(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("ROLLBACK")
	if err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}
}

// execExpectError executa a SQL e falha o teste se não retornar erro.
// Use quando se espera que o comando falhe (e.g. transação abortada).
func execExpectError(t *testing.T, db *sql.DB, sql string, contextMessage string) {
	t.Helper()
	_, err := db.Exec(sql)
	if err == nil {
		t.Fatalf("%s: expected SQL to fail, got nil. SQL: %s", contextMessage, sql)
	}
	t.Logf("SUCCESS: %s: SQL correctly failed: %v", contextMessage, err)
}

// execPgtestRollback envia "pgtest rollback" e loga em caso de erro (não falha o teste).
// Use para limpeza no fim do teste ou quando o sucesso do rollback não é assertado.
func execPgtestRollback(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("pgtest rollback")
	if err != nil {
		logger.Warn("Rollback via pgtest command failed: %v", err)
		t.Logf("pgtest rollback: %v", err)
	}
}

// execPgTestFullRollback envia "pgtest rollback" e falha o teste se der erro.
func execPgTestFullRollback(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("pgtest rollback")
	if err != nil {
		t.Fatalf("Rollback via pgtest command failed: %v", err)
	}
}

// execPgtestBegin executa "pgtest begin" para iniciar uma transação explícita no pgtest.
// Falha o teste se der erro. O testID já deve estar na connection string (application_name).
func execPgtestBegin(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("pgtest begin")
	if err != nil {
		t.Fatalf("Failed to start transaction via pgtest begin: %v", err)
	}
}

// execSavepoint executa SAVEPOINT e verifica que RowsAffected == 0.
// Falha o teste se houver erro ou se RowsAffected != 0.
func execSavepoint(t *testing.T, db *sql.DB, savepointName string) {
	t.Helper()
	result, err := db.Exec(fmt.Sprintf("SAVEPOINT %s", savepointName))
	if err != nil {
		t.Fatalf("Failed to create SAVEPOINT %s: %v", savepointName, err)
	}
	n, _ := result.RowsAffected()
	if n != 0 {
		t.Fatalf("SAVEPOINT should report 0 rows affected, got: %d", n)
	}
}

// execReleaseSavepoint executa RELEASE SAVEPOINT e verifica que RowsAffected == 0.
// Falha o teste se houver erro ou se RowsAffected != 0.
func execReleaseSavepoint(t *testing.T, db *sql.DB, savepointName string) {
	t.Helper()
	result, err := db.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
	if err != nil {
		t.Fatalf("Failed to RELEASE SAVEPOINT %s: %v", savepointName, err)
	}
	n, _ := result.RowsAffected()
	if n != 0 {
		t.Fatalf("RELEASE SAVEPOINT should report 0 rows affected, got: %d", n)
	}
}

// execReleaseSavepointExpectError executa RELEASE SAVEPOINT e espera que dê erro (e.g. savepoint já liberado).
// Falha o teste se a execução não retornar erro.
func execReleaseSavepointExpectError(t *testing.T, db *sql.DB, savepointName string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
	if err == nil {
		t.Fatalf("RELEASE SAVEPOINT %s should have failed (savepoint does not exist), got nil", savepointName)
	}
	t.Logf("SUCCESS: RELEASE SAVEPOINT %s correctly failed: %v", savepointName, err)
}

// execRollbackToSavepoint executa ROLLBACK TO SAVEPOINT e verifica que RowsAffected == 0.
// Falha o teste se houver erro ou se RowsAffected != 0.
func execRollbackToSavepoint(t *testing.T, db *sql.DB, savepointName string) {
	t.Helper()
	result, err := db.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName))
	if err != nil {
		t.Fatalf("Failed to ROLLBACK TO SAVEPOINT %s: %v", savepointName, err)
	}
	n, _ := result.RowsAffected()
	if n != 0 {
		t.Fatalf("ROLLBACK TO SAVEPOINT should report 0 rows affected, got: %d", n)
	}
}

func execRollbackToInvalidSavepoint(t *testing.T, db *sql.DB, savepointName string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName))
	if err == nil {
		t.Fatalf("Expected error for invalid savepoint: %v", err)
	}
	t.Logf("SUCCESS: Invalid savepoint error correctly detected: %v", err)
}

// dropTable executa DROP TABLE. Por padrão espera sucesso (err == nil); falha o teste se der erro.
// No PostgreSQL, sucesso = comando executado sem erro; DROP TABLE não retorna linhas.
func dropTable(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	if err != nil {
		t.Fatalf("Failed to drop table %s: %v", tableName, err)
	}
}

// dropTableBestEffort executa DROP TABLE para limpeza; se a tabela não existir, apenas loga e não falha.
// Use no fim do teste quando a existência da tabela é opcional.
func dropTableBestEffort(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	if err != nil {
		t.Fatalf("Failed to drop table %s (may not exist): %v", tableName, err)
	}
}

// dropTableIfExists executa DROP TABLE IF EXISTS para garantir que a tabela não existe.
// Não falha se a tabela não existir. Use quando precisar garantir que a tabela não existe antes de criar.
func dropTableIfExists(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Fatalf("Failed to drop table %s (even with IF EXISTS): %v", tableName, err)
	}
}
