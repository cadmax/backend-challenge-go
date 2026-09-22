//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cadmax/backend-challenge-go/internal/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type wallet struct {
	ID       string `json:"id"`
	PlayerID string `json:"playerId"`
	Balance  money  `json:"balance"`
	Version  int64  `json:"version"`
}

type command struct {
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	IdempotencyKey        string `json:"idempotencyKey,omitempty"`
	PlayerID              string `json:"playerId"`
	WalletID              string `json:"walletId"`
	RoundID               string `json:"roundId"`
	GameID                string `json:"gameId"`
	Kind                  string `json:"kind"`
	Money                 money  `json:"money"`
	Reference             string `json:"referenceExternalTransactionId,omitempty"`
}

type result struct {
	TransactionID string `json:"transactionId"`
	Status        string `json:"status"`
	Balance       *money `json:"balance"`
	FailureCode   string `json:"failureCode"`
	Replay        bool   `json:"idempotentReplay"`
}

type ledgerEntry struct {
	ID            string `json:"id"`
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Money         money  `json:"money"`
}

type response struct {
	Code int
	Body []byte
}

type process struct {
	cmd              *exec.Cmd
	base             string
	logPath          string
	done             chan struct{}
	err              error
	expectCrash      bool
	verifiedShutdown bool
}

type suite struct {
	t                                         *testing.T
	binary, root, databaseURL, issuer, logDir string
	queueName, eventName, dlqName             string
	queueURL, eventURL, dlqURL                string
	pool                                      *pgxpool.Pool
	sqs                                       *sqs.Client
	processes                                 []*process
	allProcesses                              []*process
	queues                                    []string
	client                                    *http.Client
	tokens                                    map[string]string
	tokenMu                                   sync.Mutex
	occurredAt                                time.Time
}

func newSuite(t *testing.T) *suite {
	t.Helper()
	_, source, _, _ := runtime.Caller(0)
	s := &suite{t: t, root: filepath.Clean(filepath.Join(filepath.Dir(source), "../..")), client: &http.Client{Timeout: 12 * time.Second}, tokens: map[string]string{}}
	s.occurredAt = time.Now().UTC()
	s.binary = filepath.Join(t.TempDir(), "wager-api")
	s.logDir = t.TempDir()
	build := exec.Command("go", "build", "-race", "-o", s.binary, "./cmd/wager-api")
	build.Dir = s.root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build application: %v\n%s", err, output)
	}
	s.issuer = envOr("INTEGRATION_OIDC_ISSUER_URL", "http://localhost:8081/realms/jungle")
	adminURL := envOr("INTEGRATION_DATABASE_URL", "postgres://wager_owner:wager-owner-local@localhost:55432/wager?sslmode=disable")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect real PostgreSQL (start compose dependencies first): %v", err)
	}
	dbName := "integration_" + strings.ReplaceAll(uniqueID(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatalf("create isolated test database: %v", err)
	}
	parsed, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + dbName
	s.databaseURL = parsed.String()
	s.pool, err = pgxpool.New(ctx, s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.stopAll()
		s.pool.Close()
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		for _, queueURL := range s.queues {
			_, _ = s.sqs.DeleteQueue(cleanup, &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
		}
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		_ = admin.Close(cleanup)
	})
	if err := postgres.Migrate(ctx, s.pool, "up"); err != nil {
		t.Fatalf("migrate isolated database: %v", err)
	}
	parsed.User = url.UserPassword(envOr("INTEGRATION_RUNTIME_DB_USER", "wager"), envOr("INTEGRATION_RUNTIME_DB_PASSWORD", "wager-local"))
	s.databaseURL = parsed.String()
	endpoint := envOr("INTEGRATION_SQS_ENDPOINT", "http://localhost:4567")
	s.sqs = sqs.New(sqs.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")})
	prefix := "integration-" + uniqueID()
	s.queueName, s.eventName, s.dlqName = prefix+".fifo", prefix+"-events.fifo", prefix+"-dlq.fifo"
	s.dlqURL = s.createQueue(s.dlqName, nil)
	attrs, err := s.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(s.dlqURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": "3"})
	s.queueURL = s.createQueue(s.queueName, map[string]string{"RedrivePolicy": string(redrive), "VisibilityTimeout": "2"})
	s.eventURL = s.createQueue(s.eventName, nil)
	for _, client := range []string{"provider-a", "provider-b", "internal-service"} {
		s.token(client)
	}
	s.startThree()
	return s
}

func (s *suite) createQueue(name string, attrs map[string]string) string {
	s.t.Helper()
	if attrs == nil {
		attrs = map[string]string{}
	}
	attrs["FifoQueue"] = "true"
	attrs["ContentBasedDeduplication"] = "false"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := s.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	if err != nil {
		s.t.Fatalf("create real SQS queue: %v", err)
	}
	queueURL := aws.ToString(out.QueueUrl)
	s.queues = append(s.queues, queueURL)
	return queueURL
}

func (s *suite) startThree() {
	s.processes = nil
	for range 3 {
		s.processes = append(s.processes, s.start(nil))
	}
	if s.processes[0].cmd.Process.Pid == s.processes[1].cmd.Process.Pid || s.processes[1].cmd.Process.Pid == s.processes[2].cmd.Process.Pid {
		s.t.Fatal("expected three independent OS processes")
	}
	s.t.Logf("three application processes: %d, %d, %d", s.processes[0].cmd.Process.Pid, s.processes[1].cmd.Process.Pid, s.processes[2].cmd.Process.Pid)
}

func (s *suite) start(overrides map[string]string) *process {
	s.t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	env := map[string]string{
		"HTTP_ADDR": address, "DATABASE_URL": s.databaseURL,
		"OIDC_ISSUER_URL": s.issuer, "OIDC_JWKS_URL": s.issuer + "/protocol/openid-connect/certs", "OIDC_AUDIENCE": "wagering-api",
		"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
		"SQS_ENDPOINT": envOr("INTEGRATION_SQS_ENDPOINT", "http://localhost:4567"), "SQS_QUEUE_NAME": s.queueName, "SQS_EVENT_QUEUE_NAME": s.eventName, "SQS_DLQ_NAME": s.dlqName,
		"WORKERS_ENABLED": "true", "CONSUMER_ENABLED": "true", "PUBLISHER_ENABLED": "true", "REFERENCE_WORKER_ENABLED": "true",
		"REFERENCE_MAX_ATTEMPTS": "20", "REFERENCE_TTL": "10s", "REFERENCE_POLL_INTERVAL": "100ms", "OUTBOX_POLL_INTERVAL": "100ms",
		"SQS_WAIT_SECONDS": "1", "SQS_VISIBILITY_SECONDS": "7", "PROCESS_TIMEOUT": "1s", "SHUTDOWN_TIMEOUT": "5s", "ENABLE_TEST_FAILPOINTS": "false", "TEST_FAILPOINT": "",
	}
	for key, value := range overrides {
		env[key] = value
	}
	cmd := exec.Command(s.binary)
	cmd.Dir = s.root
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if _, exists := env[key]; !exists {
			cmd.Env = append(cmd.Env, item)
		}
	}
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	logPath := filepath.Join(s.logDir, fmt.Sprintf("process-%d.log", len(s.allProcesses)))
	logFile, err := os.Create(logPath)
	if err != nil {
		s.t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	p := &process{cmd: cmd, base: "http://" + address, logPath: logPath, done: make(chan struct{}), expectCrash: env["ENABLE_TEST_FAILPOINTS"] == "true"}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		s.t.Fatal(err)
	}
	s.allProcesses = append(s.allProcesses, p)
	go func() { p.err = cmd.Wait(); _ = logFile.Close(); close(p.done) }()
	if env["TEST_FAILPOINT"] == "after_outbox_send" {
		return p
	}
	eventually(s.t, 30*time.Second, "process readiness", func() bool {
		select {
		case <-p.done:
			data, _ := os.ReadFile(logPath)
			s.t.Fatalf("application exited: %v\n%s", p.err, data)
		default:
		}
		r, err := s.request(p, http.MethodGet, "/health/ready", "", "", nil)
		return err == nil && r.Code == http.StatusOK
	})
	return p
}

func (s *suite) stopAll() {
	for _, p := range s.allProcesses {
		select {
		case <-p.done:
			continue
		default:
		}
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range s.allProcesses {
		select {
		case <-p.done:
		case <-time.After(8 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
			s.t.Errorf("process did not stop within Fx shutdown deadline")
		}
		if p.err != nil {
			data, _ := os.ReadFile(p.logPath)
			if !p.expectCrash {
				s.t.Errorf("application failed during lifecycle: %v\n%s", p.err, data)
			}
			if bytes.Contains(data, []byte("DATA RACE")) {
				s.t.Errorf("process race detector failure:\n%s", data)
			}
		}
		if p.err == nil && !p.verifiedShutdown {
			data, err := os.ReadFile(p.logPath)
			if err != nil {
				s.t.Errorf("read process lifecycle evidence: %v", err)
				continue
			}
			started, stopped := map[string]bool{}, map[string]bool{}
			httpStopped := false
			for _, line := range bytes.Split(data, []byte("\n")) {
				var record struct {
					Message string `json:"msg"`
					Worker  string `json:"worker"`
				}
				if json.Unmarshal(line, &record) != nil {
					continue
				}
				switch record.Message {
				case "worker started":
					started[record.Worker] = true
				case "worker stopped":
					stopped[record.Worker] = true
				case "HTTP server stopped":
					httpStopped = true
				}
			}
			if !httpStopped {
				s.t.Errorf("process %d omitted HTTP shutdown evidence", p.cmd.Process.Pid)
			}
			for name := range started {
				if !stopped[name] {
					s.t.Errorf("process %d worker %s did not report stopping", p.cmd.Process.Pid, name)
				}
			}
			p.verifiedShutdown = true
		}
	}
	s.processes = nil
	if s.pool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var active int
		if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=$1", envOr("INTEGRATION_RUNTIME_DB_USER", "wager")).Scan(&active); err != nil {
			s.t.Errorf("check runtime connection cleanup: %v", err)
		} else if active != 0 {
			s.t.Errorf("%d runtime database connections remain after shutdown", active)
		}
	}
}

func (s *suite) token(client string) string {
	s.t.Helper()
	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()
	if token := s.tokens[client]; token != "" {
		return token
	}
	secret := "local-" + client + "-secret"
	if client == "internal-service" {
		secret = "local-internal-secret"
	}
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {secret}}
	resp, err := s.client.PostForm(s.issuer+"/protocol/openid-connect/token", form)
	if err != nil {
		s.t.Fatalf("real Keycloak token request: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK || body.AccessToken == "" {
		s.t.Fatalf("Keycloak token client=%s status=%d: %v", client, resp.StatusCode, err)
	}
	s.tokens[client] = body.AccessToken
	return body.AccessToken
}

func (s *suite) request(p *process, method, path, token, key string, payload any) (response, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return response{}, err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, p.base+path, body)
	if err != nil {
		return response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return response{Code: resp.StatusCode, Body: data}, err
}

func (s *suite) call(p *process, method, path, token, key string, payload any, want int, target any) response {
	s.t.Helper()
	r, err := s.request(p, method, path, token, key, payload)
	if err != nil {
		s.t.Fatal(err)
	}
	if r.Code != want {
		s.t.Fatalf("%s %s: HTTP %d, want %d: %s", method, path, r.Code, want, r.Body)
	}
	if target != nil {
		if err := json.Unmarshal(r.Body, target); err != nil {
			s.t.Fatalf("decode response: %v: %s", err, r.Body)
		}
	}
	return r
}

func (s *suite) open(amount string) wallet {
	s.t.Helper()
	var w wallet
	s.call(s.processes[0], "POST", "/wallets", s.token("internal-service"), "", map[string]any{"playerId": uniqueID(), "initialBalance": money{amount, "BRL"}}, http.StatusCreated, &w)
	return w
}

func operation(w wallet, kind, amount string) command {
	return command{ProviderID: "provider-a", ExternalTransactionID: uniqueID(), PlayerID: w.PlayerID, WalletID: w.ID, RoundID: "round-1", GameID: "jungle-test", Kind: kind, Money: money{amount, "BRL"}}
}

func (s *suite) submit(p *process, c command, key string, want int) result {
	s.t.Helper()
	var out result
	c.IdempotencyKey = ""
	s.call(p, "POST", "/wagering/transactions", s.token(c.ProviderID), key, c, want, &out)
	return out
}

func (s *suite) balance(w wallet, amount string, entries int) {
	s.t.Helper()
	var actual wallet
	s.call(s.processes[0], "GET", "/wallets/"+w.ID, s.token("internal-service"), "", nil, http.StatusOK, &actual)
	if actual.Balance != (money{amount, "BRL"}) {
		s.t.Fatalf("wallet balance %+v, want %s BRL", actual.Balance, amount)
	}
	var reconciliation struct {
		Consistent bool  `json:"consistent"`
		Difference money `json:"difference"`
		Checked    int   `json:"checkedEntries"`
	}
	s.call(s.processes[len(s.processes)-1], "POST", "/wallets/"+w.ID+"/reconciliation", s.token("internal-service"), "", nil, http.StatusOK, &reconciliation)
	if !reconciliation.Consistent || reconciliation.Difference.Amount != "0.00" || reconciliation.Checked != entries {
		s.t.Fatalf("reconciliation mismatch: %+v, expected %d entries", reconciliation, entries)
	}
}

func (s *suite) enqueue(c command, messageID string) {
	s.t.Helper()
	body, _ := json.Marshal(map[string]any{"messageId": messageID, "type": "WagerTransactionRequested", "occurredAt": s.occurredAt, "data": c})
	s.send(string(body), c.WalletID)
}

func (s *suite) send(body, group string) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := s.sqs.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(s.queueURL), MessageBody: aws.String(body), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(uniqueID())})
	if err != nil {
		s.t.Fatal(err)
	}
}

func (s *suite) awaitStatus(c command, status string) result {
	s.t.Helper()
	var out result
	eventually(s.t, 20*time.Second, "transaction becomes "+status, func() bool {
		r, err := s.request(s.processes[0], "GET", "/providers/"+c.ProviderID+"/wagering/transactions/"+c.ExternalTransactionID, s.token(c.ProviderID), "", nil)
		return err == nil && r.Code == 200 && json.Unmarshal(r.Body, &out) == nil && out.Status == status
	})
	return out
}

func uniqueID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}
	raw[6], raw[8] = (raw[6]&0x0f)|0x40, (raw[8]&0x3f)|0x80
	s := hex.EncodeToString(raw[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}

func eventually(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(75 * time.Millisecond)
	}
	t.Fatalf("timed out after %s: %s", timeout, description)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func (s *suite) emptyQueue() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := s.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(s.queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	if err != nil {
		return false
	}
	for _, key := range []string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"} {
		count, err := strconv.Atoi(out.Attributes[key])
		if err != nil || count != 0 {
			return false
		}
	}
	return true
}

func (s *suite) sqlCount(query string, args ...any) int {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var count int
	if err := s.pool.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		s.t.Fatalf("query integration evidence: %v", err)
	}
	return count
}

func (r response) String() string { return fmt.Sprintf("HTTP %d: %s", r.Code, r.Body) }
