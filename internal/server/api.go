package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/useless-husband/conclave/internal/kv"
	"github.com/useless-husband/conclave/internal/node"
	"github.com/useless-husband/conclave/internal/raft"
)

// The HTTP API. Every error body is JSON with a machine-readable code:
//
//	not-leader       421  not executed; leader/leader_api name the leader if known
//	retry            503  not executed; try again shortly
//	unknown          503  the server lost leadership with the write in its log
//	timeout          504  no answer in time; the write may still take effect
//	session-expired  410  the session is gone; earlier attempts may have applied
//	stale            410  the session already applied a later write
//	bad-request      400
//
// Writes may carry Conclave-Session and Conclave-Seq headers; a retry with
// the same pair takes effect at most once.

const (
	maxKey   = 1 << 10
	maxValue = 1 << 20
)

// ErrorBody is the JSON body of every error response.
type ErrorBody struct {
	Code      string `json:"code"`
	Error     string `json:"error"`
	Leader    uint64 `json:"leader,omitempty"`
	LeaderAPI string `json:"leader_api,omitempty"`
}

// KVBody is the JSON body of a successful key-value response.
type KVBody struct {
	Found   bool   `json:"found"`
	Value   string `json:"value,omitempty"`
	Swapped *bool  `json:"swapped,omitempty"`
	Index   uint64 `json:"index"`
}

// CASRequest is the body of POST /v1/cas/{key}.
type CASRequest struct {
	Expect       string `json:"expect"`
	ExpectAbsent bool   `json:"expect_absent"`
	Value        string `json:"value"`
}

// MemberRequest is the body of POST /v1/members.
type MemberRequest struct {
	ID   uint64 `json:"id"`
	Raft string `json:"raft"`
	API  string `json:"api"`
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key...}", s.handleGet)
	mux.HandleFunc("PUT /v1/kv/{key...}", s.handlePut)
	mux.HandleFunc("DELETE /v1/kv/{key...}", s.handleDelete)
	mux.HandleFunc("POST /v1/cas/{key...}", s.handleCAS)
	mux.HandleFunc("POST /v1/sessions", s.handleRegister)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("POST /v1/members", s.handleAddMember)
	mux.HandleFunc("DELETE /v1/members/{id}", s.handleRemoveMember)
	mux.HandleFunc("POST /v1/transfer/{id}", s.handleTransfer)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) fail(w http.ResponseWriter, status int, code, msg string, leader raft.NodeID) {
	body := ErrorBody{Code: code, Error: msg}
	if leader != raft.None {
		body.Leader = uint64(leader)
		body.LeaderAPI = s.memberAPI(leader)
	}
	writeJSON(w, status, body)
}

// exec runs a request and writes any failure. It returns the response and
// whether the caller should write a success body.
func (s *Server) exec(w http.ResponseWriter, r *http.Request, req node.Request) (node.Response, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	resp, err := s.do(ctx, req)
	if err != nil {
		if errors.Is(err, errStopped) {
			s.fail(w, http.StatusServiceUnavailable, "unknown", err.Error(), raft.None)
		} else {
			s.fail(w, http.StatusGatewayTimeout, "timeout", "no answer in time; a write may still take effect", raft.None)
		}
		return resp, false
	}
	switch resp.Status {
	case node.StatusOK:
	case node.StatusNotLeader:
		s.fail(w, http.StatusMisdirectedRequest, "not-leader", "this server is not the leader", resp.Leader)
		return resp, false
	case node.StatusRetry:
		s.fail(w, http.StatusServiceUnavailable, "retry", "the leader cannot serve this yet", resp.Leader)
		return resp, false
	case node.StatusUnknown:
		s.fail(w, http.StatusServiceUnavailable, "unknown", "leadership was lost; the write may or may not take effect", resp.Leader)
		return resp, false
	default:
		s.fail(w, http.StatusBadRequest, "bad-request", "rejected", raft.None)
		return resp, false
	}
	switch resp.Result.Code {
	case kv.SessionExpired:
		s.fail(w, http.StatusGone, "session-expired", "the session no longer exists", raft.None)
		return resp, false
	case kv.Stale:
		s.fail(w, http.StatusGone, "stale", "the session has already applied a later write", raft.None)
		return resp, false
	case kv.BadCommand:
		s.fail(w, http.StatusBadRequest, "bad-request", "malformed command", raft.None)
		return resp, false
	}
	return resp, true
}

func keyOf(w http.ResponseWriter, r *http.Request, s *Server) (string, bool) {
	k := r.PathValue("key")
	if k == "" || len(k) > maxKey {
		s.fail(w, http.StatusBadRequest, "bad-request", "keys must be 1 to 1024 bytes", raft.None)
		return "", false
	}
	return k, true
}

// session reads the optional de-duplication headers.
func session(w http.ResponseWriter, r *http.Request, s *Server, c *kv.Command) bool {
	sh, qh := r.Header.Get("Conclave-Session"), r.Header.Get("Conclave-Seq")
	if sh == "" && qh == "" {
		return true
	}
	var err1, err2 error
	c.Session, err1 = strconv.ParseUint(sh, 10, 64)
	c.Seq, err2 = strconv.ParseUint(qh, 10, 64)
	if err1 != nil || err2 != nil || c.Session == 0 || c.Seq == 0 {
		s.fail(w, http.StatusBadRequest, "bad-request", "Conclave-Session and Conclave-Seq must both be positive integers", raft.None)
		return false
	}
	return true
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key, ok := keyOf(w, r, s)
	if !ok {
		return
	}
	resp, ok := s.exec(w, r, node.Request{Op: node.OpCommand, Cmd: kv.Command{Kind: kv.Get, Key: key}})
	if !ok {
		return
	}
	body := KVBody{Found: resp.Result.Found, Value: resp.Result.Value, Index: resp.Index}
	status := http.StatusOK
	if !body.Found {
		status = http.StatusNotFound
	}
	writeJSON(w, status, body)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key, ok := keyOf(w, r, s)
	if !ok {
		return
	}
	val, err := io.ReadAll(io.LimitReader(r.Body, maxValue+1))
	if err != nil || len(val) > maxValue {
		s.fail(w, http.StatusBadRequest, "bad-request", "values are limited to 1 MiB", raft.None)
		return
	}
	c := kv.Command{Kind: kv.Put, Key: key, Value: string(val)}
	if !session(w, r, s, &c) {
		return
	}
	resp, ok := s.exec(w, r, node.Request{Op: node.OpCommand, Cmd: c})
	if ok {
		writeJSON(w, http.StatusOK, map[string]uint64{"index": resp.Index})
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key, ok := keyOf(w, r, s)
	if !ok {
		return
	}
	c := kv.Command{Kind: kv.Delete, Key: key}
	if !session(w, r, s, &c) {
		return
	}
	resp, ok := s.exec(w, r, node.Request{Op: node.OpCommand, Cmd: c})
	if !ok {
		return
	}
	body := KVBody{Found: resp.Result.Found, Value: resp.Result.Value, Index: resp.Index}
	status := http.StatusOK
	if !body.Found {
		status = http.StatusNotFound
	}
	writeJSON(w, status, body)
}

func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request) {
	key, ok := keyOf(w, r, s)
	if !ok {
		return
	}
	var req CASRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 3*maxValue)).Decode(&req); err != nil || len(req.Value) > maxValue {
		s.fail(w, http.StatusBadRequest, "bad-request", "body must be {\"expect\": ..., \"expect_absent\": ..., \"value\": ...}", raft.None)
		return
	}
	c := kv.Command{Kind: kv.CAS, Key: key, Value: req.Value, Expect: req.Expect, ExpectAbsent: req.ExpectAbsent}
	if !session(w, r, s, &c) {
		return
	}
	resp, ok := s.exec(w, r, node.Request{Op: node.OpCommand, Cmd: c})
	if !ok {
		return
	}
	swapped := resp.Result.Code == kv.OK
	body := KVBody{Swapped: &swapped, Found: resp.Result.Found, Value: resp.Result.Value, Index: resp.Index}
	status := http.StatusOK
	if !swapped {
		status = http.StatusConflict
	}
	writeJSON(w, status, body)
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	resp, ok := s.exec(w, r, node.Request{Op: node.OpCommand, Cmd: kv.Command{Kind: kv.Register}})
	if ok {
		writeJSON(w, http.StatusOK, map[string]uint64{"session": resp.Result.Session, "index": resp.Index})
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Status())
}

func (s *Server) handleAddMember(w http.ResponseWriter, r *http.Request) {
	var req MemberRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil || req.ID == 0 || req.Raft == "" || req.API == "" {
		s.fail(w, http.StatusBadRequest, "bad-request", "body must be {\"id\": N, \"raft\": \"host:port\", \"api\": \"host:port\"}", raft.None)
		return
	}
	resp, ok := s.exec(w, r, node.Request{Op: node.OpAddMember, Member: raft.NodeID(req.ID), Meta: EncodeMeta(req.Raft, req.API)})
	if ok {
		writeJSON(w, http.StatusOK, map[string]uint64{"index": resp.Index})
	}
}

func (s *Server) handleRemoveMember(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		s.fail(w, http.StatusBadRequest, "bad-request", "member id must be a positive integer", raft.None)
		return
	}
	resp, ok := s.exec(w, r, node.Request{Op: node.OpRemoveMember, Member: raft.NodeID(id)})
	if ok {
		writeJSON(w, http.StatusOK, map[string]uint64{"index": resp.Index})
	}
}

func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		s.fail(w, http.StatusBadRequest, "bad-request", "target id must be a positive integer", raft.None)
		return
	}
	if _, ok := s.exec(w, r, node.Request{Op: node.OpTransfer, Member: raft.NodeID(id)}); ok {
		writeJSON(w, http.StatusOK, map[string]string{"status": "transfer started"})
	}
}
