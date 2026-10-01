package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestClusterScript runs scripts/cluster.sh as a user would: start,
// write and read through the CLI, kill and restart a server, stop.
func TestClusterScript(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the script is POSIX sh")
	}
	dir := filepath.Join(t.TempDir(), "cluster")
	sh := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("sh", append([]string{"scripts/cluster.sh"}, args...)...)
		cmd.Dir = "../.."
		cmd.Env = append(os.Environ(), "FSYNC=fsync")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("cluster.sh %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	sh("start", dir)
	t.Cleanup(func() { sh("stop", dir) })
	env, err := os.ReadFile(filepath.Join(dir, "env"))
	if err != nil {
		t.Fatal(err)
	}
	addr := strings.TrimPrefix(strings.TrimSpace(string(env)), "export CONCLAVE_ADDR=")
	cli := func(args ...string) string {
		t.Helper()
		out, err := exec.Command(filepath.Join(dir, "conclave"), append(args[:1:1], append([]string{"-addr", addr}, args[1:]...)...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("conclave %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	cli("put", "greeting", "hello")
	if got := cli("get", "greeting"); got != "hello" {
		t.Fatalf("get: %q", got)
	}
	sh("kill", "1", dir)
	cli("put", "greeting", "still here")
	sh("restart", "1", dir)
	if got := cli("get", "greeting"); got != "still here" {
		t.Fatalf("get after restart: %q", got)
	}
}
