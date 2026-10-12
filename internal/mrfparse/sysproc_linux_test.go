//go:build linux

package mrfparse

import (
	"strings"
	"syscall"
	"testing"

	"github.com/EnotPoloskun/mrfparser"
)

func TestLinuxParserCommandProcessGroup(t *testing.T) {
	t.Parallel()
	cmd := parserCommand("/usr/local/bin/mrfparser", mrfparser.Config{
		Input: "/in", Output: "/out", Services: "/svc", TempDir: "/tmp",
	})
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatalf("Setpgid %+v", cmd.SysProcAttr)
	}
	if cmd.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Fatalf("Pdeathsig %v", cmd.SysProcAttr.Pdeathsig)
	}
	joined := strings.Join(cmd.Args, " ")
	if strings.Contains(joined, "sh -c") {
		t.Fatalf("shell args %v", cmd.Args)
	}
}
