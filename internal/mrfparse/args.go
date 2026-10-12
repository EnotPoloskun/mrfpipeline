package mrfparse

import "github.com/EnotPoloskun/mrfparser"

func parserCLIArgs(cfg mrfparser.Config) []string {
	return []string{
		"parse",
		"--input", cfg.Input,
		"--output", cfg.Output,
		"--services", cfg.Services,
		"--temp-dir", cfg.TempDir,
	}
}
