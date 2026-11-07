package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"messaging_service/internal/models"
)

// entrypointRelativePath is the documented process entrypoint
// (docs/engineering_guide.md, "Coding Conventions & Project Structure": the
// Makefile `run`/`build` targets and the Dockerfile all point at ./cmd/server).
const entrypointRelativePath = "cmd/server/main.go"

// TestEntrypointIsARunnableMainPackage guards the process entrypoint itself: it
// must stay next to this test, declare `package main` and keep a `main`
// function, otherwise the service has no runnable entrypoint at all.
func TestEntrypointIsARunnableMainPackage(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("process entrypoint %s is missing: %v", entrypointRelativePath, err)
	}
	if len(source) == 0 {
		t.Fatalf("process entrypoint %s is empty", entrypointRelativePath)
	}

	body := string(source)
	if !regexp.MustCompile(`(?m)^package main$`).MatchString(body) {
		t.Errorf("%s must declare `package main` so `go build ./cmd/server` produces a binary",
			entrypointRelativePath)
	}
	if !regexp.MustCompile(`(?m)^func main\(\)`).MatchString(body) {
		t.Errorf("%s must define func main(), the entrypoint of the built binary",
			entrypointRelativePath)
	}
}

// TestEntrypointIsNotDiscardedByGitIgnore guards the delivery hazard behind
// these tests: an ignore rule that swallows the entrypoint makes a local
// deletion look like a clean tree. `git check-ignore --no-index` is asked
// directly so the assertion uses git's own rule matching instead of a
// re-implementation, and the tracked-entrypoint exemption (`--index`) is
// bypassed on purpose - the rules must be safe on their own.
func TestEntrypointIsNotDiscardedByGitIgnore(t *testing.T) {
	t.Parallel()

	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is not installed; cannot evaluate ignore rules")
	}
	rootDir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolving repository root: %v", err)
	}
	if !isGitWorkTree(gitPath, rootDir) {
		t.Skip("not a git work tree; cannot evaluate ignore rules")
	}
	entrypoint := filepath.Join(rootDir, filepath.FromSlash(entrypointRelativePath))

	err = gitCheckIgnore(gitPath, rootDir, "--quiet", entrypoint)
	if err == nil {
		t.Fatalf(".gitignore discards the process entrypoint %s; the matching rule is:\n%s",
			entrypointRelativePath, gitIgnoreRule(gitPath, rootDir, entrypoint))
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		// git reserves exit code 1 for "not ignored"; anything else is a real
		// failure and must not be mistaken for a pass.
		t.Fatalf("git check-ignore failed for %s: %v", entrypointRelativePath, err)
	}
}

// TestConsumedTopicsMatchTheProjectionsWireEveryProjectionTopic checks the
// consumer wiring assembled by this entrypoint: dropping a topic here would
// silently stop a projection (membership, conversations, automod) from being
// maintained, and no service or handler test can observe that.
func TestConsumedTopicsMatchTheProjectionsWireEveryProjectionTopic(t *testing.T) {
	t.Parallel()

	want := []string{
		models.TopicCommunityEvents,
		models.TopicSocialEvents,
		models.TopicModerationEvents,
	}
	got := consumedTopics()

	if len(got) != len(want) {
		t.Fatalf("consumedTopics() = %v, want exactly %v", got, want)
	}
	seen := make(map[string]bool, len(got))
	for _, topic := range got {
		if strings.TrimSpace(topic) == "" {
			t.Fatalf("consumedTopics() contains an empty topic: %v", got)
		}
		if seen[topic] {
			t.Fatalf("consumedTopics() subscribes to %q twice: %v", topic, got)
		}
		seen[topic] = true
	}
	for _, topic := range want {
		if !seen[topic] {
			t.Errorf("consumedTopics() is missing a projection topic %q: got %v", topic, got)
		}
	}
}

// isGitWorkTree reports whether dir is inside a git work tree, so the ignore
// guard skips instead of failing when the suite runs from an exported archive.
func isGitWorkTree(gitPath, dir string) bool {
	output, err := exec.Command(gitPath, "-C", dir, "rev-parse", "--is-inside-work-tree").Output()
	return err == nil && strings.TrimSpace(string(output)) == "true"
}

// gitCheckIgnore runs git check-ignore with the given arguments.
func gitCheckIgnore(gitPath, dir string, args ...string) error {
	cmd := exec.Command(gitPath, append([]string{"-C", dir, "check-ignore"}, args...)...)
	return cmd.Run()
}

// gitIgnoreRule returns the ignore rule that matches path, for diagnostics.
func gitIgnoreRule(gitPath, dir, path string) string {
	output, err := exec.Command(gitPath, "-C", dir, "check-ignore", "--no-index", "--verbose", path).Output()
	if err != nil && len(output) == 0 {
		return "(no rule reported)"
	}
	return strings.TrimSpace(string(output))
}
