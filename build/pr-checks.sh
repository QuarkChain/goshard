#!/usr/bin/env bash
# Copyright 2026-2027, QuarkChain.

# Run the repository-wide checks expected before opening a pull request.
# The script continues after a failed stage so a long server run reports all
# failures at once, then exits non-zero if any stage failed.

set -uo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly TEST_JOBS="${PR_CHECK_JOBS:-8}"
readonly BASE_REF="${PR_CHECK_BASE_REF:-origin/goshard/base}"

passed=()
failed=()
skipped=()
goimports_cmd=()

usage() {
	cat <<'EOF'
Usage: build/pr-checks.sh

Environment:
  PR_CHECK_JOBS=N       Number of parallel Go test processes (default: 8).
  PR_CHECK_BASE_REF=REF Base ref used to find changed files (default: origin/goshard/base).
  PR_CHECK_SKIP_386=1   Skip the Linux 386 short-test job.

Prerequisites:
  - Go 1.24 or 1.25 (CI tests both versions)
  - initialized git submodules
  - gcc-multilib on Linux for the 386 test job
EOF
}

elapsed() {
	local seconds="$1"
	printf '%dm%02ds' "$((seconds / 60))" "$((seconds % 60))"
}

run_check() {
	local name="$1"
	shift
	local started=$SECONDS

	printf '\n==> %s\n' "$name"
	"$@"
	local status=$?
	local duration
	duration="$(elapsed "$((SECONDS - started))")"
	if ((status == 0)); then
		passed+=("$name ($duration)")
		printf '<== PASS: %s (%s)\n' "$name" "$duration"
	else
		failed+=("$name: exit $status ($duration)")
		printf '<== FAIL: %s (exit %d, %s)\n' "$name" "$status" "$duration" >&2
	fi
}

check_prerequisites() {
	local command_name
	local missing=0
	for command_name in git go gofmt make; do
		if ! command -v "$command_name" >/dev/null 2>&1; then
			printf 'missing required command: %s\n' "$command_name" >&2
			missing=1
		fi
	done
	((missing == 0)) || return 1

	if [[ ! "$TEST_JOBS" =~ ^[1-9][0-9]*$ ]]; then
		printf 'PR_CHECK_JOBS must be a positive integer, got: %s\n' "$TEST_JOBS" >&2
		return 1
	fi
	if ! git rev-parse --verify --quiet "${BASE_REF}^{commit}" >/dev/null; then
		printf 'base ref not found: %s\n' "$BASE_REF" >&2
		printf 'fetch it or set PR_CHECK_BASE_REF to the PR base ref\n' >&2
		return 1
	fi

	if command -v goimports >/dev/null 2>&1; then
		goimports_cmd=(goimports)
	else
		skipped+=("standalone goimports check (covered by the lint stage)")
		printf 'goimports not found; the lint stage will enforce import formatting\n'
	fi

	local uninitialized
	uninitialized="$(git submodule status --recursive 2>/dev/null | awk '$1 ~ /^-/ { print }')" || return 1
	if [[ -n "$uninitialized" ]]; then
		printf 'uninitialized submodules:\n%s\n' "$uninitialized" >&2
		printf 'run: git submodule update --init --recursive\n' >&2
		return 1
	fi

	go version
	printf 'test parallelism: %s\n' "$TEST_JOBS"
	printf 'PR base ref: %s\n' "$BASE_REF"
}

check_formatting() {
	local changed_go_files=()
	local gofmt_output
	local goimports_output
	local status=0

	while IFS= read -r path; do
		[[ -n "$path" ]] && changed_go_files+=("$path")
	done < <(
		{
			git diff --name-only --diff-filter=ACMR "${BASE_REF}...HEAD" -- '*.go'
			git diff --name-only --diff-filter=ACMR HEAD -- '*.go'
			git ls-files --others --exclude-standard -- '*.go'
		} | sort -u
	)

	if ((${#changed_go_files[@]} != 0)); then
		gofmt_output="$(gofmt -s -l "${changed_go_files[@]}")" || return 1
		if ((${#goimports_cmd[@]} != 0)); then
			goimports_output="$("${goimports_cmd[@]}" -l "${changed_go_files[@]}")" || return 1
		else
			goimports_output=""
		fi
	else
		gofmt_output=""
		goimports_output=""
	fi
	if [[ -n "$gofmt_output" ]]; then
		printf 'gofmt required for:\n%s\n' "$gofmt_output" >&2
		status=1
	fi

	if [[ -n "$goimports_output" ]]; then
		printf 'goimports required for:\n%s\n' "$goimports_output" >&2
		status=1
	fi

	if ! git diff --check "${BASE_REF}...HEAD"; then
		status=1
	fi
	if ! git diff --check || ! git diff --cached --check; then
		status=1
	fi
	return "$status"
}

print_summary() {
	local item
	printf '\n===== PR check summary =====\n'
	for item in "${passed[@]}"; do
		printf 'PASS  %s\n' "$item"
	done
	for item in "${skipped[@]}"; do
		printf 'SKIP  %s\n' "$item"
	done
	for item in "${failed[@]}"; do
		printf 'FAIL  %s\n' "$item"
	done
	printf '%d passed, %d skipped, %d failed\n' \
		"${#passed[@]}" "${#skipped[@]}" "${#failed[@]}"
}

main() {
	if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
		usage
		return 0
	fi
	if (($# != 0)); then
		usage >&2
		return 2
	fi

	cd "$REPO_ROOT" || return 1
	run_check "prerequisites" check_prerequisites
	if ((${#failed[@]} != 0)); then
		print_summary
		return 1
	fi

	run_check "source formatting and whitespace" check_formatting
	run_check "lint" go run ./build/ci.go lint
	run_check "generated files and go.mod tidy" go run ./build/ci.go check_generate
	run_check "forbidden dependencies" go run ./build/ci.go check_baddeps
	run_check "all command builds" make all
	run_check "full tests" ./build/travis_keepalive.sh go run ./build/ci.go test -p "$TEST_JOBS"
	run_check "keeper target builds" go run ./build/ci.go keeper

	if [[ "$(go env GOOS)" != "linux" ]]; then
		skipped+=("386 short tests (Linux only)")
	elif [[ "${PR_CHECK_SKIP_386:-0}" == "1" ]]; then
		skipped+=("386 short tests (PR_CHECK_SKIP_386=1)")
	else
		run_check "386 short tests" ./build/travis_keepalive.sh \
			go run ./build/ci.go test -arch 386 -short -p "$TEST_JOBS"
	fi

	print_summary
	((${#failed[@]} == 0))
}

main "$@"
