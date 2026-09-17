#!/usr/bin/env bash
# Copyright 2026-2027, QuarkChain.

# Run the repository-wide checks expected before opening a pull request.
# The script continues after a failed stage so a long server run reports all
# failures at once, then exits non-zero if any stage failed.

set -uo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly TEST_JOBS="${PR_CHECK_JOBS:-1}"
readonly TEST_PARALLEL="${PR_CHECK_TEST_PARALLEL:-1}"
readonly GO_MEMORY_LIMIT="${PR_CHECK_GOMEMLIMIT:-8GiB}"
readonly BASE_REF="${PR_CHECK_BASE_REF:-origin/goshard/base}"
readonly REPORT_DIR="${PR_CHECK_REPORT_DIR:-build/cache}"
readonly LOG_FILE="${REPORT_DIR}/pr-checks.log"
readonly SUMMARY_FILE="${REPORT_DIR}/pr-checks-summary.txt"

passed=()
failed=()
skipped=()
goimports_cmd=()
current_check=""
report_ready=0
run_complete=0
final_reported=0

usage() {
	cat <<'EOF'
Usage: build/pr-checks.sh

Environment:
  PR_CHECK_JOBS=N       Number of parallel Go package tests (default: 1).
  PR_CHECK_TEST_PARALLEL=N
                        Parallel tests within each package (default: 1).
  PR_CHECK_GOMEMLIMIT=N Go runtime memory target for tests (default: 8GiB).
  PR_CHECK_BASE_REF=REF Base ref used to find changed files (default: origin/goshard/base).
  PR_CHECK_REPORT_DIR=D Directory for the full log and summary (default: build/cache).
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

	current_check="$name"
	printf '\n==> %s\n' "$name" >>"$LOG_FILE"
	save_summary
	"$@" >>"$LOG_FILE" 2>&1
	local status=$?
	local duration
	duration="$(elapsed "$((SECONDS - started))")"
	if ((status == 0)); then
		passed+=("$name ($duration)")
		printf '<== PASS: %s (%s)\n' "$name" "$duration" >>"$LOG_FILE"
	else
		failed+=("$name: exit $status ($duration)")
		printf '<== FAIL: %s (exit %d, %s)\n' "$name" "$status" "$duration" >>"$LOG_FILE"
	fi
	current_check=""
	save_summary
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
	if [[ ! "$TEST_PARALLEL" =~ ^[1-9][0-9]*$ ]]; then
		printf 'PR_CHECK_TEST_PARALLEL must be a positive integer, got: %s\n' "$TEST_PARALLEL" >&2
		return 1
	fi
	if [[ -z "$GO_MEMORY_LIMIT" ]]; then
		printf 'PR_CHECK_GOMEMLIMIT must not be empty\n' >&2
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
	printf 'package test parallelism: %s\n' "$TEST_JOBS"
	printf 'in-package test parallelism: %s\n' "$TEST_PARALLEL"
	printf 'Go memory target: %s\n' "$GO_MEMORY_LIMIT"
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

save_summary() {
	local item
	{
		printf 'commit: %s\n' "$(git rev-parse HEAD 2>/dev/null || printf unavailable)"
		go version 2>/dev/null || printf 'go version: unavailable\n'
		printf 'base: %s\n' "$BASE_REF"
		printf 'package jobs: %s\n' "$TEST_JOBS"
		printf 'in-package parallelism: %s\n' "$TEST_PARALLEL"
		printf 'Go memory target: %s\n' "$GO_MEMORY_LIMIT"
		if [[ -n "$current_check" ]]; then
			printf 'status: running %s\n' "$current_check"
		elif ((run_complete == 1)); then
			printf 'status: complete\n'
		else
			printf 'status: incomplete\n'
		fi
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

		if [[ -n "$current_check" ]]; then
			printf 'RUN   %s\n' "$current_check"
		fi
		if ((${#failed[@]} != 0)); then
			printf '\n===== failure signatures (max 100) =====\n'
			grep -En -m 100 \
				'(<== FAIL:|^[[:space:]]*--- FAIL:|^FAIL([[:space:]]|$)|panic:|fatal:|undefined:|signal: killed|out of memory|missing required command:|must be a positive integer|must not be empty|base ref not found:|uninitialized submodules:|build failed|File changed:|generated files were updated|untidy module|Bad dependencies detected|:[0-9]+:[0-9]+:)' \
				"$LOG_FILE" || true
		fi

		printf '\nfull log: %s\n' "$LOG_FILE"
		printf 'summary: %s\n' "$SUMMARY_FILE"
	} >"$SUMMARY_FILE"
}

finish_report() {
	final_reported=1
	save_summary
	cat "$SUMMARY_FILE"
}

handle_signal() {
	local signal="$1"
	local status="$2"
	trap - HUP INT TERM
	if [[ -n "$current_check" ]]; then
		failed+=("$current_check: interrupted by $signal")
		printf '<== FAIL: %s (interrupted by %s)\n' "$current_check" "$signal" >>"$LOG_FILE"
	else
		failed+=("script: interrupted by $signal")
	fi
	current_check=""
	finish_report
	exit "$status"
}

handle_exit() {
	local status="$1"
	trap - EXIT HUP INT TERM
	if ((report_ready == 1 && final_reported == 0)); then
		if [[ -n "$current_check" ]]; then
			failed+=("$current_check: script exited before completion")
			current_check=""
		elif ((status != 0)); then
			failed+=("script: exit $status before completion")
		fi
		finish_report
	fi
	exit "$status"
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
	if ! mkdir -p "$REPORT_DIR"; then
		printf 'cannot create report directory: %s\n' "$REPORT_DIR" >&2
		return 1
	fi
	if ! : >"$LOG_FILE"; then
		printf 'cannot write full log: %s\n' "$LOG_FILE" >&2
		return 1
	fi
	report_ready=1
	trap 'handle_signal HUP 129' HUP
	trap 'handle_signal INT 130' INT
	trap 'handle_signal TERM 143' TERM
	trap 'handle_exit $?' EXIT
	save_summary
	run_check "prerequisites" check_prerequisites
	if ((${#failed[@]} != 0)); then
		run_complete=1
		finish_report
		return 1
	fi

	run_check "source formatting and whitespace" check_formatting
	run_check "lint" go run ./build/ci.go lint
	run_check "generated files and go.mod tidy" go run ./build/ci.go check_generate
	run_check "forbidden dependencies" go run ./build/ci.go check_baddeps
	run_check "all command builds" make all
	run_check "full tests" env GOMAXPROCS="$TEST_PARALLEL" GOMEMLIMIT="$GO_MEMORY_LIMIT" \
		./build/travis_keepalive.sh go run ./build/ci.go test -p "$TEST_JOBS"
	run_check "keeper target builds" go run ./build/ci.go keeper

	if [[ "$(go env GOOS)" != "linux" ]]; then
		skipped+=("386 short tests (Linux only)")
	elif [[ "${PR_CHECK_SKIP_386:-0}" == "1" ]]; then
		skipped+=("386 short tests (PR_CHECK_SKIP_386=1)")
	else
		run_check "386 short tests" env GOMAXPROCS="$TEST_PARALLEL" GOMEMLIMIT="$GO_MEMORY_LIMIT" \
			./build/travis_keepalive.sh \
			go run ./build/ci.go test -arch 386 -short -p "$TEST_JOBS"
	fi

	run_complete=1
	finish_report
	((${#failed[@]} == 0))
}

main "$@"
