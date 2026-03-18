Performs thorough code reviews across KDB/q, Python, and Bash. Use this skill whenever the user shares code and asks for a review, feedback, suggestions, improvements, or a critique — even if they just paste code and say "thoughts?" or "what do you think?". Also triggers for: "check this", "look at this code", "how can I improve", "is this good", "unit tests for this", "write tests for this", "coverage for this", or any request to add/improve/generate tests. Covers correctness, efficiency, idiomatic style, security, and testability. Always suggests concrete improvements with justification.Code Review Skill
Perform structured, actionable code reviews for KDB/q, Python, and Bash. Reviews cover
correctness, efficiency, idiomatic style, security, and testability. Always output
suggested improvements with reasoning — never just flag issues.

Workflow

Detect language(s) — q/kdb+, Python, Bash, or mixed
Load the relevant reference section below (or all if mixed)
Review across all dimensions (see Dimensions section)
Generate unit tests unless the user opts out
Output structured review using the Output Format


Dimensions
Evaluate every submission across these five dimensions. Every finding must include:

What: the specific issue or smell
Why: impact on correctness / performance / security / maintainability
Fix: a concrete, idiomatic replacement snippet

1. Correctness
Are there bugs, edge cases not handled, type mismatches, or off-by-one errors?
2. Efficiency
Are there unnecessary copies, O(n²) patterns where O(n) exists, missed vectorisation,
or repeated computations? For KDB: missed attributes, row-by-row iteration, unguarded
partitioned queries.
3. Idiomatic Style
Does the code follow the conventions of the language?

q: right-to-left evaluation, vector-first, protected eval, .lg logging (TorQ)
Python: PEP 8, list/dict comprehensions, context managers, f-strings, type hints
Bash: [[ ]] over [ ], "$var" quoting, set -euo pipefail, $(...) over backticks

4. Security

q: injection via value/parse on user strings; unguarded hopen; unsanitised IPC
Python: subprocess shell=True, eval/exec, hardcoded secrets, insecure deserialization
Bash: unquoted variables, command injection, world-writable temp files, curl | sh

5. Testability
Is logic entangled with I/O or side-effects? Can units be tested in isolation?

Language-Specific Rules
KDB/q — load /mnt/skills/user/qkdb--developer/SKILL.md for full reference
Key review points (beyond the developer skill):

Right-to-left: flag any comment or logic that assumes left-to-right precedence
Vector over loop: any do/while iterating over a list that a map/each/adverb can replace
Attributes: is p# missing on sym in partitioned tables? Is s# missing on sorted time?
aj requirements: is the right-hand table sorted before the join?
Protected eval: every I/O path (hopen, read0, read1, file set) should use @[;;] or .[;;]
Null handling: results of joins or aggregations may introduce nulls — are they checked?
qSQL where order: date filter must come first in partitioned queries
Functional qSQL: required when column names are dynamic — flag hard-coded string eval

q Unit Test pattern (use .Q.assert or a lightweight harness):
q/ Lightweight test harness — include at top of test file
.test.pass: 0; .test.fail: 0
.test.assert:{[desc;got;exp]
    $[got~exp;
        [.test.pass+:1; -1 "PASS: ",desc];
        [.test.fail+:1; -1 "FAIL: ",desc," | got: ",(.Q.s got)," exp: ",(.Q.s exp)]
    ]}
.test.summary:{-1 "Passed: ",string[.test.pass]," Failed: ",string[.test.fail]}

/ Example usage
.test.assert["add atoms"; 3+4; 7]
.test.assert["sym type"; type `AAPL; -11h]
.test.assert["empty table count"; count ([] sym:`symbol$()); 0]
.test.summary[]
Cover: happy path, empty inputs, null inputs, single-row edge cases, type correctness.

Python
Key review points:

Type hints: all public functions should have parameter and return annotations
Comprehensions: prefer [x for x in ...] over map(lambda ...); use generator expressions for large data
Context managers: any open(), database connection, or lock must use with
f-strings: prefer over .format() and % formatting (Python 3.6+)
Exception specificity: never bare except:; catch the narrowest exception
Mutable defaults: def f(x=[]) is a bug — use None and assign inside
__slots__: suggest for data-heavy classes to reduce memory
pathlib: prefer over os.path string manipulation

Python Unit Test pattern (pytest, targeting ~80 % branch coverage):
pythonimport pytest

# Group tests in classes by function/class under test
class TestFunctionName:
    def test_happy_path(self):
        assert function_name(valid_input) == expected

    def test_empty_input(self):
        assert function_name([]) == []          # or raises, as appropriate

    def test_none_input(self):
        with pytest.raises(TypeError):
            function_name(None)

    def test_edge_single_element(self):
        assert function_name([42]) == [42]

    @pytest.mark.parametrize("inp,exp", [
        (0, 0), (-1, 1), (100, 100),
    ])
    def test_parametrize(self, inp, exp):
        assert function_name(inp) == exp

# Fixtures for shared setup
@pytest.fixture
def sample_data():
    return {"key": "value"}
Aim for: happy path, boundary values, empty/None inputs, exception paths, parametrised
variations. Target ~80% branch coverage (industry standard). Avoid testing implementation
details — test observable behaviour.

Bash
Key review points:

Strict mode: every script should start with set -euo pipefail
Quoting: all variable expansions must be "$var" — unquoted is a word-splitting bug
[[ ]] over [ ]: double brackets handle spaces and patterns correctly
$(...) over backticks: modern, nestable, readable
local: all function variables should be local to avoid namespace pollution
Temp files: use mktemp not hardcoded /tmp/file; trap cleanup with trap 'rm -f "$tmp"' EXIT
readonly: constants should be readonly VAR=value
Error messages to stderr: echo "Error" >&2
Exit codes: functions should return 0/1; scripts should exit 0/1 explicitly
Avoid eval: almost always replaceable with arrays or parameter expansion

Bash Unit Test pattern (bats-core):
bash#!/usr/bin/env bats
# Requires: bats-core (https://github.com/bats-core/bats-core)

setup() {
    source "${BATS_TEST_DIRNAME}/../script_under_test.sh"
}

@test "function returns 0 on valid input" {
    run my_function "valid"
    [ "$status" -eq 0 ]
}

@test "function outputs expected string" {
    run my_function "hello"
    [ "$output" = "hello world" ]
}

@test "function fails on empty input" {
    run my_function ""
    [ "$status" -ne 0 ]
}

@test "function handles spaces in arguments" {
    run my_function "hello world"
    [ "$status" -eq 0 ]
}
If bats is unavailable, write plain shell tests with assert helpers and trap ERR.

Output Format
Structure every review exactly as follows:
## Code Review — [Language(s)] — [brief title]

### Summary
[2-3 sentence overall assessment: what's good, what's the most important issue]

### Issues

#### [SEVERITY] [Dimension] — [short title]
**Problem:** [what and why]
**Fix:**
\`\`\`[lang]
[corrected code]
\`\`\`

[repeat per issue]

### Improvement Suggestions
[Ranked list of improvements not rising to "issue" level, each with justification]

### Unit Tests
\`\`\`[lang]
[complete, runnable test file]
\`\`\`

### Coverage Notes
[What the tests cover, what's deliberately excluded and why]
Severity levels: CRITICAL (bug/security), HIGH (performance/correctness risk),
MEDIUM (style/maintainability), LOW (minor nit).

Improvement Suggestion Guidelines
Every suggestion must answer: why choose this over the current approach?
Good suggestion template:

Use pathlib.Path instead of os.path.join
pathlib provides an object-oriented interface, avoids OS separator bugs, and is
the idiomatic Python 3 approach. os.path remains valid but pathlib is cleaner
for new code and composes better with type hints.

Avoid vague suggestions like "make it more readable" without a concrete example.

Test Coverage Targets (Industry Standard)
LanguageMinimumTargetNotesPython70%80%Branch coverage via pytest-covKDB/q70%80%Manual assertion harnessBash60%75%bats-core; lower due to env complexity
Coverage means branch coverage, not just line coverage. Every if/$[...]/case
branch should have at least one test.
Always test:

Happy path (typical valid input)
Boundary values (0, empty, max)
Null / None / missing inputs
Error / exception paths
Type edge cases (single element vs list, atom vs vector in q)