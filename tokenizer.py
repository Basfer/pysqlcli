"""
SQL Tokenizer supporting Oracle and PostgreSQL dialects.

Features:
- Tokenize SQL into tokens with types and positions.
- Correct handling of string literals:
  - Oracle: single-quoted strings with '' escapes and Q-quoting: q'[...]', q'(...)', q'{...}', q'<...>' and also q'x...x'
  - PostgreSQL: single-quoted strings and dollar-quoted strings: $$...$$ or $tag$...$tag$
- Multi-statement input: split statements on semicolons that are not inside strings or comments.
- Comments: -- single-line, /* ... */ block comments (supports nesting where applicable).
- Double-quoted identifiers supporting escaped double quotes by doubling: "some""id"
- Returns tokens as dataclass Token(type, value, start, end)

Limitations / Notes:
- This is a lexer/tokenizer, not a full parser. It tries to be conservative about dialect differences but intentionally focuses on string/comment edge cases and statement splitting.
- Recognizes common operators and punctuation as single-character tokens or multi-char when obvious (<=, >=, !=, <>, ||, :=)

Functions:
- tokenize_sql(sql: str, dialect: str = 'oracle') -> List[Token]
- split_statements(sql: str, dialect: str = 'oracle') -> List[tuple[str, int, int]]  # (text, start, end)

Example usage (see bottom of file for tests):
    tokens = tokenize_sql("select q'[abc]' from dual; select 'x' from t;", dialect='oracle')
    for t in tokens:
        print(t)

"""
from dataclasses import dataclass
from typing import List, Tuple
import re

@dataclass
class Token:
    type: str
    value: str
    start: int
    end: int

# Helpers
OP_MULTI = {"<>", "!=", ">=", "<=", "||", ":=", "->", "->>"}
SINGLE_OPS = set('(),.+-*/%<>=|&^~!?:[]{};')
WHITESPACE = ' \t\r\n'


def _is_ident_start(ch: str) -> bool:
    return ch.isalpha() or ch == '_'


def _is_ident_part(ch: str) -> bool:
    return ch.isalnum() or ch == '_' or ch == '$'


def tokenize_sql(sql: str, dialect: str = 'oracle') -> List[Token]:
    """Tokenize SQL string.

    dialect: 'oracle' or 'postgres'
    """
    n = len(sql)
    i = 0
    tokens: List[Token] = []

    while i < n:
        ch = sql[i]

        # Whitespace
        if ch in WHITESPACE:
            j = i + 1
            while j < n and sql[j] in WHITESPACE:
                j += 1
            tokens.append(Token('WHITESPACE', sql[i:j], i, j))
            i = j
            continue

        # Single-line comment --
        if ch == '-' and i + 1 < n and sql[i+1] == '-':
            j = i + 2
            while j < n and sql[j] != '\n':
                j += 1
            tokens.append(Token('COMMENT_LINE', sql[i:j], i, j))
            i = j
            continue

        # Block comment /* ... */ (support nesting)
        if ch == '/' and i + 1 < n and sql[i+1] == '*':
            start = i
            i += 2
            depth = 1
            while i < n and depth > 0:
                if sql[i] == '/' and i + 1 < n and sql[i+1] == '*':
                    depth += 1
                    i += 2
                    continue
                if sql[i] == '*' and i + 1 < n and sql[i+1] == '/':
                    depth -= 1
                    i += 2
                    continue
                i += 1
            tokens.append(Token('COMMENT_BLOCK', sql[start:i], start, i))
            continue

        # Dollar-quoted string for Postgres: $tag$...$tag$
        if dialect.lower() == 'postgres' and ch == '$':
            # try to match $tag$
            m = re.match(r"\$([A-Za-z0-9_]*)\$", sql[i:])
            if m:
                tag = m.group(1)
                open_len = len(m.group(0))
                start = i
                i += open_len
                pattern = f"${tag}$"
                idx = sql.find(pattern, i)
                if idx >= 0:
                    # include closing tag
                    i = idx + len(pattern)
                    tokens.append(Token('STRING_DOLLAR', sql[start:i], start, i))
                    continue
                else:
                    # unterminated till EOF
                    tokens.append(Token('STRING_DOLLAR_UNTERMINATED', sql[start:], start, n))
                    i = n
                    continue

        # Double-quoted identifier "..."
        if ch == '"':
            start = i
            i += 1
            while i < n:
                if sql[i] == '"':
                    if i + 1 < n and sql[i+1] == '"':
                        # escaped double quote -> skip both
                        i += 2
                        continue
                    else:
                        i += 1
                        break
                else:
                    i += 1
            # If loop exited because of EOF, token will be unterminated
            tok_type = 'IDENT_QUOTED' if i <= n else 'IDENT_QUOTED_UNTERMINATED'
            tokens.append(Token(tok_type, sql[start:i], start, i))
            continue

        # Single-quoted string '...'
        if ch == "'":
            start = i
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i+1] == "'":
                        # escaped ''
                        i += 2
                        continue
                    else:
                        i += 1
                        break
                else:
                    i += 1
            tok_type = 'STRING_SINGLE' if i <= n else 'STRING_SINGLE_UNTERMINATED'
            tokens.append(Token(tok_type, sql[start:i], start, i))
            continue

        # Oracle Q-quote: q'X...X' where X is a delimiter
        if dialect.lower() == 'oracle' and (ch.lower() == 'q' and i + 1 < n and sql[i+1] == "'"):
            start = i
            i += 2
            if i >= n:
                tokens.append(Token('QQUOTE_UNTERMINATED', sql[start:], start, n))
                break
            opener = sql[i]
            pairs = {'[': ']', '(' : ')', '{': '}', '<': '>'}
            closer = pairs.get(opener, opener)
            i += 1
            # scan until closer followed by a single quote
            while i < n:
                if sql[i] == closer:
                    if i + 1 < n and sql[i+1] == "'":
                        i += 2
                        break
                i += 1
            tok_type = 'QQUOTE' if i <= n else 'QQUOTE_UNTERMINATED'
            tokens.append(Token(tok_type, sql[start:i], start, i))
            continue

        # Numbers (simple)
        if ch.isdigit() or (ch == '.' and i + 1 < n and sql[i+1].isdigit()):
            start = i
            has_dot = ch == '.'
            i += 1
            while i < n and (sql[i].isdigit() or (sql[i] == '.' and not has_dot)):
                if sql[i] == '.':
                    has_dot = True
                i += 1
            # exponent
            if i < n and sql[i] in 'eE':
                j = i + 1
                if j < n and sql[j] in '+-':
                    j += 1
                while j < n and sql[j].isdigit():
                    j += 1
                tokens.append(Token('NUMBER', sql[start:j], start, j))
                i = j
            else:
                tokens.append(Token('NUMBER', sql[start:i], start, i))
            continue

        # Identifiers/keywords
        if _is_ident_start(ch):
            start = i
            i += 1
            while i < n and _is_ident_part(sql[i]):
                i += 1
            tokens.append(Token('IDENT', sql[start:i], start, i))
            continue

        # Multi-char operators
        if i + 1 < n and sql[i:i+2] in OP_MULTI:
            tokens.append(Token('OP', sql[i:i+2], i, i+2))
            i += 2
            continue

        # Single-char operators / punctuation
        if ch == ';':
            tokens.append(Token('SEMICOLON', ch, i, i+1))
            i += 1
            continue
        if ch == ',':
            tokens.append(Token('COMMA', ch, i, i+1))
            i += 1
            continue
        if ch in SINGLE_OPS:
            tokens.append(Token('OP', ch, i, i+1))
            i += 1
            continue

        # Anything else - single char token
        tokens.append(Token('CHAR', ch, i, i+1))
        i += 1

    return tokens


def split_statements(sql: str, dialect: str = 'oracle') -> List[Tuple[str, int, int, bool]]:
    """Split SQL text into statements by semicolons that are not inside strings/comments.

    Returns list of (stmt_text, start_index, end_index) where end_index is the index after the terminating semicolon
    or the end of the final statement.
    """
    tokens = tokenize_sql(sql, dialect=dialect)
    stmts: List[Tuple[str, int, int]] = []

    cur_start = 0
    i = 0
    n = len(tokens)
    while i < n:
        t = tokens[i]
        if t.type == 'SEMICOLON' or (t.type == 'OP' and t.value == ';'):
            # statement is from cur_start to t.end
            stmt_text = sql[cur_start:t.end]
            stmts.append((stmt_text, cur_start, t.end, True))
            cur_start = t.end
        i += 1
    # remaining tail
    if cur_start < len(sql):
        tail = sql[cur_start:]
        if tail.strip() != '':
            stmts.append((tail, cur_start, len(sql), False))
    return stmts


# ----------------- Extended test harness -----------------
if __name__ == '__main__':
    examples = [
        ("/* open comment \n select 1;", 'oracle'),
        ("/* open comment \n close comment */           select          1;         \n\n\n     / \n\n +-", 'oracle'),
    ]

    for sql_text, dialect in examples:
        print('\n--- Example (', dialect, ') ---')
        print(sql_text)
        toks = tokenize_sql(sql_text, dialect=dialect)
        for t in toks:
            print(f"{t.start:4}-{t.end:4} {t.type:20} {repr(t.value)}")
        print('\nStatements:')
        for stmt, s, e in split_statements(sql_text, dialect=dialect):
            print(f"{s}-{e}: {repr(stmt)}")

    print('\nAll tests executed.')
