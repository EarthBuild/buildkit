#!/usr/bin/env python3
"""Validate project skills (.agents/skills/) and cross-AI symlinks."""

import argparse
import json
import os
import re
import sys
from pathlib import Path


FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)
MD_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
INLINE_PATH_RE = re.compile(r"`([a-zA-Z0-9_./-]+\.(?:go|proto|md|py|sh|hcl|yml|yaml))`")

ALLOWED_FRONTMATTER_KEYS = {
    "argument-hint",
    "compatibility",
    "context",
    "description",
    "disable-model-invocation",
    "license",
    "metadata",
    "name",
    "user-invocable",
}

BUDGETS = {
    "agents_md_lines": 30,
    "agents_md_words": 200,
    "desc_words": 20,
    "desc_chars": 160,
    "skill_lines": 40,
    "skill_words": 220,
}


def parse_simple_yaml(yaml_text: str) -> dict:
    data = {}
    current_key = None
    for raw_line in yaml_text.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if not line.startswith(" ") and ":" in line:
            k, v = line.split(":", 1)
            k, v = k.strip(), v.strip()
            if v in (">-", ">", "|", "|-", ""):
                data[k] = ""
                current_key = k
            else:
                data[k] = v.strip("\"'")
                current_key = None
        elif current_key is not None and line.startswith(" "):
            data[current_key] = (data[current_key] + " " + line.strip()).strip()
    return data


def count_lines_words(text: str) -> tuple[int, int]:
    return len(text.splitlines()), len(text.split())


def check_no_redundant_links(md_path: Path, repo_root: Path, errors: list[str]) -> None:
    text = md_path.read_text(encoding="utf-8")
    rel_src = md_path.relative_to(repo_root)
    for match in MD_LINK_RE.finditer(text):
        label, target = match.group(1), match.group(2).strip()
        if label == "label" and target == "path":
            continue
        errors.append(
            f"{rel_src}: redundant markdown link '[{label}]({target})'; use plain inline code (`{label}`)"
        )


def check_inline_paths(md_path: Path, repo_root: Path, errors: list[str]) -> None:
    text = md_path.read_text(encoding="utf-8")
    rel_src = md_path.relative_to(repo_root)
    for match in INLINE_PATH_RE.finditer(text):
        p = match.group(1)
        if p.startswith(".") and not p.startswith(".agents/"):
            continue
        if "/" not in p and p != "AGENTS.md":
            continue
        if not (repo_root / p).exists() and not (md_path.parent / p).exists():
            errors.append(f"{rel_src}: referenced path '{p}' does not exist")


def validate_repo(repo_root: Path) -> dict:
    errors: list[str] = []
    warnings: list[str] = []
    checked_skills: list[str] = []

    agents_md = repo_root / "AGENTS.md"
    claude_md = repo_root / "CLAUDE.md"
    skills_dir = repo_root / ".agents" / "skills"
    claude_skills_link = repo_root / ".claude" / "skills"
    claude_agents_dir = repo_root / ".claude" / "agents"

    if not agents_md.is_file():
        errors.append("Missing AGENTS.md")
    else:
        text = agents_md.read_text(encoding="utf-8")
        lines, words = count_lines_words(text)
        if lines > BUDGETS["agents_md_lines"] or words > BUDGETS["agents_md_words"]:
            errors.append(
                f"AGENTS.md exceeds budget ({lines}L/{words}W > {BUDGETS['agents_md_lines']}L/{BUDGETS['agents_md_words']}W)"
            )
        check_no_redundant_links(agents_md, repo_root, errors)

    if claude_md.exists() or claude_md.is_symlink():
        errors.append("CLAUDE.md should not exist; use AGENTS.md directly")

    if not claude_skills_link.is_symlink() or os.readlink(claude_skills_link) != "../.agents/skills":
        errors.append(".claude/skills must symlink to ../.agents/skills")

    if claude_agents_dir.exists():
        errors.append(".claude/agents should not exist; use unified skills in .agents/skills/")

    if not skills_dir.is_dir():
        errors.append("Missing .agents/skills")
    else:
        for skill_folder in sorted(skills_dir.iterdir()):
            if not skill_folder.is_dir():
                continue
            skill_md = skill_folder / "SKILL.md"
            rel_skill = f".agents/skills/{skill_folder.name}/SKILL.md"
            if not skill_md.is_file():
                errors.append(f"{rel_skill}: missing")
                continue
            content = skill_md.read_text(encoding="utf-8")
            lines, words = count_lines_words(content)
            if lines > BUDGETS["skill_lines"] or words > BUDGETS["skill_words"]:
                errors.append(
                    f"{rel_skill}: exceeds budget ({lines}L/{words}W > {BUDGETS['skill_lines']}L/{BUDGETS['skill_words']}W)"
                )
            fm_match = FRONTMATTER_RE.match(content)
            if not fm_match:
                errors.append(f"{rel_skill}: missing YAML frontmatter")
            else:
                fm = parse_simple_yaml(fm_match.group(1))
                unsupported = sorted(set(fm.keys()) - ALLOWED_FRONTMATTER_KEYS)
                if unsupported:
                    errors.append(f"{rel_skill}: unsupported frontmatter keys {unsupported}")
                if fm.get("name") != skill_folder.name:
                    errors.append(f"{rel_skill}: frontmatter name mismatch")
                desc = fm.get("description", "")
                desc_w = len(desc.split())
                if not desc or desc_w > BUDGETS["desc_words"] or len(desc) > BUDGETS["desc_chars"]:
                    errors.append(
                        f"{rel_skill}: description budget violation ({desc_w}W/{len(desc)}C > {BUDGETS['desc_words']}W/{BUDGETS['desc_chars']}C)"
                    )
            for tok in ("ROE", "BLUF-SBAR"):
                if tok not in content:
                    errors.append(f"{rel_skill}: missing token '{tok}'")
            check_no_redundant_links(skill_md, repo_root, errors)
            refs_dir = skill_folder / "references"
            if refs_dir.is_dir():
                for ref_md in sorted(refs_dir.glob("*.md")):
                    check_no_redundant_links(ref_md, repo_root, errors)
                    check_inline_paths(ref_md, repo_root, errors)
            checked_skills.append(skill_folder.name)

    return {
        "valid": len(errors) == 0,
        "skills": checked_skills,
        "errors": errors,
        "warnings": warnings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate unified skills and symlinks.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[4]))
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    result = validate_repo(Path(args.repo_root).resolve())
    out_path = Path(args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    if not result["valid"]:
        print(f"FAILED ({len(result['errors'])} errors): {result['errors']}", file=sys.stderr)
        return 1
    print(f"OK: {len(result['skills'])} unified skills validated -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
