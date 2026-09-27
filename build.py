#!/usr/bin/env python3
"""
Static site builder — processes Jekyll include/front-matter syntax from source
and writes flat static HTML to github_deploy/.

Usage:  python3 build.py
        python3 build.py --dry-run
"""
import re
import sys
from pathlib import Path

SOURCE = Path('/home/j/_DEV/portfolio-polish-wt')
OUTPUT = Path('/home/j/github_deploy')

SKIP_DIRS = {'_site', 'node_modules', '.git', '.impeccable', '.playwright-mcp'}
# Files handled separately or not HTML-processed
SKIP_NAMES = {'build.py'}

NAV_SRC  = (SOURCE / '_includes/site-nav.html').read_text(encoding='utf-8')
FOOT_SRC = (SOURCE / '_includes/site-footer.html').read_text(encoding='utf-8')

# Pattern: {% if include.active == "VALUE" %}CONTENT{% endif %}
IF_RE = re.compile(
    r'\{%-?\s*if include\.active\s*==\s*"([^"]+)"\s*-?%\}(.*?)\{%-?\s*endif\s*-?%\}',
    re.DOTALL,
)
# Pattern: {% include site-nav.html active="VALUE" %} or active omitted
NAV_RE = re.compile(
    r'\{%-?\s*include site-nav\.html(?:\s+active="([^"]*)")?\s*-?%\}',
)
FOOT_RE = re.compile(r'\{%-?\s*include site-footer\.html\s*-?%\}')
# Strip YAML front matter block at start of file
FM_RE = re.compile(r'\A---\s*\n.*?---\s*\n', re.DOTALL)


def resolve_nav(active: str) -> str:
    def repl(m):
        return m.group(2) if m.group(1) == active else ''
    return IF_RE.sub(repl, NAV_SRC)


def process(src: Path) -> str:
    content = src.read_text(encoding='utf-8')
    # Strip front matter
    content = FM_RE.sub('', content)
    # Inject nav (active param from template tag)
    content = NAV_RE.sub(lambda m: resolve_nav(m.group(1) or ''), content)
    # Inject footer
    content = FOOT_RE.sub(FOOT_SRC, content)
    return content


def needs_processing(src: Path) -> bool:
    if src.suffix != '.html':
        return False
    text = src.read_text(encoding='utf-8')
    return text.startswith('---') or '{% include' in text


dry_run = '--dry-run' in sys.argv
changed = 0

for src in sorted(SOURCE.rglob('*.html')):
    rel = src.relative_to(SOURCE)
    if any(part in SKIP_DIRS for part in rel.parts):
        continue
    if src.name in SKIP_NAMES:
        continue
    if not needs_processing(src):
        continue

    processed = process(src)
    dst = OUTPUT / rel

    if dst.exists() and dst.read_text(encoding='utf-8') == processed:
        continue

    if dry_run:
        print(f'  would write: {rel}')
    else:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(processed, encoding='utf-8')
        print(f'  wrote: {rel}')
    changed += 1

# Also copy assets/site.css if it differs
site_css_src = SOURCE / 'assets/site.css'
site_css_dst = OUTPUT / 'assets/site.css'
if site_css_src.exists():
    if not site_css_dst.exists() or site_css_src.read_text() != site_css_dst.read_text():
        if dry_run:
            print('  would write: assets/site.css')
        else:
            site_css_dst.parent.mkdir(parents=True, exist_ok=True)
            site_css_dst.write_text(site_css_src.read_text())
            print('  wrote: assets/site.css')
        changed += 1

label = 'would change' if dry_run else 'changed'
print(f'\nBuild complete — {changed} file(s) {label}.')
