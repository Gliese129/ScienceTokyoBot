from __future__ import annotations

import re
from urllib.parse import urlparse

URL_RE = re.compile(r"https?://[^\s)]+")


def extract_urls(text: str) -> list[str]:
    return URL_RE.findall(text or "")


def _is_allowed_host(host: str, allowed_domains: list[str]) -> bool:
    host = host.lower()
    for domain in allowed_domains:
        domain = domain.lower()
        if host == domain or host.endswith(f".{domain}"):
            return True
    return False


def has_allowed_source_url(text: str, allowed_domains: list[str]) -> bool:
    for url in extract_urls(text):
        host = urlparse(url).hostname or ""
        if host and _is_allowed_host(host, allowed_domains):
            return True
    return False


def format_structured_response(
    sources: list[str],
    answer_lines: list[str],
    inference_lines: list[str],
) -> str:
    source_block = "\n".join(f"- {line}" for line in sources) if sources else "- 无"
    answer_block = "\n".join(f"- {line}" for line in answer_lines) if answer_lines else "- 无"
    inference_block = "\n".join(f"- {line}" for line in inference_lines) if inference_lines else "- 无"
    return (
        "Sources:\n"
        f"{source_block}\n\n"
        "Answer:\n"
        f"{answer_block}\n\n"
        "Inference / Uncertainty:\n"
        f"{inference_block}"
    )


def enforce_answer_guard(output_text: str, allowed_domains: list[str]) -> str:
    if has_allowed_source_url(output_text, allowed_domains):
        return output_text
    return format_structured_response(
        sources=["未找到允许域名下的可验证链接。"],
        answer_lines=["当前结果缺少可验证来源，已停止输出结论。"],
        inference_lines=[
            "请提供更具体关键词、扩展允许域名，或直接提供可信链接后重试。"
        ],
    )
