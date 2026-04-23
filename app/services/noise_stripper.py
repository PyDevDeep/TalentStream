import re


def strip_noise(raw_text: str) -> str:
    """Remove navigation, footers, and similar-jobs blocks from raw page text. Truncates to 5000 chars."""
    if not raw_text:
        return ""

    cleaned_text = raw_text

    # Remove trailing blocks that typically list unrelated jobs.
    noise_blocks_patterns = [
        r"(?i)(similar jobs|related jobs|people also viewed|explore more jobs).*$",
    ]
    for pattern in noise_blocks_patterns:
        cleaned_text = re.sub(pattern, "", cleaned_text, flags=re.DOTALL)

    # Drop lines that are common navigation/footer elements.
    line_patterns = [
        r"(?i)^(home|menu|sign in|log in|privacy policy|terms of service|cookie policy|about us|contact us)$",
    ]

    lines = cleaned_text.splitlines()
    valid_lines: list[str] = []
    for line in lines:
        line_stripped = line.strip()
        is_noise = any(re.match(p, line_stripped) for p in line_patterns)
        if not is_noise and line_stripped:
            valid_lines.append(line_stripped)

    cleaned_text = " ".join(valid_lines)
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()

    # Truncate to save LLM tokens.
    if len(cleaned_text) > 5000:
        cleaned_text = cleaned_text[:5000]

    return cleaned_text
