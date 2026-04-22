import re


def strip_noise(raw_text: str) -> str:
    """
    Очищає сирий текст сторінки вакансії від навігації, футерів та блоків зі схожими вакансіями.
    Нормалізує пробіли та обрізає результат до 5000 символів.
    """
    if not raw_text:
        return ""

    cleaned_text = raw_text

    # 1. Видалення блоків, які зазвичай знаходяться в кінці і містять інші вакансії
    # (?i) робить пошук case-insensitive, .*$ видаляє все до кінця тексту
    noise_blocks_patterns = [
        r"(?i)(similar jobs|related jobs|people also viewed|explore more jobs).*$",
    ]
    for pattern in noise_blocks_patterns:
        cleaned_text = re.sub(pattern, "", cleaned_text, flags=re.DOTALL)

    # 2. Видалення рядків з типовими елементами навігації та футера
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

    # 3. Нормалізація пробілів (заміна множинних пробілів/переносів на один)
    cleaned_text = " ".join(valid_lines)
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()

    # 4. Усічення до 5000 символів для економії токенів LLM
    if len(cleaned_text) > 5000:
        cleaned_text = cleaned_text[:5000]

    return cleaned_text
