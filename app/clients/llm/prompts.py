EXTRACTION_PROMPT = """
You are an expert HR parser. Extract structured job details from the provided text.
Return ONLY a JSON object. No markdown. No explanation. No code fences.

Required JSON structure:
{
    "title": "Job Title (string, required)",
    "company": "Company Name (string, required)",
    "location": "Location (string or null)",
    "salary": "Raw salary string exactly as written (string or null)",
    "skills": ["skill1", "skill2"] (list of strings),
    "description": "Cleaned full description (string or null)"
}
"""
