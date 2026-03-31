"""Claude API client for expanding title lists with similar/related titles."""

import json
import anthropic


def expand_titles(api_key: str, user_titles: list[str]) -> dict:
    """Use Claude to expand a list of job titles with related/similar titles.

    Returns dict with:
      - expanded_titles: full list of titles to search (includes originals)
      - explanation: brief note on what was added
    """
    client = anthropic.Anthropic(api_key=api_key)

    prompt = f"""I'm searching for people at landscaping and essential service companies using Apollo.io's people search API.

The user wants to find people with these kinds of titles:
{json.dumps(user_titles)}

Your job: expand this list with similar, related, or alternative titles that the same kinds of people might have at landscaping/construction/facilities companies. Think about:
- Different ways the same role might be titled (e.g., "CFO" vs "Chief Financial Officer" vs "Controller")
- Slightly different seniority levels of the same function (e.g., "VP Sales" vs "Director of Sales" vs "Sales Manager")
- Industry-specific title variations (e.g., "Landscape Architect" vs "Landscape Designer")

Rules:
- Keep the list focused. Don't add unrelated roles.
- Include the original titles exactly as provided.
- Return 15-30 total titles max (originals + expansions).
- Return ONLY valid JSON, no other text.

Return this exact JSON format:
{{
  "expanded_titles": ["title1", "title2", ...],
  "explanation": "Brief 1-2 sentence explanation of what was added"
}}"""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )

    text = message.content[0].text.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[: text.rfind("```")]
        text = text.strip()

    return json.loads(text)
