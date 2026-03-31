# Apollo Contact Enricher

A web app that automates finding and enriching contacts at target landscaping/essential service companies using the Apollo API.

## What It Does

1. You input target companies and the kinds of titles you're looking for
2. Claude AI expands your title list with similar/related titles
3. The app searches Apollo for people at those companies matching those titles
4. Enriches all found contacts (names, emails, LinkedIn URLs)
5. Exports everything to a clean, color-coded Excel spreadsheet

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Environment Variables
```bash
cp .env.example .env
# Edit .env with your API keys
```

You need:
- **Apollo API Key** - from your Apollo.io account settings
- **Anthropic API Key** - from console.anthropic.com

### 3. Run the App
```bash
python app.py
```

Open `http://localhost:5000` in your browser.

## How It Works

### Architecture
- `app.py` - Flask web server + routes
- `apollo_client.py` - Apollo API wrapper (search orgs, search people, bulk enrich)
- `claude_client.py` - Claude API for expanding title lists
- `enrichment.py` - Core orchestration logic (ties everything together)
- `excel_builder.py` - Excel spreadsheet generator with formatting

### Flow
1. User inputs companies + titles
2. Claude expands title list (e.g., "CFO" -> also searches "Controller", "VP Finance")
3. For each company, search Apollo org database to get org IDs
4. Search people at those orgs matching expanded titles
5. Bulk enrich in batches of 10 (extracts only key fields to stay lean)
6. Build formatted Excel with results
7. User downloads the file

### Apollo Credit Usage
- People Search: free (no credits)
- People Enrichment: 1 credit per person
- The app shows estimated credit usage before you confirm

## Notes
- Rate limits: the app includes built-in delays between API calls
- Batch size: enrichment runs 10 people at a time (Apollo max)
- The app handles pagination automatically for companies with many contacts
