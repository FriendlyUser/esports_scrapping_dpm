import asyncio
import os
import pandas as pd
from bs4 import BeautifulSoup
from pydoll.constants import Key
from pydoll.browser.chromium import Chrome
from datetime import datetime, timedelta
import dateparser
import re

# --- 1. Pure Logic / Parsing Functions (No Browser Dependency) ---

def sanitize_filename(url_or_string):
    """Sanitizes a URL or string to be a valid filename."""
    s = str(url_or_string)
    s = s.replace("http://", "").replace("https://", "")
    s = re.sub(r'[^\w\-\._]', '_', s)
    return s

def parse_relative_date_string(date_str, current_dt):
    """
    Parses a relative date string (e.g., "Today 11:00 pm", "Mon 1:00pm").
    """
    if not date_str or date_str == "N/A":
        return None

    event_dt_candidate = None
    
    # Try basic parsing for "Today/Tomorrow/Weekday HH:MM am/pm"
    try:
        parts = date_str.lower().split()
        if not parts:
            raise ValueError("Empty date string parts")
            
        day_part = parts[0]
        time_part_input = "".join(parts[1:])

        # Clean time string
        time_str_cleaned = time_part_input
        if (time_str_cleaned.endswith('am') or time_str_cleaned.endswith('pm')):
            if not time_str_cleaned[:-2].endswith(' '): 
                 time_str_cleaned = time_str_cleaned[:-2] + " " + time_str_cleaned[-2:]
        time_str_cleaned = time_str_cleaned.upper()

        # Parse Time
        event_time = None
        try:
            event_time = datetime.strptime(time_str_cleaned, '%I:%M %p').time()
        except ValueError:
            event_time = datetime.strptime(time_str_cleaned.replace(" ", ""), '%I:%M%p').time()

        event_date_base = current_dt.date()

        # Resolve Date
        if day_part == "today":
            event_dt_candidate = datetime.combine(event_date_base, event_time)
        elif day_part == "tomorrow":
            event_date_base += timedelta(days=1)
            event_dt_candidate = datetime.combine(event_date_base, event_time)
        else: 
            days_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
            target_weekday = days_map.get(day_part[:3])

            if target_weekday is not None:
                current_weekday = current_dt.weekday()
                days_ahead = target_weekday - current_weekday
                if days_ahead < 0: 
                    days_ahead += 7
                prospective_date = event_date_base + timedelta(days=days_ahead)
                event_dt_candidate = datetime.combine(prospective_date, event_time)
                
                # Handle edge case: same day of week but time passed
                if event_dt_candidate < current_dt and prospective_date <= current_dt.date():
                     prospective_date += timedelta(days=7)
                     event_dt_candidate = datetime.combine(prospective_date, event_time)

    except ValueError:
        pass # Fallthrough to dateparser

    # Fallback to dateparser library
    if event_dt_candidate is None:
        try:
            settings = {'RELATIVE_BASE': current_dt, 'PREFER_DATES_FROM': 'future'}
            parsed_dp = dateparser.parse(date_str, settings=settings)
            if parsed_dp:
                event_dt_candidate = parsed_dp
        except Exception:
            pass
            
    return event_dt_candidate

def parse_esports_data(html_content):
    """
    Parses PlayNow HTML content to extract eSports match data.
    Input: HTML String
    Output: List of Dictionaries
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    parsed_games = []

    # Find all top-level time band groups
    time_band_groups = soup.find_all('div', class_=lambda x: x and x.startswith('timeBandGroup-'))
    current_dt_for_parsing = datetime.now()
    
    for time_band_group in time_band_groups:
        content_container = time_band_group.find('div', class_=lambda x: x and x.startswith('timeBandGroupContent-'))
        if not content_container:
            continue

        current_region = "Unknown Region"
        
        # Iterate children (Region Headers or Game Lists)
        for element in content_container.find_all(recursive=False):
            # 1. Handle Region Header
            if element.name == 'div' and element.get('data-testid') == 'event-header':
                region_span = element.find('span', class_=lambda x: x and x.startswith('sportsHeaderName-'))
                if region_span:
                    region_text = region_span.text.strip()
                    if region_text.startswith('[') and ']' in region_text:
                        current_region = region_text.split(']', 1)[-1].strip()
                    else:
                        current_region = region_text
            
            # 2. Handle Game List
            elif element.name == 'div':
                game_list_ul = element.find('ul', class_=lambda x: x and x.startswith('eventList-'))
                if game_list_ul:
                    games_li = game_list_ul.find_all('li', class_=lambda x: x and x.startswith('eventListItem-'), recursive=False)
                    
                    for game_li in games_li:
                        game_data = {"Region": current_region}

                        # Team Names
                        team1_el = game_li.find('div', {'data-testid': 'event-card-team-name-a'})
                        game_data['Team 1'] = team1_el.text.strip() if team1_el else "N/A"

                        team2_el = game_li.find('div', {'data-testid': 'event-card-team-name-b'})
                        game_data['Team 2'] = team2_el.text.strip() if team2_el else "N/A"
                        
                        # Status & Date
                        live_indicator = game_li.find('div', {'data-testid': 'event-card-event-clock'})
                        if live_indicator:
                            game_data['Status'] = "Live Now"
                            game_data['Date Raw'] = "Live"
                            game_data['DateTime'] = current_dt_for_parsing # Treat live as 'now' for sorting
                        else:
                            game_data['Status'] = "Scheduled"
                            date_el = game_li.find('span', class_=lambda x: x and x.startswith('eventCardEventStartTimeText-'))
                            raw_date = date_el.text.strip() if date_el else ""
                            game_data['Date Raw'] = raw_date
                            
                            parsed_date = parse_relative_date_string(raw_date, current_dt_for_parsing)
                            game_data['DateTime'] = parsed_date if parsed_date else pd.NaT

                        # Odds
                        outcome_buttons = game_li.find_all('button', {'data-testid': 'outcome-button'})
                        game_data['Odds 1'] = "N/A"
                        game_data['Odds 2'] = "N/A"

                        if len(outcome_buttons) >= 1:
                            odds1 = outcome_buttons[0].find('span', class_=lambda x: x and x.startswith('outcomePriceCommon-'))
                            if odds1: game_data['Odds 1'] = odds1.text.strip()
                        
                        if len(outcome_buttons) >= 2:
                            odds2 = outcome_buttons[1].find('span', class_=lambda x: x and x.startswith('outcomePriceCommon-'))
                            if odds2: game_data['Odds 2'] = odds2.text.strip()
                        
                        parsed_games.append(game_data)
    return parsed_games

# --- 2. Pydoll Browser Logic ---

async def scrape_playnow_live(url):
    """
    Uses Pydoll v2 to navigate to PlayNow, wait for load, and return HTML.
    """
    print(f"🚀 Starting Browser to scrape: {url}")
    
    # Configure Options (if needed, currently using defaults within Chrome class)
    # options = Options() 
    # options.add_argument('--headless=new')

    async with Chrome() as browser:
        tab = await browser.start()

        try:
            await tab.go_to(url)
            print("⏳ Page loaded, waiting for dynamic content (10s)...")
            await asyncio.sleep(10) # Simple wait for SPA hydration

            # Optional: Scroll to bottom to trigger lazy loading if list is long
            # await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            # await asyncio.sleep(2)

            html_content = await tab.page_source
            return html_content

        except Exception as e:
            print(f"❌ Error during browser interaction: {e}")
            return None

async def query_gemini_for_response(input_text="Hello Gemini, how are you today?"):
    url="https://gemini.google.com/u/1/app"
    # The 'async with' context manager ensures the browser process is reaped 
    # even in the event of an unhandled exception.
    async with Chrome() as browser:
        # Pydoll v2 start() returns the initial Tab instance, 
        # which is automatically registered in the browser's _tabs_opened.
        tab = await browser.start()
        await tab.go_to(url)
        
        # Instead of wait_for_selector, we use find() with a built-in timeout.
        # This returns a WebElement object with full type safety.
        # We target the editor div specifically using its class name.
        editor = await tab.find(class_name="ql-editor", timeout=15)
        
        if not editor:
            print("Failed to locate Gemini editor. The DOM structure may have changed.")
            return

        # Explicitly focusing the element is recommended before keyboard interaction.
        await editor.click()
        
        # v2 humanized typing replaces the fixed 'delay' parameter.
        # It simulates realistic keystroke dynamics and potential errors.
        await editor.type_text(input_text, humanize=True)

        # Using the centralized keyboard API to press the Enter key as an alternative 
        # to clicking the submit button, modeling human behavior.
        await tab.keyboard.press(Key.ENTER)

        # For the submit button, we use the tag_name and class_name combined for precision.
        # Pydoll internally constructs the most efficient selector.
        send_button = await tab.find(
            tag_name="button", 
            class_name="send-button.submit", 
            timeout=5,
            raise_exc=False # Returns None instead of raising an exception if not found 
        )
        
        if send_button:
            await send_button.click()
        await asyncio.sleep(25) # Wait for the response to load
        response_container = await tab.find(class_name="markdown-main-panel", timeout=25)

        if response_container:
            # get_text() provides clean, human-readable text
            clean_text = await response_container.text

            
            print("\n--- Scraped Response ---")
            print(clean_text)
            print("------------------------\n")
            return clean_text
        else:
            print("Could not find the response content in the DOM.")


async def main():
    target_url = "https://www.playnow.com/sports/sports/category/2945/esports/league-of-legends/matches"
    
    # Check if we have a local file to test with (Fast Dev Loop)
    local_test_file = "data/play_now_league.html"
    html_source = None

    if os.path.exists(local_test_file):
        print(f"📂 Found local file '{local_test_file}'. using it for parsing test...")
        # Uncomment the next line to force local file usage instead of scraping
        # with open(local_test_file, "r", encoding="utf-8") as f: html_source = f.read()
    
    # If no local file used, scrape live
    if not html_source:
        html_source = await scrape_playnow_live(target_url)
        
        # Save for future testing
        if html_source:
            if not os.path.exists("data"): os.makedirs("data")
            with open(local_test_file, "w", encoding='utf-8') as f:
                f.write(html_source)
                print(f"💾 Saved scraped HTML to {local_test_file}")

    analysis_results = []
    if html_source:
        # Parse
        print("\n🧠 Parsing HTML data...")
        games_data = parse_esports_data(html_source)
        
        if games_data:
            # Display as Table
            df = pd.DataFrame(games_data)
            
            leagues = ['LCS', 'WORLDS', 'MSI', 'LCK', 'LEC', 'LCP']
            pattern = '|'.join(leagues) # Creates 'LCS|WORLDS|MSI...'
            
            # 2. Filter for specific leagues (case-insensitive)
            # We check the 'Region' column (or whichever column contains the league name)
            league_filter = df['Region'].str.contains(pattern, case=False, na=False)
            
            # 3. Filter for unbalanced odds (<= 1.33 OR >= 3.0)
            # Ensure Odds columns are numeric
            df['Odds 1'] = pd.to_numeric(df['Odds 1'], errors='coerce')
            df['Odds 2'] = pd.to_numeric(df['Odds 2'], errors='coerce')
            
            odds_filter = (df['Odds 1'] <= 1.33) | (df['Odds 1'] >= 3.0) | \
                        (df['Odds 2'] <= 1.33) | (df['Odds 2'] >= 3.0)

            # Apply both filters
            filtered_df = df[league_filter & odds_filter]

            # Display Results
            cols = ['Date Raw', 'Region', 'Team 1', 'Odds 1', 'Team 2', 'Odds 2', 'Status']
            final_cols = [c for c in cols if c in filtered_df.columns]
            
            print("\n" + "="*45)
            print(" 🎯 High-Value / Unbalanced LoL Matches ")
            print("="*45)
            if not filtered_df.empty:
                print(filtered_df[final_cols].to_string(index=False))
            else:
                print("No matches found matching those criteria.")
            
            # Optional: Save to CSV
            # df.to_csv("data/esports_odds.csv", index=False)
            # print("\n✅ Data saved to data/esports_odds.csv")
            for index, row in filtered_df.iterrows():
                # Construct a descriptive string for Gemini
                match_context = (
                    f"Match: {row['Team 1']} vs {row['Team 2']} in {row['Region']}. "
                    f"Odds: {row['Team 1']} ({row['Odds 1']}), {row['Team 2']} ({row['Odds 2']}). "
                    f"Analyze this match for betting value or potential upsets."
                )
                
                print(f"Analyzing: {row['Team 1']} vs {row['Team 2']}...")
                
                # Call your function and store the response
                # (Assuming query_gemini_for_response is an async function based on your 'await')
                try:
                    response = await query_gemini_for_response(match_context)
                    analysis_results.append(response)
                except Exception as e:
                    print(f"❌ Error during Gemini interaction: {e}")
                    analysis_results.append(f"❌ Error during Gemini interaction: {e}")
            # save to text file, all of analysis_results
            with open("data/analysis_results.txt", "w") as f:
                f.write("\n".join(analysis_results))

            # open the file in windows
            os.startfile("data/analysis_results.txt")
            print("\n✅ Data saved to data/analysis_results.txt")
        else:
            print("⚠️ No games found in the parsed content.")
    else:
        print("❌ Failed to retrieve HTML content.")

    

if __name__ == '__main__':
    asyncio.run(main())