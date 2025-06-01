import asyncio
import os
import pandas as pd
import random
from dotenv import load_dotenv
from io import StringIO
from bs4 import BeautifulSoup, NavigableString
from pydoll.browser.chrome import Chrome
from pydoll.browser.options import Options
from pydoll.constants import By
from datetime import datetime, timedelta
import dateparser

def parse_esports_data(html_content):
    """
    Parses HTML content to extract eSports match data.

    Args:
        html_content (str): The HTML string to parse.

    Returns:
        list: A list of dictionaries, where each dictionary contains
              information about a single match.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    parsed_games = []

    # Find all top-level time band groups.
    time_band_groups = soup.find_all('div', class_=lambda x: x and x.startswith('timeBandGroup-'))
    current_dt_for_parsing = datetime.now()
    print(f"\nCurrent reference time for live data parsing: {current_dt_for_parsing.strftime('%Y-%m-%d %A %I:%M %p')}\n")
    for time_band_group in time_band_groups:
        # Find the content container within this time band group
        content_container = time_band_group.find('div', class_=lambda x: x and x.startswith('timeBandGroupContent-'))
        if not content_container:
            continue

        current_region = "Unknown Region"
        # Iterate through direct children of the content container.
        # These children are either region headers or divs containing game lists.
        for element in content_container.find_all(recursive=False):
            # Check if the element is a region header
            if element.name == 'div' and element.get('data-testid') == 'event-header':
                region_span = element.find('span', class_=lambda x: x and x.startswith('sportsHeaderName-'))
                if region_span:
                    region_text = region_span.text.strip()
                    # Extract region name, removing prefixes like "[LoL]"
                    if region_text.startswith('[') and ']' in region_text:
                        current_region = region_text.split(']', 1)[-1].strip()
                    else:
                        current_region = region_text
                else:
                    current_region = "Unknown Region" # Fallback
            
            # Check if the element is a div containing a game list (ul)
            elif element.name == 'div':
                game_list_ul = element.find('ul', class_=lambda x: x and x.startswith('eventList-'))
                if game_list_ul:
                    # Find all list items (games) within this ul
                    games_li = game_list_ul.find_all('li', class_=lambda x: x and x.startswith('eventListItem-'), recursive=False)
                    
                    for game_li in games_li:
                        game_data = {"region": current_region}

                        # Extract Team 1 Name
                        team1_el = game_li.find('div', {'data-testid': 'event-card-team-name-a'})
                        game_data['team1_name'] = team1_el.text.strip() if team1_el else "N/A"

                        # Extract Team 2 Name
                        team2_el = game_li.find('div', {'data-testid': 'event-card-team-name-b'})
                        game_data['team2_name'] = team2_el.text.strip() if team2_el else "N/A"
                        
                        # Determine if the game is live and extract date string accordingly
                        live_indicator = game_li.find('div', {'data-testid': 'event-card-event-clock'})
                        if live_indicator:
                            game_data['live_status'] = "Live Now"
                            game_data['date_string'] = "N/A" # Or specific live text if available elsewhere
                        else:
                            game_data['live_status'] = "Scheduled"
                            date_el = game_li.find('span', class_=lambda x: x and x.startswith('eventCardEventStartTimeText-'))
                            game_data['date_string'] = date_el.text.strip() if date_el else ""
                            parsed_date = parse_relative_date_string(game_data['date_string'], current_dt_for_parsing)
                            game_data['date_time'] = parsed_date
                            game_data['date_time_formatted'] = parsed_date.strftime("%Y-%m-%d %A %I:%M %p")

                        # Extract Odds
                        outcome_buttons = game_li.find_all('button', {'data-testid': 'outcome-button'})
                        
                        team1_odds = "N/A"
                        team2_odds = "N/A"

                        if len(outcome_buttons) >= 1: # Team 1 odds
                            odds1_price_el = outcome_buttons[0].find('span', class_=lambda x: x and x.startswith('outcomePriceCommon-'))
                            if odds1_price_el:
                                team1_odds = odds1_price_el.text.strip()
                        
                        if len(outcome_buttons) >= 2: # Team 2 odds
                            odds2_price_el = outcome_buttons[1].find('span', class_=lambda x: x and x.startswith('outcomePriceCommon-'))
                            if odds2_price_el:
                                team2_odds = odds2_price_el.text.strip()
                        
                        game_data['team1_odds'] = team1_odds
                        game_data['team2_odds'] = team2_odds
                        
                        parsed_games.append(game_data)
    return parsed_games

def parse_relative_date_string(date_str, current_dt):
    """
    Parses a relative date string (e.g., "Today 11:00 pm", "Mon 1:00pm")
    and converts it to a datetime object.
    Assumes all input dates are today or in the future relative to current_dt.
    Uses dateparser as a fallback if initial logic fails.

    Args:
        date_str (str): The relative date string.
        current_dt (datetime): The current datetime to resolve against.

    Returns:
        datetime: The parsed datetime object, or None if parsing fails.
    """
    if not date_str or date_str == "N/A":
        return None

    event_dt_candidate = None
    # --- Primary parsing logic ---
    try:
        parts = date_str.lower().split()
        if not parts:
            raise ValueError("Empty date string parts")
            
        day_part = parts[0]
        time_part_input = "".join(parts[1:])

        time_str_cleaned = time_part_input
        if (time_str_cleaned.endswith('am') or time_str_cleaned.endswith('pm')):
            if not time_str_cleaned[:-2].endswith(' '): 
                 time_str_cleaned = time_str_cleaned[:-2] + " " + time_str_cleaned[-2:]
        time_str_cleaned = time_str_cleaned.upper()

        event_time = None
        try:
            event_time = datetime.strptime(time_str_cleaned, '%I:%M %p').time()
        except ValueError:
            event_time = datetime.strptime(time_str_cleaned.replace(" ", ""), '%I:%M%p').time()

        event_date_base = current_dt.date()

        if day_part == "today":
            event_dt_candidate = datetime.combine(event_date_base, event_time)
        elif day_part == "tomorrow":
            event_date_base += timedelta(days=1)
            event_dt_candidate = datetime.combine(event_date_base, event_time)
        else: 
            days_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
            target_weekday = days_map.get(day_part[:3])

            if target_weekday is None:
                raise ValueError(f"Unknown day part: '{day_part}'")

            current_weekday = current_dt.weekday()
            days_ahead = target_weekday - current_weekday
            
            if days_ahead < 0: # Target day was earlier in this week
                days_ahead += 7
            
            prospective_date = event_date_base + timedelta(days=days_ahead)
            event_dt_candidate = datetime.combine(prospective_date, event_time)

            # If it's the same day of the week, but the calculated time is in the past for the *current* date,
            # advance to the next week.
            if event_dt_candidate < current_dt and prospective_date <= current_dt.date() :
                 prospective_date += timedelta(days=7)
                 event_dt_candidate = datetime.combine(prospective_date, event_time)
        
        # Final check: if the successfully parsed date is still in the past,
        # it might be an issue with "Today HH:MM" where HH:MM has passed.
        # The problem statement implies all dates are today or future.
        # If event_dt_candidate is in the past, and day_part was "today", this logic is okay.
        # If it was a specific day (e.g. "Mon") and it resolved to past Mon, the above logic handles it.
        # This check is more of a safeguard for very specific edge cases not covered by advancing weeks.
        if event_dt_candidate and event_dt_candidate < current_dt and day_part != "today":
             # This condition might need refinement based on how strictly "today or in the future" is interpreted
             # for days like "Mon" that could be today but time has passed.
             # The current logic for specific days already tries to advance by a week if needed.
             pass


    except ValueError as e:
        # print(f"Primary parsing failed for '{date_str}': {e}. Trying dateparser.")
        event_dt_candidate = None # Ensure it's None before trying dateparser

    # --- Fallback to dateparser ---
    if event_dt_candidate is None:
        try:
            # PREFER_DATES_FROM: 'future' helps resolve ambiguities like "Monday" to be the next Monday.
            # RELATIVE_BASE: provides context for terms like "tomorrow".
            settings = {'RELATIVE_BASE': current_dt, 'PREFER_DATES_FROM': 'future'}
            parsed_dp = dateparser.parse(date_str, settings=settings)
            
            if parsed_dp:
                # If dateparser returns only a date (midnight), and original string had time, try to combine.
                # This is tricky because dateparser might already include the time.
                # For simplicity, we'll trust dateparser's output if it's a full datetime.
                # If it's just a date, and we had an original time_part_input, could attempt to combine,
                # but this adds complexity. Let's first see what dateparser yields.
                event_dt_candidate = parsed_dp
                
                # If dateparser result is in the past, and it wasn't "today", it might be an error
                # or dateparser didn't respect 'PREFER_DATES_FROM' as expected for all cases.
                if event_dt_candidate < current_dt and not (day_part == "today" and event_dt_candidate.date() == current_dt.date()):
                    # If dateparser gave a past date for a future-implied string (e.g. "Next Monday" but gave past Monday)
                    # This might indicate a need to adjust dateparser settings or post-process.
                    # For now, we'll accept its result but be mindful.
                    # A simple fix if it's just the date part that's off for a future day:
                    if event_dt_candidate.date() < current_dt.date():
                         event_dt_candidate = None # Could try to add 7 days if it's a weekday string.
                         # print(f"Dateparser result for '{date_str}' was in the past ({event_dt_candidate}), nullifying.")


        except Exception as dp_e:
            print(f"Dateparser also failed for '{date_str}': {dp_e}")
            event_dt_candidate = None
            
    return event_dt_candidate

def create_dpm_lol_date_urls(esports_games):
    """
    Creates a list of dpm.lol URLs for each unique date found in the esports_games.

    Args:
        esports_games (list): A list of dictionaries, where each dictionary
                              contains information about a single match,
                              including a 'date_time' datetime object.

    Returns:
        list: A list of unique dpm.lol URLs formatted as
              https://dpm.lol/esport/?date=M-D
    """
    base_url = "https://dpm.lol/esport/?date="
    unique_date_strings = set()
    games_by_date = {}
    for game in esports_games:
        # Ensure 'date_time' exists and is a datetime object
        if 'date_time' in game and isinstance(game['date_time'], datetime):
            dt_object = game['date_time']
            # Format as M-D (e.g., 6-1 for June 1st, 12-15 for December 15th)
            # Using .month and .day directly gives integers, f-string handles conversion.
            date_str = f"{dt_object.day}-{dt_object.month}"
            unique_date_strings.add(date_str)


            # Populate the games_by_date dictionary
            if date_str not in games_by_date:
                games_by_date[date_str] = []
            games_by_date[date_str].append(game)

    sorted_date_keys = sorted(
        list(unique_date_strings),
        key=lambda d_str: tuple(map(int, d_str.split('-')))
    )
    
    date_urls = [f"{base_url}{date_s}" for date_s in sorted_date_keys]
    return date_urls, games_by_date
    # # Using simple sort of M-D strings for now:
    # date_urls = [f"{base_url}{date_s}" for date_s in sorted(list(unique_date_strings))]
    # return date_urls

def parse_dpm_match_and_h2h_data(dpm_soup, playnow_games_list, dpm_url_date_str, script_run_dt):
    """
    Parses a dpm.lol page for match details and H2H statistics, then updates the playnow_games_list.

    Args:
        dpm_soup (BeautifulSoup): Parsed HTML of a dpm.lol date page.
        playnow_games_list (list): List of game dicts from parse_esports_data (PlayNow).
                                   This list will be modified in-place.
        dpm_url_date_str (str): The date string from the DPM URL (e.g., "6-1" for June 1st, format M-D).
        script_run_dt (datetime): The datetime when the script was initiated (for context).
    """
    # Find all identifiable match headers on the DPM page
    # These are divs with classes like "grid grid-cols-3 animate-fade-in" containing team info
    dpm_match_headers = dpm_soup.find_all('div', class_=lambda c: c and all(cls in c for cls in ['grid', 'grid-cols-3', 'animate-fade-in']))

    if not dpm_match_headers:
        print(f"  [DPM Parser] No DPM match headers found on page for date {dpm_url_date_str}.")
        return

    for dpm_match_header in dpm_match_headers:
        team_links = dpm_match_header.find_all('a', href=lambda h: h and h.startswith('/esport/teams/'))
        if len(team_links) < 2:
            continue
        
        dpm_t1_img = team_links[0].find('img', alt=True)
        dpm_t1_name_full = dpm_t1_img['alt'].strip() if dpm_t1_img else None
        
        dpm_t2_img = team_links[1].find('img', alt=True)
        dpm_t2_name_full = dpm_t2_img['alt'].strip() if dpm_t2_img else None

        if not dpm_t1_name_full or not dpm_t2_name_full:
            continue

        for playnow_game in playnow_games_list:
            if 'dpm_h2h' in playnow_game: # Already matched and processed this PlayNow game
                continue

            playnow_t1_name = playnow_game['team1_name']
            playnow_t2_name = playnow_game['team2_name']
            playnow_dt = playnow_game.get('date_time')

            if not playnow_dt: # Skip PlayNow game if no datetime
                continue

            # 1. Check Date Match
            dpm_page_month, dpm_page_day = map(int, dpm_url_date_str.split('-'))
            if not (playnow_dt.month == dpm_page_month and playnow_dt.day == dpm_page_day):
                continue # Dates don't match this PlayNow game

            # 2. Check Team Names (case-insensitive, order-insensitive)
            pn_t1_norm = playnow_t1_name.lower().strip()
            pn_t2_norm = playnow_t2_name.lower().strip()
            dpm_t1_norm = dpm_t1_name_full.lower().strip()
            dpm_t2_norm = dpm_t2_name_full.lower().strip()

            teams_match = (
                (pn_t1_norm == dpm_t1_norm and pn_t2_norm == dpm_t2_norm) or
                (pn_t1_norm == dpm_t2_norm and pn_t2_norm == dpm_t1_norm)
            )

            if teams_match:
                print(f"  [DPM Parser] Matched PlayNow: '{playnow_t1_name} vs {playnow_t2_name}' with DPM: '{dpm_t1_name_full} vs {dpm_t2_name_full}' on date {dpm_url_date_str}")
                
                # Find H2H section: div with id starting with "radix-"
                # This div is usually within a broader "game card" container.
                game_card_root = None
                temp_element = dpm_match_header
                for _ in range(5): # Search up to 5 levels for a div[data-state="open"] containing a radix-id div
                    if temp_element.name == 'div' and temp_element.get('data-state') == 'open' and \
                       temp_element.find('div', id=lambda x: x and x.startswith('radix-')):
                        game_card_root = temp_element
                        break
                    if temp_element.parent:
                        temp_element = temp_element.parent
                    else:
                        break
                
                h2h_section = None
                if game_card_root:
                    h2h_section = game_card_root.find('div', id=lambda x: x and x.startswith('radix-'))

                if not h2h_section:
                    print(f"    [DPM Parser] Could not find H2H section for {dpm_t1_name_full} vs {dpm_t2_name_full}")
                    continue # Try next PlayNow game or next DPM header

                # --- Parse "ALL TIME WINS" ---
                all_time_wins_data = {"team1_dpm_order_wins": "N/A", "team2_dpm_order_wins": "N/A"}
                # The structure is: <div ... grid-cols-7 ...> NUM <span ...>ALL TIME WINS</span> NUM </div>
                all_wins_container = h2h_section.find('div', class_=lambda c: c and 'grid-cols-7' in c and c and 'text-hxs' in c) # More specific
                if all_wins_container and "ALL TIME WINS" in all_wins_container.text:
                    numbers = [s.strip() for s in all_wins_container.stripped_strings if s.strip().isdigit()]
                    if len(numbers) == 2:
                        all_time_wins_data["team1_dpm_order_wins"] = numbers[0]
                        all_time_wins_data["team2_dpm_order_wins"] = numbers[1]
                
                # --- Parse "LAST 5 GAMES" ---
                last_5_games_list = []
                last_5_header_span = h2h_section.find('span', string=lambda s: s and "LAST 5 GAMES" in s.upper())
                if last_5_header_span:
                    # Game history divs are typically siblings or near siblings after this header
                    # Class: "grid grid-cols-3 w-full h-44 ..."
                    game_history_divs = h2h_section.find_all('div', class_=lambda c: c and all(cls in c for cls in ['grid', 'grid-cols-3', 'h-44', 'items-center']))
                    
                    for game_div in game_history_divs:
                        hist_game_data = {"date": "N/A", "team1_name": "N/A", "score1": "N/A", 
                                          "team2_name": "N/A", "score2": "N/A", "winner": "N/A"}
                        
                        date_el = game_div.find('span', class_=lambda c: c and 'text-bxs' in c and 'text-start' in c)
                        if date_el: hist_game_data["date"] = date_el.text.strip()

                        score_details_div = game_div.find('div', class_=lambda c: c and 'tabular-nums' in c) # Contains team imgs and scores
                        if score_details_div:
                            team_imgs = score_details_div.find_all('img', alt=True)
                            if len(team_imgs) >= 1: hist_game_data["team1_name"] = team_imgs[0]['alt'].strip()
                            if len(team_imgs) >= 2: hist_game_data["team2_name"] = team_imgs[1]['alt'].strip()
                            
                            # Score structure: <div ... gap-8> <span class="opacity-50">SCORE1</span> - <span class="font-black">SCORE2</span> </div>
                            score_span_container = score_details_div.find('div', class_=lambda c: c and 'gap-8' in c and 'justify-center' in c)
                            if score_span_container:
                                score_spans = score_span_container.find_all('span', recursive=False) # Direct children spans
                                if len(score_spans) == 2: # Expecting two spans for scores
                                    hist_game_data["score1"] = score_spans[0].text.strip()
                                    hist_game_data["score2"] = score_spans[1].text.strip()

                                    s1_classes = score_spans[0].get('class', [])
                                    s2_classes = score_spans[1].get('class', [])
                                    
                                    # Winner based on font-black (winner) vs opacity-50 (loser)
                                    if 'font-black' in s1_classes or ('opacity-50' not in s1_classes and 'opacity-50' in s2_classes):
                                        hist_game_data["winner"] = hist_game_data["team1_name"]
                                    elif 'font-black' in s2_classes or ('opacity-50' not in s2_classes and 'opacity-50' in s1_classes):
                                        hist_game_data["winner"] = hist_game_data["team2_name"]
                        
                        last_5_games_list.append(hist_game_data)

                # Store H2H data in the playnow_game object
                final_h2h_data = {}
                # Map DPM's order of all_time_wins to PlayNow's team order
                if dpm_t1_norm == pn_t1_norm: # DPM T1 is PlayNow T1
                    final_h2h_data['all_time_wins_t1'] = all_time_wins_data["team1_dpm_order_wins"]
                    final_h2h_data['all_time_wins_t2'] = all_time_wins_data["team2_dpm_order_wins"]
                else: # DPM T1 was PlayNow T2 (teams were swapped in DPM display relative to PlayNow)
                    final_h2h_data['all_time_wins_t1'] = all_time_wins_data["team2_dpm_order_wins"]
                    final_h2h_data['all_time_wins_t2'] = all_time_wins_data["team1_dpm_order_wins"]
                
                final_h2h_data['last_5_games'] = last_5_games_list
                playnow_game['dpm_h2h'] = final_h2h_data
                
                print(f"    [DPM Parser] Successfully parsed and stored H2H for {playnow_t1_name} vs {playnow_t2_name}")
                break # Found match and processed H2H for this playnow_game, move to next DPM header

async def main():
    starting_url = "https://www.playnow.com/sports/sports/category/2945/esports/league-of-legends/matches"
    async with Chrome() as browser:
        # options = Options()
        # options.binary_location = '/usr/bin/google-chrome-stable'
        # options.add_argument('--headless=new')
        # options.add_argument('--start-maximized')
        # options.add_argument('--disable-notifications')
        await browser.start()
        page = await browser.get_page()

        await page.go_to(starting_url)

        await asyncio.sleep(12) 

        # get page source
        page_source = await page.page_source 
        soup = BeautifulSoup(page_source, 'html.parser')
        if not os.path.exists("data"):
            os.makedirs("data")

        # we want to grab the element filtered-event-list__events
        # filtered_event_list = soup.find('div', {'id': 'sports-wrapper'})

        # apply the date logic we want to take the relative days, compare to the current dates get the day string 
        # when the games are happening
        with open ("data/play_now_league.html", "w", errors='ignore') as f:
            f.write(page_source)
        esports_games = parse_esports_data(page_source)


        dpm_date_urls, games_by_date = create_dpm_lol_date_urls(esports_games)
        for url in dpm_date_urls:
            print(url)

            # take screenshot
            await page.go_to(url)
            await asyncio.sleep(5)
            page_source = await page.page_source 
            with open (f"data/{url}.html", "w", errors='ignore') as f:
                f.write(page_source)
            await page.screenshot(f"data/{url}.png")

            # grab the games making with the "current date"
            current_date_from_url = url.split("?date=")[-1]
            ref_games = games_by_date[current_date_from_url]

            print("games happening today", ref_games)

            page_source_dpm = await page.page_source

            # Save DPM HTML and screenshot
            safe_dpm_url_filename_part = url
            dpm_html_path = os.path.join('data', f"dpm_{safe_dpm_url_filename_part}.html")

            with open(dpm_html_path, "w", encoding='utf-8', errors='ignore') as f:
                f.write(page_source_dpm)
            print(f"  Saved DPM HTML to {dpm_html_path}")
            
            # await page.screenshot({'path': dpm_screenshot_path}) # pydoll screenshot syntax might differ
            # print(f"  Saved DPM screenshot to {dpm_screenshot_path}")

            dpm_soup = BeautifulSoup(page_source_dpm, 'html.parser')
            dpm_page_date_str = url.split("?date=")[-1] # Extracts "M-D"

            # --- 4. Parse DPM page and update PlayNow games with H2H data ---
            # Consider passing games_by_dpm_date[dpm_page_date_str] to parse_dpm_match_and_h2h_data
            # if you want to optimize and only pass games relevant to the current DPM page's date.
            # For now, it still uses the full esports_games_playnow list and filters by date inside.
            parse_dpm_match_and_h2h_data(dpm_soup, esports_games, dpm_page_date_str, script_run_datetime)
        


asyncio.run(main())