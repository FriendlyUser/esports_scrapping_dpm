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
import re

def sanitize_filename(url_or_string):
    """Sanitizes a URL or string to be a valid filename."""
    s = str(url_or_string)
    s = s.replace("http://", "").replace("https://", "")
    s = re.sub(r'[^\w\-\._]', '_', s)
    return s

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

async def find_and_click_dpm_match_on_schedule_page(page, playnow_game_to_match, dpm_page_date_str):
    """
    Refactored logic: Finds a match on the DPM schedule page by searching for team text,
    clicking its grandparent, then clicking a "More" button, and finally
    returning the soup of the detailed match view.

    Args:
        page: pydoll page object.
        playnow_game_to_match: Dict of the PlayNow game to match.
        dpm_page_date_str: Date string for the current DPM page.

    Returns:
        BeautifulSoup object of the detailed match view, or None.
    """
    pn_t1_norm = playnow_game_to_match['team1_name'].strip()
    pn_t2_norm = playnow_game_to_match['team2_name'].strip()
    print(f"➡️ Attempting to match game: '{pn_t1_norm}' vs '{pn_t2_norm}' on DPM page for {dpm_page_date_str}")

    # 1. Find the main content area
    # The selector "div.lg:col-span-4" targets a div with both "lg:col-span-4" classes.
    # CSS selectors require escaping for colons, so "lg\\:col-span-4".
    main_content_area_css_selector = r"div.lg\\:col-span-4"
    main_content_area = None
    try:
        # Use page.find_elements which typically returns a list
        main_content_elements = await page.find_elements(By.CSS_SELECTOR, main_content_area_css_selector)
        if not main_content_elements:
            print(f"⚠️ Could not find main content area with selector '{main_content_area_css_selector}'.")
            return None
        main_content_area = main_content_elements[0] # Use the first element found
        print(f"✅ Found main content area.")
    except Exception as e:
        print(f"❌ ERROR finding main content area '{main_content_area_css_selector}': {e}")
        return None

    # 2. Look for an element containing team text, then click its grandparent
    clicked_game_element = False
    target_grandparent_to_click = None

    # reference xpath //*[contains(text(), 'Karmine Corp') or contains(text(), 'Team Heretics')]

    
    try:
        # format using raw string team_text_path = f"//*[contains(text(), '{pn_t1_norm}') or contains(text(), '{pn_t2_norm}')]"   
        team_text_path = f"//*[contains(text(), '{pn_t1_norm}') or contains(text(), '{pn_t2_norm}')]"
        team_text_elements = await main_content_area.find_elements(By.XPATH, team_text_path)
        if team_text_elements:
            target_grandparent_to_click = team_text_elements[0]
            print(target_grandparent_to_click)
            await target_grandparent_to_click.click()
            print(f"✅ Clicked the grandparent element for game '{pn_t1_norm}' vs '{pn_t2_norm}'.")
            await asyncio.sleep(2) # Wait for page to potentially update
        else:
            print(f"⚠️ Could not find an element with team text '{pn_t1_norm}' or '{pn_t2_norm}'.") 

    except Exception as e:
        print(f"❌ ERROR finding or clicking game element via text search: {e}")
        return None

    # 3. Look for a button that contains the word "More" and click it
    # Based on original function, this is likely for H2H details.
    # If the button literally contains "text", change 'more' to 'text' in the XPath.
    more_button_clicked = False
    try:
        # XPath to find any button on the page containing "More" (case-insensitive).
        more_button_xpath = "//button[contains(translate(normalize-space(.), 'MORE', 'more'), 'more')]"
        
        h2h_more_buttons = await page.find_elements(By.XPATH, more_button_xpath)
        
        if h2h_more_buttons:
            print(f"ℹ️ Found {len(h2h_more_buttons)} button(s) containing 'More'.")
            # click the 3rd button
            last_button = h2h_more_buttons[-1]
            await last_button.click()
            print("✅ Clicked a 'More' button.")
            await asyncio.sleep(1.5) # Wait for content to load/expand
        else:
            print("ℹ️ No button containing 'More' found. Details might be visible or accessed differently.")

    except Exception as e:
        print(f"❌ ERROR interacting with 'More' button: {e}")
        # Depending on requirements, this might not be a fatal error.

    # 4. Return the soup of the detailed match view
    try:
        page_source_after_interactions = await page.page_source # Or relevant pydoll method
        print("✅ Successfully retrieved page source after interactions.")
        return BeautifulSoup(page_source_after_interactions, 'html.parser')
    except Exception as e:
        print(f"❌ ERROR getting final page source: {e}")
        return None


def parse_h2h_from_dpm_match_view(dpm_match_view_soup, playnow_game_to_update, dpm_url_date_str, script_run_dt):
    """
    Parses H2H data from a DPM match's detailed view soup and updates the playnow_game_to_update.
    Args:
        dpm_match_view_soup (BeautifulSoup): Soup of the DPM detailed match view.
        playnow_game_to_update (dict): The PlayNow game dictionary to update.
        dpm_url_date_str (str): Date string of the DPM page (for context).
        script_run_dt (datetime): Script execution datetime (for context).
    """
    print(f"    Parsing H2H data for: {playnow_game_to_update['team1_name']} vs {playnow_game_to_update['team2_name']}")

    # Find the H2H section. In the provided H2H HTML, it's a div with id="radix-..." and data-state="open"
    # This is the container for "ALL TIME WINS" and "LAST 5 GAMES"
    h2h_section = dpm_match_view_soup.find('div', id=lambda x: x and x.startswith('radix-'), attrs={'data-state': 'open'})
    
    if not h2h_section:
        # Fallback: sometimes the main content of a tab/collapsible doesn't have data-state itself but its parent does.
        # Or the ID might be on a different element. Let's try to find a known H2H header.
        all_time_wins_header = dpm_match_view_soup.find('span', string=lambda s: s and "ALL TIME WINS" in s.upper())
        if all_time_wins_header:
            # Try to find a common ancestor that seems like the H2H block
            h2h_section = all_time_wins_header.find_parent('div', class_=lambda c: c and 'flex-col' in c and 'items-center' in c and 'gap-8' in c)
            if not h2h_section: # If specific class not found, go up a few generic divs
                 parent_candidate = all_time_wins_header
                 for _ in range(3): # Go up 3 levels
                     if parent_candidate.parent:
                         parent_candidate = parent_candidate.parent
                         if parent_candidate.name == 'div' and parent_candidate.find('span', string=lambda s: s and "LAST 5 GAMES" in s.upper()):
                             h2h_section = parent_candidate
                             break
                     else: break
        if not h2h_section:
            print(f"      [DPM H2H Parser] Could not find H2H section in the provided DPM match view.")
            return

    # DPM Team Names from this H2H view for correct mapping
    # The H2H view should have team name elements similar to the original example
    dpm_teams_in_h2h_view = h2h_section.find_all('a', href=lambda h: h and h.startswith('/esport/teams/'))
    dpm_t1_name_h2h = "N/A"
    dpm_t2_name_h2h = "N/A"

    if len(dpm_teams_in_h2h_view) >= 1:
        img_t1 = dpm_teams_in_h2h_view[0].find('img', alt=True)
        dpm_t1_name_h2h = img_t1['alt'].strip() if img_t1 else dpm_teams_in_h2h_view[0].get_text(strip=True)
    if len(dpm_teams_in_h2h_view) >= 2:
        img_t2 = dpm_teams_in_h2h_view[1].find('img', alt=True)
        dpm_t2_name_h2h = img_t2['alt'].strip() if img_t2 else dpm_teams_in_h2h_view[1].get_text(strip=True)
    
    # Normalize DPM team names from H2H view
    dpm_t1_norm_h2h = dpm_t1_name_h2h.lower().strip()
    dpm_t2_norm_h2h = dpm_t2_name_h2h.lower().strip()

    # Normalize PlayNow team names (from the game object being updated)
    pn_t1_norm = playnow_game_to_update['team1_name'].lower().strip()
    pn_t2_norm = playnow_game_to_update['team2_name'].lower().strip()


    # --- Parse "ALL TIME WINS" ---
    all_time_wins_data = {"team1_dpm_order_wins": "N/A", "team2_dpm_order_wins": "N/A"}
    all_wins_container = h2h_section.find('div', class_=lambda c: c and 'grid-cols-7' in c and 'text-hxs' in c)
    if all_wins_container and "ALL TIME WINS" in all_wins_container.text:
        numbers = [s.strip() for s in all_wins_container.stripped_strings if s.strip().isdigit()]
        if len(numbers) == 2:
            all_time_wins_data["team1_dpm_order_wins"] = numbers[0] # This is DPM's T1 in the H2H display
            all_time_wins_data["team2_dpm_order_wins"] = numbers[1] # This is DPM's T2 in the H2H display
    
    # --- Parse "LAST 5 GAMES" ---
    last_5_games_list = []
    last_5_header_span = h2h_section.find('span', string=lambda s: s and "LAST 5 GAMES" in s.upper())
    if last_5_header_span:
        game_history_divs = h2h_section.find_all('div', class_=lambda c: c and all(cls in c for cls in ['grid', 'grid-cols-3', 'h-44', 'items-center']))
        for game_div in game_history_divs:
            hist_game_data = {"date": "N/A", "team1_name": "N/A", "score1": "N/A", 
                              "team2_name": "N/A", "score2": "N/A", "winner": "N/A"}
            
            date_el = game_div.find('span', class_=lambda c: c and 'text-bxs' in c and 'text-start' in c)
            if date_el: hist_game_data["date"] = date_el.text.strip()

            score_details_div = game_div.find('div', class_=lambda c: c and 'tabular-nums' in c)
            if score_details_div:
                team_imgs = score_details_div.find_all('img', alt=True)
                if len(team_imgs) >= 1 and team_imgs[0].has_attr('alt'): hist_game_data["team1_name"] = team_imgs[0]['alt'].strip()
                if len(team_imgs) >= 2 and team_imgs[1].has_attr('alt'): hist_game_data["team2_name"] = team_imgs[1]['alt'].strip()
                
                score_span_container = score_details_div.find('div', class_=lambda c: c and 'gap-8' in c and 'justify-center' in c)
                if score_span_container:
                    score_spans = score_span_container.find_all('span', recursive=False)
                    if len(score_spans) == 2:
                        hist_game_data["score1"] = score_spans[0].text.strip()
                        hist_game_data["score2"] = score_spans[1].text.strip()
                        s1_classes = score_spans[0].get('class', [])
                        s2_classes = score_spans[1].get('class', [])
                        if 'font-black' in s1_classes or ('opacity-50' not in s1_classes and 'opacity-50' in s2_classes):
                            hist_game_data["winner"] = hist_game_data["team1_name"]
                        elif 'font-black' in s2_classes or ('opacity-50' not in s2_classes and 'opacity-50' in s1_classes):
                            hist_game_data["winner"] = hist_game_data["team2_name"]
            last_5_games_list.append(hist_game_data)

    # Store H2H data in the playnow_game_to_update object
    final_h2h_data = {}
    # Map DPM's order of all_time_wins to PlayNow's team order
    if dpm_t1_norm_h2h == pn_t1_norm and dpm_t2_norm_h2h == pn_t2_norm : # DPM T1 (H2H view) is PlayNow T1
        final_h2h_data['all_time_wins_t1'] = all_time_wins_data["team1_dpm_order_wins"]
        final_h2h_data['all_time_wins_t2'] = all_time_wins_data["team2_dpm_order_wins"]
    elif dpm_t1_norm_h2h == pn_t2_norm and dpm_t2_norm_h2h == pn_t1_norm: # DPM T1 (H2H view) is PlayNow T2
        final_h2h_data['all_time_wins_t1'] = all_time_wins_data["team2_dpm_order_wins"]
        final_h2h_data['all_time_wins_t2'] = all_time_wins_data["team1_dpm_order_wins"]
    else:
        # If DPM team names in H2H view don't perfectly match PlayNow names (after normalization)
        # this mapping might be incorrect. Log a warning.
        print(f"      [DPM H2H Warning] Team name mismatch between PlayNow ({pn_t1_norm}, {pn_t2_norm}) and DPM H2H view ({dpm_t1_norm_h2h}, {dpm_t2_norm_h2h}). All-time wins assignment might be ambiguous.")
        # Assign based on DPM order as a fallback, but flag it
        final_h2h_data['all_time_wins_dpm_order_t1'] = all_time_wins_data["team1_dpm_order_wins"]
        final_h2h_data['all_time_wins_dpm_order_t2'] = all_time_wins_data["team2_dpm_order_wins"]


    final_h2h_data['last_5_games'] = last_5_games_list
    playnow_game_to_update['dpm_h2h'] = final_h2h_data
    
    print(f"      Successfully parsed and stored H2H for {playnow_game_to_update['team1_name']} vs {playnow_game_to_update['team2_name']}")

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


        dpm_date_urls, games_by_dpm_date = create_dpm_lol_date_urls(esports_games)
        print(f"\nGenerated {len(dpm_date_urls)} DPM.lol date URLs to visit.")
        script_run_datetime = datetime.now()

        for dpm_url in dpm_date_urls:
            print(f"\nProcessing DPM URL: {dpm_url}")
            try:
                await page.go_to(dpm_url)
                print("  Waiting for DPM page to load (5 seconds)...")
                await asyncio.sleep(5)
                page_source_dpm_schedule = await page.page_source # This is the schedule view for the date

                safe_dpm_url_filename_part = sanitize_filename(dpm_url)
                dpm_schedule_html_path = f"data/dpm_schedule_{safe_dpm_url_filename_part}.html"
                with open(dpm_schedule_html_path, "w", encoding='utf-8', errors='ignore') as f:
                    f.write(page_source_dpm_schedule)
                print(f"  Saved DPM schedule HTML to {dpm_schedule_html_path}")
                
                dpm_schedule_soup = BeautifulSoup(page_source_dpm_schedule, 'html.parser')
                dpm_page_date_str = dpm_url.split("?date=")[-1] 
                
                playnow_games_for_this_dpm_date = games_by_dpm_date.get(dpm_page_date_str, [])

                if not playnow_games_for_this_dpm_date:
                    print(f"  No PlayNow games found for DPM date {dpm_page_date_str}. Skipping detailed DPM parsing for this date's games.")
                    continue

                for playnow_game_entry in playnow_games_for_this_dpm_date:
                    if 'dpm_h2h' in playnow_game_entry: # Already processed
                        print(f"  Skipping already processed PlayNow game: {playnow_game_entry['team1_name']} vs {playnow_game_entry['team2_name']}")
                        continue
                    
                    print(f"  Attempting to find and process DPM match for PlayNow game: {playnow_game_entry['team1_name']} vs {playnow_game_entry['team2_name']}")
                    
                    # This function will find the match on DPM schedule, click it, click "More", and return the new soup
                    soup_of_detailed_dpm_match_view = await find_and_click_dpm_match_on_schedule_page(
                        page, 
                        playnow_game_entry, 
                        dpm_page_date_str,
                    )

                    if soup_of_detailed_dpm_match_view:
                        # Save the HTML of the detailed view for debugging
                        detailed_view_filename = f"dpm_detail_{playnow_game_entry['team1_name']}_vs_{playnow_game_entry['team2_name']}_{dpm_page_date_str}.html"
                        detailed_view_path = f"data/{detailed_view_filename}"
                        with open(detailed_view_path, "w", encoding='utf-8', errors='ignore') as f:
                            f.write(soup_of_detailed_dpm_match_view.prettify())
                        print(f"    Saved DPM detailed match view HTML to {detailed_view_path}")
                        
                        parse_h2h_from_dpm_match_view(
                            soup_of_detailed_dpm_match_view, 
                            playnow_game_entry, 
                            dpm_page_date_str, 
                            script_run_datetime
                        )
                        # Go back to the schedule page for this date to process the next PlayNow game for this date
                        # This is important if clicking a game navigates away fully.
                        # If it's an overlay, this might not be strictly necessary but good for robustness.
                        # print(f"    Navigating back to DPM schedule page {dpm_url} to look for next match on this date.")
                        await page.go_to(dpm_url)
                        await asyncio.sleep(3) # Allow schedule page to reload
                        # dpm_schedule_soup = BeautifulSoup(await page.page_source, 'html.parser') # Re-parse schedule soup
                    else:
                        print(f"    Could not get detailed DPM match view for {playnow_game_entry['team1_name']} vs {playnow_game_entry['team2_name']}.")
            
            except Exception as e:
                print(f"  Error processing DPM URL {dpm_url} or its games: {e}")

        print("\n\n--- Combined eSports Data with DPM Head-to-Head ---")
        for game in esports_games:
            print(f"\nRegion: {game.get('region', 'N/A')}")
            print(f"  Status: {game.get('live_status', 'N/A')}")
            print(f"  PlayNow Date String: {game.get('date_string', 'N/A')}")
            print(f"  Parsed DateTime: {game.get('date_time_formatted', 'N/A')}")
            print(f"  Match: {game.get('team1_name', 'N/A')} vs {game.get('team2_name', 'N/A')}")
            print(f"  Odds (PlayNow): {game.get('team1_name', 'N/A')} ({game.get('team1_odds', 'N/A')}) vs {game.get('team2_name', 'N/A')} ({game.get('team2_odds', 'N/A')})")
            
            if 'dpm_h2h' in game:
                h2h = game['dpm_h2h']
                t1_name_playnow = game['team1_name'] 
                t2_name_playnow = game['team2_name']

                print("  DPM Head-to-Head:")
                # Check which key was used for all_time_wins based on successful mapping
                if 'all_time_wins_t1' in h2h:
                    print(f"    All Time Wins ({t1_name_playnow}): {h2h.get('all_time_wins_t1', 'N/A')}")
                    print(f"    All Time Wins ({t2_name_playnow}): {h2h.get('all_time_wins_t2', 'N/A')}")
                elif 'all_time_wins_dpm_order_t1' in h2h:
                    print(f"    All Time Wins (DPM Order T1): {h2h.get('all_time_wins_dpm_order_t1', 'N/A')} (Warning: Team name mapping was ambiguous)")
                    print(f"    All Time Wins (DPM Order T2): {h2h.get('all_time_wins_dpm_order_t2', 'N/A')}")
                else:
                    print(f"    All Time Wins: Data not fully parsed or available.")

                if h2h.get('last_5_games'):
                    print("    Last 5 Games (DPM Data):")
                    for i, hist_game in enumerate(h2h.get('last_5_games', [])):
                        print(f"      {i+1}. Date: {hist_game.get('date', 'N/A')} | "
                            f"{hist_game.get('team1_name', 'T1')} {hist_game.get('score1', 'S1')} - "
                            f"{hist_game.get('score2', 'S2')} {hist_game.get('team2_name', 'T2')} "
                            f"| Winner: {hist_game.get('winner', 'N/A')}")
                else:
                    print("    Last 5 Games (DPM Data): Not available or not parsed.")
            else:
                print("  DPM Head-to-Head: Not found or not matched on DPM.lol for this date.")
            print("-" * 30)

if __name__ == '__main__':
    asyncio.run(main())
