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
                            game_data['date_time'] = parsed_date.strftime('%Y-%m-%d %A %I:%M %p')

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

        await asyncio.sleep(15) 

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


        
        print(esports_games)


asyncio.run(main())