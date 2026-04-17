import disnake
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import os
from datetime import datetime
import aiohttp
from conf import *


def is_role(roles, member: disnake.Member):
    """
    Performs check to determine if a user has a given role.
    Used to perform RBAC on commands.
    :param roles:
    :param member:
    :return boolean:
    """
    if not member:
        return False
    for role in member.roles:
        if role.id in roles:
            return True
    return False

def google_auth():
    """
    authenticates with google api
    :return google auth creds:
    """
    creds = service_account.Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    return creds

def load_pending_reviews():
    """Load pending reviews from JSON file"""
    if os.path.exists(PENDING_REVIEWS_FILE):
        try:
            with open(PENDING_REVIEWS_FILE, "r") as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
        except json.JSONDecodeError:
            print(f"Error reading {PENDING_REVIEWS_FILE}, returning empty dict")
            return {}
    return {}

def load_instructor_tracking():
    """Load instructor tracking data from JSON file"""
    if os.path.exists(INSTRUCTOR_TRACKING_FILE):
        try:
            with open(INSTRUCTOR_TRACKING_FILE, "r") as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
        except json.JSONDecodeError:
            print(f"Error reading {INSTRUCTOR_TRACKING_FILE}, returning empty dict")
            return {}
    return {}


def increment_instructor_count(instructor_id: int, instructor_name: str, exam_type: str):
    """Increment the approval count for an instructor"""
    tracking = load_instructor_tracking()

    instructor_key = str(instructor_id)

    if instructor_key not in tracking:
        tracking[instructor_key] = {
            "name": instructor_name,
            "total": 0,
            "post": 0,
            "scene": 0,
            "aviation": 0
        }

    tracking[instructor_key]["name"] = instructor_name

    tracking[instructor_key]["total"] += 1
    tracking[instructor_key][exam_type] += 1

    with open(INSTRUCTOR_TRACKING_FILE, "w") as f:
        json.dump(tracking, f, indent=2)

    return tracking[instructor_key]["total"]


def reset_instructor_tracking():
    """Reset all instructor tracking data"""
    with open(INSTRUCTOR_TRACKING_FILE, "w") as f:
        json.dump({}, f, indent=2)

def get_instructor_stats(instructor_id: int = None):
    """Get instructor statistics. If instructor_id is None, return all stats"""
    tracking = load_instructor_tracking()

    if instructor_id is not None:
        return tracking.get(str(instructor_id))

    return tracking

# --- State Tracking ---
def load_state(sheet_state):
    if os.path.exists(sheet_state):
        try:
            with open(sheet_state, "r") as f:
                content = f.read().strip()
                if not content:
                    return {"last_timestamp": None}
                data = json.loads(content)
                # Handle old format (migrate from last_row to last_timestamp)
                if "last_row" in data and "last_timestamp" not in data:
                    return {"last_timestamp": None}
                return data
        except json.JSONDecodeError:
            print(f"Error reading sheet: {sheet_state}")
            return {"last_timestamp": None}
    return {"last_timestamp": None}

def save_state(sheet_state, state):
    with open(sheet_state, "w") as f:
        json.dump(state, f)

def get_new_rows(sheet, sheet_data):
    state = load_state(sheet)
    last_row = state.get("last_row", 1)
    new_rows = sheet_data[last_row:]
    return new_rows, len(sheet_data)

def parse_google_timestamp(timestamp_str):
    """
    Parse Google Sheets timestamp string to datetime object

    Handles formats like:
    - "3/18/2026 15:13:00"
    - "11/25/2025 10:30:45"
    """
    try:
        # Try MM/DD/YYYY HH:MM:SS format first (Google Sheets default)
        return datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
    except ValueError:
        try:
            # Try M/D/YYYY HH:MM:SS format (no leading zeros)
            return datetime.strptime(timestamp_str, "%-m/%-d/%Y %H:%M:%S")
        except ValueError:
            try:
                # Try with just date
                return datetime.strptime(timestamp_str, "%m/%d/%Y")
            except ValueError:
                print(f"Warning: Could not parse timestamp: {timestamp_str}")
                return None

def get_new_rows_by_timestamp(sheet_state, sheet_data, timestamp_column="Timestamp"):
    """
    Get new rows based on timestamp instead of row number

    Args:
        sheet_state: Path to state file
        sheet_data: Full sheet data including headers
        timestamp_column: Name of the timestamp column (default: "Timestamp")

    Returns:
        tuple: (new_rows, latest_timestamp)
    """
    if not sheet_data or len(sheet_data) < 2:
        return [], None

    state = load_state(sheet_state)
    last_timestamp = state.get("last_timestamp")

    headers = sheet_data[0]

    # Find timestamp column index
    try:
        timestamp_idx = headers.index(timestamp_column)
    except ValueError:
        print(f"Warning: '{timestamp_column}' column not found in headers")
        # Fallback to first column
        timestamp_idx = 0

    new_rows = []
    latest_timestamp = last_timestamp
    timestamp_dt = parse_google_timestamp(latest_timestamp) if latest_timestamp else None

    for row in sheet_data[1:]:  # Skip header row
        if len(row) <= timestamp_idx:
            continue

        try:
            row_timestamp = row[timestamp_idx]
            row_timestamp_dt = parse_google_timestamp(row_timestamp)

            if last_timestamp is None:
                new_rows.append(row)
                if timestamp_dt is None or row_timestamp_dt > timestamp_dt:
                    timestamp_dt = row_timestamp_dt
                    latest_timestamp = row_timestamp
                continue

            elif row_timestamp_dt > timestamp_dt:
                new_rows.append(row)
                if row_timestamp_dt > (timestamp_dt or datetime.min):
                    timestamp_dt = row_timestamp_dt
                    latest_timestamp = row_timestamp

        except (IndexError, TypeError) as e:
            print(f"Error processing row timestamp: {e}")
            continue

    return new_rows, latest_timestamp

def read_sheet(tab_name, sheet_id):
    try:
        creds = google_auth()
        service = build("sheets", "v4", credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=tab_name
        ).execute()
        return result.get("values", [])
    except HttpError as e:
        print(f"Sheet read error: {e}")
        return []
    finally:
        if service:
            service.close()

def cert_validator(db_cert, role_cert, roles):
    """
    Performs a check on if the user meets the requirements.
    Returns True or False

    :param db_cert:
    :param role_cert:
    :param roles:
    :return bool:
    """

    has_role = False
    has_db_cert = False

    if not roles:
        return False, "User has certification in Database but is not in RMA"

    for role in roles:
        if role.id == role_cert:
            has_role = True

    if db_cert == "TRUE":
        has_db_cert = True

    if has_db_cert and has_role:
        return True, ""
    if not has_db_cert and not has_role:
        return True, ""

    if has_db_cert and not has_role:
        return False, "Cert is in Database, does not have role in RMA"
    if not has_db_cert and has_role:
        return False, "Does not have cert in Database but has Role in RMA"

def split_field_value(value, max_length=1024):
    """
    Split a field value into chunks of max_length.
    Returns a list of chunks.
    """
    if len(value) <= max_length:
        return [value]

    chunks = []
    while value:
        if len(value) <= max_length:
            chunks.append(value)
            break

        split_pos = value.rfind(' ', 0, max_length)
        if split_pos == -1:
            split_pos = max_length

        chunks.append(value[:split_pos])
        value = value[split_pos:].lstrip()

    return chunks

def add_field_safe(embed, name, value, inline=False):
    """
    Safely add a field to an embed, splitting if necessary.
    Returns True if field was added successfully.
    """

    if len(name) > 256:
        name = name[:253] + "..."

    if len(value) <= 1024:
        embed.add_field(name=name, value=value, inline=inline)
        return True

    chunks = split_field_value(value, max_length=1024)

    for i, chunk in enumerate(chunks):
        if i == 0:
            embed.add_field(name=name, value=chunk, inline=inline)
        else:
            embed.add_field(name=f"{name} (cont.)", value=chunk, inline=inline)

    return True
