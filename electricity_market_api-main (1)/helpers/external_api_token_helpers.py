
import random
from urllib.parse import *
from cachetools import TTLCache
from helpers.metadata_helper import SecurityTokens
import threading

lock = threading.Lock()

# A TTLCache that holds “tokens on cooldown”
COOLDOWN_SECONDS = 10 * 60
entsoe_cooldown_cache = TTLCache(maxsize=len(SecurityTokens), ttl=COOLDOWN_SECONDS)

def get_available_tokens():
    """Return the tokens _not_ currently on cooldown."""
    with lock:
        return [t for t in SecurityTokens if t not in entsoe_cooldown_cache]

def mark_cooldown(token: str):
    """Put token into the cooldown cache."""
    with lock:
        entsoe_cooldown_cache[token] = True

def getRandomSecurityToken() -> str:

    '''Returns a random security token from the available tokens.
    If all tokens are on cooldown, raises a RuntimeError.'''

    avail = get_available_tokens()
    if not avail:
        # everybody’s cooling down
        raise RuntimeError("All ENTSOE tokens on cooldown; please retry in 10 minutes.")
    return random.choice(avail)



def replace_token(url: str, new_token: str, api_token_param_name: str) -> str:

    '''Replaces the security token in a URL with a new one.
    Args:
        url (str): The URL to modify.
        new_token (str): The new token to insert.
        api_token_param_name (str): The name of the parameter containing the token.'''

    # Parse the URL into components
    parts = urlparse(url)

    # Parse the query string into a dict of lists
    qs = parse_qs(parts.query, keep_blank_values=True)

    # Replace the token
    qs[api_token_param_name] = [new_token]

    #Re-encode the query
    new_query = urlencode(qs, doseq=True)

    # Build a new URL
    new_parts = parts._replace(query=new_query)

    return urlunparse(new_parts)


def extract_token(url: str, api_token_param_name: str) -> str:

    '''Extracts the security token from a URL.
    Args:
        url (str): The URL to extract the token from.
        api_token_param_name (str): The name of the parameter containing the token.'''
    
    #split off the query string
    qs = urlparse(url).query

    #parse the query string into a dictionary
    params = parse_qs(qs)

    #check if the securityToken key exists and has a value
    if api_token_param_name not in params or not params[api_token_param_name]:
        raise ValueError("No security token found in the URL")

    return params[api_token_param_name][0]