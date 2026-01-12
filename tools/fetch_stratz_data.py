#!/usr/bin/env python3
"""
Fetch comprehensive hero data from Stratz GraphQL API.

Two main queries:
1. Constants (heroes, abilities, stats, roles)
2. Hero matchups (vs and with synergies)

Outputs:
- data/stratz_hero_dump.json - Raw API data
- data/hero_features_processed.json - Processed features for ML model
"""

import asyncio
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote

import aiohttp

# Import API keys from base
sys.path.insert(0, str(Path(__file__).parent.parent / "base"))
from keys import api_to_proxy

STRATZ_API_URL = "https://api.stratz.com/graphql"

# Keywords for ability classification
STUN_KEYWORDS = ["stun", "stunned", "stunning", "ministun"]
HEAL_KEYWORDS = ["heal", "heals", "healing", "restore", "restores", "regenerat"]
SLOW_KEYWORDS = ["slow", "slows", "slowing"]
SILENCE_KEYWORDS = ["silence", "silenced", "silencing", "mute"]
ROOT_KEYWORDS = ["root", "rooted", "rooting", "ensnare", "entangle"]
DISABLE_KEYWORDS = ["hex", "hexed", "sleep", "cyclone", "banish", "taunt"]

# Big ultimate keywords (teamfight-changing ults)
BIG_ULT_KEYWORDS = [
    "black hole", "ravage", "chronosphere", "echo slam", "reverse polarity",
    "song of the siren", "supernova", "reaper's scythe", "doom", "fiend's grip",
    "primal roar", "winter's curse", "static storm", "guardian angel",
    "global silence", "overgrowth", "chaotic offering", "nether swap"
]

# Known BKB-piercing abilities (Stratz doesn't have explicit flags)
# These are abilities that go through spell immunity
BKB_PIERCE_ABILITIES: Set[str] = {
    # Ultimates that pierce BKB
    "enigma_black_hole",
    "faceless_void_chronosphere", 
    "magnataur_reverse_polarity",
    "tidehunter_ravage",
    "beastmaster_primal_roar",
    "batrider_flaming_lasso",
    "pudge_dismember",
    "bane_fiends_grip",
    "axe_culling_blade",
    "doom_bringer_doom",
    "abaddon_borrowed_time",
    "winter_wyvern_winters_curse",
    # Regular abilities that pierce BKB
    "vengefulspirit_nether_swap",
    "magnus_skewer",
    "earth_spirit_boulder_smash",
    "tusk_walrus_punch",
    "tusk_walrus_kick",
    "snapfire_gobble_up",
    "primal_beast_onslaught",
    "primal_beast_pulverize",
    "marci_unleash",
}

# Heroes with global abilities
GLOBAL_ABILITY_HEROES: Set[int] = {
    22,   # Zeus (Thundergod's Wrath)
    32,   # Spectre (Haunt)
    43,   # Nature's Prophet (Teleportation, Wrath of Nature)
    91,   # Io (Relocate)
    68,   # Ancient Apparition (Ice Blast)
    14,   # Silencer (Global Silence)
    37,   # Treant Protector (Eyes in the Forest)
    112,  # Winter Wyvern (Arctic Burn vision)
    5,    # Crystal Maiden (Freezing Field global slow with talent)
    102,  # Abaddon (Mist Coil global with shard)
    135,  # Dawnbreaker (Solar Guardian)
}

# Escape artists - heroes with strong mobility/escape abilities
ESCAPE_HEROES: Set[int] = {
    1,    # Anti-Mage (Blink)
    39,   # Queen of Pain (Blink)
    63,   # Weaver (Shukuchi, Time Lapse)
    93,   # Slark (Pounce, Shadow Dance)
    13,   # Puck (Phase Shift, Illusory Orb)
    17,   # Storm Spirit (Ball Lightning)
    106,  # Ember Spirit (Fire Remnant)
    41,   # Faceless Void (Time Walk)
    10,   # Morphling (Waveform)
    32,   # Riki (Tricks of the Trade, Blink Strike)
    62,   # Bounty Hunter (Shadow Walk)
    56,   # Clinkz (Skeleton Walk)
    9,    # Mirana (Leap)
    16,   # Sand King (Burrowstrike)
    120,  # Pangolier (Rolling Thunder, Swashbuckle)
    12,   # Phantom Lancer (Doppelganger)
    47,   # Viper (can't be slowed)
    35,   # Sniper (not really escape but range)
    74,   # Invoker (Ghost Walk)
    119,  # Dark Willow (Shadow Realm)
}

# Save heroes - heroes with strong ally-saving abilities
SAVE_HEROES: Set[int] = {
    111,  # Oracle (False Promise, Fate's Edict)
    50,   # Dazzle (Shallow Grave)
    79,   # Shadow Demon (Disruption)
    76,   # Outworld Destroyer (Astral Imprisonment)
    112,  # Winter Wyvern (Cold Embrace)
    102,  # Abaddon (Aphotic Shield, Borrowed Time)
    57,   # Omniknight (Guardian Angel, Heavenly Grace)
    53,   # Io (Relocate, Tether)
    84,   # Ogre Magi (Bloodlust for escape speed)
    5,    # Crystal Maiden (not really save)
}

# Evasiveness rating (0-3): How hard is it to catch and kill this hero
# 3 = Impossible to catch, 2 = Hard to catch, 1 = Can survive, 0 = Sitting duck
EVASIVENESS_RATINGS: Dict[int, int] = {
    # Level 3: Impossible to catch
    1: 3,    # Anti-Mage (Blink, Counterspell)
    13: 3,   # Puck (Phase Shift, Orb, Coil)
    63: 3,   # Weaver (Shukuchi, Time Lapse)
    17: 3,   # Storm Spirit (Ball Lightning)
    106: 3,  # Ember Spirit (Remnants, Sleight)
    34: 3,   # Tinker (Rearm + BoTs)
    93: 3,   # Slark (Pounce, Shadow Dance, Dispel)
    32: 3,   # Riki (Permanent Invis, Tricks)
    10: 3,   # Morphling (Waveform, Replicate)
    126: 3,  # Void Spirit (Multiple dashes)
    
    # Level 2: Hard to catch
    89: 2,   # Naga Siren (Song, Illusions)
    12: 2,   # Phantom Lancer (Doppelganger, Illusions)
    9: 2,    # Mirana (Leap, Moonlight Shadow)
    39: 2,   # Queen of Pain (Blink)
    21: 2,   # Windranger (Windrun)
    55: 2,   # Dark Seer (Surge)
    62: 2,   # Bounty Hunter (Shadow Walk)
    56: 2,   # Clinkz (Skeleton Walk)
    73: 2,   # Nyx Assassin (Vendetta, Spiked Carapace)
    123: 2,  # Hoodwink (Scurry, Acorn)
    41: 2,   # Faceless Void (Time Walk)
    120: 2,  # Pangolier (Rolling Thunder)
    74: 2,   # Invoker (Ghost Walk)
    119: 2,  # Dark Willow (Shadow Realm)
    67: 2,   # Spectre (Spectral Dagger pathing)
    
    # Level 1: Can survive
    8: 1,    # Juggernaut (Blade Fury)
    54: 1,   # Lifestealer (Rage, Infest)
    111: 1,  # Oracle (False Promise)
    50: 1,   # Dazzle (Shallow Grave)
    112: 1,  # Winter Wyvern (Cold Embrace)
    102: 1,  # Abaddon (Borrowed Time)
    57: 1,   # Omniknight (Guardian Angel)
    79: 1,   # Shadow Demon (Disruption self)
    76: 1,   # Outworld Destroyer (Astral self)
    16: 1,   # Sand King (Burrowstrike escape)
    47: 1,   # Viper (Corrosive Skin slow immunity)
}


def build_heroes_query() -> str:
    """Build GraphQL query for hero constants."""
    return """
    {
        constants {
            heroes(language: ENGLISH) {
                id
                name
                displayName
                shortName
                stats {
                    startingArmor
                    startingMagicArmor
                    startingDamageMin
                    startingDamageMax
                    attackRange
                    attackRate
                    attackAnimationPoint
                    moveSpeed
                    moveTurnRate
                    hpRegen
                    mpRegen
                    primaryAttribute
                    strengthBase
                    strengthGain
                    intelligenceBase
                    intelligenceGain
                    agilityBase
                    agilityGain
                    visionDaytimeRange
                    visionNighttimeRange
                }
                roles {
                    roleId
                    level
                }
                abilities {
                    slot
                    abilityId
                }
            }
        }
    }
    """


def build_abilities_query() -> str:
    """Build GraphQL query for all abilities with full descriptions."""
    return """
    {
        constants {
            abilities(language: ENGLISH) {
                id
                name
                language {
                    displayName
                    description
                    lore
                    notes
                    aghanimDescription
                    shardDescription
                }
                stat {
                    manaCost
                    cooldown
                    castRange
                    damage
                    duration
                    isUltimate
                    hasScepterUpgrade
                }
            }
        }
    }
    """


def build_matchup_query(hero_id: int) -> str:
    """Build GraphQL query for hero matchups."""
    return f"""
    {{
        heroStats {{
            matchUp(
                heroId: {hero_id}
                bracketBasicIds: [DIVINE_IMMORTAL]
            ) {{
                heroId
                vs {{
                    heroId2
                    synergy
                    matchCount
                }}
                with {{
                    heroId2
                    synergy
                    matchCount
                }}
            }}
        }}
    }}
    """


class StratzAPIClient:
    """Async client for Stratz API with proxy rotation and rate limiting."""
    
    def __init__(self, api_to_proxy_dict: Dict[str, str]):
        self.credentials = list(api_to_proxy_dict.items())
        self.current_index = 0
        self.rate_limited_until: Dict[int, float] = {}
        
    def _get_headers(self, token: str, query: str) -> Dict[str, str]:
        """Build headers matching the working pattern from maps_research.py."""
        encoded_query = quote(query)
        referer = f"https://api.stratz.com/graphiql?query={encoded_query}"
        
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://api.stratz.com",
            "Referer": referer,
            "User-Agent": "STRATZ_API",
            "Authorization": f"Bearer {token}"
        }
    
    async def _get_available_credential(self) -> tuple[int, str, str]:
        """Get next available credential, waiting if all are rate limited."""
        now = time.time()
        
        for _ in range(len(self.credentials)):
            idx = self.current_index
            self.current_index = (self.current_index + 1) % len(self.credentials)
            
            if idx in self.rate_limited_until:
                if now < self.rate_limited_until[idx]:
                    continue
                else:
                    del self.rate_limited_until[idx]
            
            proxy_url, token = self.credentials[idx]
            return idx, proxy_url, token
        
        # All credentials rate limited
        min_wait = min(self.rate_limited_until.values()) - now
        if min_wait > 0:
            print(f"⏳ All credentials rate limited, waiting {min_wait:.1f}s...")
            await asyncio.sleep(min_wait + 1)
        
        now = time.time()
        self.rate_limited_until = {k: v for k, v in self.rate_limited_until.items() if v > now}
        
        idx = self.current_index
        self.current_index = (self.current_index + 1) % len(self.credentials)
        proxy_url, token = self.credentials[idx]
        return idx, proxy_url, token
    
    async def execute_query(
        self,
        query: str,
        max_retries: int = 5
    ) -> Optional[Dict[str, Any]]:
        """Execute a GraphQL query with retry logic."""
        for attempt in range(max_retries):
            idx, proxy_url, token = await self._get_available_credential()
            headers = self._get_headers(token, query)
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        STRATZ_API_URL,
                        proxy=proxy_url,
                        json={"query": query},
                        headers=headers,
                        ssl=False,
                        timeout=aiohttp.ClientTimeout(total=60)
                    ) as response:
                        data = await response.json()
                        
                        if isinstance(data, dict):
                            if data.get("message") == "API rate limit exceeded":
                                print(f"  ⛔ Rate limited on credential #{idx + 1}")
                                self.rate_limited_until[idx] = time.time() + 180
                                continue
                            
                            if "errors" in data:
                                print(f"  ⚠️ API error: {data['errors']}")
                                await asyncio.sleep(2 ** attempt)
                                continue
                        
                        return data
                        
            except asyncio.TimeoutError:
                print(f"  ⏱️ Timeout on attempt {attempt + 1}")
                await asyncio.sleep(2 ** attempt)
            except aiohttp.ClientError as e:
                print(f"  ❌ Request error: {e}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"  ❌ Unexpected error: {e}")
                await asyncio.sleep(2 ** attempt)
        
        return None


async def fetch_hero_constants(client: StratzAPIClient) -> Optional[Dict[str, Any]]:
    """Fetch all hero constants (stats, abilities, roles)."""
    print("\n📊 Fetching hero constants...")
    query = build_heroes_query()
    return await client.execute_query(query)


async def fetch_all_abilities(client: StratzAPIClient) -> Optional[Dict[int, Dict[str, Any]]]:
    """Fetch all abilities with full descriptions."""
    print("\n📜 Fetching all abilities...")
    query = build_abilities_query()
    response = await client.execute_query(query)
    
    if not response or "data" not in response:
        return None
    
    abilities_list = response.get("data", {}).get("constants", {}).get("abilities", [])
    
    # Convert to dict by ability ID
    abilities_dict: Dict[int, Dict[str, Any]] = {}
    for ab in abilities_list:
        ab_id = ab.get("id")
        if ab_id:
            abilities_dict[ab_id] = ab
    
    print(f"✅ Fetched {len(abilities_dict)} abilities")
    return abilities_dict


async def fetch_all_matchups(
    client: StratzAPIClient,
    hero_ids: List[int],
    delay: float = 0.3
) -> Dict[int, Dict[str, Any]]:
    """Fetch matchup data for all heroes."""
    print(f"\n🎯 Fetching matchups for {len(hero_ids)} heroes...")
    
    all_matchups: Dict[int, Dict[str, Any]] = {}
    
    for idx, hero_id in enumerate(hero_ids, 1):
        print(f"[{idx}/{len(hero_ids)}] Hero {hero_id}...", end=" ", flush=True)
        
        query = build_matchup_query(hero_id)
        response = await client.execute_query(query)
        
        if response and "data" in response:
            hero_stats = response.get("data", {}).get("heroStats", {})
            matchup_list = hero_stats.get("matchUp", [])
            
            if matchup_list and len(matchup_list) > 0:
                matchup_data = matchup_list[0]
                all_matchups[hero_id] = {
                    "vs": matchup_data.get("vs", []),
                    "with": matchup_data.get("with", [])
                }
                vs_count = len(matchup_data.get("vs", []))
                with_count = len(matchup_data.get("with", []))
                print(f"✓ vs:{vs_count} with:{with_count}")
            else:
                print("No data")
        else:
            print("FAILED")
        
        await asyncio.sleep(delay)
    
    return all_matchups


def check_ability_for_keywords(
    description: Any,
    keywords: List[str]
) -> bool:
    """Check if ability description contains any of the keywords."""
    if not description:
        return False
    
    # Handle list type (API sometimes returns arrays)
    if isinstance(description, list):
        description = " ".join(str(d) for d in description if d)
    
    if not isinstance(description, str):
        description = str(description)
    
    desc_lower = description.lower()
    return any(kw in desc_lower for kw in keywords)


def check_bkb_pierce(abilities: List[Dict[str, Any]]) -> bool:
    """Check if hero has any BKB-piercing abilities."""
    for ability_data in abilities:
        ability = ability_data.get("ability")
        if not ability or not isinstance(ability, dict):
            continue
        
        ability_name = ability.get("name", "")
        if ability_name in BKB_PIERCE_ABILITIES:
            return True
    
    return False


def check_big_ult(abilities: List[Dict[str, Any]]) -> bool:
    """
    Check if hero has a 'big ultimate' (teamfight-changing).
    Criteria: isUltimate=True AND (cooldown >= 100 OR matches big ult keywords)
    """
    for ability_data in abilities:
        ability = ability_data.get("ability")
        if not ability or not isinstance(ability, dict):
            continue
        
        stat = ability.get("stat") or {}
        is_ultimate = stat.get("isUltimate", False)
        
        if not is_ultimate:
            continue
        
        # Check cooldown (>= 100 seconds at max level)
        cooldown = stat.get("cooldown", [])
        if isinstance(cooldown, list) and cooldown:
            max_cd = max(cd for cd in cooldown if cd) if any(cooldown) else 0
        else:
            max_cd = cooldown if cooldown else 0
        
        # Check description for big ult keywords
        lang = ability.get("language") or {}
        desc = lang.get("description", "")
        if isinstance(desc, list):
            desc = " ".join(str(d) for d in desc if d)
        desc_lower = (desc or "").lower()
        
        ability_name = (ability.get("name") or "").lower()
        
        # Big ult if: high cooldown OR matches keywords
        has_big_cd = max_cd >= 100
        has_big_keyword = any(kw in desc_lower or kw.replace(" ", "_") in ability_name 
                             for kw in BIG_ULT_KEYWORDS)
        
        if has_big_cd or has_big_keyword:
            return True
    
    return False


def check_global_ability(
    hero_id: int,
    abilities: List[Dict[str, Any]]
) -> bool:
    """
    Check if hero has a global ability.
    Uses known list + checks for 'global' keyword in descriptions.
    """
    # Check known global heroes
    if hero_id in GLOBAL_ABILITY_HEROES:
        return True
    
    # Check ability descriptions for 'global' keyword
    for ability_data in abilities:
        ability = ability_data.get("ability")
        if not ability or not isinstance(ability, dict):
            continue
        
        lang = ability.get("language") or {}
        desc = lang.get("description", "")
        if isinstance(desc, list):
            desc = " ".join(str(d) for d in desc if d)
        
        aghs_desc = lang.get("aghanimDescription") or ""
        full_text = f"{desc} {aghs_desc}".lower()
        
        # Check for global indicators
        if "global" in full_text or "anywhere on the map" in full_text:
            return True
    
    return False


def calculate_burst_rating(abilities: List[Dict[str, Any]]) -> float:
    """
    Calculate burst damage rating from abilities.
    
    Formula: sum(damage) / sum(cooldown) for damaging abilities
    """
    total_damage = 0.0
    total_cooldown = 0.0
    
    for ability_data in abilities:
        ability = ability_data.get("ability", {})
        if not ability:
            continue
            
        stat = ability.get("stat", {})
        if not stat:
            continue
        
        damage = stat.get("damage")
        cooldown = stat.get("cooldown")
        
        # Handle arrays (different levels)
        if isinstance(damage, list) and damage:
            damage = max(damage)  # Max level damage
        if isinstance(cooldown, list) and cooldown:
            cooldown = min(cd for cd in cooldown if cd and cd > 0) if any(cooldown) else None
        
        if damage and cooldown and cooldown > 0:
            total_damage += float(damage)
            total_cooldown += float(cooldown)
    
    if total_cooldown > 0:
        return round(total_damage / total_cooldown, 2)
    return 0.0


def get_stun_duration(abilities: List[Dict[str, Any]]) -> float:
    """Get maximum stun duration from abilities."""
    max_duration = 0.0
    
    for ability_data in abilities:
        ability = ability_data.get("ability")
        if not ability or not isinstance(ability, dict):
            continue
        
        lang = ability.get("language")
        if not lang or not isinstance(lang, dict):
            continue
        
        desc = lang.get("description", "") or ""
        
        if check_ability_for_keywords(desc, STUN_KEYWORDS):
            stat = ability.get("stat", {})
            duration = stat.get("duration")
            
            if isinstance(duration, list) and duration:
                # Filter valid numeric values
                valid_durations = []
                for d in duration:
                    try:
                        if d is not None:
                            valid_durations.append(float(d))
                    except (ValueError, TypeError):
                        pass
                duration = max(valid_durations) if valid_durations else 0
            
            try:
                duration = float(duration) if duration else 0
                if duration > max_duration:
                    max_duration = duration
            except (ValueError, TypeError):
                pass
    
    return max_duration


# Role ID mapping from Stratz (can be string or int)
ROLE_ID_MAP: Dict[str, str] = {
    "CARRY": "Carry",
    "ESCAPE": "Escape",
    "NUKER": "Nuker",
    "INITIATOR": "Initiator",
    "DURABLE": "Durable",
    "DISABLER": "Disabler",
    "JUNGLER": "Jungler",
    "SUPPORT": "Support",
    "PUSHER": "Pusher",
    # Also support numeric IDs as strings
    "0": "Carry",
    "1": "Escape",
    "2": "Nuker",
    "3": "Initiator",
    "4": "Durable",
    "5": "Disabler",
    "6": "Jungler",
    "7": "Support",
    "8": "Pusher"
}


def process_hero_features(
    heroes_data: List[Dict[str, Any]],
    matchups_data: Dict[int, Dict[str, Any]],
    abilities_data: Optional[Dict[int, Dict[str, Any]]] = None
) -> Dict[int, Dict[str, Any]]:
    """
    Process raw hero data into ML-ready features.
    
    Args:
        heroes_data: Raw hero constants from API
        matchups_data: Raw matchup data from API
        abilities_data: Optional dict of ability_id -> ability details
        
    Returns:
        Dict mapping hero_id to processed features
    """
    processed: Dict[int, Dict[str, Any]] = {}
    abilities_data = abilities_data or {}
    
    for hero in heroes_data:
        hero_id = hero.get("id")
        if not hero_id:
            continue
        
        # Get hero's ability IDs and look up full ability data
        hero_ability_refs = hero.get("abilities", []) or []
        abilities: List[Dict[str, Any]] = []
        
        for ab_ref in hero_ability_refs:
            ab_id = ab_ref.get("abilityId")
            if ab_id and ab_id in abilities_data:
                abilities.append({"ability": abilities_data[ab_id], "slot": ab_ref.get("slot")})
        
        # Extract roles
        roles: List[str] = []
        role_levels: Dict[str, int] = {}
        for role_data in hero.get("roles", []) or []:
            role_id = role_data.get("roleId")
            level = role_data.get("level", 0)
            if role_id is not None:
                # Convert to string for lookup (API returns string like 'CARRY')
                role_key = str(role_id).upper()
                if role_key in ROLE_ID_MAP:
                    role_name = ROLE_ID_MAP[role_key]
                    if level >= 1:  # Only include if level >= 1
                        roles.append(role_name)
                        role_levels[role_name] = level
        
        # Check ability keywords
        has_stun = False
        has_heal = False
        has_slow = False
        has_silence = False
        has_root = False
        has_disable = False
        stun_duration = 0.0
        
        for ability_data in abilities:
            ability = ability_data.get("ability")
            if not ability or not isinstance(ability, dict):
                continue
            
            lang = ability.get("language")
            if not lang or not isinstance(lang, dict):
                continue
            
            desc = lang.get("description", "") or ""
            notes = lang.get("notes", "") or ""
            full_text = f"{desc} {notes}"
            
            if check_ability_for_keywords(full_text, STUN_KEYWORDS):
                has_stun = True
            if check_ability_for_keywords(full_text, HEAL_KEYWORDS):
                has_heal = True
            if check_ability_for_keywords(full_text, SLOW_KEYWORDS):
                has_slow = True
            if check_ability_for_keywords(full_text, SILENCE_KEYWORDS):
                has_silence = True
            if check_ability_for_keywords(full_text, ROOT_KEYWORDS):
                has_root = True
            if check_ability_for_keywords(full_text, DISABLE_KEYWORDS):
                has_disable = True
        
        stun_duration = get_stun_duration(abilities)
        
        # Note: stun_duration validation removed - API doesn't always provide duration
        # has_stun is based on keyword detection in ability descriptions
        
        # Calculate burst rating
        burst_rating = calculate_burst_rating(abilities)
        
        # Extract stats
        stats = hero.get("stats", {}) or {}
        
        # Process matchups
        hero_matchups: Dict[str, float] = {}
        hero_synergies: Dict[str, float] = {}
        
        if hero_id in matchups_data:
            matchup_info = matchups_data[hero_id]
            
            # VS matchups (counter-picks)
            for vs_entry in matchup_info.get("vs", []) or []:
                opponent_id = vs_entry.get("heroId2")
                synergy = vs_entry.get("synergy")
                match_count = vs_entry.get("matchCount", 0)
                
                if opponent_id and synergy is not None and match_count >= 50:
                    hero_matchups[str(opponent_id)] = round(synergy, 4)
            
            # WITH synergies
            for with_entry in matchup_info.get("with", []) or []:
                ally_id = with_entry.get("heroId2")
                synergy = with_entry.get("synergy")
                match_count = with_entry.get("matchCount", 0)
                
                if ally_id and synergy is not None and match_count >= 50:
                    hero_synergies[str(ally_id)] = round(synergy, 4)
        
        processed[hero_id] = {
            "hero_id": hero_id,
            "name": hero.get("displayName", ""),
            "short_name": hero.get("shortName", ""),
            
            # Roles
            "roles": roles,
            "role_levels": role_levels,
            "is_carry": "Carry" in roles,
            "is_support": "Support" in roles,
            "is_nuker": "Nuker" in roles,
            "is_disabler": "Disabler" in roles,
            "is_initiator": "Initiator" in roles,
            "is_durable": "Durable" in roles,
            "is_pusher": "Pusher" in roles,
            
            # Ability flags
            "has_stun": has_stun,
            "has_heal": has_heal,
            "has_slow": has_slow,
            "has_silence": has_silence,
            "has_root": has_root,
            "has_disable": has_disable,
            "stun_duration": stun_duration,
            
            # Elite features
            "has_bkb_pierce": check_bkb_pierce(abilities),
            "has_big_ult": check_big_ult(abilities),
            "has_global": check_global_ability(hero_id, abilities),
            
            # Super features (melee, escape, save, evasiveness)
            "is_melee": stats.get("attackRange", 600) <= 300,
            "has_escape": hero_id in ESCAPE_HEROES,
            "is_save_hero": hero_id in SAVE_HEROES,
            "evasiveness_rating": EVASIVENESS_RATINGS.get(hero_id, 0),
            
            # Damage
            "burst_damage_rating": burst_rating,
            
            # Base stats
            "attack_range": stats.get("attackRange", 0),
            "attack_rate": stats.get("attackRate", 0),
            "move_speed": stats.get("moveSpeed", 0),
            "starting_armor": stats.get("startingArmor", 0),
            "starting_magic_armor": stats.get("startingMagicArmor", 0),
            "primary_attribute": stats.get("primaryAttribute", ""),
            "str_base": stats.get("strengthBase", 0),
            "str_gain": stats.get("strengthGain", 0),
            "agi_base": stats.get("agilityBase", 0),
            "agi_gain": stats.get("agilityGain", 0),
            "int_base": stats.get("intelligenceBase", 0),
            "int_gain": stats.get("intelligenceGain", 0),
            
            # Matchups
            "matchups": hero_matchups,
            "synergies": hero_synergies
        }
    
    return processed


async def main_async() -> None:
    """Main async entry point."""
    print("=" * 60)
    print("Stratz Ultimate Hero Data Fetcher")
    print("=" * 60)
    
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "stratz_hero_dump.json"
    
    # Check if cached dump exists
    if raw_path.exists():
        print(f"📂 Found cached dump at {raw_path}")
        print("   Loading from cache (use --force to refetch)...")
        
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_dump = json.load(f)
        
        heroes_data = raw_dump.get("heroes", [])
        abilities_data = {int(k): v for k, v in raw_dump.get("abilities", {}).items()}
        matchups_data = {int(k): v for k, v in raw_dump.get("matchups", {}).items()}
        
        print(f"✅ Loaded {len(heroes_data)} heroes, {len(abilities_data)} abilities, {len(matchups_data)} matchups")
    else:
        # Fetch fresh data
        if not api_to_proxy:
            print("ERROR: No API credentials found in base/keys.py")
            return
        
        print(f"Found {len(api_to_proxy)} API credentials")
        
        client = StratzAPIClient(api_to_proxy)
        
        # Step 1: Fetch hero constants
        constants_response = await fetch_hero_constants(client)
        
        if not constants_response or "data" not in constants_response:
            print("❌ Failed to fetch hero constants")
            return
        
        heroes_data = constants_response.get("data", {}).get("constants", {}).get("heroes", [])
        
        if not heroes_data:
            print("❌ No heroes data in response")
            return
        
        print(f"✅ Fetched {len(heroes_data)} heroes")
        
        # Step 2: Fetch all abilities with full descriptions
        abilities_data = await fetch_all_abilities(client)
        
        # Extract hero IDs for matchup queries
        hero_ids = [h["id"] for h in heroes_data if h.get("id")]
        
        # Step 3: Fetch matchups for all heroes
        matchups_data = await fetch_all_matchups(client, hero_ids, delay=0.3)
        
        print(f"✅ Fetched matchups for {len(matchups_data)} heroes")
        
        # Save raw dump
        raw_dump = {
            "heroes": heroes_data,
            "abilities": {str(k): v for k, v in (abilities_data or {}).items()},
            "matchups": {str(k): v for k, v in matchups_data.items()},
            "fetched_at": datetime.utcnow().isoformat()
        }
        
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(raw_dump, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Saved raw dump to {raw_path}")
    
    # Process features (always runs)
    processed = process_hero_features(heroes_data, matchups_data, abilities_data)
    
    processed_path = output_dir / "hero_features_processed.json"
    with open(processed_path, "w", encoding="utf-8") as f:
        json.dump(processed, f, indent=2, ensure_ascii=False)
    print(f"💾 Saved processed features to {processed_path}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Total heroes: {len(processed)}")
    
    stun_count = sum(1 for h in processed.values() if h.get("has_stun"))
    heal_count = sum(1 for h in processed.values() if h.get("has_heal"))
    matchup_count = sum(len(h.get("matchups", {})) for h in processed.values())
    synergy_count = sum(len(h.get("synergies", {})) for h in processed.values())
    
    print(f"  Heroes with stun: {stun_count}")
    print(f"  Heroes with heal: {heal_count}")
    print(f"  Total matchup entries: {matchup_count}")
    print(f"  Total synergy entries: {synergy_count}")
    print("=" * 60)


def main() -> None:
    """Main entry point."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
