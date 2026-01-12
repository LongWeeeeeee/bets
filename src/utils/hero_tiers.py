"""
Hero Tier Lists: экспертные оценки героев по различным категориям.

Эти тиры основаны на экспертном знании меты, а не на статистике.
"""

from typing import Dict

# ============== HIGH GROUND DEFENSE TIERS ==============
# Герои, которые делают осаду базы невозможной/сложной
#
# Tier S (3.0): НЕВОЗМОЖНО зайти без Aegis
# Tier A (2.0): Очень сложно зайти, нужен идеальный тайминг
# Tier B (1.0): Хороший деф, но можно обойти
# Tier C (0.0): Ближники без отпуша, бесполезны в дефе

HG_DEFENSE_TIERS: Dict[int, float] = {
    # ============ TIER S (Gods) - Weight: 3.0 ============
    # Герои, против которых НЕВОЗМОЖНО зайти без Aegis
    35: 3.0,   # Sniper - Shrapnel + Take Aim = смерть на хайграунде
    34: 3.0,   # Tinker - March of the Machines + Laser spam
    80: 3.0,   # Techies - Mines everywhere, instant wipe
    113: 3.0,  # Arc Warden - Double Spark Wraith + Flux spam
    112: 3.0,  # Winter Wyvern - Splinter Blast + Cold Embrace saves
    33: 3.0,   # Enigma - Black Hole threat = can't group up
    97: 3.0,   # Magnus - Reverse Polarity threat + Shockwave spam
    
    # ============ TIER A (Strong) - Weight: 2.0 ============
    # Очень сложно зайти, нужен идеальный тайминг
    90: 2.0,   # Keeper of the Light - Illuminate spam, Blinding Light
    64: 2.0,   # Jakiro - Ice Path + Macropyre = area denial
    22: 2.0,   # Zeus - Arc Lightning + Thundergod's Wrath poke
    16: 2.0,   # Sand King - Epicenter + Sandstorm area control
    98: 2.0,   # Underlord - Firestorm + Pit of Malice
    7: 2.0,    # Earthshaker - Echo Slam threat in choke points
    40: 2.0,   # Venomancer - Plague Wards + Poison Nova
    6: 2.0,    # Drow Ranger - Multishot + Frost Arrows slow
    94: 2.0,   # Medusa - Stone Gaze + Split Shot = can't approach
    
    # ============ TIER B (Good) - Weight: 1.0 ============
    # Хороший деф, но можно обойти
    25: 1.0,   # Lina - Dragon Slave + Light Strike Array
    52: 1.0,   # Leshrac - Diabolic Edict + Split Earth
    13: 1.0,   # Puck - Dream Coil threat + Illusory Orb poke
    14: 1.0,   # Pudge - Hook threat from highground
    20: 1.0,   # Vengeful Spirit - Swap threat + Wave of Terror
    65: 1.0,   # Batrider - Firefly + Flaming Lasso threat
    26: 1.0,   # Lion - Earth Spike + Finger burst
    5: 1.0,    # Crystal Maiden - Freezing Field in choke
    37: 1.0,   # Warlock - Fatal Bonds + Chaotic Offering
    86: 1.0,   # Rubick - Stolen spell threat
    
    # ============ TIER C (Zero) - Weight: 0.0 ============
    # Ближники без отпуша, бесполезны в дефе HG
    93: 0.0,   # Slark - melee, needs to jump in
    32: 0.0,   # Riki - melee assassin, useless in def
    36: 0.0,   # Night Stalker - melee, no waveclear
    70: 0.0,   # Ursa - melee, no waveclear
    91: 0.0,   # Io - no waveclear, needs partner
    62: 0.0,   # Bounty Hunter - melee, no waveclear
    54: 0.0,   # Lifestealer - melee, no waveclear
}

# Default weight for heroes not in the list
HG_DEFENSE_DEFAULT: float = 0.5


def get_hg_defense_score(hero_id: int) -> float:
    """Returns High Ground Defense score for a hero."""
    return HG_DEFENSE_TIERS.get(hero_id, HG_DEFENSE_DEFAULT)


# ============== BURST DAMAGE TIERS ==============
# Герои с мгновенным уроном (нельзя отхилить)
#
# Tier S (3.0): Insta-Kill (100-0 за секунду)
# Tier A (2.0): Heavy Nukes (большой урон за короткое время)
# Tier B (1.0): Mixed (средний burst)
# Tier C (0.0): Slow Burn (урон растянут во времени, можно отхилить)

BURST_TIERS: Dict[int, float] = {
    # ============ TIER S (Insta-Kill) - Weight: 3.0 ============
    # 100-0 за секунду, невозможно среагировать
    25: 3.0,   # Lina - Laguna Blade + Light Strike Array combo
    26: 3.0,   # Lion - Finger of Death + Earth Spike + Hex
    44: 3.0,   # Phantom Assassin - Coup de Grace crit = instant death
    10: 3.0,   # Morphling - Shotgun (Ethereal + Adaptive Strike)
    55: 3.0,   # Nyx Assassin - Vendetta + Impale + Mana Burn combo
    19: 3.0,   # Tiny - Avalanche + Toss combo = instant kill
    80: 3.0,   # Techies - Proximity Mines = instant wipe
    
    # ============ TIER A (Heavy Nukes) - Weight: 2.0 ============
    # Большой урон за короткое время
    22: 2.0,   # Zeus - Thundergod's Wrath + Arc Lightning spam
    39: 2.0,   # Queen of Pain - Sonic Wave + Scream + Blink combo
    52: 2.0,   # Leshrac - Split Earth + Diabolic Edict burst
    101: 2.0,  # Skywrath Mage - Mystic Flare + Arcane Bolt spam
    11: 2.0,   # Shadow Fiend - Requiem of Souls + Razes
    7: 2.0,    # Earthshaker - Echo Slam = team wipe potential
    18: 2.0,   # Sven - God's Strength + Storm Hammer = one-shot
    74: 2.0,   # Invoker - Sun Strike + Meteor + Deafening Blast
    34: 2.0,   # Tinker - Laser + Rockets + Rearm spam
    69: 2.0,   # Doom - Doom + Infernal Blade = guaranteed kill
    
    # ============ TIER B (Mixed) - Weight: 1.0 ============
    # Средний burst, но не instant
    72: 1.0,   # Gyrocopter - Call Down + Rocket Barrage
    48: 1.0,   # Luna - Eclipse + Lucent Beam
    8: 1.0,    # Juggernaut - Omnislash burst
    47: 1.0,   # Viper - Viper Strike + Nethertoxin (mixed)
    43: 1.0,   # Death Prophet - Exorcism (sustained but high)
    17: 1.0,   # Storm Spirit - Ball Lightning + Overload burst
    106: 1.0,  # Ember Spirit - Sleight of Fist + Chains
    
    # ============ TIER C (Slow Burn) - Weight: 0.0 ============
    # Урон растянут во времени, можно отхилить/убежать
    40: 0.0,   # Venomancer - Poison Nova = slow DoT
    93: 0.0,   # Slark - needs time to stack Essence Shift
    67: 0.0,   # Spectre - Desolate + Dispersion = sustained
    12: 0.0,   # Phantom Lancer - illusion army = slow grind
    89: 0.0,   # Naga Siren - illusion army = slow push
    99: 0.0,   # Bristleback - Quill Spray stacks = slow damage
    61: 0.0,   # Broodmother - spiderlings = sustained pressure
    56: 0.0,   # Clinkz - Searing Arrows = sustained DPS
    81: 0.0,   # Chaos Knight - RNG-based, not reliable burst
    1: 0.0,    # Anti-Mage - Mana Void only, otherwise sustained
}

# Default weight for heroes not in the list
BURST_DEFAULT: float = 1.0


def get_burst_score(hero_id: int) -> float:
    """Returns Burst Damage score for a hero."""
    return BURST_TIERS.get(hero_id, BURST_DEFAULT)


# ============== SAVE & DISENGAGE TIERS ==============
# Герои, которые ломают инициацию врага и спасают союзников
#
# Tier S (3.0): Hard Save / Banish - герой исчезает из карты, урон не проходит
# Tier A (2.5): Reset / Global Protection - останавливает драку или дает бессмертие
# Tier B (1.5): Damage Mitigation / Soft Save - сильно снижает урон или меняет позицию
# Tier C (1.0): Counter-Initiation / Stop - сквозь БКБ останавливает атакующего

SAVE_TIERS: Dict[int, float] = {
    # ============ TIER S (Hard Save / Banish) - Weight: 3.0 ============
    # Герой исчезает из карты. Урон не проходит.
    79: 3.0,   # Shadow Demon - Disruption = полная неуязвимость
    76: 3.0,   # Outworld Destroyer - Astral Imprisonment = banish
    100: 3.0,  # Tusk - Snowball = прячет союзника внутри
    54: 3.0,   # Lifestealer - Infest = прячется внутри союзника
    110: 3.0,  # Phoenix - Supernova (Aghs прячет союзника в яйцо)
    145: 3.0,  # Ringmaster - Escape Act = полный banish
    20: 3.0,   # Vengeful Spirit - Nether Swap = 100% спасение
    
    # ============ TIER A (Reset / Global Protection) - Weight: 2.5 ============
    # Останавливает драку или дает бессмертие
    89: 2.5,   # Naga Siren - Song of the Siren = полный reset драки
    50: 2.5,   # Dazzle - Shallow Grave = бессмертие на 5 сек
    111: 2.5,  # Oracle - False Promise = отложенный урон + heal
    57: 2.5,   # Omniknight - Guardian Angel = физ. иммунитет
    112: 2.5,  # Winter Wyvern - Cold Embrace = heal + физ. иммунитет
    91: 2.5,   # Io - Relocate = глобальное спасение
    41: 2.5,   # Faceless Void - Chronosphere = может использоваться как стоп
    75: 2.5,   # Silencer - Global Silence = ломает все комбо врага
    
    # ============ TIER B (Damage Mitigation / Soft Save) - Weight: 1.5 ============
    # Сильно снижает урон или меняет позицию
    31: 1.5,   # Lich - Frost Shield = damage reduction
    102: 1.5,  # Abaddon - Aphotic Shield + Borrowed Time
    63: 1.5,   # Weaver - Time Lapse (Aghs на союзника)
    14: 1.5,   # Pudge - Dismember (Shard = Swallow ally)
    85: 1.5,   # Undying - Soul Rip heal + Tombstone
    45: 1.5,   # Pugna - Decrepify = magic immunity для физ. урона
    135: 1.5,  # Dawnbreaker - Solar Guardian = глобальный heal + stun
    5: 1.5,    # Crystal Maiden - Frostbite root + slow
    
    # ============ TIER C (Counter-Initiation / Stop) - Weight: 1.0 ============
    # Сквозь БКБ останавливает атакующего (чаще используется для атаки)
    33: 1.0,   # Enigma - Black Hole = BKB-pierce stop
    97: 1.0,   # Magnus - Reverse Polarity = BKB-pierce stop
    38: 1.0,   # Beastmaster - Primal Roar = BKB-pierce single target
    83: 1.0,   # Treant Protector - Overgrowth = BKB-pierce root
    37: 1.0,   # Warlock - Chaotic Offering = stun + golem
}

# Default weight for heroes not in the list
SAVE_DEFAULT: float = 0.0


def get_save_score(hero_id: int) -> float:
    """Returns Save & Disengage score for a hero."""
    return SAVE_TIERS.get(hero_id, SAVE_DEFAULT)


# ============== BIG ULTIMATE TIERS ==============
# Герои с долгими кулдаунами ультов (>100s), которые меняют игру
#
# Гипотеза: Много таких героев -> Ждут КД -> Пассивность -> ТМ
# Вес: 1.0 для всех (бинарный признак)

BIG_ULT_HEROES: set[int] = {
    33,   # Enigma - Black Hole (160s CD)
    19,   # Tidehunter - Ravage (150s CD)
    97,   # Magnus - Reverse Polarity (120s CD)
    41,   # Faceless Void - Chronosphere (140s CD)
    83,   # Treant Protector - Overgrowth (100s CD)
    75,   # Silencer - Global Silence (130s CD)
    7,    # Earthshaker - Echo Slam (110s CD)
    37,   # Warlock - Chaotic Offering (170s CD)
    110,  # Phoenix - Supernova (110s CD)
    94,   # Medusa - Stone Gaze (90s CD, but game-changing)
    89,   # Naga Siren - Song of the Siren (180s CD)
    112,  # Winter Wyvern - Winter's Curse (80s CD, but game-changing)
}


def get_big_ult_count(hero_id: int) -> int:
    """Returns 1 if hero has a big ultimate, 0 otherwise."""
    return 1 if hero_id in BIG_ULT_HEROES else 0
