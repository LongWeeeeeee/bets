"""
Bookmaker presence check via OCR screenshots.
Fallback mode when HTML parsing fails.
"""

import time
import re
import os
import tempfile
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class OCRSiteResult:
    site: str
    match_found: bool
    status: str
    details: str
    matched_teams: Optional[Tuple[str, str]] = None


# Bookmaker URLs for OCR mode
BOOKMAKER_OCR_URLS = {
    "live": {
        "betboom": "https://betboom.ru/esport/live/dota-2",
        "pari": "https://pari.ru/esports-live/category/dota2",
        "winline": "https://winline.ru/stavki/sport/kibersport",
    },
    "all": {
        "betboom": "https://betboom.ru/esport/dota-2?period=all",
        "pari": "https://pari.ru/esports/category/dota2",
        "winline": "https://winline.ru/stavki/sport/kibersport",
    },
}


def _norm(s: str) -> str:
    """Normalize team name for comparison."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9а-я]+", " ", s.lower())).strip()


def _unique_team_names(names: List[str]) -> List[str]:
    """Deduplicate team names."""
    out = []
    seen = set()
    for raw in names:
        value = str(raw or "").strip()
        norm = _norm(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(value)
    return out


def _text_contains_teams(text: str, team1: str, team2: str, team1_aliases: List[str] = None, team2_aliases: List[str] = None) -> Tuple[bool, str, str]:
    """
    Check if text contains both teams.
    Returns (found, matched_team1, matched_team2).
    """
    candidates1 = _unique_team_names([team1] + (team1_aliases or []))
    candidates2 = _unique_team_names([team2] + (team2_aliases or []))

    text_lower = text.lower()

    for c1 in candidates1:
        for c2 in candidates2:
            if _norm(c1) == _norm(c2):
                continue

            # Check various patterns
            patterns = [
                f"{re.escape(c1.lower())}.{{0,100}}{re.escape(c2.lower())}",
                f"{re.escape(c2.lower())}.{{0,100}}{re.escape(c1.lower())}",
            ]

            for pattern in patterns:
                if re.search(pattern, text_lower):
                    return True, c1, c2

    return False, "", ""


def take_screenshots_with_scroll(driver, site_url: str, scroll_count: int = 5, tab_handle: str = None) -> List[str]:
    """
    Take screenshots while scrolling down the page.
    Returns list of screenshot paths.
    Optionally switch to a specific tab first.
    """
    screenshots = []
    tmp_dir = tempfile.mkdtemp(prefix="bookmaker_ocr_")

    try:
        # Switch to target tab if specified
        if tab_handle:
            try:
                driver.switch_to.window(tab_handle)
                time.sleep(1)
            except Exception:
                pass

        driver.get(site_url)
        time.sleep(4)  # Wait for initial load

        # Take screenshots while scrolling
        for i in range(scroll_count + 1):
            screenshot_path = os.path.join(tmp_dir, f"screenshot_{i}.png")
            try:
                driver.save_screenshot(screenshot_path)
                screenshots.append(screenshot_path)
            except Exception as e:
                print(f"⚠️ Screenshot error: {e}")

            if i < scroll_count:
                # Scroll down in chunks for better coverage
                try:
                    for _ in range(3):
                        driver.execute_script("window.scrollBy(0, 500);")
                        time.sleep(0.5)
                    time.sleep(1)  # Extra wait for rendering
                except Exception:
                    pass

    except Exception as e:
        print(f"⚠️ Screenshot error: {e}")

    return screenshots


def ocr_screenshot(screenshot_path: str, lang: str = "rus+eng") -> str:
    """
    Extract text from screenshot using EasyOCR (primary) or pytesseract (fallback).
    """
    # Try EasyOCR first - better accuracy for Russian text
    text = _ocr_easyocr(screenshot_path, lang)
    if text:
        return text

    # Fallback to pytesseract
    try:
        from PIL import Image
        import pytesseract

        img = Image.open(screenshot_path)
        # Map lang to tesseract language codes
        tess_lang = "rus+eng" if "rus" in lang else "eng"
        custom_config = r"--oem 3 --psm 6"
        text = pytesseract.image_to_string(img, lang=tess_lang, config=custom_config)
        return text.strip()
    except Exception as e:
        print(f"⚠️ pytesseract error for {screenshot_path}: {e}")
        return ""


def _ocr_easyocr(screenshot_path: str, lang: str = "rus+eng") -> str:
    """
    OCR using EasyOCR library - better accuracy for Russian text.
    Returns extracted text or empty string on failure.
    """
    try:
        import easyocr

        # Determine languages for EasyOCR
        easy_langs = ["ru", "en"] if "rus" in lang else ["en"]

        # Module-level reader cache (lazy init, reused across calls)
        if not hasattr(_ocr_easyocr, "reader"):
            _ocr_easyocr.reader = easyocr.Reader(easy_langs, gpu=False, verbose=False)

        results = _ocr_easyocr.reader.readtext(screenshot_path)

        # Combine all detected text
        all_text = " ".join([r[1] for r in results])
        return all_text.strip()

    except Exception as e:
        print(f"⚠️ EasyOCR error for {screenshot_path}: {e}")
        return ""


def check_bookmaker_presence_via_ocr(driver, site: str, url: str, team1: str, team2: str, team1_aliases: List[str] = None, team2_aliases: List[str] = None, tab_handle: str = None) -> OCRSiteResult:
    """
    Check if match is present on bookmaker site using OCR screenshots.
    Optionally switch to a specific tab first.
    """
    print(f"🔍 OCR presence check for {site}: {team1} vs {team2}")

    try:
        # Take screenshots while scrolling
        screenshots = take_screenshots_with_scroll(driver, url, scroll_count=3, tab_handle=tab_handle)

        if not screenshots:
            return OCRSiteResult(
                site=site,
                match_found=False,
                status="error",
                details="No screenshots captured"
            )

        # Extract text from all screenshots
        all_text = ""
        for i, screenshot in enumerate(screenshots):
            text = ocr_screenshot(screenshot)
            if text:
                all_text += text + "\n"
            # Cleanup screenshot
            try:
                os.unlink(screenshot)
            except Exception:
                pass

        if not all_text.strip():
            return OCRSiteResult(
                site=site,
                match_found=False,
                status="error",
                details="OCR returned empty text"
            )

        # Check for team names
        found, matched1, matched2 = _text_contains_teams(
            all_text, team1, team2,
            team1_aliases=team1_aliases or [],
            team2_aliases=team2_aliases or []
        )

        if found:
            return OCRSiteResult(
                site=site,
                match_found=True,
                status="ok",
                details=f"Found: {matched1} vs {matched2}",
                matched_teams=(matched1, matched2)
            )
        else:
            return OCRSiteResult(
                site=site,
                match_found=False,
                status="ok",
                details="Teams not found in OCR text"
            )

    except Exception as e:
        return OCRSiteResult(
            site=site,
            match_found=False,
            status="error",
            details=str(e)
        )


def check_all_bookmakers_via_ocr(driver, match_key: str, teams: Dict[str, List[str]], mode: str = "live") -> Dict[str, OCRSiteResult]:
    """
    Check presence on all bookmaker sites via OCR.

    Args:
        driver: Selenium WebDriver
        match_key: Match identifier
        teams: Dict with 'team1' and 'team2' keys, each containing list of [name, alias1, alias2...]
        mode: 'live' or 'all'

    Returns:
        Dict mapping site name to OCRSiteResult
    """
    results = {}

    urls = BOOKMAKER_OCR_URLS.get("live" if mode == "live" else "all", {})
    if not urls:
        return results

    team1 = teams.get("team1", [""])[0]
    team2 = teams.get("team2", [""])[0]
    team1_aliases = teams.get("team1", [])[1:]
    team2_aliases = teams.get("team2", [])[1:]

    if not team1 or not team2:
        return results

    for site, url in urls.items():
        result = check_bookmaker_presence_via_ocr(
            driver, site, url, team1, team2,
            team1_aliases=team1_aliases,
            team2_aliases=team2_aliases
        )
        results[site] = result

    return results
