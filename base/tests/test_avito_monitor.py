from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


def test_parse_avito_items_extracts_unique_listing_links() -> None:
    import avito_monitor

    html = """
    <html><body>
      <div data-marker="item">
        <a href="/naberezhnye_chelny/velosipedy/stels_navigator_1234567890?context=abc">
          Stels Navigator
        </a>
        <span data-marker="item-price">12 000 ₽</span>
      </div>
      <div data-marker="item">
        <a href="https://www.avito.ru/kazan/velosipedy/format_9876543210">
          Format 1412
        </a>
      </div>
      <a href="/naberezhnye_chelny/velosipedy/stels_navigator_1234567890">duplicate</a>
      <a href="/favorites">not a listing</a>
    </body></html>
    """

    items = avito_monitor.parse_avito_items(html, "https://www.avito.ru/search")

    assert [item.item_id for item in items] == ["1234567890", "9876543210"]
    assert items[0].title == "Stels Navigator"
    assert items[0].price == "12 000 ₽"
    assert items[0].url.startswith("https://www.avito.ru/")


def test_detect_avito_ip_block_message() -> None:
    import avito_monitor

    html = "<html><title>Доступ ограничен: проблема с IP</title><body>Продолжить для решения капчи</body></html>"

    assert avito_monitor._detect_avito_block(html) == "Avito ограничил доступ по IP"


def test_parse_avito_items_stops_before_other_cities_block() -> None:
    import avito_monitor

    html = """
    <html><body>
      <div data-marker="item">
        <a data-marker="item-title" href="/naberezhnye_chelny/velosipedy/local_1111111111">Local</a>
        <span data-marker="item-location">р-н Автозаводский</span>
      </div>
      <h2>1 918 объявлений есть в других городах</h2>
      <div data-marker="item">
        <a data-marker="item-title" href="/nizhnekamsk/velosipedy/other_2222222222">Other city</a>
      </div>
    </body></html>
    """

    items = avito_monitor.parse_avito_items(html, "https://www.avito.ru/naberezhnye_chelny/velosipedy")

    assert [item.item_id for item in items] == ["1111111111"]
    assert items[0].title == "Local"


def test_parse_avito_items_uses_catalog_serp_and_base_city() -> None:
    import avito_monitor

    html = """
    <html><body>
      <div data-marker="catalog-serp">
        <div data-marker="item">
          <a data-marker="item-title" href="/naberezhnye_chelny/velosipedy/local_1111111111">Local</a>
        </div>
        <div data-marker="item">
          <a data-marker="item-title" href="/kazan/velosipedy/other_2222222222">Other city</a>
        </div>
      </div>
      <section>
        <div data-marker="item">
          <a data-marker="item-title" href="/naberezhnye_chelny/velosipedy/carousel_3333333333">Carousel</a>
        </div>
      </section>
    </body></html>
    """

    items = avito_monitor.parse_avito_items(html, "https://www.avito.ru/naberezhnye_chelny/velosipedy")

    assert [item.item_id for item in items] == ["1111111111"]


def test_parse_avito_items_uses_city_count_from_page_title() -> None:
    import avito_monitor

    html = """
    <html><body>
      <div data-marker="page-title">
        <h1 data-marker="page-title/text">«Велосипед»: объявления для Набережных Челнов</h1>
        <span data-marker="page-title/count">2</span>
      </div>
      <div data-marker="item">
        <a data-marker="item-title" href="/naberezhnye_chelny/velosipedy/local_1111111111">Local 1</a>
      </div>
      <div data-marker="item">
        <a data-marker="item-title" href="/naberezhnye_chelny/velosipedy/local_2222222222">Local 2</a>
      </div>
      <div data-marker="item">
        <a data-marker="item-title" href="/nizhnekamsk/velosipedy/other_3333333333">Other city</a>
      </div>
    </body></html>
    """

    items = avito_monitor.parse_avito_items(html, "https://www.avito.ru/naberezhnye_chelny/velosipedy")

    assert [item.item_id for item in items] == ["1111111111", "2222222222"]


def test_merge_watch_items_requires_second_sighting_before_new_notification(tmp_path, monkeypatch) -> None:
    import avito_monitor

    state_path = tmp_path / "avito_state.json"
    lock_path = tmp_path / "avito_state.json.lock"
    monkeypatch.setattr(avito_monitor, "AVITO_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(avito_monitor, "AVITO_LOCK_PATH", lock_path, raising=False)

    ok, _message = avito_monitor.add_watch_url("https://www.avito.ru/naberezhnye_chelny/velosipedy")
    assert ok is True
    watch_id = avito_monitor._watch_id_for_url("https://www.avito.ru/naberezhnye_chelny/velosipedy")

    first_success, new_items = avito_monitor._merge_watch_items(
        watch_id,
        [avito_monitor.AvitoItem("1111111111", "https://www.avito.ru/naberezhnye_chelny/velosipedy/a_1111111111", "Old")],
    )
    assert first_success is True
    assert new_items == []

    first_success, new_items = avito_monitor._merge_watch_items(
        watch_id,
        [
            avito_monitor.AvitoItem("1111111111", "https://www.avito.ru/naberezhnye_chelny/velosipedy/a_1111111111", "Old"),
            avito_monitor.AvitoItem("2222222222", "https://www.avito.ru/naberezhnye_chelny/velosipedy/b_2222222222", "New"),
        ],
    )
    assert first_success is False
    assert new_items == []

    first_success, new_items = avito_monitor._merge_watch_items(
        watch_id,
        [
            avito_monitor.AvitoItem("1111111111", "https://www.avito.ru/naberezhnye_chelny/velosipedy/a_1111111111", "Old"),
            avito_monitor.AvitoItem("2222222222", "https://www.avito.ru/naberezhnye_chelny/velosipedy/b_2222222222", "New"),
        ],
    )
    assert first_success is False
    assert [item.item_id for item in new_items] == ["2222222222"]


def test_avito_state_add_list_remove_roundtrip(tmp_path, monkeypatch) -> None:
    import avito_monitor

    state_path = tmp_path / "avito_state.json"
    lock_path = tmp_path / "avito_state.json.lock"
    monkeypatch.setattr(avito_monitor, "AVITO_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(avito_monitor, "AVITO_LOCK_PATH", lock_path, raising=False)

    ok, message = avito_monitor.add_watch_url("www.avito.ru/kazan/velosipedy?q=test")

    assert ok is True
    assert "ссылка добавлена" in message
    listed = avito_monitor.format_watch_list()
    assert "id=" in listed
    assert "https://www.avito.ru/kazan/velosipedy?q=test" in listed

    ok, message = avito_monitor.remove_watch("1")

    assert ok is True
    assert "удалил" in message
    assert "пул пуст" in avito_monitor.format_watch_list()


def test_drain_telegram_admin_commands_extracts_avito_command(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _Response:
        status_code = 200

        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    def _fake_post(url, **_kwargs):
        if url.endswith("/getUpdates"):
            return _Response(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 301,
                            "message": {
                                "chat": {"id": 100},
                                "from": {"id": 100},
                                "text": "avito add https://www.avito.ru/kazan/velosipedy?q=test",
                            },
                        }
                    ],
                }
            )
        return _Response({"ok": True, "result": {"message_id": 1}})

    with functions.TELEGRAM_ADMIN_COMMANDS_LOCK:
        functions.TELEGRAM_PENDING_ADMIN_COMMANDS.clear()

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    commands = functions.drain_telegram_admin_commands(refresh=True)

    assert len(commands) == 1
    assert commands[0]["command"] == "avito"
    assert commands[0]["raw_text"].startswith("avito add")
