import os
import re
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

import instagram
import youtube
import tiktok

_SAFE_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]{1,128}$')

# Порядок обхода площадок в одном запуске. Threads первым — он самый
# быстрый и не зависит от внешнего токена в базе. YouTube/TikTok уже
# рабочие, но публикуются с ограничениями, пока не прошли аудит площадки
# (см. youtube.py / tiktok.py) — это ограничение самих площадок, не наше.
PUBLISH_ORDER = ("threads", "instagram", "youtube", "tiktok")

GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
TIKTOK_CLIENT_KEY    = os.environ.get("TIKTOK_CLIENT_KEY", "")
TIKTOK_CLIENT_SECRET = os.environ.get("TIKTOK_CLIENT_SECRET", "")

# ─── Логирование ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)


# ─── Настройки из окружения ──────────────────────────────────────────────────
THREADS_USER_ID   = os.environ.get("THREADS_USER_ID", "")
THREADS_TOKEN     = os.environ.get("THREADS_TOKEN", "")
WORKSPACE_API_URL = os.environ.get("WORKSPACE_API_URL", "")
WORKSPACE_API_KEY = os.environ.get("WORKSPACE_API_KEY", "")


# ─── Timezone ────────────────────────────────────────────────────────────────
# Публикация идёт по окну 07:30–20:00 МСК (см. kanban-board.tsx) — используем
# ту же зону здесь, чтобы дневная очистка/статистика были на неё завязаны.
TZ = ZoneInfo("Europe/Moscow")


# ─── Threads API ─────────────────────────────────────────────────────────────
API_BASE            = "https://graph.threads.net/v1.0"
API_TIMEOUT_SEC     = 15
WAIT_BEFORE_PUBLISH = 3
WAIT_BETWEEN_PARTS  = 2
MAX_POST_LEN        = 500

# Пауза между постами (не частями одного поста — см. WAIT_BETWEEN_PARTS выше)
# внутри одного прогона. Если триггер долго не срабатывал (например, истёкший
# токен на cron-job.org) и накопилось несколько просроченных постов, они не
# должны выйти пачкой почти одновременно — это выглядит как спам в ленте.
WAIT_BETWEEN_POSTS  = 45

# Подстраховка от тихого зависания: если job падает по таймауту GitHub Actions
# ровно во время публикации этого поста, report_failure не успевает
# вызваться, слот освобождается по истечении аренды, и попытка тихо
# повторяется каждый прогон. Раз в несколько часов — это уже не "подождать
# ещё", а сломанная публикация, которую пора показать в UI, а не пытаться
# бесконечно.
STALE_QUEUE_HOURS  = 3


# ─── Retry ───────────────────────────────────────────────────────────────────
RETRY_MAX_ATTEMPTS  = 5
RETRY_BASE_DELAY    = 5
RETRY_BACKOFF       = 3


# ─── Статистика вовлечённости ────────────────────────────────────────────────
STATS_WINDOW_DAYS   = 10  # опрашивать публикации не старше N дней — лайки/охваты набираются за первые дни


# ═════════════════════════════════════════════════════════════════════════════
#  Валидация
# ═════════════════════════════════════════════════════════════════════════════
def validate_config():
    missing = [v for v, val in [
        ("THREADS_USER_ID",   THREADS_USER_ID),
        ("THREADS_TOKEN",     THREADS_TOKEN),
        ("WORKSPACE_API_URL", WORKSPACE_API_URL),
        ("WORKSPACE_API_KEY", WORKSPACE_API_KEY),
    ] if not val]
    if missing:
        log.error(f"❌ Не заданы переменные окружения: {', '.join(missing)}")
        raise SystemExit(1)


def validate_threads_token():
    log.info("🔑 Проверяю токен Threads...")
    r = requests.get(
        f"{API_BASE}/me",
        params={"access_token": THREADS_TOKEN, "fields": "id,username"},
        timeout=API_TIMEOUT_SEC,
    )
    if r.status_code == 200:
        data = r.json()
        log.info(f"✅ Threads токен валиден. Аккаунт: @{data.get('username')} (id={data.get('id')})")
    else:
        log.error(f"❌ Threads токен невалиден ({r.status_code}): {r.text[:200]}")
        raise SystemExit(1)


# ═════════════════════════════════════════════════════════════════════════════
#  HTTP с retry
# ═════════════════════════════════════════════════════════════════════════════
def request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    last_exc = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            r = requests.request(method, url, **kwargs)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                delay = RETRY_BASE_DELAY * (RETRY_BACKOFF ** (attempt - 1))
                log.warning(f"⏳ HTTP {r.status_code} (попытка {attempt}/{RETRY_MAX_ATTEMPTS}). Жду {delay}с...")
                time.sleep(delay)
                continue
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            delay = RETRY_BASE_DELAY * (RETRY_BACKOFF ** (attempt - 1))
            log.warning(f"⏳ Сетевая ошибка: {e} (попытка {attempt}/{RETRY_MAX_ATTEMPTS}). Жду {delay}с...")
            time.sleep(delay)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"API недоступен после {RETRY_MAX_ATTEMPTS} попыток")


# ═════════════════════════════════════════════════════════════════════════════
#  Workspace API
# ═════════════════════════════════════════════════════════════════════════════
def load_ready_posts(platform: str) -> list[dict]:
    """Посты с queued_at, которым эта площадка ещё не закрыта.

    Уже опубликованные на ней отсекает сервер — пост остаётся в ready,
    пока не закрыты все его площадки, и без этого фильтра публикатор
    брал бы один и тот же пост повторно.
    """
    r = request_with_retry(
        "GET",
        f"{WORKSPACE_API_URL}/api/v1/posts",
        params={"status": "ready", "platform": platform},
        headers={"X-API-Key": WORKSPACE_API_KEY},
        timeout=API_TIMEOUT_SEC,
    )
    return r.json().get("posts", [])


def delete_published_posts(now: datetime):
    """Удалить посты, опубликованные до сегодняшней полуночи."""
    try:
        today_midnight = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=TZ)
        r = request_with_retry(
            "DELETE",
            f"{WORKSPACE_API_URL}/api/v1/posts",
            headers={"X-API-Key": WORKSPACE_API_KEY},
            params={"before": today_midnight.isoformat()},
            timeout=API_TIMEOUT_SEC,
        )
        deleted = r.json().get("deleted", 0)
        log.info(f"🗑️ Очистка: удалено {deleted} постов опубликованных до {today_midnight.strftime('%d.%m')}")
    except Exception as e:
        log.error(f"❌ Ошибка очистки опубликованных постов: {e}")


def claim_platform(post_id: str, platform: str) -> bool:
    """Забрать слот (пост, площадка). False — слот уже занят или закрыт.

    Замок на уровне площадки, а не поста: у одного поста их несколько, и
    обнуление queued_at (как было раньше) спрятало бы пост от остальных
    публикаторов.
    """
    # Do not retry an ambiguous claim: it may already have reached the server.
    response = requests.patch(
        f"{WORKSPACE_API_URL}/api/v1/posts/{post_id}",
        headers={"X-API-Key": WORKSPACE_API_KEY},
        json={"claim_platform": platform},
        timeout=API_TIMEOUT_SEC,
    )
    if response.status_code == 409:
        return False
    response.raise_for_status()
    return True


def report_published(post_id: str, platform: str, external_ids: list[str] = None):
    """Отчитаться об успешной публикации на площадке.

    Пост уходит в архив и статус published только когда закрыта последняя
    из выбранных площадок — это решает record_platform_publish на сервере.
    """
    try:
        request_with_retry(
            "PATCH",
            f"{WORKSPACE_API_URL}/api/v1/posts/{post_id}",
            headers={"X-API-Key": WORKSPACE_API_KEY},
            json={
                "platform": platform,
                "published_at": datetime.now(tz=TZ).isoformat(),
                "external_ids": external_ids or [],
            },
            timeout=API_TIMEOUT_SEC,
        )
    except Exception as e:
        log.error(f"❌ Не удалось отметить {platform} у поста {post_id}: {e}")
        raise


def report_failure(post_id: str, platform: str, error: str):
    """Пометить площадку упавшей и снять пост с очереди.

    Слот остаётся повторяемым, но queued_at гасим: пост с ошибкой должен
    ждать решения человека, а не перезапускаться каждые пять минут. Уже
    закрытые площадки этого поста при повторной постановке в очередь
    заново не публикуются — их слоты остались published.
    """
    # posts.publish_error — одно поле на пост, а площадок теперь четыре: без
    # префикса ошибка одной площадки молча стирает уже показанную ошибку
    # другой, и на доске не видно, к какой площадке она вообще относится.
    tagged_error = f"[{platform}] {error}"[:500]
    try:
        request_with_retry(
            "PATCH",
            f"{WORKSPACE_API_URL}/api/v1/posts/{post_id}",
            headers={"X-API-Key": WORKSPACE_API_KEY},
            json={"platform": platform, "publish_error": tagged_error},
            timeout=API_TIMEOUT_SEC,
        )
    except Exception as e:
        log.error(f"❌ Не удалось записать ошибку {platform} у поста {post_id}: {e}")
    clear_queued(post_id)


def load_social_token(platform: str) -> dict | None:
    """Токен площадки из базы приложения. None — аккаунт не подключён."""
    response = requests.get(
        f"{WORKSPACE_API_URL}/api/v1/social-tokens",
        params={"platform": platform},
        headers={"X-API-Key": WORKSPACE_API_KEY},
        timeout=API_TIMEOUT_SEC,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json().get("token")


def store_social_token(platform: str, access_token: str, expires_at: str | None, refresh_token: str | None = None):
    body = {"platform": platform, "access_token": access_token, "expires_at": expires_at}
    if refresh_token:
        body["refresh_token"] = refresh_token
    request_with_retry(
        "PATCH",
        f"{WORKSPACE_API_URL}/api/v1/social-tokens",
        headers={"X-API-Key": WORKSPACE_API_KEY},
        json=body,
        timeout=API_TIMEOUT_SEC,
    )


def clear_queued(post_id: str, error: str | None = None):
    """Сбросить queued_at у поста при ошибке публикации. Записывает причину ошибки."""
    payload: dict = {"queued_at": None}
    if error:
        payload["publish_error"] = error[:500]
    try:
        request_with_retry(
            "PATCH",
            f"{WORKSPACE_API_URL}/api/v1/posts/{post_id}",
            headers={"X-API-Key": WORKSPACE_API_KEY},
            json=payload,
            timeout=API_TIMEOUT_SEC,
        )
    except Exception as e:
        log.error(f"❌ Не удалось сбросить queued_at у {post_id}: {e}")


# ═════════════════════════════════════════════════════════════════════════════
#  Парсинг контента
# ═════════════════════════════════════════════════════════════════════════════
def parse_spoilers(content: str) -> tuple[str, list[dict] | None]:
    """Конвертирует ||спойлер|| → (plain_text, text_entities)."""
    entities = []
    plain = ""
    offset = 0
    for m in re.finditer(r'\|\|([^|]+)\|\|', content):
        plain += content[offset:m.start()]
        entities.append({
            "entity_type": "SPOILER",
            "offset": len(plain),
            "length": len(m.group(1)),
        })
        plain += m.group(1)
        offset = m.end()
    plain += content[offset:]
    return plain, (entities if entities else None)


def parse_thread(text: str) -> list[str]:
    """Разбить текст на части ветки по строкам начинающимся с '!'."""
    lines = text.split('\n')
    parts = []
    current = []
    for line in lines:
        if line.startswith('!'):
            if current:
                parts.append('\n'.join(current).strip())
            current = [line[1:].strip()]
        else:
            current.append(line)
    if current:
        parts.append('\n'.join(current).strip())
    parts = [p for p in parts if p]
    return parts if parts else [text.strip()]


# ═════════════════════════════════════════════════════════════════════════════
#  Threads API
# ═════════════════════════════════════════════════════════════════════════════
def is_video_url(url: str) -> bool:
    return url.lower().split("?")[0].endswith((".mp4", ".mov", ".webm"))


def create_media_container(url: str) -> str | None:
    """Создать дочерний контейнер для карусели (одно фото/видео)."""
    video = is_video_url(url)
    body = {
        "media_type": "VIDEO" if video else "IMAGE",
        ("video_url" if video else "image_url"): url,
        "is_carousel_item": "true",
    }
    r = request_with_retry(
        "POST",
        f"{API_BASE}/{THREADS_USER_ID}/threads",
        params={"access_token": THREADS_TOKEN},
        data=body,
        timeout=API_TIMEOUT_SEC,
    )
    cid = r.json().get("id")
    if cid and video:
        log.info(f"🎬 Видео-контейнер {cid} создан, жду обработки...")
        if not wait_for_container_ready(cid):
            return None
    return cid


def create_container(
    text: str,
    reply_to_id: str = None,
    media_items: list[dict] = None,
) -> str | None:
    """
    `media_items` — [{"url": str, "spoiler": bool}, ...] для одной ветки.
    is_spoiler_media задаётся один раз на контейнер (одиночное фото/видео) или
    на карусель целиком — Threads не поддерживает пометку отдельных элементов
    карусели по отдельности, поэтому если спойлер стоит хоть у одного вложения
    в ветке, спойлером помечается вся карусель.
    """
    plain_text, entities = parse_spoilers(text)
    media_items = media_items or []
    media_urls = [m["url"] for m in media_items]
    is_spoiler_media = any(m.get("spoiler") for m in media_items)

    if len(media_urls) == 1:
        url = media_urls[0]
        video = is_video_url(url)
        params = {
            "media_type": "VIDEO" if video else "IMAGE",
            ("video_url" if video else "image_url"): url,
            "text": plain_text,
            "access_token": THREADS_TOKEN,
        }
        if is_spoiler_media:
            params["is_spoiler_media"] = True
    elif len(media_urls) > 1:
        # Параллельно, а не по очереди: видео на стороне Threads обрабатывается
        # долго (десятки секунд — единицы минут), и при последовательном создании
        # первые карточки карусели успевают "протухнуть" за то время, пока
        # дожидаемся обработки последних видео — Threads потом отвечает [24]
        # "The requested resource does not exist" на попытке собрать карусель.
        log.info(f"⏳ Создаём {len(media_urls)} медиаконтейнеров параллельно...")
        with ThreadPoolExecutor(max_workers=len(media_urls)) as pool:
            child_ids = list(pool.map(create_media_container, media_urls))
        if None in child_ids:
            log.error("❌ Не удалось создать один из медиа-контейнеров карусели")
            return None
        params = {
            "media_type": "CAROUSEL",
            "children": ",".join(child_ids),
            "text": plain_text,
            "access_token": THREADS_TOKEN,
        }
        if is_spoiler_media:
            params["is_spoiler_media"] = True
    else:
        params = {
            "media_type": "TEXT",
            "text": plain_text,
            "access_token": THREADS_TOKEN,
        }

    if entities:
        params["text_entities"] = entities  # dict, не строка — JSON-тело не кодирует # как %23
    if reply_to_id:
        params["reply_to_id"] = reply_to_id

    # Явно задаём topic_tag из первого #хэштега, чтобы Threads не вырезал # из текста
    topic_match = re.search(r'#(\w+)', plain_text)
    if topic_match:
        params["topic_tag"] = topic_match.group(1).lower()

    # access_token идёт в URL, тело — JSON (иначе form-encoding кодирует # → %23 и Threads его обрезает)
    token = params.pop("access_token")
    hashtags_count = len(re.findall(r'#\w+', plain_text))
    log.info(f"📤 Текст: {len(plain_text)} симв., хэштегов: {hashtags_count}, topic_tag: {params.get('topic_tag') or 'нет'}, entities: {'да' if params.get('text_entities') else 'нет'}")
    r = request_with_retry(
        "POST",
        f"{API_BASE}/{THREADS_USER_ID}/threads",
        params={"access_token": token},
        json=params,
        timeout=API_TIMEOUT_SEC,
    )
    cid = r.json().get("id")
    # Одиночное видео и любая карусель (даже из одних фото) требуют, чтобы Threads
    # закончил сборку контейнера — иначе threads_publish отвечает "media ... cannot
    # be found", хотя id только что был выдан при создании.
    needs_wait = len(media_urls) > 1 or (len(media_urls) == 1 and is_video_url(media_urls[0]))
    if cid and needs_wait:
        log.info(f"🎬 Контейнер {cid} создан, жду готовности перед публикацией...")
        if not wait_for_container_ready(cid):
            return None
    return cid


def wait_for_container_ready(container_id: str, timeout_sec: int = 180, poll_sec: int = 5) -> bool:
    """Ждёт пока Threads обработает медиа-контейнер (нужно для видео)."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = request_with_retry(
                "GET",
                f"{API_BASE}/{container_id}",
                params={"fields": "status,error_message", "access_token": THREADS_TOKEN},
                timeout=API_TIMEOUT_SEC,
            )
            data = r.json()
            status = data.get("status")
            log.info(f"⏳ Контейнер {container_id}: статус = {status}")
            if status == "FINISHED":
                return True
            if status in ("ERROR", "EXPIRED"):
                log.error(f"❌ Контейнер {container_id}: {status} — {data.get('error_message', '')}")
                return False
        except Exception as e:
            log.warning(f"⚠️ Опрос статуса контейнера: {e}")
        time.sleep(poll_sec)
    log.error(f"❌ Контейнер {container_id} не готов за {timeout_sec}с")
    return False


def publish_container(container_id: str) -> str | None:
    r = request_with_retry(
        "POST",
        f"{API_BASE}/{THREADS_USER_ID}/threads_publish",
        params={"creation_id": container_id, "access_token": THREADS_TOKEN},
        timeout=API_TIMEOUT_SEC,
    )
    return r.json().get("id")


def publish_parts(parts: list[str], media: list[dict] = None) -> tuple[list[str] | None, str | None]:
    """
    Опубликовать одну или несколько частей ветки.
    `media` — список {"url": str, "branch": int, "spoiler": bool}, branch — номер
    ветки (1-indexed, как в UI). Каждой части достаются только медиа с её
    собственным branch.
    Возвращает (thread_ids, error_message). error_message = None при успехе.
    """
    try:
        post_ids = []
        prev_id = None
        for i, text in enumerate(parts):
            branch_no = i + 1
            part_media = [
                {"url": m["url"], "spoiler": bool(m.get("spoiler"))}
                for m in (media or []) if m.get("branch") == branch_no
            ]
            container_id = create_container(text, reply_to_id=prev_id, media_items=part_media)
            if not container_id:
                return None, "Threads API не вернул ID контейнера"
            time.sleep(WAIT_BEFORE_PUBLISH)
            post_id = publish_container(container_id)
            if not post_id:
                return None, "Threads API не вернул ID публикации"
            log.info(f"✅ Часть {i+1}/{len(parts)} опубликована! ID: {post_id}")
            post_ids.append(post_id)
            prev_id = post_id
            if i < len(parts) - 1:
                time.sleep(WAIT_BETWEEN_PARTS)
        return post_ids, None
    except requests.HTTPError as e:
        try:
            err_data = e.response.json().get("error", {})
            msg = err_data.get("error_user_msg") or err_data.get("message") or str(e)
            code = err_data.get("code")
            subcode = err_data.get("error_subcode")
            # Путь без query string — там лежит access_token, его нельзя писать
            # ни в publish_error (показывается пользователю в UI), ни в лог.
            failed_path = e.response.url.split("?")[0] if e.response is not None else None
            tag = f"{code}/{subcode}" if subcode else str(code) if code else None
            if tag:
                msg = f"[{tag}] {msg}"
            if failed_path:
                msg = f"{msg} ({failed_path})"
            log.error(f"🔎 Полный ответ Threads: {e.response.text[:1000] if e.response is not None else 'n/a'}")
        except Exception:
            msg = str(e)
        log.error(f"❌ Ошибка публикации (HTTP): {msg}")
        return None, msg
    except (requests.RequestException, RuntimeError) as e:
        log.error(f"❌ Ошибка публикации: {e}")
        return None, str(e)


# ═════════════════════════════════════════════════════════════════════════════
#  Статистика вовлечённости (архив "залетевших" постов и "стволов")
# ═════════════════════════════════════════════════════════════════════════════
def load_recent_publications(since: datetime) -> list[dict]:
    """Публикации за последние STATS_WINDOW_DAYS дней, у которых есть Threads ID."""
    r = request_with_retry(
        "GET",
        f"{WORKSPACE_API_URL}/api/v1/publications",
        params={"since": since.isoformat()},
        headers={"X-API-Key": WORKSPACE_API_KEY},
        timeout=API_TIMEOUT_SEC,
    )
    return r.json().get("publications", [])


def get_post_insights(threads_post_id: str) -> tuple[int, int]:
    """Лайки и просмотры одной публикации Threads. (0, 0) при ошибке."""
    try:
        r = request_with_retry(
            "GET",
            f"{API_BASE}/{threads_post_id}/insights",
            params={"metric": "views,likes", "access_token": THREADS_TOKEN},
            timeout=API_TIMEOUT_SEC,
        )
        likes = 0
        views = 0
        for metric in r.json().get("data", []):
            values = metric.get("values") or []
            total = values[0].get("value", 0) if values else metric.get("total_value", {}).get("value", 0)
            if metric.get("name") == "likes":
                likes = total
            elif metric.get("name") == "views":
                views = total
        return likes, views
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить статистику {threads_post_id}: {e}")
        return 0, 0


def update_publication_stats(pub_id: str, likes: int, views: int):
    try:
        request_with_retry(
            "PATCH",
            f"{WORKSPACE_API_URL}/api/v1/publications/{pub_id}",
            headers={"X-API-Key": WORKSPACE_API_KEY},
            json={"likes": likes, "views": views, "stats_checked_at": datetime.now(tz=TZ).isoformat()},
            timeout=API_TIMEOUT_SEC,
        )
    except Exception as e:
        log.error(f"❌ Не удалось обновить статистику публикации {pub_id}: {e}")


def check_engagement():
    """Раз в день опрашивает лайки/охваты по всем публикациям за последние
    STATS_WINDOW_DAYS дней — и для 'обычных' постов (на случай, что залетели),
    и для 'стволов' (branch_count > 2), чтобы копить статистику для анализа."""
    log.info("📊 Опрос статистики вовлечённости")

    validate_config()
    validate_threads_token()

    since = datetime.now(tz=TZ) - timedelta(days=STATS_WINDOW_DAYS)
    publications = load_recent_publications(since)
    log.info(f"🔎 Публикаций для опроса: {len(publications)}")

    checked = 0
    for pub in publications:
        pub_id = pub.get("id")
        threads_ids = pub.get("threads_ids") or []
        if not threads_ids:
            continue
        try:
            # Для тредов из нескольких частей суммируем лайки/просмотры по всем частям.
            total_likes = 0
            total_views = 0
            for tid in threads_ids:
                likes, views = get_post_insights(tid)
                total_likes += likes
                total_views += views
                time.sleep(1)
            update_publication_stats(pub_id, total_likes, total_views)
            checked += 1
        except Exception as e:
            log.exception(f"❌ Ошибка опроса публикации {str(pub_id)[:8]}...: {e}")

    log.info(f"🏁 Готово. Проверено: {checked}")


# ═════════════════════════════════════════════════════════════════════════════
#  Главный цикл
# ═════════════════════════════════════════════════════════════════════════════
def due_posts(platform: str, now: datetime) -> list[dict]:
    """Посты этой площадки, которым уже пора публиковаться."""
    result = []
    for post in load_ready_posts(platform):
        post_id = str(post.get("id"))
        queued_at_str = post.get("queued_at")
        if not queued_at_str:
            continue
        queued_at = datetime.fromisoformat(queued_at_str.replace("Z", "+00:00"))
        if queued_at.tzinfo is None:
            queued_at = queued_at.replace(tzinfo=TZ)
        if now < queued_at:
            continue  # ещё рано
        if not _SAFE_ID_RE.match(post_id):
            log.error(f"❌ Небезопасный post_id: {post_id[:40]!r} — пропускаем")
            continue
        result.append(post)
    return result


def publish_threads_post(post: dict) -> bool:
    post_id = post["id"]
    parts = parse_thread(post.get("content") or "")
    media = post.get("media") or []

    too_long = [i + 1 for i, t in enumerate(parts) if len(t) > MAX_POST_LEN]
    if too_long:
        max_len = max(len(t) for t in parts)
        msg = f"Слишком длинная часть {too_long} ({max_len} симв., максимум {MAX_POST_LEN})"
        log.warning(f"⚠️ Пост {post_id[:8]}...: {msg} — пропускаем")
        report_failure(post_id, "threads", msg)
        return False

    log.info(f"🕐 Threads: публикуем {post_id[:8]}... ({len(parts)} частей)")
    # Слот забираем до обращения к API площадки: если упадём между
    # успешной публикацией и отчётом, слот останется занятым, и повтор
    # случится не раньше чем через 15 минут — время заметить дубль.
    if not claim_platform(post_id, "threads"):
        return False

    threads_ids, pub_error = publish_parts(parts, media=media or None)
    if threads_ids:
        log.info("Threads IDs for %s: %s", post_id, threads_ids)
        report_published(post_id, "threads", threads_ids)
        log.info(f"✅ Threads: пост {post_id[:8]}... опубликован")
        return True

    log.error(f"❌ Threads: пост {post_id[:8]}...: {pub_error or 'ошибка публикации'}")
    report_failure(post_id, "threads", pub_error or "ошибка публикации")
    return False


def publish_instagram_post(post: dict, account_id: str, token: str) -> bool:
    post_id = post["id"]
    media = post.get("media") or []
    video_url = next((m.get("url") for m in media if is_video_url(m.get("url") or "")), None)
    if not video_url:
        msg = "Для рилса нужно видео, а в посте его нет"
        log.warning(f"⚠️ Instagram: пост {post_id[:8]}... — {msg}")
        report_failure(post_id, "instagram", msg)
        return False

    # Подпись — весь текст поста: ветки Threads склеиваем, чтобы ничего не
    # потерялось, в рилсе они всё равно не имеют смысла по отдельности.
    caption = "\n\n".join(parse_thread(post.get("content") or ""))

    log.info(f"🕐 Instagram: публикуем {post_id[:8]}...")
    if not claim_platform(post_id, "instagram"):
        return False

    media_id, error = instagram.publish_reel(account_id, token, video_url, caption, log)
    if media_id:
        report_published(post_id, "instagram", [media_id])
        log.info(f"✅ Instagram: рилс {post_id[:8]}... опубликован")
        return True

    log.error(f"❌ Instagram: пост {post_id[:8]}...: {error}")
    report_failure(post_id, "instagram", error or "ошибка публикации")
    return False


def instagram_account() -> tuple[str, str] | None:
    """(account_id, токен) либо None, если Instagram не подключён.

    Заодно продлевает токен, когда до истечения осталось немного: у
    Instagram он живёт 60 дней, и продлевать его может только тот, кто
    умеет записать новый — секрет workflow себя переписать не может.
    """
    try:
        stored = load_social_token("instagram")
    except Exception as e:
        log.error(f"❌ Не удалось получить токен Instagram: {e}")
        return None
    if not stored:
        return None

    account_id = stored.get("account_id")
    access_token = stored.get("access_token")
    if not account_id or not access_token:
        log.error("❌ Instagram подключён не полностью: нет account_id или токена")
        return None

    expires_at = stored.get("expires_at")
    if not expires_at:
        return account_id, access_token

    try:
        days_left = (datetime.fromisoformat(expires_at.replace("Z", "+00:00")) - datetime.now(tz=TZ)).days
    except ValueError:
        return account_id, access_token
    if days_left > instagram.REFRESH_WHEN_DAYS_LEFT:
        return account_id, access_token

    new_token, expires_in, error = instagram.refresh_token(access_token)
    if error:
        # Не фатально: старый токен ещё жив, публикуем на нём.
        log.error(f"❌ {error} (осталось {days_left} дн.)")
        return account_id, access_token

    new_expires = (
        (datetime.now(tz=TZ) + timedelta(seconds=expires_in)).isoformat() if expires_in else None
    )
    try:
        store_social_token("instagram", new_token, new_expires)
    except Exception as e:
        # Продлённый токен не сохранился — старый ещё валиден, работаем на
        # нём и попробуем сохранить в следующий запуск.
        log.error(f"❌ Токен Instagram продлён, но не сохранён: {e}")
        return account_id, access_token

    log.info(f"🔑 Токен Instagram продлён (оставалось {days_left} дн.)")
    return account_id, new_token


def publish_youtube_post(post: dict, channel_id: str, token: str) -> bool:
    post_id = post["id"]
    media = post.get("media") or []
    video_url = next((m.get("url") for m in media if is_video_url(m.get("url") or "")), None)
    if not video_url:
        msg = "Для Shorts нужно видео, а в посте его нет"
        log.warning(f"⚠️ YouTube: пост {post_id[:8]}... — {msg}")
        report_failure(post_id, "youtube", msg)
        return False

    parts = parse_thread(post.get("content") or "")
    full_text = "\n\n".join(parts)
    # Заголовок — первая строка (YouTube режет длинные заголовки в интерфейсе),
    # описание — весь текст, чтобы ничего не потерялось.
    title = parts[0].split("\n")[0].strip() or "Reels"

    log.info(f"🕐 YouTube: публикуем {post_id[:8]}...")
    if not claim_platform(post_id, "youtube"):
        return False

    video_id, error = youtube.upload_short(token, video_url, title, full_text, log)
    if video_id:
        report_published(post_id, "youtube", [video_id])
        log.info(f"✅ YouTube: Shorts {post_id[:8]}... опубликован")
        return True

    log.error(f"❌ YouTube: пост {post_id[:8]}...: {error}")
    report_failure(post_id, "youtube", error or "ошибка публикации")
    return False


def publish_tiktok_post(post: dict, open_id: str, token: str) -> bool:
    post_id = post["id"]
    media = post.get("media") or []
    video_url = next((m.get("url") for m in media if is_video_url(m.get("url") or "")), None)
    if not video_url:
        msg = "Для TikTok нужно видео, а в посте его нет"
        log.warning(f"⚠️ TikTok: пост {post_id[:8]}... — {msg}")
        report_failure(post_id, "tiktok", msg)
        return False

    caption = "\n\n".join(parse_thread(post.get("content") or ""))

    log.info(f"🕐 TikTok: публикуем {post_id[:8]}...")
    if not claim_platform(post_id, "tiktok"):
        return False

    publish_id, error = tiktok.publish_video(token, video_url, caption, log)
    if publish_id:
        report_published(post_id, "tiktok", [publish_id])
        log.info(f"✅ TikTok: видео {post_id[:8]}... опубликовано")
        return True

    log.error(f"❌ TikTok: пост {post_id[:8]}...: {error}")
    report_failure(post_id, "tiktok", error or "ошибка публикации")
    return False


def youtube_account() -> tuple[str, str] | None:
    """(channel_id, access_token) либо None, если YouTube не подключён.

    access_token у Google живёт всего час — обновляем его почти на каждый
    запуск, а не когда истекает: дешевле одного лишнего запроса, зато без
    гонки между "ещё жив" и "уже протух" на границе часа.
    """
    try:
        stored = load_social_token("youtube")
    except Exception as e:
        log.error(f"❌ Не удалось получить токен YouTube: {e}")
        return None
    if not stored:
        return None

    channel_id = stored.get("account_id")
    refresh = stored.get("refresh_token")
    if not channel_id or not refresh:
        log.error("❌ YouTube подключён не полностью: нет account_id или refresh_token")
        return None
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        log.error("❌ GOOGLE_CLIENT_ID/GOOGLE_CLIENT_SECRET не заданы — обновить токен YouTube нечем")
        return None

    new_token, expires_in, error = youtube.refresh_token(refresh, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET)
    if error:
        log.error(f"❌ {error}")
        return None

    new_expires = (datetime.now(tz=TZ) + timedelta(seconds=expires_in)).isoformat() if expires_in else None
    try:
        store_social_token("youtube", new_token, new_expires)
    except Exception as e:
        log.error(f"❌ Токен YouTube обновлён, но не сохранён: {e}")
    return channel_id, new_token


def tiktok_account() -> tuple[str, str] | None:
    """(open_id, access_token) либо None, если TikTok не подключён.

    access_token у TikTok живёт ~24 часа, но refresh_token каждый раз меняется —
    в отличие от YouTube, здесь важно не потерять новый и записать его сразу.
    """
    try:
        stored = load_social_token("tiktok")
    except Exception as e:
        log.error(f"❌ Не удалось получить токен TikTok: {e}")
        return None
    if not stored:
        return None

    open_id = stored.get("account_id")
    refresh = stored.get("refresh_token")
    if not open_id or not refresh:
        log.error("❌ TikTok подключён не полностью: нет account_id или refresh_token")
        return None
    if not TIKTOK_CLIENT_KEY or not TIKTOK_CLIENT_SECRET:
        log.error("❌ TIKTOK_CLIENT_KEY/TIKTOK_CLIENT_SECRET не заданы — обновить токен TikTok нечем")
        return None

    new_token, new_refresh, expires_in, error = tiktok.refresh_token(refresh, TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET)
    if error:
        log.error(f"❌ {error}")
        return None

    new_expires = (datetime.now(tz=TZ) + timedelta(seconds=expires_in)).isoformat() if expires_in else None
    try:
        store_social_token("tiktok", new_token, new_expires, refresh_token=new_refresh)
    except Exception as e:
        # Не подстраховаться нельзя: старый refresh_token TikTok уже аннулировал.
        log.error(f"❌ Токен TikTok обновлён, но не сохранён — следующий запуск не сможет обновиться: {e}")
        return None
    return open_id, new_token


def run():
    log.info("🚀 Scheduler: запуск")

    validate_config()
    validate_threads_token()

    now = datetime.now(tz=TZ)

    # Ежедневная очистка опубликованных постов — на первом часе вечернего окна
    # (18:xx больше не подходит: скрипт теперь не крутится вне окон публикации).
    if now.hour == 19:
        delete_published_posts(now)

    published = 0
    # Пауза нужна только между реально ушедшими постами, чтобы после
    # простоя триггера они не вышли пачкой почти одновременно.
    paced = False

    for platform in PUBLISH_ORDER:
        posts = due_posts(platform, now)
        if not posts:
            continue

        account = None
        if platform in ("instagram", "youtube", "tiktok"):
            account = {"instagram": instagram_account, "youtube": youtube_account, "tiktok": tiktok_account}[platform]()
            if not account:
                log.info(f"ℹ️ {platform} не подключён — {len(posts)} постов ждут")
                continue

        for post in posts:
            post_id = post.get("id")
            queued_at_str = post.get("queued_at")
            if queued_at_str:
                queued_at = datetime.fromisoformat(queued_at_str.replace("Z", "+00:00"))
                if queued_at.tzinfo is None:
                    queued_at = queued_at.replace(tzinfo=TZ)
                if now - queued_at > timedelta(hours=STALE_QUEUE_HOURS):
                    log.error(f"❌ {platform}: пост {post_id[:8]}... висит в очереди дольше {STALE_QUEUE_HOURS}ч — снимаю")
                    report_failure(post_id, platform, f"Публикация не завершилась за {STALE_QUEUE_HOURS}ч подряд — вероятно, зависает job, проверьте вручную")
                    continue
            try:
                if paced:
                    log.info(f"⏳ Пауза {WAIT_BETWEEN_POSTS}с перед следующим постом...")
                    time.sleep(WAIT_BETWEEN_POSTS)
                if platform == "threads":
                    ok = publish_threads_post(post)
                elif platform == "instagram":
                    ok = publish_instagram_post(post, account[0], account[1])
                elif platform == "youtube":
                    ok = publish_youtube_post(post, account[0], account[1])
                else:
                    ok = publish_tiktok_post(post, account[0], account[1])
                if ok:
                    published += 1
                    paced = True
            except Exception as e:
                log.exception(f"❌ Непредвиденная ошибка ({platform}, пост {str(post_id)[:8]}...): {e}")
                try:
                    report_failure(post_id, platform, f"Проверьте площадку перед повтором: {e}")
                except Exception:
                    pass

    log.info(f"🏁 Готово. Опубликовано: {published}")


if __name__ == "__main__":
    check_engagement() if "--stats" in sys.argv else run()
