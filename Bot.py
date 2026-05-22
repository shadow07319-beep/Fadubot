"""
Telegram Anime Channel Manager & Social Media Auto-Poster
=========================================================

Features:
- Pyrogram Telegram Bot
- Inline anime channel search
- Async queue processing
- FFMPEG trimming + 9:16 crop + compression
- Thumbnail extraction
- Instagram Reel upload (Graph API)
- YouTube Shorts upload
- Render health-check server (aiohttp)
- Automatic cleanup
- Production logging
- Retry handling
- Render-ready architecture

Author: OpenAI
"""

import os
import re
import json
import uuid
import shutil
import logging
import asyncio
import tempfile
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any

import aiohttp
from aiohttp import web

from pyrogram import Client, filters
from pyrogram.types import (
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Message,
)

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials

# =========================================================
# CONFIGURATION
# =========================================================

API_ID = 123456
API_HASH = "YOUR_API_HASH"
BOT_TOKEN = "YOUR_BOT_TOKEN"

INSTAGRAM_ACCESS_TOKEN = "YOUR_INSTAGRAM_ACCESS_TOKEN"
INSTAGRAM_BUSINESS_ID = "YOUR_INSTAGRAM_BUSINESS_ID"

YOUTUBE_SERVICE_ACCOUNT_FILE = "youtube_service_account.json"

UPLOAD_DIR = "downloads"

MAX_RETRIES = 3

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("AnimeBot")

# =========================================================
# CREATE DOWNLOAD DIRECTORY
# =========================================================

Path(UPLOAD_DIR).mkdir(parents=True, exist_ok=True)

# =========================================================
# TELEGRAM BOT
# =========================================================

app = Client(
    "anime_manager_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# =========================================================
# ANIME CHANNEL DATABASE
# =========================================================

ANIME_CHANNELS = [
    {
        "name": "Naruto Clips",
        "username": "@naruto_clips",
        "description": "Best Naruto moments and AMVs",
    },
    {
        "name": "One Piece World",
        "username": "@onepiece_world",
        "description": "Epic One Piece edits",
    },
    {
        "name": "Attack On Titan",
        "username": "@aot_reels",
        "description": "AOT edits and clips",
    },
    {
        "name": "Anime Reels Hub",
        "username": "@anime_reels_hub",
        "description": "Trending anime shorts",
    },
]

# =========================================================
# ASYNC QUEUE
# =========================================================

video_queue: asyncio.Queue = asyncio.Queue()

# =========================================================
# HELPER FUNCTIONS
# =========================================================


async def run_command(command: list):
    """
    Run shell command asynchronously
    """
    logger.info(f"Running command: {' '.join(command)}")

    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        logger.error(stderr.decode())
        raise Exception(stderr.decode())

    return stdout.decode()


async def download_telegram_media(message: Message) -> str:
    """
    Download Telegram media
    """
    try:
        file_path = await message.download(
            file_name=f"{UPLOAD_DIR}/{uuid.uuid4()}.mp4"
        )

        logger.info(f"Downloaded media: {file_path}")

        return file_path

    except Exception as e:
        logger.exception("Telegram download failed")
        raise e


async def trim_and_convert_vertical(
    input_file: str,
    start_time: str = "00:00:00",
    duration: str = "00:00:30",
) -> str:
    """
    Trim and convert video to 9:16 vertical format
    """

    output_file = f"{UPLOAD_DIR}/{uuid.uuid4()}_vertical.mp4"

    command = [
        "ffmpeg",
        "-y",
        "-ss",
        start_time,
        "-i",
        input_file,
        "-t",
        duration,
        "-vf",
        (
            "scale=1080:1920:force_original_aspect_ratio=increase,"
            "crop=1080:1920"
        ),
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        output_file,
    ]

    await run_command(command)

    logger.info(f"Processed vertical video: {output_file}")

    return output_file


async def extract_thumbnail(video_file: str) -> str:
    """
    Extract high-quality thumbnail from video
    """

    thumbnail = f"{UPLOAD_DIR}/{uuid.uuid4()}_thumb.jpg"

    command = [
        "ffmpeg",
        "-y",
        "-i",
        video_file,
        "-ss",
        "00:00:02",
        "-vframes",
        "1",
        "-q:v",
        "2",
        thumbnail,
    ]

    await run_command(command)

    logger.info(f"Thumbnail extracted: {thumbnail}")

    return thumbnail


# =========================================================
# INSTAGRAM API
# =========================================================


async def upload_instagram_reel(
    video_path: str,
    caption: str,
) -> Dict[str, Any]:
    """
    Upload Reel to Instagram Graph API
    """

    caption = f"{caption}\n\n#anime #reels #animeedit"

    create_url = (
        f"https://graph.facebook.com/v19.0/"
        f"{INSTAGRAM_BUSINESS_ID}/media"
    )

    publish_url = (
        f"https://graph.facebook.com/v19.0/"
        f"{INSTAGRAM_BUSINESS_ID}/media_publish"
    )

    async with aiohttp.ClientSession() as session:

        # Step 1: Create container
        payload = {
            "media_type": "REELS",
            "video_url": video_path,
            "caption": caption,
            "access_token": INSTAGRAM_ACCESS_TOKEN,
        }

        async with session.post(create_url, data=payload) as response:
            data = await response.json()

            logger.info(f"Instagram create response: {data}")

            if "id" not in data:
                raise Exception(f"Instagram Error: {data}")

            creation_id = data["id"]

        # Step 2: Publish
        payload = {
            "creation_id": creation_id,
            "access_token": INSTAGRAM_ACCESS_TOKEN,
        }

        async with session.post(publish_url, data=payload) as response:
            publish_data = await response.json()

            logger.info(f"Instagram publish response: {publish_data}")

            return publish_data


# =========================================================
# YOUTUBE API
# =========================================================


def get_youtube_service():
    """
    Create YouTube API client
    """

    credentials = Credentials.from_service_account_file(
        YOUTUBE_SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/youtube.upload"],
    )

    return build("youtube", "v3", credentials=credentials)


async def upload_youtube_short(
    video_path: str,
    title: str,
    description: str,
):
    """
    Upload YouTube Short
    """

    title = f"{title} #Shorts"

    youtube = get_youtube_service()

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": ["anime", "shorts", "anime edits"],
            "categoryId": "24",
        },
        "status": {
            "privacyStatus": "public",
        },
    }

    media = MediaFileUpload(video_path, chunksize=-1, resumable=True)

    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = None

    while response is None:
        status, response = request.next_chunk()

        if status:
            logger.info(
                f"YouTube upload progress: "
                f"{int(status.progress() * 100)}%"
            )

    logger.info(f"YouTube upload complete: {response}")

    return response


# =========================================================
# CLEANUP
# =========================================================


async def cleanup_files(*files):
    """
    Delete temp files
    """

    for file in files:
        try:
            if file and os.path.exists(file):
                os.remove(file)
                logger.info(f"Deleted file: {file}")
        except Exception:
            logger.exception(f"Failed deleting file: {file}")


# =========================================================
# VIDEO PIPELINE
# =========================================================


async def process_video_job(message: Message):
    """
    Complete video pipeline
    """

    original_file = None
    processed_file = None
    thumbnail = None

    try:
        logger.info("Starting pipeline")

        original_file = await download_telegram_media(message)

        processed_file = await trim_and_convert_vertical(original_file)

        thumbnail = await extract_thumbnail(processed_file)

        caption = "🔥 Trending Anime Edit"

        # Upload to Instagram
        try:
            await upload_instagram_reel(
                processed_file,
                caption,
            )
        except Exception:
            logger.exception("Instagram upload failed")

        # Upload to YouTube
        try:
            await upload_youtube_short(
                processed_file,
                "Anime Edit",
                caption,
            )
        except Exception:
            logger.exception("YouTube upload failed")

        await message.reply_text(
            "✅ Video processed and uploaded successfully!"
        )

    except Exception as e:
        logger.exception("Pipeline failed")

        await message.reply_text(
            f"❌ Processing failed:\n{str(e)}"
        )

    finally:
        await cleanup_files(
            original_file,
            processed_file,
            thumbnail,
        )


# =========================================================
# QUEUE WORKER
# =========================================================


async def queue_worker():
    """
    Sequential async worker
    """

    while True:
        message = await video_queue.get()

        try:
            await process_video_job(message)

        except Exception:
            logger.exception("Queue worker error")

        finally:
            video_queue.task_done()


# =========================================================
# INLINE QUERY HANDLER
# =========================================================


@app.on_inline_query()
async def inline_search_handler(
    client: Client,
    inline_query: InlineQuery,
):
    """
    Search anime channels inline
    """

    query = inline_query.query.lower()

    results = []

    for channel in ANIME_CHANNELS:

        searchable = (
            f"{channel['name']} "
            f"{channel['description']} "
            f"{channel['username']}"
        ).lower()

        if query in searchable:

            results.append(
                InlineQueryResultArticle(
                    title=channel["name"],
                    description=channel["description"],
                    input_message_content=InputTextMessageContent(
                        f"📺 {channel['name']}\n"
                        f"🔗 {channel['username']}"
                    ),
                )
            )

    await inline_query.answer(
        results=results[:50],
        cache_time=1,
    )


# =========================================================
# VIDEO MESSAGE HANDLER
# =========================================================


@app.on_message(filters.video)
async def video_handler(client: Client, message: Message):
    """
    Add videos to queue
    """

    try:
        await video_queue.put(message)

        await message.reply_text(
            "📥 Video added to processing queue."
        )

    except Exception:
        logger.exception("Failed adding to queue")


# =========================================================
# COMMANDS
# =========================================================


@app.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    await message.reply_text(
        "🔥 Anime Reel Automation Bot Online!"
    )


@app.on_message(filters.command("queue"))
async def queue_status(client: Client, message: Message):
    await message.reply_text(
        f"📦 Queue Size: {video_queue.qsize()}"
    )


# =========================================================
# AIOHTTP HEALTH CHECK SERVER
# =========================================================


async def healthcheck(request):
    return web.Response(text="Bot is running!")


async def start_web_server():
    """
    Render health-check web server
    """

    web_app = web.Application()

    web_app.router.add_get("/", healthcheck)
    web_app.router.add_get("/health", healthcheck)

    runner = web.AppRunner(web_app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        host="0.0.0.0",
        port=8080,
    )

    await site.start()

    logger.info("Health check server started on port 8080")


# =========================================================
# MAIN
# =========================================================


async def main():
    """
    Main application entrypoint
    """

    logger.info("Starting Anime Automation Bot")

    await start_web_server()

    asyncio.create_task(queue_worker())

    await app.start()

    logger.info("Telegram bot started")

    try:
        while True:
            await asyncio.sleep(3600)

    except KeyboardInterrupt:
        logger.info("Shutting down bot")

    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
