import os
from typing import Optional

from moviepy.editor import VideoFileClip
from PIL import Image

from ...core.logger import logging
from ...core.managers import edit_or_reply
from ..tools import media_type
from .utils import runcmd

LOGS = logging.getLogger(__name__)


async def media_to_pic(event, reply, noedits=False):  # sourcery no-metrics
    mediatype = media_type(reply)
    if mediatype not in [
        "Photo",
        "Round Video",
        "Gif",
        "Sticker",
        "Video",
        "Voice",
        "Audio",
        "Document",
    ]:
        return event, None
    if not noedits:
        offevent = await edit_or_reply(
            event, "`Transfiguration Time! Converting to ....`"
        )

    else:
        offevent = event
    offmedia = None
    offfile = os.path.join("./temp/", "meme.png")
    if os.path.exists(offfile):
        os.remove(offfile)
    if mediatype == "Photo":
        offmedia = await reply.download_media(file="./temp")
        im = Image.open(offmedia)
        im.save(offfile)
    elif mediatype in ["Audio", "Voice"]:
        await event.client.download_media(reply, offfile, thumb=-1)
    elif mediatype == "Sticker":
        offmedia = await reply.download_media(file="./temp")
        if offmedia.endswith(".tgs"):
            offcmd = f"lottie_convert.py --frame 0 -if lottie -of png '{offmedia}' '{offfile}'"
            stdout, stderr = (await runcmd(offcmd))[:2]
            if stderr:
                LOGS.info(stdout + stderr)
        elif offmedia.endswith(".webm"):
            clip = VideoFileClip(offmedia)
            try:
                clip = clip.save_frame(offfile, 0.1)
            except Exception:
                clip = clip.save_frame(offfile, 0)
        elif offmedia.endswith(".webp"):
            im = Image.open(offmedia)
            im.save(offfile)
    elif mediatype in ["Round Video", "Video", "Gif"]:
        await event.client.download_media(reply, offfile, thumb=-1)
        if not os.path.exists(offfile):
            offmedia = await reply.download_media(file="./temp")
            clip = VideoFileClip(offmedia)
            try:
                clip = clip.save_frame(offfile, 0.1)
            except Exception:
                clip = clip.save_frame(offfile, 0)
    elif mediatype == "Document":
        mimetype = reply.document.mime_type
        mtype = mimetype.split("/")
        if mtype[0].lower() == "image":
            offmedia = await reply.download_media(file="./temp")
            im = Image.open(offmedia)
            im.save(offfile)
    if offmedia and os.path.lexists(offmedia):
        os.remove(offmedia)
    if os.path.lexists(offfile):
        return offevent, offfile, mediatype
    return offevent, None


async def take_screen_shot(
    video_file: str, duration: int, path: str = ""
) -> Optional[str]:
    thumb_image_path = path or os.path.join(
        "./temp/", f"{os.path.basename(video_file)}.jpg"
    )
    command = f"ffmpeg -ss {duration} -i '{video_file}' -vframes 1 '{thumb_image_path}'"
    err = (await runcmd(command))[1]
    if err:
        LOGS.error(err)
    return thumb_image_path if os.path.exists(thumb_image_path) else None
