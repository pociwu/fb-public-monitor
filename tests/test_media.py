from pathlib import Path

from PIL import Image, ImageDraw

from fb_monitor.media import MediaStore, media_representation_key


def test_facebook_representation_key_ignores_tokens_but_keeps_rendition_size():
    first = (
        "https://scontent-a.xx.fbcdn.net/v/t39.30808-1/photo.jpg"
        "?stp=dst-jpg_tt6&ctp=s720x720&_nc_sid=1d2534&oh=old&oe=1"
    )
    rotated = (
        "https://scontent-b.xx.fbcdn.net/v/t39.30808-1/photo.jpg"
        "?oe=2&oh=new&_nc_sid=different&ctp=s720x720&stp=dst-jpg_tt6"
    )
    thumbnail = (
        "https://scontent-b.xx.fbcdn.net/v/t39.30808-1/photo.jpg"
        "?stp=dst-jpg_tt6&ctp=s24x24&_nc_sid=different&oh=new&oe=2"
    )

    assert media_representation_key(first) == media_representation_key(rotated)
    assert media_representation_key(first) != media_representation_key(thumbnail)


def test_image_helpers_apply_exif_orientation_before_hashing_and_comparison(tmp_path: Path):
    base = Image.new("RGB", (96, 48), "white")
    draw = ImageDraw.Draw(base)
    draw.rectangle((0, 0, 30, 47), fill="navy")
    draw.ellipse((55, 8, 88, 40), fill="orange")
    normal = tmp_path / "normal.jpg"
    oriented = tmp_path / "oriented.jpg"
    base.save(normal, quality=95)

    stored = base.rotate(90, expand=True)
    exif = stored.getexif()
    exif[274] = 6  # rotate the stored pixels 90 degrees clockwise for display
    stored.save(oriented, quality=95, exif=exif)

    assert MediaStore.image_dimensions(oriented, "image/jpeg") == (96, 48)
    assert MediaStore.images_visually_equivalent(normal, oriented)
    assert MediaStore.perceptual_hash(normal, "image/jpeg") == MediaStore.perceptual_hash(
        oriented,
        "image/jpeg",
    )
