import os

import requests


def download_file(
    url: str,
    dest_path: str,
    chunk_size: int = 1024 * 1024,
    soft: bool = False,
    timeout: int = 300,
    headers: dict | None = None,
) -> str | None:
    """Download a URL to dest_path atomically.

    Content is written to ``dest_path.part`` and renamed into place only after
    the full response has been received, so interrupted or truncated downloads
    are never cached under the final name. When the server provides a
    Content-Length header, the received byte count is validated against it and
    a mismatch is treated as a failure.

    On a non-200 response with ``soft=True``, returns None so the caller can
    decide how to handle "not available". Otherwise raises on any failure.
    """
    part_path = dest_path + ".part"
    try:
        with requests.get(url, stream=True, timeout=timeout, headers=headers) as resp:
            if resp.status_code != 200:
                if soft:
                    return None
                resp.raise_for_status()
            expected = resp.headers.get("Content-Length")
            with open(part_path, "wb") as fp:
                total = 0
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fp.write(chunk)
                        total += len(chunk)
        if expected is not None and total != int(expected):
            raise RuntimeError(
                f"Truncated download for {url}: expected {expected} bytes, got {total}"
            )
        os.replace(part_path, dest_path)
    except Exception:
        if os.path.exists(part_path):
            os.remove(part_path)
        raise
    return dest_path
