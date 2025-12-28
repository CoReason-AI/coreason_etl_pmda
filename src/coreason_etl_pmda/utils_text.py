# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import unicodedata


def normalize_text(text: str | bytes | None, encoding: str = "utf-8") -> str | None:
    """
    Normalizes text to handle mojibake and character width issues.

    1. Decodes bytes if provided, trying fallback encodings (cp932, euc-jp).
    2. Normalizes unicode characters (NFKC) to fix half-width Katakana.
    3. Strips whitespace.
    """
    if text is None:
        return None

    decoded_text = ""

    if isinstance(text, bytes):
        encodings_to_try = [encoding, "cp932", "euc-jp", "utf-8", "shift_jis"]
        # Deduplicate while preserving order
        seen = set()
        unique_encodings = []
        for enc in encodings_to_try:
            if enc not in seen:
                unique_encodings.append(enc)
                seen.add(enc)

        success = False
        for enc in unique_encodings:
            try:
                decoded_text = text.decode(enc)
                success = True
                break
            except (UnicodeDecodeError, LookupError):
                continue

        if not success:
            # Fallback: decode with errors ignore or replace?
            # Spec says "Quarantine cp932 decode errors" in Error Handling, but for this utility
            # we should probably return None or raise.
            # Given "Quarantine", returning None or a specific error indicator allows the caller to handle it.
            # Let's return None for now as an indicator of failure.
            return None
    else:
        decoded_text = text

    # NFKC Normalization (Half-width Katakana -> Full-width)
    normalized = unicodedata.normalize("NFKC", decoded_text)

    return normalized.strip()
