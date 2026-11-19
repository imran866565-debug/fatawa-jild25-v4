#!/usr/bin/env python3
"""Merge Fatawa Razviya Jild 25 easy-Urdu tashreeh v4 chunks into jild-25.json.

Reads the 12 leaf chunk files listed in chunks-manifest-v4-jild25.json,
concatenates them in order 01 -> 02 -> ... -> 12, JOINS the 4 simple
duplicate-id groups (masala-25, masala-113, masala-142, masala-157)
and FOLDS the 20 masala-158 parts (2 same-id parts + 18 sub-items)
into single items, STABLE SORTS by start_page (mirroring the v3
merge), derives start_page = min(pages) where the field is absent (0
derived: all chunks carry start_page), normalises page markers,
validates, and writes {"masail": [...]} to jild-25.json.

ORDER -- chunk order is the order, then a stable sort by start_page:
  The v3 merged file ~/workspace/fatawa-app/content/tashreeh/jild-25.json
  has ID order == stable sort by start_page of the concatenation
  (verified locally by the coordinator: the v3 order IS the stable
  sort, including its tied start_pages). Do NOT sort or reorder before
  the joins. Within each chunk, items keep file order.
  CHUNK_ORDER is an explicit 12-leaf list.

JOINS: parts are found BY ID anywhere in the list. Parts must be in
  ascending page order. Sawal mode: "first" (25, 113, 142, 157 — the
  single non-empty sawal, in the first part). Titles: the joined title
  is the FIRST part's title, asserted against the embedded merged-v3
  title (V3_TITLES).
  masala-25 [107-160, 161-203], masala-113 [354-356, 357],
  masala-142 [457-458, 459], masala-157 [502, 503] (head sawal
  1491ch, jawab EMPTY; tail sawal empty, jawab 2683ch — the joined
  jawab comes from the tail alone; seam checks skip the empty-jawab
  head end).
MEGA-FOLD (1): masala-158 — TWENTY parts in page order: c10 head
  jild25-masala-158 [503-543] (sawal, jawab 3562ch) + c11 tail
  jild25-masala-158 [544-600] (sawal empty, jawab 84503ch) + the 18
  sub-items in chunk 12 (faida-14 ... hukm-akheer, all sawals
  empty, all jawabs non-empty) -> ONE item jild25-masala-158
  [503-630], 128 pages. jawab = "".join of the 20 stripped jawabs
  with NO separator (proven from the v3 chunk files: "".join matches
  the merged v3 jawab exactly; " ".join adds 19 extra chars).
  pages = sorted union of all 20 parts' pages; notes =
  concatenation in order; sawal/title from the head (== merged-v3
  title). The other 19 items are DELETED from the list.
  After the joins + fold there are 168 items and 168 unique ids —
  the duplicate-id assertion runs AFTER the joins, not before.
  NO renames at merge: the 18 sub-items are absorbed by the fold;
  no print-error renames were applied in the leaves.
  All chunks carry start_page == min(pages) (0 derived). Boundary
  pages sit in two items by design (762 page-list entries, 608
  unique pages) — page assertions use SETS throughout.
  Empty jawab after the merge: ONLY jild25-masala-139a (empty in
  v3/source, allow-listed).
"""
import json
import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = "chunks-manifest-v4-jild25.json"

# Concatenation order: chunk files 01 -> 18, in order.
CHUNK_ORDER = ["jild-25-chunk-01.json", "jild-25-chunk-02.json", "jild-25-chunk-03.json", "jild-25-chunk-04.json", "jild-25-chunk-05.json", "jild-25-chunk-06.json", "jild-25-chunk-07.json", "jild-25-chunk-08.json", "jild-25-chunk-09.json", "jild-25-chunk-10.json", "jild-25-chunk-11.json", "jild-25-chunk-12.json"]

EXPECTED_ITEMS = 168
EXPECTED_PAGES = 608
EXPECTED_MIN_PAGE = 5
EXPECTED_MAX_PAGE = 658
EXPECTED_VOLUME_IDS = {37}
EXPECTED_FIRST_ID = "jild25-pesh-lafz"
EXPECTED_LAST_ID = "jild25-masala-183"

# v3 ID sequence, verbatim from content/tashreeh/jild-25.json.
V3_IDS = ["jild25-pesh-lafz", "jild25-masala-1", "jild25-masala-2", "jild25-masala-3", "jild25-masala-4", "jild25-masala-5", "jild25-masala-6", "jild25-masala-7", "jild25-masala-8", "jild25-masala-9", "jild25-masala-10", "jild25-masala-11", "jild25-masala-12", "jild25-masala-13", "jild25-masala-14", "jild25-masala-15", "jild25-masala-16", "jild25-masala-17", "jild25-masala-18", "jild25-masala-19", "jild25-masala-20", "jild25-masala-21", "jild25-masala-22", "jild25-masala-23", "jild25-masala-24", "jild25-masala-25", "jild25-masala-26-28", "jild25-masala-29", "jild25-masala-30", "jild25-masala-31", "jild25-masala-32", "jild25-masala-33", "jild25-masala-34", "jild25-masala-35-36", "jild25-masala-37", "jild25-masala-38", "jild25-masala-39", "jild25-masala-40-42", "jild25-masala-43", "jild25-masala-44", "jild25-masala-45", "jild25-masala-46", "jild25-masala-47", "jild25-masala-48", "jild25-masala-49", "jild25-masala-50", "jild25-masala-51", "jild25-masala-52", "jild25-masala-53", "jild25-masala-54", "jild25-masala-55", "jild25-masala-56", "jild25-masala-57", "jild25-masala-58", "jild25-masala-59", "jild25-masala-60", "jild25-masala-61", "jild25-masala-62-63", "jild25-masala-64", "jild25-masala-65", "jild25-masala-66", "jild25-masala-67", "jild25-masala-68", "jild25-masala-69", "jild25-masala-70", "jild25-masala-71-73", "jild25-masala-74-75", "jild25-masala-76", "jild25-masala-77-78", "jild25-masala-79", "jild25-masala-80-83", "jild25-masala-84", "jild25-masala-85", "jild25-masala-86", "jild25-masala-87", "jild25-masala-88", "jild25-masala-89-91", "jild25-masala-92", "jild25-masala-93", "jild25-masala-94", "jild25-masala-95", "jild25-masala-96-97", "jild25-masala-98", "jild25-masala-99", "jild25-masala-100", "jild25-masala-101", "jild25-masala-102", "jild25-kitab-al-wasaya", "jild25-masala-103", "jild25-masala-104", "jild25-masala-105", "jild25-masala-106", "jild25-masala-107", "jild25-masala-108", "jild25-masala-109", "jild25-masala-110", "jild25-masala-111", "jild25-masala-112", "jild25-masala-113", "jild25-masala-114", "jild25-masala-115", "jild25-masala-116", "jild25-masala-117", "jild25-masala-118", "jild25-masala-119", "jild25-masala-120", "jild25-masala-121", "jild25-masala-122", "jild25-masala-123", "jild25-masala-124", "jild25-masala-125", "jild25-masala-126", "jild25-masala-127", "jild25-masala-128", "jild25-masala-129", "jild25-masala-130", "jild25-masala-131", "jild25-masala-132", "jild25-masala-133", "jild25-masala-134", "jild25-masala-135", "jild25-masala-136", "jild25-masala-137", "jild25-masala-138", "jild25-masala-139a", "jild25-masala-139b", "jild25-masala-140", "jild25-masala-141", "jild25-masala-142", "jild25-masala-143", "jild25-masala-144", "jild25-masala-145", "jild25-masala-146", "jild25-masala-147", "jild25-masala-148", "jild25-masala-149", "jild25-masala-150", "jild25-masala-151", "jild25-masala-152-153", "jild25-masala-154", "jild25-masala-155", "jild25-masala-156", "jild25-masala-157", "jild25-masala-158", "jild25-masala-159", "jild25-masala-160", "jild25-masala-161", "jild25-masala-162", "jild25-masala-163", "jild25-masala-164", "jild25-masala-165", "jild25-masala-166", "jild25-masala-167", "jild25-masala-168", "jild25-masala-169", "jild25-masala-170", "jild25-masala-171", "jild25-masala-172", "jild25-masala-173", "jild25-masala-174", "jild25-masala-175", "jild25-masala-176-177", "jild25-masala-178", "jild25-masala-179", "jild25-masala-180", "jild25-masala-181", "jild25-masala-182", "jild25-masala-183"]

# v3 pages per item id, verbatim from content/tashreeh/jild-25.json.
V3_PAGES = {
    "jild25-pesh-lafz": [5, 6, 7, 8, 9, 10],
    "jild25-masala-1": [53, 54],
    "jild25-masala-2": [54, 55, 56],
    "jild25-masala-3": [56, 57, 58, 59],
    "jild25-masala-4": [59, 60, 61],
    "jild25-masala-5": [61, 62],
    "jild25-masala-6": [62, 63, 64, 65, 66, 67],
    "jild25-masala-7": [67],
    "jild25-masala-8": [68],
    "jild25-masala-9": [68, 69],
    "jild25-masala-10": [70],
    "jild25-masala-11": [70, 71],
    "jild25-masala-12": [71],
    "jild25-masala-13": [71, 72],
    "jild25-masala-14": [72, 73],
    "jild25-masala-15": [74],
    "jild25-masala-16": [74, 75],
    "jild25-masala-17": [75],
    "jild25-masala-18": [77, 78],
    "jild25-masala-19": [79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97],
    "jild25-masala-20": [98, 99, 100],
    "jild25-masala-21": [100, 101, 102, 103, 104],
    "jild25-masala-22": [104, 105],
    "jild25-masala-23": [105, 106],
    "jild25-masala-24": [106],
    "jild25-masala-25": [107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203],
    "jild25-masala-26-28": [204],
    "jild25-masala-29": [204, 205],
    "jild25-masala-30": [205, 206],
    "jild25-masala-31": [206, 207, 208],
    "jild25-masala-32": [208, 209],
    "jild25-masala-33": [209, 210],
    "jild25-masala-34": [210],
    "jild25-masala-35-36": [210],
    "jild25-masala-37": [210, 211],
    "jild25-masala-38": [211, 212],
    "jild25-masala-39": [212, 213],
    "jild25-masala-40-42": [213],
    "jild25-masala-43": [213],
    "jild25-masala-44": [213, 214, 215, 216],
    "jild25-masala-45": [217, 218],
    "jild25-masala-46": [218, 219, 220, 221],
    "jild25-masala-47": [221, 222],
    "jild25-masala-48": [222, 223, 224, 225, 226],
    "jild25-masala-49": [226, 227],
    "jild25-masala-50": [227, 228],
    "jild25-masala-51": [228, 229, 230, 231, 232, 233, 234],
    "jild25-masala-52": [234, 235, 236, 237],
    "jild25-masala-53": [237, 238, 239],
    "jild25-masala-54": [239, 240],
    "jild25-masala-55": [240],
    "jild25-masala-56": [240, 241, 242, 243],
    "jild25-masala-57": [244],
    "jild25-masala-58": [244, 245],
    "jild25-masala-59": [245, 246, 247, 248, 249],
    "jild25-masala-60": [249, 250, 251, 252, 253],
    "jild25-masala-61": [253, 254],
    "jild25-masala-62-63": [254, 255, 256],
    "jild25-masala-64": [256, 257],
    "jild25-masala-65": [257, 258, 259],
    "jild25-masala-66": [259],
    "jild25-masala-67": [260, 261],
    "jild25-masala-68": [261, 262, 263, 264],
    "jild25-masala-69": [265],
    "jild25-masala-70": [266, 267, 268, 269, 270, 271, 272, 273],
    "jild25-masala-71-73": [273, 274, 275],
    "jild25-masala-74-75": [275, 276],
    "jild25-masala-76": [276, 277],
    "jild25-masala-77-78": [277, 278],
    "jild25-masala-79": [278],
    "jild25-masala-80-83": [279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294],
    "jild25-masala-84": [294, 295],
    "jild25-masala-85": [295],
    "jild25-masala-86": [295, 296],
    "jild25-masala-87": [296],
    "jild25-masala-88": [296, 297],
    "jild25-masala-89-91": [297, 298],
    "jild25-masala-92": [298],
    "jild25-masala-93": [298, 299],
    "jild25-masala-94": [299],
    "jild25-masala-95": [299],
    "jild25-masala-96-97": [299, 300],
    "jild25-masala-98": [300, 301],
    "jild25-masala-99": [301, 302],
    "jild25-masala-100": [302],
    "jild25-masala-101": [302],
    "jild25-masala-102": [303],
    "jild25-kitab-al-wasaya": [305],
    "jild25-masala-103": [305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315],
    "jild25-masala-104": [315, 316, 317, 318, 319, 320],
    "jild25-masala-105": [320, 321, 322, 323],
    "jild25-masala-106": [323, 324, 325, 326, 327, 328, 329, 330],
    "jild25-masala-107": [330],
    "jild25-masala-108": [330, 331],
    "jild25-masala-109": [331],
    "jild25-masala-110": [332],
    "jild25-masala-111": [332, 333, 334, 335, 336, 337, 338],
    "jild25-masala-112": [338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354],
    "jild25-masala-113": [354, 355, 356, 357],
    "jild25-masala-114": [357, 358],
    "jild25-masala-115": [358, 359, 360, 361, 362],
    "jild25-masala-116": [362, 363, 364, 365, 366, 367],
    "jild25-masala-117": [367, 368],
    "jild25-masala-118": [368, 369, 370],
    "jild25-masala-119": [370, 371],
    "jild25-masala-120": [371],
    "jild25-masala-121": [371, 372],
    "jild25-masala-122": [372, 373],
    "jild25-masala-123": [373, 374],
    "jild25-masala-124": [373, 374, 375],
    "jild25-masala-125": [376],
    "jild25-masala-126": [376, 377],
    "jild25-masala-127": [377, 378, 379, 380],
    "jild25-masala-128": [380],
    "jild25-masala-129": [380, 381],
    "jild25-masala-130": [381],
    "jild25-masala-131": [382, 383],
    "jild25-masala-132": [384, 385, 386],
    "jild25-masala-133": [386, 387, 388, 389],
    "jild25-masala-134": [389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400],
    "jild25-masala-135": [403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432],
    "jild25-masala-136": [433, 434],
    "jild25-masala-137": [434, 435, 436],
    "jild25-masala-138": [436, 437],
    "jild25-masala-139a": [437, 438, 439, 440, 441],
    "jild25-masala-139b": [441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452],
    "jild25-masala-140": [452, 453, 454],
    "jild25-masala-141": [454, 455, 456, 457],
    "jild25-masala-142": [457, 458, 459],
    "jild25-masala-143": [459, 460, 461],
    "jild25-masala-144": [461, 462, 463, 464],
    "jild25-masala-145": [465],
    "jild25-masala-146": [465, 466],
    "jild25-masala-147": [466, 467],
    "jild25-masala-148": [467],
    "jild25-masala-149": [468, 469, 470, 471, 472, 473, 474, 475, 476],
    "jild25-masala-150": [477, 478],
    "jild25-masala-151": [478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488],
    "jild25-masala-152-153": [488, 489, 490],
    "jild25-masala-154": [490, 491, 492, 493, 494, 495],
    "jild25-masala-155": [495, 496],
    "jild25-masala-156": [496, 497, 498, 499, 500, 501, 502],
    "jild25-masala-157": [502, 503],
    "jild25-masala-158": [503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 600, 601, 602, 603, 604, 605, 606, 607, 608, 609, 610, 611, 612, 613, 614, 615, 616, 617, 618, 619, 620, 621, 622, 623, 624, 625, 626, 627, 628, 629, 630],
    "jild25-masala-159": [630, 631],
    "jild25-masala-160": [631, 632, 633, 634],
    "jild25-masala-161": [634, 635],
    "jild25-masala-162": [635, 636],
    "jild25-masala-163": [636, 637, 638],
    "jild25-masala-164": [639],
    "jild25-masala-165": [640, 641],
    "jild25-masala-166": [641, 642],
    "jild25-masala-167": [642, 643],
    "jild25-masala-168": [643, 644],
    "jild25-masala-169": [644, 645],
    "jild25-masala-170": [645],
    "jild25-masala-171": [645, 646],
    "jild25-masala-172": [646, 647],
    "jild25-masala-173": [647],
    "jild25-masala-174": [648],
    "jild25-masala-175": [648, 649, 650, 651],
    "jild25-masala-176-177": [651, 652],
    "jild25-masala-178": [652],
    "jild25-masala-179": [652, 653],
    "jild25-masala-180": [653, 654],
    "jild25-masala-181": [654],
    "jild25-masala-182": [654, 655],
    "jild25-masala-183": [656, 657, 658],
}

# The Urdu word for "page" is safha; the canonical marker is [safha N].
# Lenticular brackets are U+3010 (open) and U+3011 (close); the short
# form is "[safha-initial N]" with the single-letter abbreviation.
# All three patterns are built from escapes so this file never contains
# a literal lenticular-bracket pair.
SAFHA = "\u0635\u0641\u062d\u06c1"  # the Urdu word for page
RE_LENTICULAR = re.compile("\u3010" + SAFHA + " (\\d+)\u3011")
RE_SHORT = re.compile("\\[\u0635 (\\d+)\\]")
RE_FULL = re.compile("\\[" + SAFHA + " \\d+\\]")
LENTICULAR_PREFIX = "\u3010" + SAFHA  # for the "none remain" check
SHORT_PREFIX_RE = re.compile("\\[\u0635 \\d+\\]")

# Canonical output key order for every item (the JSON contract).
ITEM_KEYS = ["id", "title", "volume_id", "pages", "start_page",
             "tashreeh_sawal", "tashreeh_jawab", "notes"]


def normalise_markers(text):
    """Normalise page markers in one string. Returns (new_text, counts)."""
    counts = {"lenticular": 0, "short": 0, "full_before": 0}
    counts["full_before"] = len(RE_FULL.findall(text))
    text, n1 = RE_LENTICULAR.subn("[" + SAFHA + " \\1]", text)
    counts["lenticular"] = n1
    text, n2 = RE_SHORT.subn("[" + SAFHA + " \\1]", text)
    counts["short"] = n2
    return text, counts


def normalise_item(item):
    """Normalise markers in every string field of one item (recursively
    through lists/dicts). Returns per-form counts for this item."""
    totals = {"lenticular": 0, "short": 0, "full_before": 0}

    def walk(value):
        if isinstance(value, str):
            new, c = normalise_markers(value)
            for k in totals:
                totals[k] += c[k]
            return new
        if isinstance(value, list):
            return [walk(v) for v in value]
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items()}
        return value

    for key in list(item.keys()):
        item[key] = walk(item[key])
    return totals


def main():
    assert len(V3_IDS) == EXPECTED_ITEMS, "embedded V3_IDS length mismatch"
    assert set(V3_PAGES) == set(V3_IDS), "embedded V3_PAGES keys != V3_IDS"
    assert sum(len(v) for v in V3_PAGES.values()) >= EXPECTED_PAGES

    manifest = json.load(open(os.path.join(BASE, MANIFEST), encoding="utf-8"))
    entries = manifest["chunks"]
    assert len(entries) == 12, f"expected 12 manifest entries, got {len(entries)}"
    assert all(e.get("status") == "done" for e in entries), "not all chunks done"
    by_file = {e["file"]: e for e in entries}
    assert set(by_file) == set(CHUNK_ORDER), (
        f"manifest files != CHUNK_ORDER: "
        f"missing={set(CHUNK_ORDER) - set(by_file)} "
        f"extra={set(by_file) - set(CHUNK_ORDER)}"
    )

    all_items = []
    derived = []
    for fname in CHUNK_ORDER:
        path = os.path.join(BASE, fname)
        d = json.load(open(path, encoding="utf-8"))
        items = d["masail"]
        entry = by_file[fname]
        assert len(items) == entry["items"], (
            f"{fname}: {len(items)} items, manifest says {entry['items']}"
        )
        # Marker normalisation, with before/after counts per chunk.
        before = {"lenticular": 0, "short": 0, "full_before": 0}
        for it in items:
            c = normalise_item(it)
            for k in before:
                before[k] += c[k]
        after_full = sum(
            len(RE_FULL.findall(json.dumps(it, ensure_ascii=False)))
            for it in items
        )
        print(
            f"{fname}: {len(items)} items | markers before: "
            f"lenticular={before['lenticular']} short={before['short']} "
            f"full={before['full_before']} | after: full={after_full}"
        )
        assert after_full == (
            before["lenticular"] + before["short"] + before["full_before"]
        ), f"{fname}: marker count changed by normalisation!"
        # Derive start_page = min(pages) where absent (none expected: all chunks carry start_page).
        for it in items:
            if "start_page" not in it:
                it["start_page"] = min(it["pages"])
                derived.append(it["id"])
        all_items.extend(items)

        # SIMPLE JOINS (4): parts found BY ID anywhere. Sawal mode:
    # "first" (the single non-empty sawal, in the first part).
    # MEGA-FOLD (1): masala-158 — 20 parts (2 same-id parts + 18
    # sub-items) folded by fold_masala_158 with "".join, NO separator.
    # Sawal/title from the head.

    def join_consecutive(item_id, expected_parts, seam_checks, sawal_mode="first", title_suffix=None):
        idxs = [i for i, it in enumerate(all_items) if it["id"] == item_id]
        assert len(idxs) == expected_parts, (
            f"{item_id}: expected {expected_parts} parts, found {len(idxs)}"
        )
        parts = [all_items[i] for i in idxs]
        assert [min(p["pages"]) for p in parts] == sorted(min(p["pages"]) for p in parts), (
            f"{item_id}: parts not in page order"
        )
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{item_id}: volume mismatch {vols}"
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{item_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{item_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        if sawal_mode == "first":
            sawals = [p.get("tashreeh_sawal") for p in parts if p.get("tashreeh_sawal")]
            assert len(sawals) <= 1, f"{item_id}: {len(sawals)} non-empty sawals"
            if sawals:
                assert parts[0].get("tashreeh_sawal"), f"{item_id}: non-empty sawal not in first part"
            sawal = sawals[0] if sawals else ""
        else:
            raise AssertionError(f"{item_id}: unknown sawal_mode {sawal_mode}")
        title = parts[0]["title"]
        if title_suffix:
            assert title.endswith(title_suffix), f"{item_id}: first-part title lacks suffix {title_suffix!r}"
            assert all(p["title"].endswith(title_suffix) for p in parts), f"{item_id}: a part title lacks the suffix"
            title = title[: -len(title_suffix)]
        assert title == V3_TITLES[item_id], f"{item_id}: joined title != v3 title"
        pages = sorted({p for part in parts for p in part["pages"]})
        # Jild 25 has no cross-join head: expect v3 pages exactly.
        cross_tails = {}
        cross_tail_pages = {p for t in cross_tails.get(item_id, []) for it in all_items if it["id"] == t for p in it["pages"]}
        assert pages == sorted(set(V3_PAGES[item_id]) - cross_tail_pages), f"{item_id}: joined pages != v3"
        joined = {
            "id": item_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawal,
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        first = idxs[0]
        for i in reversed(idxs[1:]):
            del all_items[i]
        all_items[first] = joined
        print(f"joined {item_id}: {expected_parts} parts -> 1 item, {len(pages)} pages")

    FOLD_SEQ = [
        "jild25-masala-158",
        "jild25-masala-158",
        "jild25-p598-faida-14",
        "jild25-p601-faida-15",
        "jild25-p601-faida-16",
        "jild25-p602-faida-17",
        "jild25-p603-faida-18",
        "jild25-p603-faida-19",
        "jild25-p608-faida-20",
        "jild25-p613-faida-21",
        "jild25-p614-faida-22",
        "jild25-p616-faida-23",
        "jild25-p617-faida-24",
        "jild25-p617-faida-25",
        "jild25-p619-faida-26",
        "jild25-p621-faida-27",
        "jild25-p622-tafriaat",
        "jild25-p626-jawab-chief-court",
        "jild25-p627-jawab-jajji-khanpur",
        "jild25-p628-hukm-akheer",
    ]

    def fold_masala_158(seam_checks):
        pool = {}
        for it in all_items:
            pool.setdefault(it["id"], []).append(it)
        parts = []
        for fid in FOLD_SEQ:
            assert pool.get(fid), f"fold jild25-masala-158: missing part {fid}"
            parts.append(pool[fid].pop(0))
        assert len(parts) == 20, f"fold: {len(parts)} parts, expected 20"
        assert [min(p["pages"]) for p in parts] == sorted(min(p["pages"]) for p in parts), (
            "fold jild25-masala-158: parts not in page order"
        )
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"fold: volume mismatch {vols}"
        assert parts[0].get("tashreeh_sawal"), "fold: head sawal empty"
        assert all(not p.get("tashreeh_sawal") for p in parts[1:]), "fold: a later part sawal non-empty"
        assert all(p.get("tashreeh_jawab") for p in parts), "fold: a part jawab is empty"
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"fold jild25-masala-158 part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"fold jild25-masala-158 part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        title = parts[0]["title"]
        assert title == V3_TITLES["jild25-masala-158"], "fold: joined title != v3 title"
        pages = sorted({p for part in parts for p in part["pages"]})
        assert pages == V3_PAGES["jild25-masala-158"], "fold: joined pages != v3"
        joined = {
            "id": "jild25-masala-158",
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": parts[0]["tashreeh_sawal"],
            "tashreeh_jawab": "".join(p["tashreeh_jawab"].strip() for p in parts),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        wanted = {id(p) for p in parts}
        idxs = [i for i, it in enumerate(all_items) if id(it) in wanted]
        assert len(idxs) == 20, f"fold: found {len(idxs)} part rows in list"
        first = idxs[0]
        for i in reversed(idxs[1:]):
            del all_items[i]
        all_items[first] = joined
        print(f"folded jild25-masala-158: 20 parts -> 1 item, {len(pages)} pages")

    V3_TITLES = {
        "jild25-masala-25": "مسئلہ ۲۵: تاڑی کے خمیر سے پکی روٹی",
        "jild25-masala-113": "مسئلہ ۱۱۳: جائداد کی ہبہ، داماد کی وصیت اور ترکہ کی تقسیم (جواب صفحہ ۳۵۷ پر جاری)",
        "jild25-masala-142": "مسئلہ ۱۴۲: مرض الموت کی تعریف (جواب جاری — ص ۴۵۸ کے بعد)",
        "jild25-masala-157": "مسئلہ ۱۵۷: بیوی کے نام جائداد اور چھوٹے بیٹے کو ہبہ",
        "jild25-masala-158": "مسئلہ ۱۵۸: واحد بخش کی وصیت — بیوہ عالم خاتون اور شاہ محمد خاں کا مقدمہ (چیف کورٹ بہاولپور)",
    }

    join_consecutive(
        "jild25-masala-25", 2,
        [
            (0, "end", "گے جاری ہے، اس لیے اس مسئلے کا مکمل جواب اور اس کا آخری حکم اگلے حصے میں آئے گا۔"),
            (1, "start", "یہ مسئلہ ۲۵ کے جواب کی تحقیق کا دوسرا حصہ ہے۔ اعلیٰ حضرت اس حصے میں پہلے ان حدیث"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild25-masala-113", 2,
        [
            (0, "end", "م نہ ہو — لہٰذا صحیح نہیں۔ (جواب یہاں صفحہ ۳۵۶ پر جاری ہے؛ بقیہ صفحہ ۳۵۷ پر ہے۔)"),
            (1, "start", "اس مسئلہ کے جواب کا آخری حصہ اس صفحہ پر ہے۔ اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild25-masala-142", 2,
        [
            (0, "end", " قرار پائے گا — اس کے بعد جواب ص ۴۵۸ کے بعد جاری ہے اور اگلے چونک میں مکمل ہوگا۔"),
            (1, "start", "اس حصے میں جواب کا آخری حوالہ بیان ہوا ہے۔ مرض الموت کا مطلب ہے وہ بیماری جس میں"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild25-masala-157", 2,
        [
            (1, "start", "جواب کے اس آخری حصے میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ صرف کاغذ پر لکھ "),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    fold_masala_158(
        [
            (0, "end", "فیق اللہ ہی کی طرف سے ہے، میں نے اسی پر بھروسا کیا اور اسی کی طرف رجوع کرتا ہوں۔"),
            (1, "start", "یہ مسئلہ چیف کورٹ بہاولپور کے ایک مقدمے سے متعلق ہے جس میں ایک مرنے والے کے وصیت"),
            (1, "end", "ے اعتبار سے ہے، یہاں تک کہ زوجین پر قرابت کے نہ ہونے کی وجہ سے رد نہیں کیا جاتا۔"),
            (2, "start", "یہ فائدہ ۱۴ کا بقیہ حصہ ہے جو صفحہ ۵۹۸ سے شروع ہوا تھا۔ اعلیٰ حضرت فرماتے ہیں کہ"),
            (2, "end", "ے پیچھے رہنا واجب ہے، اور یہی بات مقصود تھی، اور ساری تعریف اللہ ودود کے لیے ہے۔"),
            (3, "start", "اعلیٰ حضرت فرماتے ہیں کہ اگر رد علی الزوجین کو رد کے مرتبے میں مان لیا جائے تو ر"),
            (3, "end", "جائے، اور قیاس کا مطلب ہے ایک مسئلے کو دوسرے جیسے مسئلے پر قیاس کر کے حکم لگانا۔"),
            (4, "start", "اعلیٰ حضرت فرماتے ہیں کہ ٹھیک ہے، مان لیا کہ متاخرین نے رد علی الزوجین کو مرتبہ "),
            (4, "end", " ہے بالکل صاف اور ظاہر بات، اور احد الزوجین کا مطلب ہے خاوند یا بیوی میں سے ایک۔"),
            (5, "start", "اعلیٰ حضرت فرماتے ہیں کہ اگلی کارروائیاں یاد دلانے کی کیا حاجت ہے، اور ممکن ہے ک"),
            (5, "end", "لب ہے شرط کے ساتھ بندھا ہوا۔ مگر اپنے ہاتھوں کے کیے کو بھول جانے کا کیا علاج ہے۔"),
            (6, "start", "اعلیٰ حضرت فرماتے ہیں کہ تمام کتابیں گواہ ہیں کہ متاخرین کے اس فتوے کی علت بیت ا"),
            (6, "end", "ا مطلب ہے وہ شخص جس کے لیے مرنے والے نے اپنے مال کے تہائی سے زائد کی وصیت کی ہو۔"),
            (7, "start", "اعلیٰ حضرت فرماتے ہیں کہ شافعیہ کے نزدیک بیت المال وارث ہے، اس لیے وہ عصبہ نہ ہو"),
            (7, "end", "ت المال کے لیے ہے اگرچہ بیت المال منتظم نہ ہو۔ ماتن کا مطلب ہے کتاب کا اصل مصنف۔"),
            (8, "start", "اعلیٰ حضرت فرماتے ہیں کہ ذی سہم نسبی پر رد کی علت ہمارے نزدیک فساد بیت المال ٹھہ"),
            (8, "end", " جلیلہ کا مطلب ہے بڑا اور عظیم مسئلہ، اور مستمر کا مطلب ہے برابر جاری رہنے والا۔"),
            (9, "start", "اعلیٰ حضرت فرماتے ہیں کہ امیر المومنین عثمان رضی اللہ تعالیٰ عنہ سے رد علی الزوج"),
            (9, "end", "امیر المومنین کی طرف اس کی نسبت باطل و فریہ محض ہے۔ فریہ کا مطلب ہے جھوٹا بہتان۔"),
            (10, "start", "اعلیٰ حضرت فرماتے ہیں کہ عول پر قیاس سے ہمارے علمائے کرام جواب شافی دے چکے ہیں۔ "),
            (10, "end", "معدوم ہے، لہٰذا فرق ظاہر اور حق خوب واضح ہو گیا۔ معدوم کا مطلب ہے موجود نہ ہونا۔"),
            (11, "start", "اعلیٰ حضرت فرماتے ہیں کہ رد علی الزوجین کا ماننا دو طرح ہے: ایک یہ کہ اسے حق اصل"),
            (11, "end", "دیکھے یا کان لگائے اس حال میں کہ متوجہ ہو۔ فقہ کا مطلب ہے شریعت کے احکام کا علم۔"),
            (12, "start", "اعلیٰ حضرت فرماتے ہیں کہ درمختار میں اول اپنے ائمہ کا مذہب بیان فرمایا کہ زوجین "),
            (12, "end", "تعالیٰ درمختار تو ایسے جنون سے منزہ ہے۔ منزہ کا مطلب ہے پاک۔ زیادہ کیا گزارش ہو۔"),
            (13, "start", "اعلیٰ حضرت فرماتے ہیں کہ بفضلہ تعالیٰ یہ مسئلہ ہم نے ایسے طور سے بیان کیا جس میں"),
            (13, "end", "قطعاً مستحق ہے، اور مستحق کی نامستحق پر تقدیم بدیہی ہے۔ جیران کا مطلب ہے ہمسائے۔"),
            (14, "start", "اعلیٰ حضرت فرماتے ہیں کہ اس سے بھی سیری نہ ہو تو مستصفی، پھر معراج الدرایہ، پھر "),
            (14, "end", "کا مطلب ہے سندیں اور حوالے۔ فتویٰ ۴ میں صرف ایک یہی سند مستصفی مصفی و مستصفی ہے۔"),
            (15, "start", "اعلیٰ حضرت فرماتے ہیں کہ اگر اس سے بھی تسکین نہ ہو تو حاشیہ درمختار میں علامہ سی"),
            (15, "end", "دم ہے۔ ایسا جزئیہ البتہ نہیں مل سکتا۔ ہم اللہ تعالیٰ سے سلامتی کا سوال کرتے ہیں۔"),
            (16, "start", "اس کے بعد اعلیٰ حضرت ان چار فتووں کی غلطیوں کی تفریعات بیان فرماتے ہیں، یعنی ہر "),
            (16, "end", " سند افادات میں واضح ہو چکی، واللہ الحمد۔ ذروہ علیا کا مطلب ہے سب سے اونچی چوٹی۔"),
            (17, "start", "اس کے بعد اعلیٰ حضرت چیف کورٹ بہاولپور کے استفتا کا جواب دیتے ہیں۔ استفتا کا مطل"),
            (17, "end", " ہے۔ مفصل کا مطلب ہے تفصیل والا، اور نصوص کا مطلب ہے قرآن و حدیث کی صاف عبارتیں۔"),
            (18, "start", "اس کے بعد اعلیٰ حضرت ججی خانپور کے استفتا کا جواب دیتے ہیں۔ (۱) ہاں، یہ وصیت مرض"),
            (18, "end", " کا اختیار نہیں۔ عین کا مطلب ہے وہی چیز خود، اور فریق کا مطلب ہے جھگڑے والا طرف۔"),
            (19, "start", "اس کے بعد اعلیٰ حضرت حکم اخیر بیان فرماتے ہیں۔ (۱) سماعہ کے مطابق وہ زیور تمام و"),
        ],
    )
    assert len(all_items) == len({it["id"] for it in all_items}), (
        f"DUPLICATE IDS after joins: "
        f"{[i for i in [it['id'] for it in all_items] if [it['id'] for it in all_items].count(i) > 1]}"
    )

    print(f"after joins: {len(all_items)} items (191 concatenated)")
    # NO renames at merge: the 18 sub-items are absorbed by the
    # masala-158 fold; no print-error renames were applied in
    # the leaves.
    print("renames applied: [] (sub-items absorbed by the masala-158 fold)")
    # STABLE SORT by start_page, mirroring the v3 merge.
    all_items.sort(key=lambda it: it["start_page"])
    print(f"sorted by start_page: {len(all_items)} items")
    print(f"start_page derived for {len(derived)} items (expected 0: all chunks carry start_page)")
    assert len(all_items) == EXPECTED_ITEMS, (
        f"expected {EXPECTED_ITEMS} items, got {len(all_items)}"
    )

    # Rebuild each item in the canonical key order (the JSON contract).
    canonical = []
    for it in all_items:
        assert set(it.keys()) == set(ITEM_KEYS), (
            f"{it.get('id')}: keys {sorted(it.keys())} != {sorted(ITEM_KEYS)}"
        )
        canonical.append({k: it[k] for k in ITEM_KEYS})
    all_items = canonical

    # Validation.
    ids = [it["id"] for it in all_items]
    assert ids == V3_IDS, (
        f"ID sequence != v3: first diff at "
        f"{next((i for i, (a, b) in enumerate(zip(ids, V3_IDS)) if a != b), min(len(ids), len(V3_IDS)))}"
    )
    assert len(ids) == len(set(ids)), (
        f"DUPLICATE IDS: {[i for i in ids if ids.count(i) > 1]}"
    )
    empty = [it["id"] for it in all_items if not it.get("tashreeh_jawab")]
    assert empty == ["jild25-masala-139a"], f"EMPTY JAWAB: {empty}"
    vols = set(it.get("volume_id") for it in all_items)
    assert vols == EXPECTED_VOLUME_IDS, f"volume_ids {vols} != {EXPECTED_VOLUME_IDS}"
    pages = set()
    for it in all_items:
        assert it["pages"] == V3_PAGES[it["id"]], (
            f"{it['id']}: pages {it['pages']} != v3 {V3_PAGES[it['id']]}"
        )
        pages.update(it["pages"])
    assert len(pages) == EXPECTED_PAGES, (
        f"expected {EXPECTED_PAGES} unique pages, got {len(pages)}"
    )
    assert min(pages) == EXPECTED_MIN_PAGE, f"min page {min(pages)}"
    assert max(pages) == EXPECTED_MAX_PAGE, f"max page {max(pages)}"
    exceptions = {
        it["id"] for it in all_items if it["start_page"] != min(it["pages"])
    }
    assert not exceptions, f"start_page != min(pages): {sorted(exceptions)}"
    assert ids[0] == EXPECTED_FIRST_ID, f"first id {ids[0]}"
    assert ids[-1] == EXPECTED_LAST_ID, f"last id {ids[-1]}"

    out = {"masail": all_items}
    out_text = json.dumps(out, ensure_ascii=False, indent=1)
    assert LENTICULAR_PREFIX not in out_text, "lenticular markers remain in output!"
    assert not SHORT_PREFIX_RE.search(out_text), "short markers remain in output!"
    total_markers = len(RE_FULL.findall(out_text))
    print(
        f"VALIDATION PASSED: {len(all_items)} items, {len(pages)} unique pages "
        f"(min {min(pages)}, max {max(pages)}), volume_ids {sorted(vols)}, "
        f"first {ids[0]}, last {ids[-1]}, start_page == min(pages) for all, "
        f"derived start_page for {len(derived)} items, markers {total_markers}"
    )

    with open(os.path.join(BASE, "jild-25.json"), "w", encoding="utf-8") as f:
        f.write(out_text)
    print(f"WROTE jild-25.json: {len(out_text.encode('utf-8'))} bytes")


if __name__ == "__main__":
    main()
