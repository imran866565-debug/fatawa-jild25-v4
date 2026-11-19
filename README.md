# Fatawa Razviya — Jild 25 tashreeh v4 merge

Merge workspace: 12 leaf chunk files of the easy-Urdu tashreeh v4 for Fatawa Razviya Jild 25 (volume_id 37; 168 items, 608 pages) are merged into `jild-25.json` by GitHub Actions (`merge_jild25_v4.py`), then published as a v1.0 release asset.

Item order reproduces the v3 merged file exactly (first jild25-pesh-lafz, last jild25-masala-183). All 12 leaf chunks carry start_page == min(pages). No renames at merge — the 18 sub-items are absorbed by the masala-158 fold — and the merge joins 4 simple duplicate-id groups (masala-25, masala-113, masala-142, masala-157) plus the masala-158 mega-fold (20 parts -> ONE item [503-630], 128 pages, jawab joined with no separator), then stable-sorts by start_page, matching the v3 merged file. No page markers are present in the chunks.
