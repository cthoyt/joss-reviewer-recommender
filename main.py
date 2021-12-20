# TODO deduplication by github handle
# TODO add github API lookup for actual primary language and recent contributions

"""Download and process JOSS reviewers.

.. seealso:: https://github.com/dfm/joss-reviewer/blob/main/joss_reviewer.py
"""

import json
import logging
import re
from collections import Counter
from pathlib import Path
from typing import Mapping, Optional

import pandas as pd
import pystow

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent.resolve()
DATA = HERE.joinpath("_data")

RULES = DATA.joinpath("rules")
RULES.mkdir(exist_ok=True, parents=True)

LANGUAGE_BLACKLIST_PATH = RULES.joinpath("language_blacklist.json")
LANGUAGE_BLACKLIST = {s.lower() for s in json.loads(LANGUAGE_BLACKLIST_PATH.read_text())}
LANGUAGE_MAPPING_PATH = RULES.joinpath("language_mapping.json")
LANGUAGE_MAPPING = json.loads(LANGUAGE_MAPPING_PATH.read_text())
LANGUAGE_REWRITES_PATH = RULES.joinpath("language_rewrites.json")
LANGUAGE_REWRITES: Mapping[str, Optional[str]] = json.loads(LANGUAGE_REWRITES_PATH.read_text())

AFFILIATION_BLACKLIST_PATH = RULES.joinpath("affiliation_blacklist.json")
AFFILIATION_BLACKLIST = {s.lower() for s in json.loads(AFFILIATION_BLACKLIST_PATH.read_text())}
AFFILIATION_REWRITES_PATH = RULES.joinpath("affiliation_rewrites.json")
AFFILIATION_REWRITES = json.loads(AFFILIATION_REWRITES_PATH.read_text())

USER_BLACKLIST_PATH = RULES.joinpath("user_blacklist.json")
USER_BLACKLIST = set(json.loads(USER_BLACKLIST_PATH.read_text()))
USERNAME_REMAPPING_PATH = RULES.joinpath("user_mapping.json")
USERNAME_REMAPPING = json.loads(USERNAME_REMAPPING_PATH.read_text())

TOPIC_BLACKLIST_PATH = RULES.joinpath("topic_blacklist.json")
TOPIC_BLACKLIST = set(json.loads(TOPIC_BLACKLIST_PATH.read_text()))
TOPIC_REWRITES_PATH = RULES.joinpath("topic_rewrites.json")
TOPIC_REWRITES = json.loads(TOPIC_REWRITES_PATH.read_text())
TOPIC_MAPPING_PATH = RULES.joinpath("topic_mapping.json")
TOPIC_MAPPING = json.loads(TOPIC_MAPPING_PATH.read_text())
TOPIC_INTERJECTIONS_PATH = RULES.joinpath("topic_interjections.json")
TOPIC_INTERJECTIONS = json.loads(TOPIC_INTERJECTIONS_PATH.read_text())

DERIVED = DATA.joinpath("output")
DERIVED.mkdir(exist_ok=True, parents=True)
FULL_PATH = DERIVED.joinpath("full.tsv")
TOPICS_PATH = DERIVED.joinpath("topics.tsv")
AFFILIATIONS_PATH = DERIVED.joinpath("affiliations.tsv")

BIOINFO_PATH = DERIVED.joinpath("bioinfo.tsv")

ID = "1PAPRJ63yq9aPC1COLjaQp8mHmEq3rZUzwUYxTulyu78"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{ID}/export"
HEADER = [
    "username",
    "language",
    "language_other",
    "affiliation",
    "email",
    "topics",
    "active_reviews",
    "total_reviews",
    "recent_year_reviews",
    "recent_quarter_reviews",
]
FORBIDDEN_CHARACTERS = ['"']
SEP = re.compile(r"[\n,/;]")


def _strip_split(s: any) -> list[str]:
    if not isinstance(s, str):
        return []
    rv = []
    for entry in SEP.split(s.strip()):
        entry = entry.strip().lower()
        if not entry or entry in LANGUAGE_BLACKLIST:
            continue
        entry = LANGUAGE_MAPPING.get(entry, entry)
        rv.append(entry)
    return rv


def _move(preferred: any, other: any) -> tuple[str, list[str]]:
    preferred_list = _strip_split(preferred)
    other_list = _strip_split(other)
    if preferred_list:
        preferred_value, *other_acquired = preferred_list
    elif other_list:
        preferred_value = other_list.pop(0)
        other_acquired = []
    else:
        preferred_value = None
        other_acquired = []

    return preferred_value, sorted(set(other_list).union(other_acquired))


def clean_username(username: any) -> Optional[str]:
    """Clean data in the username column."""
    if not isinstance(username, str):
        return None
    username = username.strip()
    username = USERNAME_REMAPPING.get(username, username)
    if username is None:
        return None
    if username in USER_BLACKLIST:
        return None
    for prefix in (
        "www.github.com/",
        "http://github.com/",
        "github.com/",
        "github. com/",
    ):
        username = username.removeprefix(prefix)
    if "bitbucket.org" in username:
        return None
    if "researchgate.net" in username:
        return None
    if "https://gitlab" in username:
        return None
    if "/" in username:
        return None
    if "@" in username:  # email given
        return None
    if len(username) <= 2:  # too short to be real
        return None
    if " " in username:  # written as a name or comment
        return None
    if any(c in username for c in FORBIDDEN_CHARACTERS):
        raise ValueError(f"illegal character in username: {username}")
    return username


def clean_language(language: any) -> Optional[str]:
    if not isinstance(language, str):
        return None
    language = language.strip()
    language = LANGUAGE_REWRITES.get(language, language)
    if language is None:
        return None
    language = language.replace("pl/pgsql", "plpgsql")
    language = language.replace("c/c++", "c, c++")
    language = language.replace("shell/bash", "bash")
    language = language.replace("tex/latex", "latex")
    language = language.replace("qml/qt", "qml")
    language = language.replace(";", ",")
    language = language.replace("/", ",")
    if "(" in language:
        logger.debug(f"FIXME (lang): {language}")
        return None
    if any(c in language for c in FORBIDDEN_CHARACTERS):
        raise ValueError(f"illegal character in language: {language}")
    return language


def clean_topic(topic: any) -> Optional[list[str]]:
    if not isinstance(topic, str):
        return None
    topic = topic.strip()
    topic = TOPIC_REWRITES.get(topic, topic)
    if topic is None:
        return None
    for interjection, replacement in TOPIC_INTERJECTIONS.items():
        topic = topic.replace(interjection, replacement).strip()
    if any(c in topic for c in FORBIDDEN_CHARACTERS):
        raise ValueError(f"invalid character in: {topic}")
    topic = topic.replace(";", ",")
    topic = topic.replace("/", ",")
    topic = topic.replace("  ", " ")
    topic = topic.replace("modelling", "modeling")
    if "." in topic:
        logger.debug(f"FIXME (topic): {topic}")
    if "(" in topic and False:
        logger.debug(f"FIXME (topic): {topic}")

    rv = []
    for line in topic.lower().strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        for s in line.split(","):
            s = s.strip().strip(".")
            if not s:
                continue
            s = TOPIC_MAPPING.get(s, s)
            if s in TOPIC_BLACKLIST:
                continue
            rv.append(s)
    return rv


def clean_affiliation(affiliation: any) -> Optional[str]:
    if not isinstance(affiliation, str):
        return None
    affiliation = affiliation.strip()
    affiliation = AFFILIATION_REWRITES.get(affiliation, affiliation)
    if affiliation is None:
        return None
    if affiliation.lower() in AFFILIATION_BLACKLIST:
        return None
    if any(c in affiliation for c in FORBIDDEN_CHARACTERS):
        raise ValueError(f"invalid character in: {affiliation}")
    return affiliation


def get_df(force: bool = False) -> pd.DataFrame:
    """Get the raw JOSS reviewer sheet via excel."""
    return pystow.ensure_excel(
        "joss",
        url=SHEET_URL,
        name="reviewers.xlsx",
        read_excel_kwargs=dict(
            skiprows=2,
            header=None,
            names=HEADER,
            dtype={
                "language": str,
                "language_other": str,
            },
        ),
        force=force,
    )


def main(force: bool = False):
    df = get_df(force=force)

    df = df[
        df.username.map(lambda u: isinstance(u, str) and u.lower() not in USER_BLACKLIST)
    ]
    # Fix username and remove missing
    df.username = df.username.map(clean_username)
    df = df[df.username.notna()]

    username_counter = Counter(df.username)
    dup_counter = {name for name, count in username_counter.items() if count > 1}
    logger.info(
        f"{len(dup_counter)} ({len(dup_counter) / len(username_counter):.2%}) GitHub"
        f" handles have duplicates of {len(username_counter):,} unique GitHub handles"
    )

    df.topics = df.topics.map(clean_topic)
    # df = df[df.topics.notna()]

    df.affiliation = df.affiliation.map(clean_affiliation)

    df.language = df.language.map(clean_language)
    df.language_other = df.language_other.map(clean_language)
    df["language"], df["language_other"] = zip(
        *(
            _move(preferred, other)
            for preferred, other in df[["language", "language_other"]].values
        )
    )
    # Discard potential reviewers who have not annotated their preferred language (or did it very wrong)
    df = df[df.language.notna()]

    for key in [
        "active_reviews",
        "total_reviews",
        "recent_year_reviews",
        "recent_quarter_reviews",
    ]:
        df[key] = df[key].fillna(0.0).astype(int)

    topic_counter = Counter(topic for topics in df.topics if topics for topic in topics)
    topic_df = pd.DataFrame(topic_counter.most_common(), columns=["topic", "count"])
    topic_df.to_csv(TOPICS_PATH, sep="\t", index=False)

    affiliation_counter = Counter(a for a in df.affiliation if a and a.strip())
    affiliation_df = pd.DataFrame(affiliation_counter.most_common(), columns=["affiliation", "count"])
    affiliation_df.to_csv(AFFILIATIONS_PATH, sep="\t", index=False)

    df = df.sort_values("username")
    df.to_csv(FULL_PATH, sep="\t", index=False)

    bio_idx = [
        (
            row["language"] == "python"
            and pd.notna(row["email"])
            and any(
            row["topics"] and x in row["topics"]
            for x in ("bioinformatics", "computational biology", "networks biology")
        )
        )
        for _, row in df.iterrows()
    ]
    bioinformatics_df = df[bio_idx]
    bioinformatics_df.to_csv(BIOINFO_PATH, sep="\t", index=False)

    to_triples(df, DERIVED.joinpath("triples.tsv"))


def to_triples(df: pd.DataFrame, path: Path) -> None:
    triples = set()
    for username, primary_language, secondary_languages, topics in df[
        ["username", "language", "language_other", "topics"]
    ].values:
        triples.add((username, "primary_language", primary_language))
        for language in secondary_languages:
            triples.add((username, "secondary_language", language))
        for topic in topics or []:
            triples.add((username, "topic", topic))
    with path.open("w") as file:
        for s, p, o in triples:
            print(s, p, o, sep="\t", file=file)


if __name__ == "__main__":
    main()
