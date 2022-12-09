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
from typing import Any, Mapping, Optional

import pandas as pd
import pystow
from tabulate import tabulate

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
TOPICS_PATH = DERIVED.joinpath("topics.tsv")
AFFILIATIONS_PATH = DERIVED.joinpath("affiliations.tsv")
FULL_TSV_PATH = DERIVED.joinpath("full_table.tsv")
FULL_JSON_PATH = DERIVED.joinpath("full.json")
BIOINFO_TSV_PATH = DERIVED.joinpath("bioinfo.tsv")
BIOINFO_JSON_PATH = DERIVED.joinpath("bioinfo.json")

ID = ""
SHEET_URL = f"https://docs.google.com/spreadsheets/d/1PAPRJ63yq9aPC1COLjaQp8mHmEq3rZUzwUYxTulyu78/export"
HEADER = [
    "username",
    "languages_primary",
    "languages_secondary",
    "affiliations",
    "email",
    "topics",
    "active_reviews",
    "total_reviews",
    "recent_year_reviews",
    "recent_quarter_reviews",
]
FORBIDDEN_CHARACTERS = ['"']
SEP = re.compile(r"[\n,/;]")


def _strip_split(s: Any) -> list[str]:
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


def clean_username(username: Any) -> Optional[str]:
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
        "@",
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


def clean_languages(language: Any) -> list[str]:
    """Clean the language column, parse, and normalize."""
    if not isinstance(language, str):
        return []
    language = language.strip()
    language = LANGUAGE_REWRITES.get(language, language)
    if language is None:
        return []
    language = language.replace("pl/pgsql", "plpgsql")
    language = language.replace("c/c++", "c, c++")
    language = language.replace("shell/bash", "bash")
    language = language.replace("tex/latex", "latex")
    language = language.replace("qml/qt", "qml")
    language = language.replace(";", ",")
    language = language.replace("/", ",")
    if "(" in language:
        logger.debug(f"FIXME (lang): {language}")
        return []
    if any(c in language for c in FORBIDDEN_CHARACTERS):
        raise ValueError(f"illegal character in language: {language}")
    return _strip_split(language)


def clean_topic(topic: Any) -> list[str]:
    """Clean the topic column, parse, and normalize."""
    if not isinstance(topic, str):
        return []
    topic = topic.strip()
    topic = TOPIC_REWRITES.get(topic, topic)
    if topic is None:
        return []
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


def clean_affiliations(affiliation: Any) -> list[str]:
    """Clean the affiliations column, parse, and normalize."""
    if not isinstance(affiliation, str):
        return []
    affiliation = affiliation.strip()
    affiliation = AFFILIATION_REWRITES.get(affiliation, affiliation)
    if affiliation is None:
        return []
    if affiliation.lower() in AFFILIATION_BLACKLIST:
        return []
    affiliation = affiliation.removeprefix("The ")
    if any(c in affiliation for c in FORBIDDEN_CHARACTERS):
        raise ValueError(f"invalid character in: {affiliation}")
    return [affiliation]


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
                "languages_primary": str,
                "languages_secondary": str,
            },
        ),
        force=force,
    )


def _nonempty(data: list) -> bool:
    return 0 < len(data)


def main(force: bool = True):
    """Run the normalization script."""
    df = get_df(force=force)

    df = df[df.username.map(lambda u: isinstance(u, str) and u.lower() not in USER_BLACKLIST)]
    # Fix username and remove missing
    df.username = df.username.map(clean_username)
    df = df[df.username.notna()]

    df.topics = df.topics.map(clean_topic)
    df = df[df.topics.map(_nonempty)]

    df.affiliations = df.affiliations.map(clean_affiliations)

    df.languages_primary = df.languages_primary.map(clean_languages)
    df = df[df.languages_primary.map(_nonempty)]
    df.languages_secondary = df.languages_secondary.map(clean_languages)

    for key in [
        "active_reviews",
        "total_reviews",
        "recent_year_reviews",
        "recent_quarter_reviews",
    ]:
        df[key] = df[key].fillna(0.0).astype(int)

    #################
    # DEDUPLICATION #
    #################
    df = pd.DataFrame(
        [_aggregate_duplicates(username, sdf) for username, sdf in df.groupby(["username"])],
        columns=HEADER,
    )

    email_counter = Counter(df.email)
    dup_email_counter = Counter(
        {
            email: count
            for email, count in email_counter.items()
            if isinstance(email, str) and count > 1
        }
    )
    logger.info(
        f"{len(dup_email_counter)} ({len(dup_email_counter) / len(email_counter):.2%}) emails"
        f" have duplicates of {len(email_counter):,} unique emails"
    )
    logger.info(tabulate(dup_email_counter.most_common(), headers=["email", "count"]))

    df = df.drop_duplicates("username", keep="last")

    ###########
    # SUMMARY #
    ###########
    topic_counter = Counter(topic for topics in df.topics if topics for topic in topics)
    topic_df = pd.DataFrame(topic_counter.most_common(), columns=["topic", "count"]).sort_values(
        ["count", "topic"], ascending=(False, True)
    )
    topic_df.to_csv(TOPICS_PATH, sep="\t", index=False)

    affiliation_counter = Counter(
        affiliation for affiliations in df.affiliations for affiliation in affiliations
    )
    affiliation_df = pd.DataFrame(
        affiliation_counter.most_common(), columns=["affiliation", "count"]
    ).sort_values(["count", "affiliation"], ascending=(False, True))
    affiliation_df.to_csv(AFFILIATIONS_PATH, sep="\t", index=False)

    ##########
    # EXPORT #
    ##########
    df = df.sort_values("username")
    df.to_csv(FULL_TSV_PATH, sep="\t", index=False)
    df.to_json(FULL_JSON_PATH, orient="records", indent=2, force_ascii=False)

    bio_idx = [
        (
            "python" in row["languages_primary"]
            and pd.notna(row["email"])
            and any(
                row["topics"] and x in row["topics"]
                for x in ("bioinformatics", "computational biology", "networks biology")
            )
        )
        for _, row in df.iterrows()
    ]
    bioinformatics_df = df[bio_idx]
    bioinformatics_df.to_csv(BIOINFO_TSV_PATH, sep="\t", index=False)
    bioinformatics_df.to_json(BIOINFO_JSON_PATH, orient="records", indent=2, force_ascii=False)

    to_triples(df, DERIVED.joinpath("triples.tsv"))


def _aggregate_duplicates(username: str, sdf: pd.DataFrame):
    return (
        username,
        sorted({i for row in sdf.languages_primary for i in row}),
        sorted({i for row in sdf.languages_secondary for i in row}),
        sorted({i for row in sdf.affiliations for i in row}),
        sdf.iloc[-1].email,
        sorted({i for row in sdf.topics for i in row}),
        sum(row for row in sdf.active_reviews),
        sum(row for row in sdf.total_reviews),
        sum(row for row in sdf.recent_year_reviews),
        sum(row for row in sdf.recent_quarter_reviews),
    )


def to_triples(df: pd.DataFrame, path: Path) -> None:
    """Write the dataframe as a triples file."""
    triples = set()
    for username, languages_primary, languages_secondary, topics in df[
        ["username", "languages_primary", "languages_secondary", "topics"]
    ].values:
        for language in languages_primary:
            triples.add((username, "primary_language", language))
        for language in languages_secondary:
            triples.add((username, "secondary_language", language))
        for topic in topics or []:
            triples.add((username, "topic", topic))
    with path.open("w") as file:
        for s, p, o in sorted(triples):
            print(s, p, o, sep="\t", file=file)  # noqa:T201


if __name__ == "__main__":
    main()
