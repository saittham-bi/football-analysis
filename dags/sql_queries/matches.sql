DROP TABLE IF EXISTS matches;

CREATE TABLE matches (
    "wk" NUMERIC ,
    "day" TEXT,
    "date" DATE,
    "time" TIME,
    "home" TEXT,
    "xg" DECIMAL,
    "score" TEXT,
    "xg_away" DECIMAL,
    "away" TEXT,
    "attendance" NUMERIC,
    "venue" TEXT,
    "refereee" TEXT,
    "match_report" TEXT,
    "notes" TEXT
);

COPY matches(
    "wk",
    "day",
    "date",
    "time",
    "home",
    "xg",
    "score",
    "xg_away",
    "away",
    "attendance",
    "venue",
    "refereee",
    "match_report",
    "notes"
)
FROM temp.name
DELIMITER ','
CSV HEADER;
