from util.loader import column_names as col


BOOLEAN = "boolean"
CHARACTER = "character"
INTEGER = "integer"
JSONB = "jsonb"

DT = "datatype"
CONSTRAINT = "constraint"

columns = {
    col.FOREIGN_ID: {DT: CHARACTER, CONSTRAINT: "varying(3000)"},
    col.LANDING_URL: {DT: CHARACTER, CONSTRAINT: "varying(1000)"},
    col.DIRECT_URL: {DT: CHARACTER, CONSTRAINT: "varying(3000)"},
    col.THUMBNAIL: {DT: CHARACTER, CONSTRAINT: "varying(3000)"},
    col.WIDTH: {DT: INTEGER, CONSTRAINT: ""},
    col.HEIGHT: {DT: INTEGER, CONSTRAINT: ""},
    col.FILESIZE: {DT: INTEGER, CONSTRAINT: ""},
    col.LICENSE: {DT: CHARACTER, CONSTRAINT: "varying(50)"},
    col.LICENSE_VERSION: {DT: CHARACTER, CONSTRAINT: "varying(25)"},
    col.CREATOR: {DT: CHARACTER, CONSTRAINT: "varying(2000)"},
    col.CREATOR_URL: {DT: CHARACTER, CONSTRAINT: "varying(2000)"},
    col.TITLE: {DT: CHARACTER, CONSTRAINT: "varying(5000)"},
    col.META_DATA: {DT: JSONB, CONSTRAINT: ""},
    col.TAGS: {DT: JSONB, CONSTRAINT: ""},
    col.WATERMARKED: {DT: BOOLEAN, CONSTRAINT: ""},
    col.PROVIDER: {DT: CHARACTER, CONSTRAINT: "varying(80)"},
    col.SOURCE: {DT: CHARACTER, CONSTRAINT: "varying(80)"},
    col.INGESTION_TYPE: {DT: CHARACTER, CONSTRAINT: "varying(80)"},
    col.DURATION: {DT: INTEGER, CONSTRAINT: ""},
    col.BIT_RATE: {DT: INTEGER, CONSTRAINT: ""},
    col.SAMPLE_RATE: {DT: INTEGER, CONSTRAINT: ""},
    col.CATEGORY: {DT: CHARACTER, CONSTRAINT: "varying(100)"},
    col.GENRES: {DT: CHARACTER, CONSTRAINT: "varying(80)[]"},
    col.AUDIO_SET: {DT: JSONB, CONSTRAINT: ""},
    col.ALT_FILES: {DT: JSONB, CONSTRAINT: ""},
}

AUDIO_COLUMNS = [
    col.FOREIGN_ID,
    col.LANDING_URL,
    col.DIRECT_URL,
    col.THUMBNAIL,
    col.FILESIZE,
    col.LICENSE,
    col.LICENSE_VERSION,
    col.CREATOR,
    col.CREATOR_URL,
    col.TITLE,
    col.META_DATA,
    col.TAGS,
    col.WATERMARKED,
    col.PROVIDER,
    col.SOURCE,
    col.INGESTION_TYPE,
    col.DURATION,
    col.BIT_RATE,
    col.SAMPLE_RATE,
    col.CATEGORY,
    col.GENRES,
    col.AUDIO_SET,
    col.ALT_FILES,
]

IMAGE_COLUMNS = [
    col.FOREIGN_ID,
    col.LANDING_URL,
    col.DIRECT_URL,
    col.THUMBNAIL,
    col.WIDTH,
    col.HEIGHT,
    col.FILESIZE,
    col.LICENSE,
    col.LICENSE_VERSION,
    col.CREATOR,
    col.CREATOR_URL,
    col.TITLE,
    col.META_DATA,
    col.TAGS,
    col.WATERMARKED,
    col.PROVIDER,
    col.SOURCE,
    col.INGESTION_TYPE,
]


def create_table_fields(column_list: col) -> str:
    column_descriptions = []
    for column in column_list:
        dt = columns[column][DT]
        constraint = columns[column][CONSTRAINT]
        if constraint != "":
            constraint = f" {constraint}"
        column_string = f"{column} {dt}{constraint}"
        column_descriptions.append(column_string)
    return ",\n".join(column_descriptions)
