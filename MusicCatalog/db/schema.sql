drop table if exists music_catalog;
drop table if exists music_catalog_import_metadata;

create table music_catalog_import_metadata (
    id integer primary key,
    reloaded_at timestamp with time zone not null,
    rows_loaded integer not null,
    unknown_codec_count integer not null,
    classical_genre_count integer not null,
    piano_genre_count integer not null
);

create table music_catalog (
    id serial primary key,
    title text,
    artist text,
    album text,
    album_artist text,
    track text,
    disc_number text,
    year text,
    genre text,
    composer text,
    conductor text,
    comment text,
    publisher text,
    copyright text,
    bpm text,
    filename text,
    extension text,
    directory text,
    path text,
    length text,
    size text,
    bitrate text,
    codec text,
    sample_rate text,
    mode text,
    tag text
);
