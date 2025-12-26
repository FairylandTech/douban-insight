\c daouban;

drop schema if exists movie cascade;
create schema if not exists movie;

-- 电影信息
create table if not exists movie.tb_movie
(
    id            serial primary key,
    movie_id      varchar(32) unique        not null,
    full_name     varchar(255)              not null,
    chinese_name  varchar(255),
    original_name varchar(255),
    release_date  date,
    score         float,
    summary       text,
    icon          varchar(255),
    created_at    timestamptz default now() not null,
    updated_at    timestamptz default now() not null,
    deleted       boolean     default false
);

-- 演员表
create table if not exists movie.tb_artist
(
    id         serial primary key,
    artist_id  varchar(128) unique       not null,
    name       varchar(128)              not null,
    created_at timestamptz default now() not null,
    updated_at timestamptz default now() not null,
    deleted    boolean     default false not null
);

-- 电影导演
create table if not exists movie.tb_movie_director_artist_relation
(
    id         serial primary key,
    movie_id   varchar(32)               not null,
    artist_id  integer                   not null,
    created_at timestamptz default now() not null,
    updated_at timestamptz default now() not null,
    deleted    boolean     default false not null,
    constraint uq_movie_id_artist_id_director unique (movie_id, artist_id)
);

-- 电影编剧
create table if not exists movie.tb_movie_writer_artist_relation
(
    id         serial primary key,
    movie_id   varchar(32)               not null,
    artist_id  integer                   not null,
    created_at timestamptz default now() not null,
    updated_at timestamptz default now() not null,
    deleted    boolean     default false not null,
    constraint uq_movie_id_artist_id_writer unique (movie_id, artist_id)
);

-- 电影演员
create table if not exists movie.tb_movie_actor_artist_relation
(
    id         serial primary key,
    movie_id   varchar(32)               not null,
    artist_id  integer                   not null,
    created_at timestamptz default now() not null,
    updated_at timestamptz default now() not null,
    deleted    boolean     default false not null,
    constraint uq_movie_id_artist_id_actor unique (movie_id, artist_id)
);

-- 电影类型
create table if not exists movie.tb_movie_type
(
    id         serial primary key,
    name       varchar(64) unique        not null,
    created_at timestamptz default now() not null,
    updated_at timestamptz default now() not null,
    deleted    boolean     default false not null
);

-- 电影<->电影类型
create table if not exists movie.tb_movie_type_relation
(
    id         serial primary key,
    movie_id   varchar(32)               not null,
    type_id    integer                   not null,
    created_at timestamptz default now() not null,
    updated_at timestamptz default now() not null,
    deleted    boolean     default false not null,
    constraint uq_movie_id_type_id unique (movie_id, type_id)
);

-- 电影国家/地区
create table if not exists movie.tb_movie_country
(
    id         serial primary key,
    name       varchar(64) unique        not null,
    created_at timestamptz default now() not null,
    updated_at timestamptz default now() not null,
    deleted    boolean     default false not null
);

-- 电影<->电影国家/地区
create table if not exists movie.tb_movie_country_relation
(
    id         serial primary key,
    movie_id   varchar(32)               not null,
    country_id integer                   not null,
    created_at timestamptz default now() not null,
    updated_at timestamptz default now() not null,
    deleted    boolean     default false not null,
    constraint uq_movie_id_country_id unique (movie_id, country_id)
);

-- 电影评论
create table if not exists movie.tb_movie_comment
(
    id         serial primary key,
    movie_id   varchar(32)             not null,
    comment_id varchar(32)             not null,
    content    text                    not null,
    created_at timestamp default now() not null,
    updated_at timestamp default now() not null,
    deleted    boolean   default false not null
);

-- 插入 电影类型
insert into
    movie.tb_movie_type (name)
values
    ('喜剧'),
    ('爱情'),
    ('动作'),
    ('科幻'),
    ('动画'),
    ('悬疑'),
    ('犯罪'),
    ('惊悚'),
    ('冒险'),
    ('音乐'),
    ('历史'),
    ('奇幻'),
    ('恐怖'),
    ('战争'),
    ('传记'),
    ('歌舞'),
    ('武侠'),
    ('情色'),
    ('灾难'),
    ('西部'),
    ('纪录片'),
    ('短片')
on conflict (name) do nothing;

-- 插入 电影国家/地区
insert into
    movie.tb_movie_country (name)
values
    ('欧美'),
    ('韩国'),
    ('日本'),
    ('中国大陆'),
    ('美国'),
    ('中国香港'),
    ('中国台湾'),
    ('英国'),
    ('法国'),
    ('德国'),
    ('意大利'),
    ('西班牙'),
    ('印度'),
    ('泰国'),
    ('俄罗斯'),
    ('加拿大'),
    ('澳大利亚'),
    ('爱尔兰'),
    ('瑞典'),
    ('巴西'),
    ('丹麦')
on conflict (name) do nothing;;
