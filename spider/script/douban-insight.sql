drop database if exists douban_insight;
create database douban_insight charset utf8mb4 collate utf8mb4_general_ci;
use douban_insight;

-- 电影信息
create table if not exists tb_movie
(
    id            integer primary key auto_increment,
    movie_id      varchar(32) unique                                             not null,
    full_name     varchar(255)                                                   not null,
    chinese_name  varchar(255),
    original_name varchar(255),
    release_date  date,
    score         float,
    summary       text,
    created_at    datetime default current_timestamp                             not null,
    updated_at    datetime default current_timestamp on update current_timestamp not null,
    deleted       boolean  default false
);

# 演员表
create table if not exists tb_artist
(
    id         integer primary key auto_increment,
    artist_id  varchar(32) unique                                             not null,
    name       varchar(128) unique                                            not null,
    birthday   date,
    photo      varchar(255),
    personage  text,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 电影导演
create table if not exists tb_movie_director_artist_relation
(
    id         integer primary key auto_increment,
    movie_id   integer                                                        not null,
    artist_id  integer                                                        not null,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 电影编剧
create table if not exists tb_movie_writer_artist_relation
(
    id         integer primary key auto_increment,
    movie_id   integer                                                        not null,
    artist_id  integer                                                        not null,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 电影演员
create table if not exists tb_movie_actor_artist_relation
(
    id         integer primary key auto_increment,
    movie_id   integer                                                        not null,
    artist_id  integer                                                        not null,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 电影类型
create table if not exists tb_movie_type
(
    id         integer primary key auto_increment,
    name       varchar(64) unique                                             not null,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 电影<->电影类型
create table if not exists tb_movie_type_relation
(
    id         integer primary key auto_increment,
    movie_id   integer                                                        not null,
    type_id    integer                                                        not null,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 电影国家/地区
create table if not exists tb_movie_country
(
    id         integer primary key auto_increment,
    name       varchar(64) unique                                             not null,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 电影<->电影国家/地区
create table if not exists tb_movie_country_relation
(
    id         integer primary key auto_increment,
    movie_id   integer                                                        not null,
    country_id integer                                                        not null,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 电影评论
create table if not exists tb_movie_comment
(
    id         integer primary key auto_increment,
    movie_id   integer                                                        not null,
    content    text                                                           not null,
    rating     integer                                                        not null,
    created_at datetime default current_timestamp                             not null,
    updated_at datetime default current_timestamp on update current_timestamp not null,
    deleted    boolean  default false                                         not null
);

# 插入 电影类型
insert into
    tb_movie_type (name)
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
    ('短片');

# 插入 电影国家/地区
insert into
    tb_movie_country (name)
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
    ('丹麦');