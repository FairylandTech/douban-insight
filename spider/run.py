#!/usr/bin/env python
# coding: UTF-8
"""
豆瓣电影爬虫启动脚本
"""
import os
import sys
from pathlib import Path

# 添加项目路径到 sys.path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def check_requirements():
    """检查必要的配置和依赖"""
    print("正在检查配置...")

    # 检查 Cookie 文件
    cookie_file = project_root / "config" / "douban.cookies"
    if not cookie_file.exists():
        print("❌ 错误：Cookie 文件不存在")
        print("   请复制 config/douban.cookies.example 到 config/douban.cookies")
        print("   并填入你的豆瓣 Cookie")
        return False

    # 检查应用配置
    config_file = project_root / "config" / "application.yaml"
    if not config_file.exists():
        print("❌ 错误：配置文件不存在")
        print("   请复制 config/application.example.yaml 到 config/application.yaml")
        return False

    # 检查数据目录
    data_dir = project_root / "data"
    if not data_dir.exists():
        print("创建数据目录...")
        data_dir.mkdir(exist_ok=True)

    # 检查 Redis 连接
    try:
        from redis import Redis

        with open(config_file, "r", encoding="utf-8") as f:
            import yaml

            config = yaml.safe_load(f)
            redis_config = config.get("cache", {})

        client = Redis(
            host=redis_config.get("host", "localhost"),
            port=redis_config.get("port", 6379),
            db=redis_config.get("db", 0),
            password=redis_config.get("password"),
            socket_connect_timeout=2,
        )
        client.ping()
        print("✅ Redis 连接成功")
    except Exception as e:
        print(f"❌ 错误：无法连接到 Redis - {e}")
        print("   请确保 Redis 服务已启动")
        return False

    print("✅ 配置检查通过")
    return True


def main():
    """主函数"""
    print("=" * 60)
    print("豆瓣电影爬虫")
    print("=" * 60)
    print()

    if not check_requirements():
        print()
        print("请先完成上述配置，然后重新运行")
        sys.exit(1)

    print()
    print("开始爬取...")
    print("-" * 60)

    # 运行爬虫
    from scrapy.cmdline import execute

    os.chdir(str(project_root))
    execute(["scrapy", "crawl", "douban-movie"])


if __name__ == "__main__":
    main()
