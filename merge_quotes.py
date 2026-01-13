"""
Объединение и экспорт собранных цитат.
Объединяет все JSON файлы с цитатами и экспортирует в единый файл.
"""

import json
import glob
import logging
from typing import List, Dict, Set

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def merge_quotes(
    input_pattern: str = "*.json",
    exclude_files: List[str] = None,
    output_json: str = "ALL_QUOTES.json",
    output_txt: str = "ALL_QUOTES.txt"
) -> List[Dict]:
    """
    Объединение всех JSON файлов с цитатами.

    Args:
        input_pattern: Паттерн для поиска JSON файлов
        exclude_files: Список файлов для исключения
        output_json: Имя выходного JSON файла
        output_txt: Имя выходного TXT файла

    Returns:
        Список всех цитат
    """
    if exclude_files is None:
        exclude_files = [output_json, "ALL_QUOTES.json"]

    all_quotes = []
    seen_texts: Set[str] = set()

    logger.info(f"Looking for JSON files matching: {input_pattern}")

    json_files = glob.glob(input_pattern)
    json_files = [f for f in json_files if f not in exclude_files]

    logger.info(f"Found {len(json_files)} JSON files")

    for file in json_files:
        try:
            logger.info(f"Loading {file}...")
            with open(file, encoding="utf-8") as f:
                quotes = json.load(f)

            # Дедупликация по тексту
            for q in quotes:
                text = q.get("text", "").strip().lower()
                if text and text not in seen_texts:
                    seen_texts.add(text)
                    all_quotes.append(q)

            logger.info(f"Loaded {len(quotes)} quotes from {file}")

        except Exception as e:
            logger.error(f"Error loading {file}: {e}")
            continue

    logger.info(f"Total unique quotes: {len(all_quotes)}")

    # Сохраняем в JSON
    if all_quotes:
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(all_quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(all_quotes)} quotes to {output_json}")

        # Сохраняем в TXT
        with open(output_txt, "w", encoding="utf-8") as f:
            for q in all_quotes:
                text = q.get("text", "")
                author = q.get("author", "")
                if author:
                    f.write(f"{text} — {author}\n")
                else:
                    f.write(f"{text}\n")
        logger.info(f"Saved {len(all_quotes)} quotes to {output_txt}")

    return all_quotes


if __name__ == "__main__":
    merge_quotes()
