#!/usr/bin/env python3
"""
Главный скрипт для автоматизации сбора цитат.

Выполняет полный пайплайн:
1. Запускает все harvest_*.py скрипты
2. Объединяет результаты
3. Импортирует в PostgreSQL
4. Дедуплицирует

Использование:
    python harvest_pipeline.py [--skip-harvest] [--skip-merge] [--skip-import] [--clear]
"""

import subprocess
import sys
import os
import glob
import logging
import time
from typing import List, Dict, Tuple
from pathlib import Path

# Импортируем функции из существующих модулей
from merge_quotes import merge_quotes
from import_to_postgres import import_to_postgres, print_db_statistics, get_db_statistics

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('harvest_pipeline.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class HarvestPipeline:
    """
    Класс для управления пайплайном сбора цитат.
    """

    def __init__(
        self,
        skip_harvest: bool = False,
        skip_merge: bool = False,
        skip_import: bool = False,
        clear_db: bool = False,
        harvest_pattern: str = "harvest_*.py",
        stats_only: bool = False
    ):
        """
        Инициализация пайплайна.

        Args:
            skip_harvest: Пропустить этап сбора
            skip_merge: Пропустить этап объединения
            skip_import: Пропустить этап импорта
            clear_db: Очистить БД перед импортом
            harvest_pattern: Паттерн для поиска harvest скриптов
        """
        self.skip_harvest = skip_harvest
        self.skip_merge = skip_merge
        self.skip_import = skip_import
        self.clear_db = clear_db
        self.harvest_pattern = harvest_pattern
        self.stats_only = stats_only
        self.stats = {
            "harvest": {
                "total": 0,
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "total_quotes": 0,
                "errors": []
            },
            "merge": {
                "input_files": 0,
                "output_quotes": 0
            },
            "import": {
                "loaded": 0,
                "saved": 0,
                "skipped": 0,
                "errors": 0
            }
        }

    def find_harvest_scripts(self) -> List[str]:
        """
        Поиск всех harvest скриптов.

        Returns:
            Список путей к harvest скриптам
        """
        scripts = glob.glob(self.harvest_pattern)
        scripts = [s for s in scripts if os.path.isfile(s)]
        scripts.sort()  # Детерминированный порядок
        return scripts

    def is_harvest_error_acceptable(self, error_output: str) -> bool:
        """
        Проверка, является ли ошибка приемлемой (сайт недоступен и т.д.).

        Args:
            error_output: Вывод ошибки

        Returns:
            True если ошибка приемлема (можно пропустить)
        """
        acceptable_errors = [
            "not accessible",
            "Connection refused",
            "ConnectionError",
            "Connection timeout",
            "Max retries exceeded",
            "No connection could be made",
            "actively refused",
            "is down or blocked",
            "rate limit",
            "403",
            "404",
            "No quotes found",
            "Site structure changed"
        ]
        error_lower = error_output.lower()
        return any(err.lower() in error_lower for err in acceptable_errors)

    def extract_quotes_count(self, output: str) -> int:
        """
        Извлечение количества собранных цитат из вывода скрипта.

        Args:
            output: Вывод скрипта

        Returns:
            Количество цитат или -1 если не найдено
        """
        import re
        # Ищем паттерны типа "Saved X quotes to file.json"
        patterns = [
            r'Saved\s+(\d+)\s+quotes',
            r'Saved\s+(\d+)\s+цитат',
            r'(\d+)\s+quotes\s+to\s+\w+\.json',
            r'(\d+)\s+цитат\s+в\s+\w+\.json',
            r'Total\s+quotes:\s+(\d+)',
            r'Всего\s+цитат:\s+(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, output, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        
        return -1

    def extract_errors(self, output: str) -> List[str]:
        """
        Извлечение ошибок и предупреждений из вывода скрипта.

        Args:
            output: Вывод скрипта

        Returns:
            Список ошибок и предупреждений
        """
        errors = []
        lines = output.split('\n')
        
        for line in lines:
            line_lower = line.lower()
            # Ищем строки с ERROR или WARNING
            if 'error' in line_lower or 'warning' in line_lower:
                # Пропускаем приемлемые ошибки
                if not self.is_harvest_error_acceptable(line):
                    errors.append(line.strip())
        
        return errors[:10]  # Ограничиваем до 10 ошибок

    def run_harvest_script(self, script_path: str) -> Tuple[bool, str, int, List[str]]:
        """
        Запуск одного harvest скрипта.

        Args:
            script_path: Путь к скрипту

        Returns:
            Кортеж (успех, сообщение, количество цитат, список ошибок)
        """
        script_name = os.path.basename(script_path)
        logger.info(f"Running {script_name}...")

        quotes_count = -1
        errors = []

        try:
            # Запускаем скрипт с перехватом вывода
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=3600,  # Максимум 1 час на скрипт
                encoding='utf-8',
                errors='replace'
            )

            # Объединяем stdout и stderr для анализа
            full_output = result.stdout + result.stderr
            
            # Извлекаем количество цитат
            quotes_count = self.extract_quotes_count(full_output)
            
            # Извлекаем ошибки
            errors = self.extract_errors(full_output)

            # Проверяем результат
            if result.returncode == 0:
                status_msg = f"✓ {script_name} completed successfully"
                if quotes_count >= 0:
                    status_msg += f" - {quotes_count} quotes harvested"
                logger.info(status_msg)
                
                # Показываем ошибки, если есть (предупреждения)
                if errors:
                    logger.warning(
                        f"  Warnings/errors from {script_name}:"
                    )
                    for error in errors:
                        logger.warning(f"    {error}")
                
                return True, "Success", quotes_count, errors

            # Если ошибка, проверяем, приемлема ли она
            if self.is_harvest_error_acceptable(full_output):
                logger.warning(
                    f"⚠ {script_name} failed with acceptable error "
                    "(site unavailable/blocked/etc). Skipping."
                )
                return False, "Acceptable error (site unavailable)", 0, errors

            # Неприемлемая ошибка
            logger.error(
                f"✗ {script_name} failed with error:\n"
                f"{full_output[:500]}"
            )
            return False, f"Error: {full_output[:200]}", 0, errors

        except subprocess.TimeoutExpired:
            logger.error(f"✗ {script_name} timed out after 1 hour")
            return False, "Timeout", 0, []
        except Exception as e:
            logger.error(f"✗ {script_name} failed with exception: {e}")
            return False, f"Exception: {str(e)}", 0, []

    def run_harvest_stage(self) -> bool:
        """
        Запуск этапа сбора цитат.

        Returns:
            True если хотя бы один скрипт успешно выполнился
        """
        if self.skip_harvest:
            logger.info("Skipping harvest stage (--skip-harvest)")
            return True

        logger.info("=" * 60)
        logger.info("STAGE 1: Harvesting quotes from sources")
        logger.info("=" * 60)

        scripts = self.find_harvest_scripts()
        self.stats["harvest"]["total"] = len(scripts)

        if not scripts:
            logger.warning("No harvest scripts found!")
            return False

        logger.info(f"Found {len(scripts)} harvest scripts")

        for script in scripts:
            success, message, quotes_count, errors = self.run_harvest_script(script)

            if success:
                self.stats["harvest"]["success"] += 1
                if quotes_count >= 0:
                    if "total_quotes" not in self.stats["harvest"]:
                        self.stats["harvest"]["total_quotes"] = 0
                    self.stats["harvest"]["total_quotes"] += quotes_count
                if errors:
                    self.stats["harvest"]["errors"].append({
                        "script": os.path.basename(script),
                        "type": "warnings",
                        "errors": errors
                    })
            elif "Acceptable" in message:
                self.stats["harvest"]["skipped"] += 1
            else:
                self.stats["harvest"]["failed"] += 1
                self.stats["harvest"]["errors"].append({
                    "script": os.path.basename(script),
                    "type": "error",
                    "error": message,
                    "errors": errors
                })

            # Небольшая задержка между скриптами
            time.sleep(1)

        logger.info("=" * 60)
        logger.info("Harvest stage completed:")
        logger.info(f"  Total scripts: {self.stats['harvest']['total']}")
        logger.info(f"  Successful: {self.stats['harvest']['success']}")
        logger.info(f"  Skipped (acceptable errors): {self.stats['harvest']['skipped']}")
        logger.info(f"  Failed: {self.stats['harvest']['failed']}")
        if self.stats['harvest']['total_quotes'] > 0:
            logger.info(f"  Total quotes harvested: {self.stats['harvest']['total_quotes']}")
        if self.stats['harvest']['errors']:
            logger.info(f"  Scripts with warnings/errors: {len(self.stats['harvest']['errors'])}")
        logger.info("=" * 60)

        return self.stats["harvest"]["success"] > 0

    def run_merge_stage(self) -> bool:
        """
        Запуск этапа объединения цитат.

        Returns:
            True если объединение прошло успешно
        """
        if self.skip_merge:
            logger.info("Skipping merge stage (--skip-merge)")
            return True

        logger.info("=" * 60)
        logger.info("STAGE 2: Merging quotes from all sources")
        logger.info("=" * 60)

        try:
            # Исключаем итоговые файлы из поиска
            exclude_files = [
                "ALL_QUOTES.json",
                "ALL_QUOTES.txt",
                "harvest_pipeline.log"
            ]

            quotes = merge_quotes(
                input_pattern="*.json",
                exclude_files=exclude_files,
                output_json="ALL_QUOTES.json",
                output_txt="ALL_QUOTES.txt"
            )

            self.stats["merge"]["output_quotes"] = len(quotes)

            logger.info("=" * 60)
            logger.info("Merge stage completed:")
            logger.info(f"  Total quotes: {self.stats['merge']['output_quotes']}")
            logger.info("=" * 60)

            return len(quotes) > 0

        except Exception as e:
            logger.error(f"Merge stage failed: {e}")
            return False

    def run_import_stage(self) -> bool:
        """
        Запуск этапа импорта в PostgreSQL.

        Returns:
            True если импорт прошёл успешно
        """
        if self.skip_import:
            logger.info("Skipping import stage (--skip-import)")
            return True

        logger.info("=" * 60)
        logger.info("STAGE 3: Importing quotes to PostgreSQL")
        logger.info("=" * 60)

        if not os.path.exists("ALL_QUOTES.json"):
            logger.error("ALL_QUOTES.json not found. Run merge stage first.")
            return False

        try:
            stats = import_to_postgres(
                input_file="ALL_QUOTES.json",
                clear_existing=self.clear_db
            )

            self.stats["import"] = stats

            logger.info("=" * 60)
            logger.info("Import stage completed")
            logger.info("=" * 60)

            return stats["saved"] > 0 or stats["skipped"] > 0

        except Exception as e:
            logger.error(f"Import stage failed: {e}")
            return False

    def run(self) -> bool:
        """
        Запуск полного пайплайна.

        Returns:
            True если пайплайн завершился успешно
        """
        # Если только статистика, показываем и выходим
        if self.stats_only:
            print_db_statistics()
            return True

        start_time = time.time()

        logger.info("=" * 60)
        logger.info("Starting Harvest Pipeline")
        logger.info("=" * 60)

        # Этап 1: Сбор цитат
        harvest_success = self.run_harvest_stage()
        if not harvest_success and not self.skip_harvest:
            logger.warning(
                "No harvest scripts succeeded. "
                "Continuing with merge/import anyway..."
            )

        # Этап 2: Объединение
        merge_success = self.run_merge_stage()
        if not merge_success:
            logger.error("Merge stage failed. Stopping pipeline.")
            return False

        # Этап 3: Импорт в БД
        import_success = self.run_import_stage()
        if not import_success:
            logger.error("Import stage failed.")
            return False

        # Итоговая статистика
        elapsed_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info(f"Total time: {elapsed_time:.2f} seconds")
        # Получаем финальную статистику из БД
        final_db_stats = get_db_statistics()

        logger.info("=" * 60)
        logger.info("Final statistics:")
        logger.info(f"  Harvest: {self.stats['harvest']['success']}/"
                    f"{self.stats['harvest']['total']} successful")
        if self.stats['harvest']['total_quotes'] > 0:
            logger.info(f"  Quotes harvested: {self.stats['harvest']['total_quotes']}")
        logger.info(f"  Merged quotes: {self.stats['merge']['output_quotes']}")
        logger.info(f"  Imported to DB: {self.stats['import']['saved']}")
        logger.info(f"  Skipped (duplicates): {self.stats['import']['skipped']}")
        logger.info("")
        logger.info("Final database statistics:")
        logger.info(f"  Total quotes in DB: {final_db_stats['total_quotes']}")
        logger.info("  By language:")
        for lang, count in sorted(
            final_db_stats['by_language'].items(),
            key=lambda x: x[1],
            reverse=True
        ):
            lang_name = "English" if lang == 'en' else "Russian" if lang == 'ru' else lang
            logger.info(f"    {lang_name} ({lang}): {count} quotes")
        logger.info(f"  Total authors: {final_db_stats['total_authors']}")
        if final_db_stats['authors_en'] > 0:
            logger.info(f"    English authors: {final_db_stats['authors_en']}")
        if final_db_stats['authors_ru'] > 0:
            logger.info(f"    Russian authors: {final_db_stats['authors_ru']}")
        
        # Показываем детали ошибок/предупреждений
        if self.stats['harvest']['errors']:
            logger.info("=" * 60)
            logger.info("Warnings/Errors from harvest scripts:")
            for error_info in self.stats['harvest']['errors']:
                script_name = error_info['script']
                if error_info.get('type') == 'warnings':
                    logger.warning(f"  {script_name}: {len(error_info.get('errors', []))} warnings")
                    for err in error_info.get('errors', [])[:3]:  # Показываем первые 3
                        logger.warning(f"    - {err}")
                else:
                    logger.error(f"  {script_name}: {error_info.get('error', 'Unknown error')}")
        
        logger.info("=" * 60)

        return True


def main():
    """Главная функция."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Harvest quotes pipeline - automated collection and import",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline
  python harvest_pipeline.py

  # Show only database statistics (no loading)
  python harvest_pipeline.py --stats

  # Skip harvest, only merge and import
  python harvest_pipeline.py --skip-harvest

  # Only harvest, don't merge or import
  python harvest_pipeline.py --skip-merge --skip-import

  # Clear database before import
  python harvest_pipeline.py --clear
        """
    )

    parser.add_argument(
        "--skip-harvest",
        action="store_true",
        help="Skip harvest stage"
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="Skip merge stage"
    )
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Skip import stage"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear database before import"
    )
    parser.add_argument(
        "--stats",
        "--statistics",
        action="store_true",
        help="Show only database statistics (no loading)"
    )

    args = parser.parse_args()

    pipeline = HarvestPipeline(
        skip_harvest=args.skip_harvest,
        skip_merge=args.skip_merge,
        skip_import=args.skip_import,
        clear_db=args.clear,
        stats_only=args.stats
    )

    success = pipeline.run()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
