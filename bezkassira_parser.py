#!/usr/bin/env python3
# relax_parser.py
# Парсер для afisha.relax.by — все события в Минске

import re
import json
import logging
import time
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any

import requests
from bs4 import BeautifulSoup
from normalizer import normalize_place, normalize_title, normalize_price, is_minsk_event

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы
BASE_URL = "https://afisha.relax.by"
SOURCE_NAME = "relax.by"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

# Категории для парсинга
CATEGORIES = [
    {"url": f"{BASE_URL}/minsk/concert/", "category": "concert", "name": "Концерты"},
    {"url": f"{BASE_URL}/minsk/theatre/", "category": "theater", "name": "Театры"},
    {"url": f"{BASE_URL}/minsk/cinema/", "category": "cinema", "name": "Кино"},
    {"url": f"{BASE_URL}/minsk/exhibition/", "category": "exhibition", "name": "Выставки"},
    {"url": f"{BASE_URL}/minsk/forchildren/", "category": "kids", "name": "Детям"},
    {"url": f"{BASE_URL}/minsk/sport/", "category": "sport", "name": "Спорт"},
]


class RelaxParser:
    """Парсер афиши Relax.by"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.events = []
        self.stats = {
            "total_found": 0,
            "minsk_only": 0,
            "saved": 0,
            "errors": 0
        }
    
    def fetch_page(self, url: str, retries: int = 3) -> Optional[str]:
        """Загружает страницу с повторными попытками"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                response.encoding = 'utf-8'
                return response.text
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1}/{retries} не удалась: {url} — {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
        return None
    
    def parse_event_card(self, card: BeautifulSoup, category: str) -> Optional[Dict[str, Any]]:
        """Парсит одну карточку события"""
        try:
            # Название
            title_elem = card.find('div', class_='place-name')
            if not title_elem:
                title_elem = card.find('a', class_='title')
            if not title_elem:
                return None
            
            title = title_elem.get_text(strip=True)
            if not title:
                return None
            
            # Ссылка
            link_elem = card.find('a', href=True)
            url = link_elem['href'] if link_elem else ''
            if url and not url.startswith('http'):
                url = BASE_URL + url
            
            # Место
            venue_elem = card.find('div', class_='place')
            if not venue_elem:
                venue_elem = card.find('span', class_='place')
            venue = venue_elem.get_text(strip=True) if venue_elem else ''
            
            # Проверка на Минск
            if not is_minsk_event(venue):
                self.stats["minsk_only"] += 1
                return None
            
            # Дата
            date_elem = card.find('div', class_='date')
            if not date_elem:
                date_elem = card.find('span', class_='date')
            date_str = date_elem.get_text(strip=True) if date_elem else ''
            
            # Цена
            price_elem = card.find('div', class_='price')
            if not price_elem:
                price_elem = card.find('span', class_='price')
            price_str = price_elem.get_text(strip=True) if price_elem else ''
            price = normalize_price(price_str)
            
            # Возрастное ограничение
            age = '0+'
            age_match = re.search(r'(\d+)\+', title + ' ' + (date_str or ''))
            if age_match:
                age = f"{age_match.group(1)}+"
            
            # Изображение
            img_elem = card.find('img')
            image = img_elem.get('src') if img_elem else ''
            if image and not image.startswith('http'):
                image = BASE_URL + image
            
            # Описание
            desc_elem = card.find('div', class_='description')
            description = desc_elem.get_text(strip=True) if desc_elem else ''
            
            # Нормализуем дату
            event_date = self.normalize_date(date_str)
            
            # Фильтр: только будущие события (до 6 месяцев)
            if not self.is_future_date(event_date):
                return None
            
            return {
                'title': title,
                'venue': normalize_place(venue),
                'date': event_date,
                'price': price,
                'age': age,
                'category': category,
                'image': image,
                'url': url,
                'source': SOURCE_NAME,
                'description': description[:500],
            }
            
        except Exception as e:
            logger.error(f"Ошибка парсинга карточки: {e}")
            self.stats["errors"] += 1
            return None
    
    def normalize_date(self, date_str: str) -> str:
        """Нормализует дату в формат YYYY-MM-DD HH:MM"""
        if not date_str:
            return ""
        
        # Словарь месяцев
        months = {
            'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04',
            'мая': '05', 'июн': '06', 'июл': '07', 'авг': '08',
            'сен': '09', 'окт': '10', 'ноя': '11', 'дек': '12'
        }
        
        date_str = date_str.lower().strip()
        
        # Формат: 25 марта 2026, 19:00
        pattern = r'(\d{1,2})\s+([а-я]+)\s+(\d{4})(?:,\s*(\d{1,2}):(\d{2}))?'
        match = re.search(pattern, date_str)
        if match:
            day, month_ru, year = match.group(1), match.group(2), match.group(3)
            hour = match.group(4) or '00'
            minute = match.group(5) or '00'
            
            month = months.get(month_ru[:3], '01')
            return f"{year}-{month}-{int(day):02d} {hour}:{minute}"
        
        # Формат: 25.03.2026 19:00
        pattern2 = r'(\d{1,2})\.(\d{1,2})\.(\d{4})\s*(\d{1,2}):(\d{2})?'
        match = re.search(pattern2, date_str)
        if match:
            day, month, year = match.group(1), match.group(2), match.group(3)
            hour = match.group(4) or '00'
            minute = match.group(5) or '00'
            return f"{year}-{month}-{int(day):02d} {hour}:{minute}"
        
        return date_str
    
    def is_future_date(self, date_str: str, max_days: int = 180) -> bool:
        """Проверяет, является ли дата будущей и не дальше max_days"""
        if not date_str:
            return False
        
        try:
            # Пробуем извлечь дату
            date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_str)
            if date_match:
                year, month, day = map(int, date_match.groups())
                event_date = date(year, month, day)
                today = date.today()
                return today <= event_date <= today + timedelta(days=max_days)
        except:
            pass
        
        return True
    
    def parse_category(self, url: str, category: str, name: str) -> List[Dict[str, Any]]:
        """Парсит одну категорию"""
        logger.info(f"📥 Парсинг {name}: {url}")
        html = self.fetch_page(url)
        
        if not html:
            logger.error(f"Не удалось загрузить {url}")
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Поиск карточек событий
        cards = soup.find_all('div', class_='event-item')
        if not cards:
            cards = soup.find_all('div', class_='item')
        if not cards:
            cards = soup.find_all('article', class_='event')
        
        logger.info(f"  Найдено карточек: {len(cards)}")
        
        events = []
        for card in cards:
            event = self.parse_event_card(card, category)
            if event:
                events.append(event)
                self.stats["total_found"] += 1
        
        logger.info(f"  → Минск, годные: {len(events)}")
        return events
    
    def save_events(self, events: List[Dict[str, Any]]) -> int:
        """Сохраняет события в базу данных (заглушка — возвращает количество)"""
        # В реальности здесь будет сохранение в БД
        # Сейчас просто возвращаем количество для совместимости
        self.stats["saved"] = len(events)
        return len(events)
    
    def run(self) -> int:
        """Главный метод запуска парсера"""
        logger.info("=" * 50)
        logger.info("🚀 Relax.by парсер запущен")
        logger.info("=" * 50)
        
        all_events = []
        
        for cat in CATEGORIES:
            events = self.parse_category(
                cat["url"],
                cat["category"],
                cat["name"]
            )
            all_events.extend(events)
            time.sleep(1)  # Пауза между категориями
        
        # Сохраняем события
        saved = self.save_events(all_events)
        
        # Отчёт
        logger.info(f"\n📊 Итог Relax.by:")
        logger.info(f"  Всего событий: {self.stats['total_found']}")
        logger.info(f"  Не Минск: {self.stats['minsk_only']}")
        logger.info(f"  Сохранено: {saved}")
        logger.info(f"  Ошибок: {self.stats['errors']}")
        
        return saved


# Для тестирования
if __name__ == "__main__":
    parser = RelaxParser()
    result = parser.run()
    print(f"\nРезультат: {result} событий")
