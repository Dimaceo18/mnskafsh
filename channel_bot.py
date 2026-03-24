import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.utils import executor
from aiogram.dispatcher.filters import Text
import aiosqlite

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
ADMIN_ID = int(os.getenv('ADMIN_ID', 0))
MAX_POSTS_PER_HOUR = int(os.getenv('MAX_POSTS_PER_HOUR', 5))
MAX_POSTS_PER_DAY = int(os.getenv('MAX_POSTS_PER_DAY', 20))

if not BOT_TOKEN:
    logger.error("BOT_TOKEN не установлен!")
    sys.exit(1)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Парсеры (будем импортировать позже, когда создадим файлы)
PARSERS_AVAILABLE = False
try:
    from relax_parser import RelaxParser
    from ticketpro_parser import TicketproParser
    from bycard_parser import BycardParser
    from bezkassira_parser import BezkassiraParser
    from normalizer import normalize_event
    PARSERS_AVAILABLE = True
    logger.info("Парсеры загружены")
except ImportError as e:
    logger.warning(f"Парсеры не загружены: {e}")

class Database:
    async def init_db(self):
        self.conn = await aiosqlite.connect('events.db')
        
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS published_events (
                id TEXT PRIMARY KEY,
                title TEXT,
                published_at TIMESTAMP,
                source TEXT
            )
        ''')
        
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                title TEXT,
                venue TEXT,
                date TEXT,
                date_start TIMESTAMP,
                price TEXT,
                age TEXT,
                category TEXT,
                image_url TEXT,
                ticket_url TEXT,
                source TEXT,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP
            )
        ''')
        
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS posts_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                posted_at TIMESTAMP,
                status TEXT
            )
        ''')
        
        await self.conn.commit()
        logger.info("База данных инициализирована")
    
    async def save_event(self, event):
        event_id = f"{event.get('title')}_{event.get('date')}_{event.get('venue')}"
        
        date_start = None
        try:
            date_str = event.get('date', '')
            for fmt in ['%d.%m.%Y %H:%M', '%d.%m.%Y', '%Y-%m-%d', '%d %B %Y', '%d %B %Y %H:%M']:
                try:
                    date_start = datetime.strptime(date_str, fmt)
                    break
                except:
                    continue
        except:
            pass
        
        await self.conn.execute('''
            INSERT OR REPLACE INTO events 
            (id, title, venue, date, date_start, price, age, category, image_url, ticket_url, source, description, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            event_id, event.get('title'), event.get('venue'), event.get('date'),
            date_start, event.get('price'), event.get('age', '0+'),
            event.get('category', 'all'), event.get('image'), event.get('url'),
            event.get('source', 'unknown'), event.get('description', ''), datetime.now()
        ))
        await self.conn.commit()
        return event_id
    
    async def is_published(self, event_id):
        cursor = await self.conn.execute(
            'SELECT id FROM published_events WHERE id = ?',
            (event_id,)
        )
        result = await cursor.fetchone()
        return result is not None
    
    async def mark_published(self, event_id, title, source):
        await self.conn.execute(
            'INSERT INTO published_events (id, title, published_at, source) VALUES (?, ?, ?, ?)',
            (event_id, title, datetime.now(), source)
        )
        await self.conn.commit()
    
    async def log_post(self, event_id, status):
        await self.conn.execute(
            'INSERT INTO posts_log (event_id, posted_at, status) VALUES (?, ?, ?)',
            (event_id, datetime.now(), status)
        )
        await self.conn.commit()
    
    async def get_posts_count_last_hour(self):
        hour_ago = datetime.now() - timedelta(hours=1)
        cursor = await self.conn.execute(
            'SELECT COUNT(*) FROM posts_log WHERE posted_at > ?',
            (hour_ago,)
        )
        result = await cursor.fetchone()
        return result[0] if result else 0
    
    async def get_posts_count_today(self):
        today = datetime.now().replace(hour=0, minute=0, second=0)
        cursor = await self.conn.execute(
            'SELECT COUNT(*) FROM posts_log WHERE posted_at > ?',
            (today,)
        )
        result = await cursor.fetchone()
        return result[0] if result else 0

db = Database()

async def can_publish():
    last_hour = await db.get_posts_count_last_hour()
    today = await db.get_posts_count_today()
    
    if last_hour >= MAX_POSTS_PER_HOUR:
        logger.warning(f"Лимит постов за час ({MAX_POSTS_PER_HOUR}) достигнут")
        return False
    if today >= MAX_POSTS_PER_DAY:
        logger.warning(f"Лимит постов за день ({MAX_POSTS_PER_DAY}) достигнут")
        return False
    return True

async def publish_to_channel(event):
    if not await can_publish():
        return False
    
    try:
        title = event.get('title', 'Без названия')
        venue = event.get('venue', 'Место не указано')
        date = event.get('date', 'Дата не указана')
        price = event.get('price', 'Цена не указана')
        age = event.get('age', '0+')
        url = event.get('url', '')
        image = event.get('image', '')
        category = event.get('category', 'Событие')
        
        category_emoji = {
            'concert': '🎤', 'theater': '🎭', 'cinema': '🎬',
            'exhibition': '🖼', 'kids': '👶', 'sport': '⚽',
            'free': '💰'
        }.get(event.get('category', 'all'), '📅')
        
        text = f"{category_emoji} *{title}*\n\n"
        text += f"📍 *Место:* {venue}\n"
        text += f"🗓 *Дата:* {date}\n"
        text += f"💰 *Цена:* {price}\n"
        text += f"👤 *Возраст:* {age}\n\n"
        
        if url:
            text += f"🔗 [Купить билет]({url})"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton(
            text="📱 Подробнее в боте",
            url=f"https://t.me/{bot.username}?start=event_{event.get('id', '')}"
        ))
        
        if image and image.startswith('http'):
            await bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=image,
                caption=text,
                parse_mode='Markdown',
                reply_markup=keyboard
            )
        else:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode='Markdown',
                reply_markup=keyboard
            )
        
        event_id = f"{title}_{date}_{venue}"
        await db.mark_published(event_id, title, event.get('source', 'unknown'))
        await db.log_post(event_id, 'published')
        
        logger.info(f"✅ Опубликовано: {title}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка публикации: {e}")
        return False

async def run_parser_and_publish():
    if not PARSERS_AVAILABLE:
        logger.error("Парсеры не доступны")
        return 0
    
    await db.init_db()
    
    parsers = [
        ('Relax', RelaxParser()),
        ('Ticketpro', TicketproParser()),
        ('Bycard', BycardParser()),
        ('Bezkassira', BezkassiraParser())
    ]
    
    all_events = []
    published_count = 0
    
    for name, parser in parsers:
        try:
            logger.info(f"🔍 Запуск парсера {name}")
            events = await parser.parse()
            
            for event in events:
                normalized = normalize_event(event)
                if normalized:
                    normalized['source'] = name
                    all_events.append(normalized)
                    
        except Exception as e:
            logger.error(f"❌ Ошибка {name}: {e}")
    
    source_priority = {'Ticketpro': 1, 'Relax': 2, 'Bycard': 3, 'Bezkassira': 4}
    all_events.sort(key=lambda x: (source_priority.get(x.get('source'), 5), x.get('date_start', datetime.max)))
    
    for event in all_events:
        event_id = f"{event.get('title')}_{event.get('date')}_{event.get('venue')}"
        
        if not await db.is_published(event_id):
            await db.save_event(event)
            success = await publish_to_channel(event)
            if success:
                published_count += 1
                await asyncio.sleep(5)
    
    logger.info(f"📊 Опубликовано {published_count} из {len(all_events)} событий")
    return published_count

async def send_morning_digest():
    await db.init_db()
    
    today = datetime.now().replace(hour=0, minute=0, second=0)
    tomorrow = today + timedelta(days=1)
    
    cursor = await db.conn.execute('''
        SELECT title, venue, date, category FROM events 
        WHERE date_start BETWEEN ? AND ? AND is_active = 1
        ORDER BY date_start ASC
        LIMIT 10
    ''', (today, tomorrow))
    
    events = await cursor.fetchall()
    
    if not events:
        text = "🌅 Доброе утро! На сегодня новых событий не найдено."
    else:
        text = "🌅 *Доброе утро!* Вот что интересного ждет вас сегодня:\n\n"
        for title, venue, date, category in events[:5]:
            emoji = {'concert': '🎤', 'theater': '🎭', 'cinema': '🎬'}.get(category, '📅')
            text += f"{emoji} *{title}*\n   📍 {venue}\n   🗓 {date}\n\n"
        
        if len(events) > 5:
            text += f"📌 *И еще {len(events) - 5} событий* — смотрите полную афишу в канале!"
    
    try:
        await bot.send_message(CHANNEL_ID, text, parse_mode='Markdown')
        logger.info("📨 Утренний дайджест отправлен")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки дайджеста: {e}")

# Команды бота
@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [
        ["📅 Сегодня", "📆 Завтра"],
        ["📊 Ближайшие 7 дней", "🎉 Выходные"],
        ["📆 Календарь", "🎭 По категориям"]
    ]
    for row in buttons:
        keyboard.row(*row)
    
    await message.answer(
        "🌟 *Привет! Я бот афиши Минска*\n\n"
        "Я помогу найти интересные события в городе. "
        "Выбери период или категорию:",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

@dp.message_handler(commands=['parse'])
async def parse_command(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет прав администратора")
        return
    
    await message.answer("🔄 Начинаю парсинг...")
    count = await run_parser_and_publish()
    await message.answer(f"✅ Парсинг завершен. Опубликовано {count} событий")

@dp.message_handler(commands=['stats'])
async def stats_command(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет прав администратора")
        return
    
    today_posts = await db.get_posts_count_today()
    last_hour = await db.get_posts_count_last_hour()
    
    await message.answer(
        f"📊 *Статистика публикаций*\n\n"
        f"📅 Сегодня: {today_posts} / {MAX_POSTS_PER_DAY}\n"
        f"⏰ За час: {last_hour} / {MAX_POSTS_PER_HOUR}\n"
        f"⚙️ Лимиты: {MAX_POSTS_PER_DAY} в день, {MAX_POSTS_PER_HOUR} в час",
        parse_mode='Markdown'
    )

@dp.message_handler(commands=['settings'])
async def settings_command(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    await message.answer(
        f"⚙️ *Текущие настройки*\n\n"
        f"📊 Постов в час: {MAX_POSTS_PER_HOUR}\n"
        f"📅 Постов в день: {MAX_POSTS_PER_DAY}\n"
        f"🕐 Утренний дайджест: {os.getenv('MORNING_DIGEST_TIME', '09:00')}\n"
        f"🔄 Интервал парсинга: {os.getenv('PUBLISH_INTERVAL_HOURS', 4)} часа\n\n"
        f"Изменить параметры можно через Render Environment Variables",
        parse_mode='Markdown'
    )

@dp.message_handler(Text(equals="📅 Сегодня"))
async def today_events(message: types.Message):
    await message.answer("🔍 Ищу события на сегодня... (в разработке)")

@dp.message_handler(Text(equals="📆 Завтра"))
async def tomorrow_events(message: types.Message):
    await message.answer("🔍 Ищу события на завтра... (в разработке)")

@dp.message_handler(Text(equals="📊 Ближайшие 7 дней"))
async def week_events(message: types.Message):
    await message.answer("🔍 Ищу события на неделю... (в разработке)")

@dp.message_handler(Text(equals="🎉 Выходные"))
async def weekend_events(message: types.Message):
    await message.answer("🔍 Ищу события на выходные... (в разработке)")

@dp.message_handler(Text(equals="📆 Календарь"))
async def calendar_command(message: types.Message):
    await message.answer("📅 Календарь событий (в разработке)")

@dp.message_handler(Text(equals="🎭 По категориям"))
async def categories_command(message: types.Message):
    await message.answer("🎭 Категории событий (в разработке)")

async def on_startup(dp):
    await db.init_db()
    logger.info("🚀 Бот запущен и готов к работе")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
