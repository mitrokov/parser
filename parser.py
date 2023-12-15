#!/usr/bin/python3
import logging
import csv
from time import sleep
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin, urlparse
import re
from collections import deque
import signal  # импорт модуля сигналов

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# RegEx для поиска номеров телефонов и e-mail
PHONE_REGEX = re.compile(r'\+?\d[\d\s\-]{9,}\d')
EMAIL_REGEX = re.compile(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", re.I)

# Функция для очистки и фильтрации адресов
def clean_url(href, base_domain):
    parsed_href = urlparse(href)
    return href if parsed_href.netloc == base_domain else None

# Функция для извлечения данных с веб-страницы
def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Если ответ не 200, вызываем HTTPError.
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to retrieve or parse the URL: {url} - {e}")
        return None

# Функция для парсинга содержимого страницы
def parse_content(url, content):
    soup = BeautifulSoup(content, 'html.parser')
    firm_names_tags = soup.select('h1.text-center.color-dark.text-uppercase')
    if firm_names_tags:
        firm_name = firm_names_tags[0].text.strip()
        emails = EMAIL_REGEX.findall(soup.text)
        phone_numbers = PHONE_REGEX.findall(soup.text)
        return firm_name, set(emails), set(phone_numbers)
    return None, None, None

# Функция для обработки одной страницы
def process_page(url, base_domain, scraped_urls, urls_queue, data_collected):
    content = fetch_data(url)
    if content:
        firm_name, emails, phone_numbers = parse_content(url, content)
        if firm_name:
            for phone in phone_numbers:
                for email in emails:
                    data_collected.append((firm_name, phone, email))

        # Находим и обрабатываем новые ссылки
        soup = BeautifulSoup(content, 'html.parser')
        for anchor in soup.find_all('a', href=True):
            href = urljoin(url, anchor['href'])
            href = clean_url(href, base_domain)
            if href and href not in scraped_urls and href not in urls_queue:
                urls_queue.append(href)

# Функция для сохранения данных в CSV файл
def save_data_to_csv(data_collected, file_name='collected_data.csv'):
    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Company Name', 'Phone Number', 'Email'])
        for entry in data_collected:
            csv_writer.writerow(entry)

    logging.info(f"Data successfully saved to {file_name}")

# Функция-обработчик для завершения программы
def exit_handler(signum, frame):
    logging.info('Signal handler called with signal', signum)
    save_data_to_csv(data_collected)
    logging.info("Exiting gracefully.")
    exit(0)

# Главная функция, где происходит весь основной цикл работы
def main():
    global data_collected
    user_url = input("Enter URL: ")
    parsed_user_url = urlparse(user_url)
    base_domain = parsed_user_url.netloc

    urls_queue = deque([user_url])    # Очередь URL для скрапинга
    scraped_urls = set()              # Множество обработанных URL
    data_collected = []               # Список для хранения собранных данных

    signal.signal(signal.SIGINT, exit_handler)  # Назначаем обработчик Ctrl+C

    while urls_queue:
        url = urls_queue.popleft()
        if url in scraped_urls:
            continue

        logging.info(f"Processing {url}")
        scraped_urls.add(url)

        process_page(url, base_domain, scraped_urls, urls_queue, data_collected)

        # Примитивная защита от бана - делаем задержку
        sleep(0.2)

        if len(scraped_urls) == 10000:  # Ограничение количества страниц для сканирования
            break

    save_data_to_csv(data_collected)
    logging.info("Finished. Collected data is saved in collected_data.csv")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:  # В случае других способов завершения (не по Ctrl+C)
        save_data_to_csv(data_collected)
        logging.info("Script interrupted and data saved.")
        exit(0)
