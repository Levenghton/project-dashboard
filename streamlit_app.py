import streamlit as st
import pandas as pd
import boto3
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('gift-dashboard')

# Настройка страницы
st.set_page_config(
    page_title='Подарочный Дашборд',
    page_icon='🎁',
    layout="wide",
    initial_sidebar_state="expanded"
)

# Заголовок приложения
st.title('🎁 Подарочный Дашборд')
st.markdown('Аналитика данных транзакций по подаркам из AWS S3')

# Функция для получения AWS настроек из секретов Streamlit или пользовательского ввода
def get_aws_settings():
    # Проверка наличия секретов в Streamlit Cloud
    if 'aws' in st.secrets:
        aws_access_key = st.secrets['aws']['aws_access_key']
        aws_secret_key = st.secrets['aws']['aws_secret_key']
        bucket_name = st.secrets['aws'].get('bucket_name', 'technicalgiftagram')
        prefix = st.secrets['aws'].get('prefix', 'processed-logs/funds-log/5min/')
        
        # Отображаем значения из секретов, но скрываем ключи
        st.sidebar.text("✅ AWS настройки загружены из Secrets")
    else:
        # Если нет секретов, запрашиваем у пользователя
        aws_access_key = st.sidebar.text_input('AWS Access Key ID', value='YOUR_ACCESS_KEY', type='password')
        aws_secret_key = st.sidebar.text_input('AWS Secret Access Key', value='YOUR_SECRET_KEY', type='password')
        bucket_name = st.sidebar.text_input('Имя бакета S3', value='technicalgiftagram')
        prefix = st.sidebar.text_input('Префикс (путь)', value='processed-logs/funds-log/5min/')
    
    return aws_access_key, aws_secret_key, bucket_name, prefix

# Получаем AWS настройки
aws_access_key, aws_secret_key, bucket_name, prefix = get_aws_settings()

# Боковая панель для настроек
st.sidebar.header('⚙️ Настройки подключения')

# Функция для сохранения настроек
def save_settings():
    settings = {
        "aws_access_key": aws_access_key,
        "aws_secret_key": aws_secret_key,
        "bucket_name": bucket_name,
        "prefix": prefix
    }
    
    config_path = Path('streamlit_config.json')
    with open(config_path, 'w') as f:
        json.dump(settings, f, indent=4)
    
    return "Настройки сохранены!"

# Функция для загрузки настроек
@st.cache_data
def load_settings():
    config_path = Path('streamlit_config.json')
    if config_path.exists():
        with open(config_path, 'r') as f:
            settings = json.load(f)
        return settings
    return None

# Кнопка для сохранения настроек отображается только при локальном запуске (без секретов)
if 'aws' not in st.secrets:
    if st.sidebar.button('Сохранить настройки'):
        status = save_settings()
        st.sidebar.success(status)

# Функция для подключения к S3
def connect_to_s3(aws_access_key, aws_secret_key):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        # Проверка подключения путем запроса списка бакетов
        s3_client.list_buckets()
        return s3_client, None
    except Exception as e:
        error_message = f"Ошибка подключения к AWS S3: {str(e)}"
        logger.error(error_message)
        return None, error_message

# Функция для получения списка файлов в бакете
def list_files_in_bucket(s3_client, bucket_name, prefix=''):
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in objects:
            return [], f"Объекты не найдены в бакете {bucket_name} с префиксом {prefix}"
        
        file_list = [obj['Key'] for obj in objects['Contents'] if obj['Key'].endswith('.json')]
        return file_list, None
    except Exception as e:
        error_message = f"Ошибка при получении списка файлов: {str(e)}"
        logger.error(error_message)
        return [], error_message

# Функция для загрузки файла из S3
@st.cache_data
def load_file_from_s3(_s3_client, bucket_name, file_key):
    try:
        response = _s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)
        return json_data, None
    except Exception as e:
        error_message = f"Ошибка при загрузке файла {file_key}: {str(e)}"
        logger.error(error_message)
        return None, error_message

# Функция для обработки данных JSON и преобразования в DataFrame
def process_json_data(json_data, file_name):
    try:
        # Проверяем формат JSON-данных (массив объектов)
        if isinstance(json_data, list):
            # Создаем DataFrame напрямую из списка словарей
            df = pd.DataFrame(json_data)
            
            # Добавляем имя файла к каждой записи
            df['file_name'] = file_name
            
            # Если есть временная метка, преобразуем её в читаемую дату
            if 'Timestamp' in df.columns:
                df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='s')
                # Добавляем колонки с часом и днем для анализа
                df['Hour'] = df['Datetime'].dt.hour
                df['Date'] = df['Datetime'].dt.date
            
            return df, None
        else:
            error_message = f"Неожиданный формат данных в файле {file_name}: Данные не являются списком JSON-объектов"
            logger.error(error_message)
            return None, error_message
    except Exception as e:
        error_message = f"Ошибка при обработке данных из файла {file_name}: {str(e)}"
        logger.error(error_message)
        return None, error_message

# Функция для определения тира на основе количества звезд (Stars)
def determine_tier(stars):
    if stars <= 25:
        return '25 Stars'
    elif stars <= 50:
        return '50 Stars'
    else:
        return '100+ Stars'

# Основной раздел приложения
st.header('📂 Данные из AWS S3')

# Кнопка для подключения к S3
if st.button('Подключиться к AWS и получить данные'):
    # Подключение к S3
    with st.spinner('Подключение к AWS S3...'):
        s3_client, connection_error = connect_to_s3(aws_access_key, aws_secret_key)
    
    if connection_error:
        st.error(connection_error)
    else:
        st.success('Успешное подключение к AWS S3')
        
        # Получение списка файлов
        with st.spinner('Получение списка файлов...'):
            file_list, list_error = list_files_in_bucket(s3_client, bucket_name, prefix)
        
        if list_error:
            st.error(list_error)
        elif not file_list:
            st.warning(f'Файлы не найдены в бакете {bucket_name} с префиксом {prefix}')
        else:
            # Отображаем найденные файлы и автоматически выбираем все файлы
            st.subheader(f'Найдено {len(file_list)} файлов:')
            st.text(f"Загружаем все доступные файлы для анализа...")
            
            # Загрузка всех файлов
            progress_bar = st.progress(0)
            all_data = []
            
            for i, file_key in enumerate(file_list):
                progress = (i + 1) / len(file_list)
                progress_bar.progress(progress, text=f'Загрузка файла {i+1} из {len(file_list)}')
                
                with st.spinner(f'Загрузка файла {file_key}...'):
                    json_data, load_error = load_file_from_s3(s3_client, bucket_name, file_key)
                
                if load_error:
                    st.warning(f"Пропускаем файл {file_key}: {load_error}")
                else:
                    file_name = Path(file_key).name
                    df, process_error = process_json_data(json_data, file_name)
                    
                    if process_error:
                        st.warning(f"Пропускаем файл {file_name}: {process_error}")
                    else:
                        all_data.append(df)
            
            if all_data:
                # Объединяем все данные
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Отображаем данные
                st.subheader('Предварительный просмотр данных')
                st.dataframe(combined_df.head(100), use_container_width=True)
                
                # Информация о данных
                st.subheader('Статистика данных')
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric('Всего записей', f"{len(combined_df):,}")
                
                if 'UserId' in combined_df.columns:
                    with col2:
                        st.metric('Уникальных пользователей', f"{combined_df['UserId'].nunique():,}")
                
                if 'Amount' in combined_df.columns:
                    with col3:
                        st.metric('Общая сумма', f"{combined_df['Amount'].sum():,.2f}")
                
                # Настройка периода анализа для графиков
                st.subheader('Выбор периода для анализа')
                
                # Определяем минимальную и максимальную дату в данных
                if 'Date' in combined_df.columns:
                    min_date = pd.to_datetime(combined_df['Date'].min())
                    max_date = pd.to_datetime(combined_df['Date'].max())
                    
                    # Исправление: проверяем, что у нас есть более одного дня данных для установки диапазона по умолчанию
                    if min_date.date() == max_date.date():
                        # Если только один день данных, используем его и для начала и для конца диапазона
                        default_start_date = min_date.date()
                        default_end_date = max_date.date()
                    else:
                        # Если у нас есть несколько дней, берем неделю или весь доступный диапазон, если он меньше недели
                        default_start_date = max(min_date.date(), (max_date - timedelta(days=7)).date())
                        default_end_date = max_date.date()
                    
                    # Виджет для выбора диапазона дат
                    date_range = st.date_input(
                        "Выберите диапазон дат",
                        value=(default_start_date, default_end_date),
                        min_value=min_date.date(),
                        max_value=max_date.date()
                    )
                    
                    if len(date_range) == 2:
                        start_date, end_date = date_range
                        # Фильтруем данные по выбранному диапазону дат
                        filtered_df = combined_df[
                            (combined_df['Date'] >= start_date) & 
                            (combined_df['Date'] <= end_date)
                        ]
                        
                        st.write(f"Выбран период: с {start_date} по {end_date}")
                        
                        # Создаем полный диапазон дат, включая пустые дни
                        date_range_full = pd.date_range(start=start_date, end=end_date, freq='D')
                        
                        # Группируем транзакции по дням
                        daily_transactions = filtered_df.groupby('Date').size().reset_index(name='Transactions')
                        
                        # Создаем DataFrame с полным диапазоном дат
                        daily_df = pd.DataFrame({'Date': date_range_full.date})
                        
                        # Объединяем с фактическими данными, заполняя пропуски нулями
                        daily_df = daily_df.merge(daily_transactions, on='Date', how='left').fillna(0)
                        
                        # График транзакций по дням
                        st.subheader('Объем транзакций по дням')
                        st.line_chart(daily_df, x='Date', y='Transactions')
                        
                        # Группировка транзакций по часам для выбранного периода
                        hourly_transactions = filtered_df.groupby('Hour').size().reset_index(name='Transactions')
                        
                        # Создаем полный диапазон часов (0-23)
                        hourly_df = pd.DataFrame({'Hour': range(24)})
                        
                        # Объединяем с фактическими данными, заполняя пропуски нулями
                        hourly_df = hourly_df.merge(hourly_transactions, on='Hour', how='left').fillna(0)
                        
                        # Сортировка по часу для правильного отображения
                        hourly_df = hourly_df.sort_values('Hour')
                        
                        # График транзакций по часам
                        st.subheader('Объем транзакций по часам')
                        st.bar_chart(hourly_df, x='Hour', y='Transactions')
                        
                        # НОВЫЕ ГРАФИКИ:
                        # -----------------------------------------------
                        
                        # Анализ по пользователям - НОВЫЙ РАЗДЕЛ
                        if 'UserId' in filtered_df.columns:
                            st.header('📊 Анализ данных по пользователям')
                            
                            # Количество транзакций на пользователя
                            user_transactions = filtered_df.groupby('UserId').size().reset_index(name='TransactionCount')
                            
                            # Объем транзакций на пользователя
                            if 'Amount' in filtered_df.columns:
                                user_amounts = filtered_df.groupby('UserId')['Amount'].sum().reset_index(name='TotalAmount')
                                
                                # Объединяем данные
                                user_stats = user_transactions.merge(user_amounts, on='UserId')
                                
                                # Средние показатели
                                avg_transactions_per_user = user_transactions['TransactionCount'].mean()
                                avg_amount_per_user = user_amounts['TotalAmount'].mean()
                                
                                # Отображаем средние показатели в отдельном контейнере
                                st.subheader('Средние показатели по пользователям')
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric('Среднее кол-во транзакций на пользователя', f"{avg_transactions_per_user:.2f}")
                                with col2:
                                    st.metric('Средняя сумма на пользователя', f"{avg_amount_per_user:.2f}")
                                
                                # Гистограмма распределения пользователей по количеству транзакций
                                st.subheader('Распределение пользователей по количеству транзакций')
                                hist_data = pd.DataFrame({
                                    'Транзакции': user_stats['TransactionCount'].clip(upper=20)  # Ограничиваем для лучшей визуализации
                                })
                                st.bar_chart(hist_data)
                        
                        # Анализ по тирам - НОВЫЙ РАЗДЕЛ
                        if 'Amount' in filtered_df.columns:
                            st.header('💰 Распределение по тирам')
                            
                            # Добавляем колонку Tier на основе суммы транзакции (Amount)
                            filtered_df['Tier'] = filtered_df['Amount'].apply(determine_tier)
                            
                            # Группировка по тирам
                            tier_counts = filtered_df.groupby('Tier').size().reset_index(name='Count')
                            
                            # Упорядочиваем тиры в правильном порядке
                            tier_order = ['25 Stars', '50 Stars', '100+ Stars']
                            tier_counts['Tier'] = pd.Categorical(tier_counts['Tier'], categories=tier_order, ordered=True)
                            tier_counts = tier_counts.sort_values('Tier')
                            
                            # Отображаем распределение в виде столбчатой диаграммы
                            st.subheader('Количество транзакций по тирам')
                            st.bar_chart(tier_counts, x='Tier', y='Count')
                            
                            # Отображаем также общую сумму по каждому тиру
                            tier_amounts = filtered_df.groupby('Tier')['Amount'].sum().reset_index(name='TotalAmount')
                            tier_amounts['Tier'] = pd.Categorical(tier_amounts['Tier'], categories=tier_order, ordered=True)
                            tier_amounts = tier_amounts.sort_values('Tier')
                            
                            st.subheader('Общая сумма транзакций по тирам')
                            st.bar_chart(tier_amounts, x='Tier', y='TotalAmount')
                
                # Сохранение данных в сессии для последующего использования
                st.session_state['data'] = combined_df
                
                st.success('Данные успешно загружены и проанализированы!')

# Добавляем информацию о приложении
st.sidebar.markdown('---')
st.sidebar.info('''
### О приложении
Это приложение предназначено для анализа транзакционных данных, 
загруженных напрямую из AWS S3.

© 2025 Подарочный Дашборд
''')