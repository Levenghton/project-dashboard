import streamlit as st
import pandas as pd
import boto3
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('gift-dashboard')

# Настройка страницы
st.set_page_config(
    page_title='Подарочный дашборд',
    page_icon='🎁',
    layout="wide",
    initial_sidebar_state="expanded"
)

# Заголовок приложения
st.title('🎁 Подарочный дашборд')
st.markdown('Анализ данных транзакций из AWS S3 (начиная с 9 апреля 2025)')

# Инициализация session_state для хранения данных
if 'data' not in st.session_state:
    st.session_state['data'] = None
if 'combined_df' not in st.session_state:
    st.session_state['combined_df'] = None

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
        
        # Фильтруем файлы, чтобы включать только те, что начинаются с 2025-04-09 или позже
        file_list = []
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
        
        for obj in objects['Contents']:
            key = obj['Key']
            if key.endswith('.json'):  # Используем расширение .json как на скриншоте
                # Извлекаем дату из имени файла
                match = date_pattern.search(key)
                if match:
                    file_date = match.group(1)
                    # Проверяем, что дата не раньше 9 апреля 2025
                    if file_date >= "2025-04-09":
                        file_list.append(key)
        
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
        
        # Пробуем сначала загрузить весь файл как один JSON
        try:
            # Пробуем загрузить весь файл как единый JSON
            json_data = json.loads(file_content)
            logger.info(f"Файл {file_key} загружен как единый JSON")
            return json_data, None
        except json.JSONDecodeError:
            # Если не получилось, пробуем построчно
            logger.info(f"Файл {file_key} не является единым JSON, пробуем построчно")
            json_data = []
            for line in file_content.strip().split('\n'):
                if line:
                    try:
                        record = json.loads(line)
                        json_data.append(record)
                    except json.JSONDecodeError:
                        logger.warning(f"Невозможно разобрать строку как JSON: {line}")
            
            if json_data:
                logger.info(f"Файл {file_key} загружен построчно, найдено {len(json_data)} записей")
                return json_data, None
            else:
                return None, f"Не удалось разобрать файл {file_key} как JSON"
    except Exception as e:
        error_message = f"Ошибка при загрузке файла {file_key}: {str(e)}"
        logger.error(error_message)
        return None, error_message

# Функция для обработки данных JSON и преобразования в DataFrame
def process_json_data(json_data, file_name):
    try:
        # Проверяем формат JSON-данных
        if isinstance(json_data, list):
            # Проверяем, есть ли данные
            if not json_data or len(json_data) == 0:
                return None, "Пустой список данных"
            
            # Проверяем формат первого элемента
            first_item = json_data[0]
            logger.info(f"Первый элемент в файле: {first_item}")
            
            # Если первый элемент - список, а не словарь
            if isinstance(first_item, list):
                # Преобразуем список списков в список словарей
                logger.info("Обнаружен список списков, преобразуем в словари")
                
                # Предполагаем, что в первой строке заголовки столбцов
                if len(json_data) > 1:
                    # Используем первый список как заголовки
                    headers = json_data[0]
                    # Преобразуем остальные списки в словари
                    data_dicts = [dict(zip(headers, row)) for row in json_data[1:]]
                    df = pd.DataFrame(data_dicts)
                else:
                    # Если только один список, создаем пустой DataFrame
                    return None, "Файл содержит только заголовки без данных"
            else:
                # Стандартный список словарей
                try:
                    logger.info(f"Ключи в первой записи: {list(first_item.keys())}")
                    df = pd.DataFrame(json_data)
                except (AttributeError, TypeError) as e:
                    # Если элементы не словари, пробуем преобразовать их
                    logger.error(f"Ошибка при обработке данных: {e}")
                    logger.info(f"Пробуем обработать как простые значения")
                    
                    # Пробуем создать DataFrame с одним столбцом
                    df = pd.DataFrame({
                        'Value': json_data,
                        'UserId': range(len(json_data)),  # Создаем уникальные ID
                        'InvoiceType': 0,  # Заглушка
                        'Amount': 0  # Заглушка
                    })
            
            # Логируем колонки DataFrame
            logger.info(f"Колонки в DataFrame: {df.columns.tolist() if not df.empty else '[пусто]'}")
            
            # Пропускаем записи с TestMode=true
            if 'TestMode' in df.columns:
                df = df[df['TestMode'] != True]
            
            # Добавляем имя файла к каждой записи
            df['file_name'] = file_name
            
            # Если есть временная метка, преобразуем её в читаемую дату
            if 'Timestamp' in df.columns:
                df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='s')
                # Добавляем колонки с часом и днем для анализа
                df['Hour'] = df['Datetime'].dt.hour
                df['Date'] = df['Datetime'].dt.date
            else:
                # Если нет временной метки, используем дату из имени файла
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
                if date_match:
                    file_date = date_match.group(1)
                    df['Date'] = pd.to_datetime(file_date)
                    
                    # Извлекаем час из имени файла
                    hour_match = re.search(r'\d{4}-\d{2}-\d{2}-(\d{2})-\d{2}', file_name)
                    hour = int(hour_match.group(1)) if hour_match else 0
                    df['Hour'] = hour
            
            # Добавляем необходимые поля, если их нет
            if 'UserId' not in df.columns:
                df['UserId'] = range(len(df))  # Создаем уникальные ID
            
            if 'InvoiceType' not in df.columns:
                df['InvoiceType'] = 0  # Заглушка
                
            if 'Amount' not in df.columns:
                df['Amount'] = 0  # Заглушка
            
            return df, None
        else:
            error_message = f"Неожиданный формат данных в файле {file_name}: Данные не являются списком JSON-объектов"
            logger.error(error_message)
            return None, error_message
    except Exception as e:
        error_message = f"Ошибка при обработке данных из файла {file_name}: {str(e)}"
        logger.error(error_message)
        return None, error_message

# Функция для подготовки данных по дням для графика всех пользователей
@st.cache_data(ttl=3600)
def prepare_users_daily_data(df):
    """Подготавливает данные для графика по дням с количеством уникальных пользователей"""
    if df is None or df.empty:
        return None
    
    try:
        # Убедимся, что даты в правильном формате
        df_copy = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df_copy['Date']):
            df_copy['Date'] = pd.to_datetime(df_copy['Date'])
        
        # Считаем уникальных пользователей по дням
        users_by_day = df_copy.groupby('Date')['UserId'].nunique().reset_index()
        users_by_day.columns = ['Date', 'Уникальные пользователи']
        users_by_day = users_by_day.sort_values('Date')
        
        return users_by_day
    except Exception as e:
        st.error(f"Ошибка при подготовке данных по пользователям: {str(e)}")
        return None

# Функция для подготовки данных по дням для графика платящих пользователей
@st.cache_data(ttl=3600)
def prepare_paying_users_daily_data(df):
    """Подготавливает данные для графика по дням с количеством платящих пользователей"""
    if df is None or df.empty:
        return None
    
    try:
        # Убедимся, что даты в правильном формате
        df_copy = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df_copy['Date']):
            df_copy['Date'] = pd.to_datetime(df_copy['Date'])
        
        # Фильтруем только оплаченные транзакции (InvoiceType = 1)
        paying_df = df_copy[df_copy['InvoiceType'] == 1]
        
        # Считаем уникальных платящих пользователей по дням
        paying_users_by_day = paying_df.groupby('Date')['UserId'].nunique().reset_index()
        paying_users_by_day.columns = ['Date', 'Платящие пользователи']
        paying_users_by_day = paying_users_by_day.sort_values('Date')
        
        return paying_users_by_day
    except Exception as e:
        st.error(f"Ошибка при подготовке данных по платящим пользователям: {str(e)}")
        return None

# Функция для расчета конверсии по номеру спина
@st.cache_data(ttl=3600)
def calculate_spin_conversion(df):
    """Рассчитывает конверсию пользователей по номеру спина"""
    if df is None or df.empty:
        return None
    
    try:
        # Фильтруем только оплаченные транзакции (InvoiceType = 1)
        paying_df = df[df['InvoiceType'] == 1]
        
        # Получаем общее количество уникальных пользователей
        total_users = df['UserId'].nunique()
        
        # Группируем транзакции по пользователям и считаем количество оплат для каждого
        user_payments = paying_df.groupby('UserId').size().reset_index(name='PaymentCount')
        
        # Создаем DataFrame для конверсии
        conversion_data = []
        
        # Рассчитываем конверсию для каждого спина (от 1 до 10)
        for spin_num in range(1, 11):
            # Количество пользователей, которые сделали хотя бы spin_num оплат
            users_with_spins = user_payments[user_payments['PaymentCount'] >= spin_num].shape[0]
            
            # Рассчитываем процент конверсии
            conversion_rate = (users_with_spins / total_users * 100) if total_users > 0 else 0
            
            conversion_data.append({
                'Номер спина': spin_num,
                'Количество пользователей': users_with_spins,
                'Процент конверсии': round(conversion_rate, 2)
            })
        
        return pd.DataFrame(conversion_data)
    except Exception as e:
        st.error(f"Ошибка при расчете конверсии по спинам: {str(e)}")
        return None

# Основной раздел приложения
st.header('📂 Данные из AWS S3')

# Кнопка для подключения к S3 или сброса данных
if st.button('Подключиться к AWS и получить данные'):
    # Сбрасываем данные в session_state
    st.session_state['data'] = None
    st.session_state['combined_df'] = None
    
    # Подключение к S3
    with st.spinner('Подключение к AWS S3...'):
        s3_client, connection_error = connect_to_s3(aws_access_key, aws_secret_key)
    
    if connection_error:
        st.error(connection_error)
    else:
        st.success('Подключение к AWS S3 успешно!')
        
        # Получаем список файлов
        with st.spinner('Получение списка файлов...'):
            file_list, file_error = list_files_in_bucket(s3_client, bucket_name, prefix)
        
        if file_error:
            st.error(file_error)
        elif not file_list:
            st.warning(f"Не найдено файлов в бакете {bucket_name} с префиксом {prefix} начиная с 9 апреля 2025")
        else:
            st.success(f"Найдено {len(file_list)} файлов")
            
            # Отображаем прогресс-бар для загрузки файлов
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Загружаем и обрабатываем файлы
            all_dataframes = []
            
            for i, file_key in enumerate(file_list):
                # Обновляем прогресс
                progress = (i + 1) / len(file_list)
                progress_bar.progress(progress)
                status_text.text(f"Обработка файла {i+1} из {len(file_list)}: {file_key}")
                
                # Загружаем файл
                json_data, load_error = load_file_from_s3(s3_client, bucket_name, file_key)
                
                if load_error:
                    st.error(f"Ошибка при загрузке {file_key}: {load_error}")
                    continue
                
                # Обрабатываем данные
                df, process_error = process_json_data(json_data, file_key)
                
                if process_error:
                    st.error(f"Ошибка при обработке {file_key}: {process_error}")
                    continue
                
                if df is not None and not df.empty:
                    all_dataframes.append(df)
            
            # Очищаем прогресс и статус
            progress_bar.empty()
            status_text.empty()
            
            if not all_dataframes:
                st.error("Не удалось загрузить данные из файлов")
            else:
                # Объединяем все DataFrame
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                
                # Проверяем наличие необходимых полей
                required_fields = ['UserId', 'InvoiceType', 'Amount']
                missing_fields = [field for field in required_fields if field not in combined_df.columns]
                
                if missing_fields:
                    st.error(f"В данных отсутствуют необходимые поля: {', '.join(missing_fields)}")
                    st.write("Доступные поля в данных:")
                    st.write(combined_df.columns.tolist())
                    # Показываем пример данных для отладки
                    st.write("Пример данных:")
                    st.write(combined_df.head(1).to_dict('records'))
                else:
                    # Сохраняем в session_state
                    st.session_state['data'] = combined_df
                    st.session_state['combined_df'] = combined_df
                    
                    st.success(f"Данные успешно загружены! Всего записей: {combined_df.shape[0]}")

# Если данные загружены, отображаем их
if st.session_state['combined_df'] is not None:
    combined_df = st.session_state['combined_df']
    
    # Отображаем предварительный просмотр данных
    st.subheader('Предварительный просмотр данных')
    st.dataframe(combined_df.head(10), use_container_width=True)
    
    # Отображаем основные метрики
    st.subheader('Основные метрики')
    
    col1, col2, col3 = st.columns(3)
    
    # Проверяем наличие необходимых полей перед расчетом метрик
    if 'UserId' in combined_df.columns:
        # Общее количество уникальных пользователей
        total_users = combined_df['UserId'].nunique()
        with col1:
            st.metric("Всего пользователей", total_users)
    
    if 'UserId' in combined_df.columns and 'InvoiceType' in combined_df.columns:
        # Количество платящих пользователей
        paying_users = combined_df[combined_df['InvoiceType'] == 1]['UserId'].nunique()
        with col2:
            st.metric("Платящих пользователей", paying_users)
    else:
        with col2:
            st.metric('Платящих пользователей', "Нет данных")
    
    if 'InvoiceType' in combined_df.columns and 'Amount' in combined_df.columns:
        # Сумма транзакций
        total_deposits = combined_df[combined_df['InvoiceType'] == 1]['Amount'].sum()
    with col3:
        st.metric('Сумма транзакций (старс)', f"{total_deposits:,.2f}")
    
    # Подготавливаем данные для графиков
    users_daily_data = prepare_users_daily_data(combined_df)
    paying_users_daily_data = prepare_paying_users_daily_data(combined_df)
    spin_conversion_data = calculate_spin_conversion(combined_df)
    
    # График всех пользователей
    st.subheader('График всех пользователей по дням')
    if users_daily_data is not None and not users_daily_data.empty:
        # Преобразуем даты в строки для корректного отображения
        # Проверяем тип данных в колонке Date
        if pd.api.types.is_datetime64_any_dtype(users_daily_data['Date']):
            users_daily_data['Date_str'] = users_daily_data['Date'].dt.strftime('%Y-%m-%d')
        else:
            # Если это не datetime, преобразуем
            users_daily_data['Date'] = pd.to_datetime(users_daily_data['Date'])
            users_daily_data['Date_str'] = users_daily_data['Date'].dt.strftime('%Y-%m-%d')
        
        # Отображаем график
        st.line_chart(users_daily_data.set_index('Date_str')['Уникальные пользователи'])
    else:
        st.info('Нет данных для отображения графика пользователей')
    
    # График платящих пользователей
    st.subheader('График платящих пользователей по дням')
    if paying_users_daily_data is not None and not paying_users_daily_data.empty:
        # Преобразуем даты в строки для корректного отображения
        # Проверяем тип данных в колонке Date
        if pd.api.types.is_datetime64_any_dtype(paying_users_daily_data['Date']):
            paying_users_daily_data['Date_str'] = paying_users_daily_data['Date'].dt.strftime('%Y-%m-%d')
        else:
            # Если это не datetime, преобразуем
            paying_users_daily_data['Date'] = pd.to_datetime(paying_users_daily_data['Date'])
            paying_users_daily_data['Date_str'] = paying_users_daily_data['Date'].dt.strftime('%Y-%m-%d')
        
        # Отображаем график
        st.line_chart(paying_users_daily_data.set_index('Date_str')['Платящие пользователи'])
    else:
        st.info('Нет данных для отображения графика платящих пользователей')
    
    # Таблица конверсии по номеру спина
    st.subheader('Конверсия по номеру спина')
    if spin_conversion_data is not None and not spin_conversion_data.empty:
        st.table(spin_conversion_data)
    else:
        st.info('Нет данных для расчета конверсии по спинам')

# Добавляем информацию о приложении
st.sidebar.markdown('---')
st.sidebar.info('''
### О приложении
Это приложение предназначено для анализа транзакционных данных, 
загруженных напрямую из AWS S3.

Данные анализируются начиная с 9 апреля 2025 года.

© 2025 Аналитика Транзакций
''')