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

# Инициализация session_state для хранения данных
# Определяем все необходимые ключи
default_states = {
    'data': None,
    'combined_df': None,
    'filtered_df': None,
    'date_range': None,
    'show_created': True,
    'show_paid': True,
    'show_refunded': True,
    'last_error': None
}

# Инициализируем все необходимые ключи в session_state
for key, default_value in default_states.items():
    if key not in st.session_state:
        st.session_state[key] = default_value

# Функция для безопасного доступа к session_state
def get_state(key, default=None):
    """Get session state key safely with default fallback"""
    if key in st.session_state:
        return st.session_state[key]
    return default
    
# Функция для записи ошибок
def log_error(message, error=None):
    error_text = f"{message}"
    if error:
        error_text += f": {str(error)}"
    st.session_state['last_error'] = error_text
    return error_text

# Функция для обновления данных без повторной загрузки из S3
def update_filtered_data():
    try:
        # Проверяем наличие данных в session_state
        if get_state('data') is None:
            logger.info("update_filtered_data: данные отсутствуют в session_state")
            return False
            
        if get_state('date_range') is None:
            logger.info("update_filtered_data: диапазон дат отсутствует в session_state")
            return False
            
        # Проверяем корректность диапазона дат
        date_range = get_state('date_range')
        if len(date_range) != 2:
            logger.warning(f"update_filtered_data: некорректный диапазон дат, длина = {len(date_range)}")
            return False
            
        # Получаем начальную и конечную дату
        start_date, end_date = date_range
        data = get_state('data')
        
        # Проверяем наличие столбца Date
        if 'Date' not in data.columns:
            error_msg = "update_filtered_data: в данных отсутствует столбец 'Date'"
            log_error(error_msg)
            logger.error(error_msg)
            return False
        
        # Фильтруем данные по диапазону дат
        try:
            filtered_df = data[
                (data['Date'] >= start_date) & 
                (data['Date'] <= end_date)
            ]
            # Логируем результаты фильтрации
            logger.info(f"update_filtered_data: отфильтровано {len(filtered_df)} записей из {len(data)} с {start_date} по {end_date}")
            
            # Сохраняем результат в session_state
            st.session_state['filtered_df'] = filtered_df
            return True
        except Exception as e:
            error_msg = f"update_filtered_data: ошибка при фильтрации данных: {str(e)}"
            log_error(error_msg, e)
            logger.error(error_msg)
            return False
    except Exception as e:
        error_msg = f"update_filtered_data: неожиданная ошибка: {str(e)}"
        log_error(error_msg, e)
        logger.error(error_msg)
        return False

# Функция для сохранения настроек фильтров
def save_filter_settings(setting_name, value):
    """Safe function to save filter settings to session state"""
    try:
        previous_value = get_state(setting_name)
        st.session_state[setting_name] = value
        if previous_value != value:
            # Если значение изменилось, логируем это
            logger.info(f'Changed filter setting {setting_name}: {previous_value} -> {value}')
        return True
    except Exception as e:
        error_msg = f"Ошибка при сохранении настройки {setting_name}: {str(e)}"
        log_error(error_msg, e)
        logger.error(error_msg)
        return False

# Основной раздел приложения
st.header('📂 Данные из AWS S3')

# Кнопка для подключения к S3 или сброса данных
if st.button('Подключиться к AWS и получить данные'):
    # Сбрасываем данные в session_state
    st.session_state['data'] = None
    st.session_state['filtered_df'] = None
    st.session_state['date_range'] = None
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
                
                col1, col2 = st.columns(2)
                
                if 'UserId' in combined_df.columns:
                    with col1:
                        st.metric('Уникальных пользователей', f"{combined_df['UserId'].nunique():,}")
                
                # Сумма спинов будет рассчитана после применения фильтров по дате
                
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
                    
                    # Виджет для выбора диапазона дат с сохранением в session_state
                    date_range = st.date_input(
                        "Выберите диапазон дат",
                        value=(default_start_date, default_end_date) if st.session_state['date_range'] is None else st.session_state['date_range'],
                        min_value=min_date.date(),
                        max_value=max_date.date(),
                        key="date_input"
                    )
                    
                    # Сохраняем выбранный диапазон в session_state
                    st.session_state['date_range'] = date_range
                    
                    if len(date_range) == 2:
                        start_date, end_date = date_range
                        # Фильтруем данные по выбранному диапазону дат
                        filtered_df = combined_df[
                            (combined_df['Date'] >= start_date) & 
                            (combined_df['Date'] <= end_date)
                        ]
                        
                        # Сохраняем отфильтрованные данные в session_state
                        st.session_state['filtered_df'] = filtered_df
                        
                        st.write(f"Выбран период: с {start_date} по {end_date}")
                        
                        # Отображаем общую сумму спинов за выбранный период
                        if 'Amount' in filtered_df.columns and 'InvoiceType' in filtered_df.columns:
                            # Фильтруем только оплаченные транзакции (InvoiceType = 1) за выбранный период
                            paid_df = filtered_df[filtered_df['InvoiceType'] == 1]
                            with col2:
                                st.metric('Сумма спинов в stars', f"{paid_df['Amount'].sum():,.2f}")
                        
                        # Создаем полный диапазон дат, включая пустые дни
                        date_range_full = pd.date_range(start=start_date, end=end_date, freq='D')
                        
                        # Добавляем выбор типов транзакций для отображения
                        st.subheader('Объем транзакций по дням')
                        
                        # Создаем чекбоксы для выбора типов транзакций с ключами для session_state
                        try:
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                # Используем значение из session_state если есть
                                show_created = st.checkbox('Созданные инвойсы', value=get_state('show_created', True), key="show_created")
                                # Записываем выбор обратно в session_state
                                st.session_state['show_created'] = show_created
                            with col2:
                                show_paid = st.checkbox('Оплаченные', value=get_state('show_paid', True), key="show_paid")
                                st.session_state['show_paid'] = show_paid
                            with col3:
                                show_refunded = st.checkbox('Возвраты', value=get_state('show_refunded', True), key="show_refunded")
                                st.session_state['show_refunded'] = show_refunded
                        except Exception as e:
                            st.error(f"Ошибка при обработке чекбоксов: {str(e)}")
                            # Устанавливаем значения по умолчанию в случае ошибки
                            show_created = True
                            show_paid = True
                            show_refunded = True
                        
                        # Создаем DataFrame с полным диапазоном дат
                        daily_df = pd.DataFrame({'Date': date_range_full.date})
                        
                        # Группируем транзакции по дням и типу инвойса
                        if 'InvoiceType' in filtered_df.columns:
                            # Подготавливаем данные для каждого типа транзакций
                            invoice_types = {
                                0: 'Созданные инвойсы',
                                1: 'Оплаченные',
                                2: 'Возвраты'
                            }
                            
                            # Создаем DataFrame для хранения данных по дням и типам
                            daily_by_type_df = daily_df.copy()
                            
                            # Добавляем колонки для каждого типа транзакций
                            for invoice_type, type_name in invoice_types.items():
                                # Фильтруем данные по типу инвойса
                                type_df = filtered_df[filtered_df['InvoiceType'] == invoice_type]
                                
                                # Группируем по дате с объединением данных
                                # Используем date() для ограничения только до даты без времени
                                if 'Datetime' in type_df.columns:
                                    # Используем Datetime для более точной группировки по дням
                                    type_df['Day'] = type_df['Datetime'].dt.date
                                    type_daily = type_df.groupby('Day').size().reset_index(name=type_name)
                                    type_daily.rename(columns={'Day': 'Date'}, inplace=True)
                                else:
                                    # Если нет Datetime, используем Date как есть
                                    type_daily = type_df.groupby('Date').size().reset_index(name=type_name)
                                
                                # Преобразуем даты в формат, который поддерживает мерж
                                type_daily['Date'] = pd.to_datetime(type_daily['Date'])
                                daily_by_type_df['Date'] = pd.to_datetime(daily_by_type_df['Date'])
                                
                                # Объединяем с основным DataFrame
                                daily_by_type_df = daily_by_type_df.merge(type_daily, on='Date', how='left').fillna(0)
                            
                            # Определяем, какие колонки отображать на графике
                            columns_to_display = []
                            if show_created:
                                columns_to_display.append('Созданные инвойсы')
                            if show_paid:
                                columns_to_display.append('Оплаченные')
                            if show_refunded:
                                columns_to_display.append('Возвраты')
                            
                            # Проверяем, что хотя бы один тип выбран
                            try:
                                if columns_to_display:
                                    # Сортируем данные по дате для правильного отображения
                                    daily_by_type_df = daily_by_type_df.sort_values('Date')
                                    
                                    # Преобразуем даты в строки для корректного отображения
                                    daily_by_type_df['Date_str'] = daily_by_type_df['Date'].dt.strftime('%Y-%m-%d')
                                    
                                    # Отображаем график
                                    st.bar_chart(daily_by_type_df, x='Date_str', y=columns_to_display)
                                    
                                    # Выводим отладочную информацию
                                    with st.expander('Детали данных по дням'):
                                        st.dataframe(daily_by_type_df[['Date_str'] + columns_to_display])
                            except Exception as e:
                                error_msg = log_error("Ошибка при отображении графика по дням", e)
                                st.error(error_msg)
                            else:
                                st.warning('Выберите хотя бы один тип транзакций для отображения')
                        else:
                            # Если нет разделения по типам, показываем общий график
                            if 'Datetime' in filtered_df.columns:
                                # Используем Datetime для более точной группировки по дням
                                filtered_df['Day'] = filtered_df['Datetime'].dt.date
                                daily_transactions = filtered_df.groupby('Day').size().reset_index(name='Транзакции')
                                daily_transactions.rename(columns={'Day': 'Date'}, inplace=True)
                            else:
                                # Если нет Datetime, используем Date как есть
                                daily_transactions = filtered_df.groupby('Date').size().reset_index(name='Транзакции')
                            
                            # Преобразуем даты для корректного мержа
                            daily_transactions['Date'] = pd.to_datetime(daily_transactions['Date'])
                            daily_df['Date'] = pd.to_datetime(daily_df['Date'])
                            
                            # Объединяем данные
                            daily_df = daily_df.merge(daily_transactions, on='Date', how='left').fillna(0)
                            
                            # Сортируем и преобразуем даты для отображения
                            daily_df = daily_df.sort_values('Date')
                            daily_df['Date_str'] = daily_df['Date'].dt.strftime('%Y-%m-%d')
                            
                            # Отображаем график
                            st.bar_chart(daily_df, x='Date_str', y='Транзакции')
                            
                            # Выводим отладочную информацию
                            with st.expander('Детали данных по дням'):
                                st.dataframe(daily_df[['Date_str', 'Транзакции']])
                        
                        # График транзакций по часам
                        st.subheader('Объем транзакций по часам')
                        
                        # Создаем полный диапазон часов (0-23)
                        hourly_df = pd.DataFrame({'Hour': range(24)})
                        
                        # Группируем транзакции по часам и типу инвойса
                        if 'InvoiceType' in filtered_df.columns:
                            # Используем те же чекбоксы, что и для дневного графика
                            
                            # Создаем DataFrame для хранения данных по часам и типам
                            hourly_by_type_df = hourly_df.copy()
                            
                            # Добавляем колонки для каждого типа транзакций
                            for invoice_type, type_name in invoice_types.items():
                                # Фильтруем данные по типу инвойса
                                type_df = filtered_df[filtered_df['InvoiceType'] == invoice_type]
                                
                                # Группируем по часу в зависимости от доступных полей
                                if 'Datetime' in type_df.columns:
                                    # Используем Datetime для более точной группировки по часам
                                    type_hourly = type_df.groupby(type_df['Datetime'].dt.hour).size().reset_index(name=type_name)
                                    type_hourly.columns = ['Hour', type_name]
                                elif 'Date' in type_df.columns and pd.api.types.is_datetime64_any_dtype(type_df['Date']):
                                    # Если Date является datetime, используем его для группировки
                                    type_hourly = type_df.groupby(type_df['Date'].dt.hour).size().reset_index(name=type_name)
                                    type_hourly.columns = ['Hour', type_name]
                                else:
                                    # Если нет ни Datetime, ни Date в формате datetime, используем Hour если есть
                                    if 'Hour' in type_df.columns:
                                        type_hourly = type_df.groupby('Hour').size().reset_index(name=type_name)
                                    else:
                                        # Если нет часов, создаем пустой DataFrame
                                        type_hourly = pd.DataFrame({'Hour': [], type_name: []})
                                
                                # Объединяем с основным DataFrame
                                hourly_by_type_df = hourly_by_type_df.merge(type_hourly, on='Hour', how='left').fillna(0)
                            
                            # Сортировка по часу для правильного отображения
                            hourly_by_type_df = hourly_by_type_df.sort_values('Hour')
                            
                            # Проверяем, что хотя бы один тип выбран
                            if columns_to_display:
                                # Сортируем данные по часам для правильного отображения
                                hourly_by_type_df = hourly_by_type_df.sort_values('Hour')
                                
                                # Отображаем график
                                st.bar_chart(hourly_by_type_df, x='Hour', y=columns_to_display)
                                
                                # Выводим отладочную информацию
                                with st.expander('Детали данных по часам'):
                                    st.dataframe(hourly_by_type_df[['Hour'] + columns_to_display])
                            else:
                                st.warning('Выберите хотя бы один тип транзакций для отображения')
                        else:
                            # Если нет разделения по типам, показываем общий график
                            # Группируем по часам в зависимости от доступных полей
                            if 'Datetime' in filtered_df.columns:
                                # Используем Datetime для более точной группировки по часам
                                hourly_transactions = filtered_df.groupby(filtered_df['Datetime'].dt.hour).size().reset_index(name='Транзакции')
                                hourly_transactions.columns = ['Hour', 'Транзакции']
                            elif 'Date' in filtered_df.columns and pd.api.types.is_datetime64_any_dtype(filtered_df['Date']):
                                # Если Date является datetime, используем его для группировки
                                hourly_transactions = filtered_df.groupby(filtered_df['Date'].dt.hour).size().reset_index(name='Транзакции')
                                hourly_transactions.columns = ['Hour', 'Транзакции']
                            elif 'Hour' in filtered_df.columns:
                                # Если есть колонка Hour, используем её
                                hourly_transactions = filtered_df.groupby('Hour').size().reset_index(name='Транзакции')
                            else:
                                # Если нет никаких данных о времени, создаем пустой DataFrame
                                st.warning('Нет данных о времени для анализа по часам')
                                hourly_transactions = pd.DataFrame({'Hour': [], 'Транзакции': []})
                            
                            # Объединяем с основным DataFrame
                            hourly_df = hourly_df.merge(hourly_transactions, on='Hour', how='left').fillna(0)
                            
                            # Сортируем данные для правильного отображения
                            hourly_df = hourly_df.sort_values('Hour')
                            
                            # Отображаем график
                            st.bar_chart(hourly_df, x='Hour', y='Транзакции')
                            
                            # Выводим отладочную информацию
                            with st.expander('Детали данных по часам'):
                                st.dataframe(hourly_df[['Hour', 'Транзакции']])
                        
                        # НОВЫЕ ГРАФИКИ:
                        # -----------------------------------------------
                        
                        # Анализ по пользователям - НОВЫЙ РАЗДЕЛ
                        if 'UserId' in filtered_df.columns:
                            st.header('📊 Анализ данных по пользователям')
                            
                            # Добавляем выбор типа транзакций для анализа пользователей
                            if 'InvoiceType' in filtered_df.columns:
                                st.subheader('Выберите тип транзакций для анализа')
                                user_invoice_type = st.radio(
                                    "Тип транзакций:",
                                    ["Все транзакции", "Созданные инвойсы", "Оплаченные", "Возвраты"],
                                    horizontal=True
                                )
                                
                                # Фильтруем данные по выбранному типу транзакций
                                if user_invoice_type == "Созданные инвойсы":
                                    user_filtered_df = filtered_df[filtered_df['InvoiceType'] == 0]
                                elif user_invoice_type == "Оплаченные":
                                    user_filtered_df = filtered_df[filtered_df['InvoiceType'] == 1]
                                elif user_invoice_type == "Возвраты":
                                    user_filtered_df = filtered_df[filtered_df['InvoiceType'] == 2]
                                else:  # Все транзакции
                                    user_filtered_df = filtered_df
                            else:
                                user_filtered_df = filtered_df
                            
                            # Количество транзакций на пользователя
                            user_transactions = user_filtered_df.groupby('UserId').size().reset_index(name='TransactionCount')
                            
                            # Объем транзакций на пользователя
                            if 'Amount' in user_filtered_df.columns:
                                # Для метрики суммы используем только оплаченные транзакции
                                if 'InvoiceType' in filtered_df.columns and user_invoice_type == "Все транзакции":
                                    paid_filtered_df = user_filtered_df[user_filtered_df['InvoiceType'] == 1]
                                else:
                                    paid_filtered_df = user_filtered_df
                                
                                user_amounts = paid_filtered_df.groupby('UserId')['Amount'].sum().reset_index(name='TotalAmount')
                                
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
                                st.subheader(f'Распределение пользователей по количеству транзакций ({user_invoice_type.lower()})')
                                hist_data = pd.DataFrame({
                                    'Транзакции': user_stats['TransactionCount'].clip(upper=20)  # Ограничиваем для лучшей визуализации
                                })
                                st.bar_chart(hist_data)
                        
                        # Анализ по тирам - НОВЫЙ РАЗДЕЛ
                        if 'Amount' in filtered_df.columns:
                            st.header('💰 Распределение по тирам')
                            
                            # Добавляем выбор типа транзакций для анализа по тирам
                            if 'InvoiceType' in filtered_df.columns:
                                st.subheader('Выберите тип транзакций для анализа')
                                tier_invoice_type = st.radio(
                                    "Тип транзакций:",
                                    ["Все транзакции", "Созданные инвойсы", "Оплаченные", "Возвраты"],
                                    horizontal=True,
                                    key="tier_radio"
                                )
                                
                                # Фильтруем данные по выбранному типу транзакций
                                if tier_invoice_type == "Созданные инвойсы":
                                    tier_filtered_df = filtered_df[filtered_df['InvoiceType'] == 0]
                                elif tier_invoice_type == "Оплаченные":
                                    tier_filtered_df = filtered_df[filtered_df['InvoiceType'] == 1]
                                elif tier_invoice_type == "Возвраты":
                                    tier_filtered_df = filtered_df[filtered_df['InvoiceType'] == 2]
                                else:  # Все транзакции
                                    tier_filtered_df = filtered_df
                            else:
                                tier_filtered_df = filtered_df
                                tier_invoice_type = "Все транзакции"
                            
                            # Для распределения по тирам всегда нужны суммы (Amount)
                            # Добавляем колонку Tier на основе суммы транзакции (Amount)
                            tier_filtered_df['Tier'] = tier_filtered_df['Amount'].apply(determine_tier)
                            
                            # Группировка по тирам
                            tier_counts = tier_filtered_df.groupby('Tier').size().reset_index(name='Count')
                            
                            # Упорядочиваем тиры в правильном порядке
                            tier_order = ['25 Stars', '50 Stars', '100+ Stars']
                            tier_counts['Tier'] = pd.Categorical(tier_counts['Tier'], categories=tier_order, ordered=True)
                            tier_counts = tier_counts.sort_values('Tier')
                            
                            # Отображаем распределение в виде столбчатой диаграммы
                            st.subheader(f'Количество транзакций по тирам ({tier_invoice_type.lower()})')
                            st.bar_chart(tier_counts, x='Tier', y='Count')
                            
                            # Отображаем также общую сумму по каждому тиру
                            tier_amounts = tier_filtered_df.groupby('Tier')['Amount'].sum().reset_index(name='TotalAmount')
                            tier_amounts['Tier'] = pd.Categorical(tier_amounts['Tier'], categories=tier_order, ordered=True)
                            tier_amounts = tier_amounts.sort_values('Tier')
                            
                            st.subheader(f'Общая сумма транзакций по тирам ({tier_invoice_type.lower()})')
                            st.bar_chart(tier_amounts, x='Tier', y='TotalAmount')
                        
                        # Анализ глубины вовлечения пользователей
                        if 'UserId' in filtered_df.columns and 'Date' in filtered_df.columns and 'InvoiceType' in filtered_df.columns:
                            st.header('🎁 Анализ глубины вовлечения пользователей')
                            
                            # Фильтруем только оплаченные транзакции (InvoiceType = 1)
                            paid_transactions = filtered_df[filtered_df['InvoiceType'] == 1]
                            
                            # Группируем данные по пользователям и дням
                            paid_transactions['Date'] = pd.to_datetime(paid_transactions['Date']).dt.date
                            
                            # Считаем количество транзакций для каждого пользователя по дням
                            user_daily_counts = paid_transactions.groupby(['UserId', 'Date']).size().reset_index(name='DailyCount')
                            
                            # Считаем общее количество транзакций на пользователя
                            user_total_counts = paid_transactions.groupby('UserId').size().reset_index(name='TotalCount')
                            
                            # Анализ глубины вовлечения - считаем распределение пользователей по количеству транзакций
                            engagement_counts = user_total_counts['TotalCount'].value_counts().reset_index()
                            engagement_counts.columns = ['SpinCount', 'UserCount']
                            engagement_counts = engagement_counts.sort_values('SpinCount')
                            
                            # Считаем общее количество пользователей
                            total_users = user_total_counts['UserId'].nunique()
                            
                            # Добавляем колонку с процентами
                            engagement_counts['Percentage'] = (engagement_counts['UserCount'] / total_users * 100).round(2)
                            
                            st.subheader('Распределение пользователей по количеству оплаченных спинов')
                            
                            # Ограничиваем до 10 первых значений для наглядности
                            if len(engagement_counts) > 10:
                                display_counts = engagement_counts.head(10)
                                st.info(f'Отображаются первые 10 значений из {len(engagement_counts)}')
                            else:
                                display_counts = engagement_counts
                            
                            # Отображаем таблицу
                            st.write('Количество пользователей по количеству спинов:')
                            display_df = display_counts.rename(columns={
                                'SpinCount': 'Количество спинов', 
                                'UserCount': 'Количество пользователей', 
                                'Percentage': 'Процент пользователей (%)'}
                            )
                            st.dataframe(display_df)
                            
                            # Визуализация распределения пользователей
                            st.bar_chart(display_counts, x='SpinCount', y='UserCount')
                            
                            # Анализ по дням
                            st.subheader('Глубина вовлечения пользователей по дням')
                            
                            # Группируем по дням и считаем количество уникальных пользователей в день
                            daily_unique_users = paid_transactions.groupby('Date')['UserId'].nunique().reset_index(name='UniqueUsers')
                            
                            # Считаем распределение пользователей по количеству спинов в каждый день
                            daily_engagement = []
                            unique_dates = sorted(paid_transactions['Date'].unique())
                            
                            # Для каждого дня считаем распределение
                            for date in unique_dates:
                                # Получаем данные за этот день
                                day_data = paid_transactions[paid_transactions['Date'] == date]
                                
                                # Считаем количество транзакций на пользователя в этот день
                                user_counts = day_data.groupby('UserId').size().reset_index(name='SpinCount')
                                
                                # Считаем распределение пользователей по количеству спинов
                                spin_distribution = user_counts['SpinCount'].value_counts().reset_index()
                                spin_distribution.columns = ['SpinCount', 'UserCount']
                                
                                # Добавляем процентное соотношение
                                total_users_day = user_counts['UserId'].nunique()
                                spin_distribution['Percentage'] = (spin_distribution['UserCount'] / total_users_day * 100).round(2)
                                
                                # Добавляем дату
                                spin_distribution['Date'] = date
                                
                                daily_engagement.append(spin_distribution)
                            
                            # Объединяем все данные в один DataFrame
                            if daily_engagement:
                                all_daily_engagement = pd.concat(daily_engagement)
                                
                                # Показываем динамику с возможностью выбора количества спинов
                                spin_counts = sorted(all_daily_engagement['SpinCount'].unique())
                                
                                # Фильтр по количеству спинов
                                selected_spins = st.multiselect(
                                    'Выберите количество спинов для отображения динамики', 
                                    options=spin_counts,
                                    default=[1, 2, 3] if 1 in spin_counts and 2 in spin_counts and 3 in spin_counts else spin_counts[:min(3, len(spin_counts))]
                                )
                                
                                if selected_spins:
                                    # Фильтруем по выбранным значениям
                                    filtered_data = all_daily_engagement[all_daily_engagement['SpinCount'].isin(selected_spins)]
                                    
                                    # Создаем сводную таблицу для графика
                                    pivot_data = filtered_data.pivot(index='Date', columns='SpinCount', values='Percentage').reset_index()
                                    
                                    # Переименовываем столбцы для удобства
                                    pivot_data.columns = ['Date'] + [f'{x} спин' if x == 1 else f'{x} спина' if 1 < x < 5 else f'{x} спинов' for x in pivot_data.columns[1:]]
                                    
                                    # Строим график
                                    st.line_chart(pivot_data, x='Date')
                                    
                                    st.info('График показывает процент пользователей, сделавших указанное количество спинов в день')
                            
                            # Анализ ретеншена пользователей
                            st.header('🔄 Анализ ретеншена пользователей')
                            
                            # Используем также только оплаченные транзакции
                            # Преобразуем дату в datetime для корректной работы
                            paid_transactions['Date'] = pd.to_datetime(paid_transactions['Date'])
                            
                            # Находим дату первой транзакции для каждого пользователя
                            first_transactions = paid_transactions.groupby('UserId')['Date'].min().reset_index()
                            first_transactions.rename(columns={'Date': 'FirstDate'}, inplace=True)
                            
                            # Добавляем дату первой транзакции к исходному датасету
                            user_activity = pd.merge(paid_transactions, first_transactions, on='UserId')
                            
                            # Рассчитываем разницу в днях между каждой транзакцией и первой транзакцией пользователя
                            user_activity['DayDifference'] = (user_activity['Date'] - user_activity['FirstDate']).dt.days
                            
                            # Отбрасываем записи с разницей в 0 дней (то есть первые транзакции)
                            retention_data = user_activity[user_activity['DayDifference'] > 0]
                            
                            # Группируем по дням и считаем уникальных пользователей
                            retention_counts = retention_data.groupby('DayDifference')['UserId'].nunique().reset_index()
                            retention_counts.columns = ['Day', 'ReturnedUsers']
                            
                            # Считаем общее количество уникальных пользователей
                            total_users = first_transactions.shape[0]
                            
                            # Добавляем колонку с процентом ретеншена
                            retention_counts['RetentionRate'] = (retention_counts['ReturnedUsers'] / total_users * 100).round(2)
                            
                            # Ограничиваем до 30 дней для наглядности
                            if len(retention_counts) > 0:
                                max_days_to_show = 30
                                if retention_counts['Day'].max() > max_days_to_show:
                                    display_retention = retention_counts[retention_counts['Day'] <= max_days_to_show]
                                    st.info(f'Показываем ретеншен за первые {max_days_to_show} дней')
                                else:
                                    display_retention = retention_counts
                                
                                # Создаем четкую визуализацию
                                st.subheader('Ретеншен пользователей по дням')
                                
                                # Отображаем таблицу с ретеншеном
                                display_df = display_retention.rename(columns={
                                    'Day': 'День', 
                                    'ReturnedUsers': 'Вернувшиеся пользователи', 
                                    'RetentionRate': 'Ретеншен (%)'
                                })
                                st.dataframe(display_df)
                                
                                # Строим график ретеншена
                                st.line_chart(display_retention, x='Day', y='RetentionRate')
                                st.info('График показывает процент пользователей, вернувшихся на указанный день после первой транзакции')
                            else:
                                st.warning('Недостаточно данных для анализа ретеншена')
                            
                            # Дополнительно: когортный анализ по дню первой транзакции
                            st.subheader('Когортный анализ пользователей')
                            
                            # Выделяем дату без времени для группировки по дням
                            user_activity['FirstDateDay'] = user_activity['FirstDate'].dt.date
                            user_activity['Date_Day'] = user_activity['Date'].dt.date
                            
                            # Подсчитываем количество новых пользователей по каждой когорте
                            first_day_users = user_activity.groupby('FirstDateDay')['UserId'].nunique().reset_index()
                            first_day_users.columns = ['Cohort', 'NewUsers']
                            
                            # Построение когортной матрицы
                            if len(first_day_users) > 0:
                                # Считаем возвращаемость по когортам
                                # Группируем по дню первого визита, текущему дню и считаем уникальных пользователей
                                cohort_activity = user_activity.groupby(['FirstDateDay', 'Date_Day'])['UserId'].nunique().reset_index()
                                cohort_activity.columns = ['Cohort', 'Date', 'Users']
                                
                                # Вычисляем разницу в днях между датой первой и текущей активности
                                try:
                                    # Показываем типы данных для диагностики
                                    with st.expander('Диагностика дат'):
                                        st.write(f"Cohort тип: {cohort_activity['Cohort'].dtype}")
                                        st.write(f"Date тип: {cohort_activity['Date'].dtype}")
                                        st.write(f"Cohort пример: {cohort_activity['Cohort'].iloc[0]}")
                                        st.write(f"Date пример: {cohort_activity['Date'].iloc[0]}")
                                    
                                    # Мы можем сделать копии столбцов, чтобы не менять исходные данные
                                    cohort_activity['Cohort_dt'] = pd.to_datetime(cohort_activity['Cohort'])
                                    cohort_activity['Date_dt'] = pd.to_datetime(cohort_activity['Date'])
                                    
                                    # Вычисляем разницу в днях
                                    cohort_activity['DayNumber'] = (cohort_activity['Date_dt'] - cohort_activity['Cohort_dt']).dt.days
                                except Exception as e:
                                    st.error(f"Ошибка при расчете разницы дат: {str(e)}")
                                    # Альтернативный метод расчёта разницы дней
                                    st.warning('Используем альтернативный метод вычисления дней')
                                    
                                    # Преобразуем в строки, а затем в даты
                                    cohort_activity['Cohort_str'] = cohort_activity['Cohort'].astype(str)
                                    cohort_activity['Date_str'] = cohort_activity['Date'].astype(str)
                                    
                                    # Преобразуем в даты и рассчитываем разницу
                                    cohort_activity['Cohort_date'] = pd.to_datetime(cohort_activity['Cohort_str']).dt.date
                                    cohort_activity['Date_date'] = pd.to_datetime(cohort_activity['Date_str']).dt.date
                                    
                                    # Вычисляем разницу в днях
                                    cohort_activity['DayNumber'] = [(date - cohort).days for cohort, date in zip(cohort_activity['Cohort_date'], cohort_activity['Date_date'])]
                                
                                # Добавляем информацию о новых пользователях в каждой когорте
                                cohort_counts = cohort_activity.merge(first_day_users, on='Cohort')
                                
                                # Рассчитываем процент удержания для каждой когорты
                                cohort_counts['RetentionRate'] = (cohort_counts['Users'] / cohort_counts['NewUsers'] * 100).round(2)
                                
                                # Создаем сводную таблицу для когортного анализа
                                retention_pivot = cohort_counts.pivot_table(index='Cohort', 
                                                                  columns='DayNumber', 
                                                                  values='RetentionRate')
                                
                                # Ограничиваем число когорт и дней для наглядности
                                max_cohorts = 10
                                max_days = 14
                                
                                # Добавляем обработку исключений
                                try:
                                    # Диагностическая информация о результатах когортного анализа
                                    with st.expander('Диагностика когортного анализа'):
                                        st.write(f"retention_pivot тип: {type(retention_pivot)}")
                                        st.write(f"retention_pivot размер: {retention_pivot.shape if hasattr(retention_pivot, 'shape') else 'N/A'}")
                                        st.write(f"retention_pivot столбцы: {list(retention_pivot.columns) if hasattr(retention_pivot, 'columns') else 'N/A'}")
                                    
                                    # Формируем ограниченную матрицу
                                    if len(retention_pivot) > max_cohorts:
                                        display_pivot = retention_pivot.iloc[:max_cohorts]
                                        st.info(f'Показываем первые {max_cohorts} когорт')
                                    else:
                                        display_pivot = retention_pivot
                                    
                                    # Ограничиваем количество дней
                                    try:
                                        if display_pivot.columns.max() > max_days:
                                            display_columns = [col for col in display_pivot.columns if col <= max_days]
                                            display_pivot = display_pivot[display_columns]
                                            st.info(f'Показываем ретеншен за первые {max_days} дней')
                                    except Exception as e2:
                                        st.warning(f"Не удалось ограничить количество дней: {str(e2)}")
                                except Exception as e:
                                    st.error(f"Ошибка при формировании когортной матрицы: {str(e)}")
                                    display_pivot = pd.DataFrame()  # Создаем пустой DataFrame для предотвращения ошибок
                                
                                # Отображаем когортную матрицу в виде таблицы
                                st.write('Матрица ретеншена по когортам (%):')
                                
                                # Форматируем даты для удобства отображения
                                display_pivot_formatted = display_pivot.copy()
                                display_pivot_formatted.index = display_pivot_formatted.index.strftime('%Y-%m-%d')
                                
                                # Переименовываем столбцы для лучшего понимания
                                display_pivot_formatted.columns = [f'День {col}' for col in display_pivot_formatted.columns]
                                
                                try:
                                    # Проверяем, что данные существуют и корректны
                                    if display_pivot_formatted.empty:
                                        st.warning('Недостаточно данных для отображения матрицы ретеншена')
                                    else:
                                        # Отображаем таблицу с заменой NaN на дефис
                                        st.dataframe(display_pivot_formatted.fillna('-').style.background_gradient(cmap='YlGnBu', axis=None), use_container_width=True)
                                except Exception as e_display:
                                    st.error(f"Ошибка при отображении матрицы: {str(e_display)}")
                                    # Показываем данные в простом виде без форматирования
                                    try:
                                        st.dataframe(display_pivot_formatted, use_container_width=True)
                                    except:
                                        st.write('Невозможно отобразить данные ретеншена')
                                
                                st.info('Матрица показывает процент пользователей, вернувшихся на N-й день после первой транзакции. По строкам - когорты по дате первой транзакции')
                                
                                try:
                                    # Проверяем, есть ли данные для отображения графика
                                    if display_pivot.empty:
                                        st.warning('Недостаточно данных для отображения графика ретеншена')
                                    else:
                                        # Визуализация динамики ретеншена по когортам
                                        st.subheader('Динамика ретеншена по когортам')
                                        
                                        # Подготавливаем данные для графика
                                        try:
                                            # Выбираем до 5 когорт для наглядности
                                            max_cohorts_chart = 5
                                            cohort_selection = min(max_cohorts_chart, len(display_pivot))
                                            
                                            # Проверяем, что есть хотя бы одна когорта
                                            if cohort_selection > 0:
                                                # Транспонируем данные, чтобы дни были по строкам, а когорты по столбцам
                                                chart_data = display_pivot.iloc[:cohort_selection].T
                                                
                                                # Форматируем названия столбцов для графика
                                                try:
                                                    chart_data.columns = [str(date.strftime('%Y-%m-%d')) for date in chart_data.columns]
                                                except Exception as e_format:
                                                    st.warning(f"Ошибка форматирования имен когорт: {str(e_format)}")
                                                    chart_data.columns = [f'Когорта {i+1}' for i in range(len(chart_data.columns))]
                                                
                                                # Проверяем данные на NaN и заполняем нулями
                                                chart_data = chart_data.fillna(0)
                                                
                                                # Строим линейный график для сравнения когорт
                                                st.line_chart(chart_data)
                                                
                                                with st.expander('Детали графика'):
                                                    st.write('График показывает процент пользователей, вернувшихся к активности в каждый день после первой транзакции.')
                                                    st.write(f'Показано первых {cohort_selection} когорт для лучшей читаемости.')
                                                    st.dataframe(chart_data)
                                            else:
                                                st.warning('Нет данных для отображения графика когорт')
                                        except Exception as e_chart:
                                            st.error(f"Ошибка при создании графика ретеншена: {str(e_chart)}")
                                except Exception as e_viz:
                                    st.error(f"Ошибка при визуализации данных ретеншена: {str(e_viz)}")
                                
                                # Добавляем пояснение
                                st.info(f'График показывает динамику ретеншена по дням для первых {cohort_selection} когорт')
                                
                                # Общая статистика по ретеншену
                                st.subheader('Общая статистика')
                                
                                # Ключевые показатели ретеншена
                                if 0 in retention_pivot.columns and 1 in retention_pivot.columns:
                                    # Получаем средний ретеншен на 1-й день
                                    day_1_retention = retention_pivot[1].mean()
                                    
                                    # Если есть данные по 7-му дню
                                    if 7 in retention_pivot.columns:
                                        day_7_retention = retention_pivot[7].mean()
                                    else:
                                        day_7_retention = None
                                    
                                    # Если есть данные по 14-му дню
                                    if 14 in retention_pivot.columns:
                                        day_14_retention = retention_pivot[14].mean()
                                    else:
                                        day_14_retention = None
                                    
                                    # Базовые метрики
                                    col1, col2, col3 = st.columns(3)
                                    
                                    with col1:
                                        st.metric('Однодневный ретеншен', f'{day_1_retention:.2f}%')
                                    with col2:
                                        if day_7_retention is not None:
                                            st.metric('7-дневный ретеншен', f'{day_7_retention:.2f}%')
                                        else:
                                            st.metric('7-дневный ретеншен', 'Недостаточно данных')
                                    with col3:
                                        if day_14_retention is not None:
                                            st.metric('14-дневный ретеншен', f'{day_14_retention:.2f}%')
                                        else:
                                            st.metric('14-дневный ретеншен', 'Недостаточно данных')
                                else:
                                    st.warning('Недостаточно данных для расчета базовых метрик ретеншена')
                            else:
                                st.warning('Недостаточно данных для анализа когорт')
                
                # Сохранение данных в сессии для последующего использования
                st.session_state['data'] = combined_df
                
                st.success('Данные успешно загружены и проанализированы!')

# Используем сохраненные данные, если они есть
elif st.session_state['data'] is not None:
    combined_df = st.session_state['data']
    
    # Отображаем данные
    st.subheader('Предварительный просмотр данных')
    st.dataframe(combined_df.head(100), use_container_width=True)
    
    # Информация о данных
    st.subheader('Статистика данных')
    
    col1, col2 = st.columns(2)
    
    if 'UserId' in combined_df.columns:
        with col1:
            st.metric('Уникальных пользователей', f"{combined_df['UserId'].nunique():,}")
    
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
        
        # Виджет для выбора диапазона дат с сохранением в session_state
        date_range = st.date_input(
            "Выберите диапазон дат",
            value=(default_start_date, default_end_date) if st.session_state['date_range'] is None else st.session_state['date_range'],
            min_value=min_date.date(),
            max_value=max_date.date(),
            key="date_input"
        )
        
        # Сохраняем выбранный диапазон в session_state
        st.session_state['date_range'] = date_range
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            # Фильтруем данные по выбранному диапазону дат
            filtered_df = combined_df[
                (combined_df['Date'] >= start_date) & 
                (combined_df['Date'] <= end_date)
            ]
            
            # Сохраняем отфильтрованные данные в session_state
            st.session_state['filtered_df'] = filtered_df
            
            st.write(f"Выбран период: с {start_date} по {end_date}")
            
            # Отображаем общую сумму спинов за выбранный период
            if 'Amount' in filtered_df.columns and 'InvoiceType' in filtered_df.columns:
                # Фильтруем только оплаченные транзакции (InvoiceType = 1) за выбранный период
                paid_df = filtered_df[filtered_df['InvoiceType'] == 1]
                with col2:
                    st.metric('Сумма спинов в stars', f"{paid_df['Amount'].sum():,.2f}")
                
            # Добавляем выбор типов транзакций для отображения
            st.subheader('Объем транзакций по дням')

# Добавляем информацию о приложении
st.sidebar.markdown('---')
st.sidebar.info('''
### О приложении
Это приложение предназначено для анализа транзакционных данных, 
загруженных напрямую из AWS S3.

© 2025 Подарочный Дашборд
''')