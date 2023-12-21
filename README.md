# Итоговый проект

## Описание задачи.
Разработать ETL процесс, получающий ежедневную выгрузку данных (предоставляется за 3 дня), загружающий ее в хранилище данных и ежедневно строящий отчет.

## Выгрузка данных.
Ежедневно некие информационные системы выгружают следующие файлы:
* список транзакций за текущий день. Формат – CSV.
* список терминалов полным срезом. Формат – XLSX.
* список паспортов, включенных в «черный список» - с накоплением с начала месяца. Формат – XLSX.
Сведения о картах, счетах и клиентах хранятся в СУБД Oracle в схеме BANK. 
Выгрузка предоставляется за последние три дня, ее надо обработать. 

## Структура хранилища.
Данные должны быть загружены в хранилище со следующей структурой (имена сущностей указаны по существу, без особенностей правил нейминга, указанных далее):

![image](https://github.com/SvKhlapova/sql-projet_mipt/assets/113574956/6899c4ea-782c-404c-84a4-42a82fb8c696)

Типы данных в полях можно изменять на однородные если для этого есть необходимость.   Имена полей менять нельзя. Ко всем таблицам SCD1 должны быть 
добавлены технические поля *create_dt*, *update_dt*; ко всем таблицам SCD2 должны быть добавлены технические поля *effective_from*, *effective_to*, *deleted_flg*.

## Построение отчета.
По результатам загрузки ежедневно необходимо строить витрину отчетности по мошенническим операциям. Витрина строится накоплением, каждый новый отчет укладывается в эту же таблицу с новым report_dt. В витрине должны содержаться следующие поля:
* **event_dt** - время наступления события. Если событие наступило по результату нескольких действий – указывается время действия, по которому установлен факт мошенничества.
* **passport** - номер паспорта клиента, совершившего мошенническую операцию.
* **fio** - ФИО клиента, совершившего мошенническую операцию. 
* **phone** - номер телефона клиента, совершившего мошенническую операцию.
* **event_type** - описание типа мошенничества.
* **report_dt** - время построения отчета.

## Признаки мошеннических операций.
* Совершение операции при просроченном или заблокированном паспорте.  
* Совершение операции при недействующем договоре.  
* Совершение операций в разных городах в течение одного часа.  
* Попытка подбора суммы. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.


## Правила именования таблиц.
Необходимо придерживаться следующих правил именования (для автоматизации проверки):  
**STG_<TABLE_NAME>** - Таблицы для размещения стейджинговых таблиц (первоначальная загрузка), промежуточное выделение инкремента если требуется.   
Временные таблицы, если такие потребуются в расчете, можно также складывать с таким именованием. Имя таблиц можно выбирать произвольное, но смысловое.  
**DWH_FACT_<TABLE_NAME>** - Таблицы фактов, загруженных в хранилище. В качестве фактов выступают сами транзакции и «черный список» паспортов.   
Имя таблиц – как в ER диаграмме.  
**DWH_DIM_<TABLE_NAME>** - Таблицы измерений, хранящиеся в формате SCD1.   
Имя таблиц – как в ER диаграмме.  
**DWH_DIM_<TABLE_NAME>_HIST** - Таблицы измерений, хранящиеся в SCD2 формате (только для тех, кто выполняет усложненное задание).   
Имя таблиц – как в ER диаграмме.   
**REP_FRAUD** - Таблица с отчетом.  
**META_<TABLE_NAME>** - Таблицы для хранения метаданных. Имя таблиц можно выбирать произвольное, но смысловое.

## Обработка файлов
Выгружаемые файлы именуются согласно следующему шаблону:
* transactions_DDMMYYYY.txt 
* passport_blacklist_DDMMYYYY.xlsx 
* terminals_DDMMYYYY.xlsx  
Предполагается что в один день приходит по одному такому файлу. После загрузки соответствующего файла он должен быть переименован в файл с расширением .backup чтобы при следующем запуске файл не искался и перемещен в каталог archive:
* transactions_DDMMYYYY.txt.backup 
* passport_blacklist_DDMMYYYY.xlsx.backup 
* terminals_DDMMYYYY.xlsx.backup
Желающие могут придумать, обосновать и реализовать более технологичные и учитывающие сбои способы обработки (за это будет повышен балл).

## Проверка результата.
Проверка задания состоит из нескольких частей, обязательных к одновременному выполнению.
В ЦДПО выкладывается zip-архив, содержащий следующие файлы и каталоги:
* **main.py**  (файл, обязательный). Основной процесс обработки.  
* **файлы с данными** (файл, обязательный). Те файлы, которые получили в качестве задания.
* **archive** (каталог, обязательный). Пустой, сюда должны перемещаться отработанные файлы.
* **sql_scripts** (каталог, необязательный). Если  в main.py включаете какие-то SQL скрипты, вынесенные в отдельные файлы – помещайте их сюда.
* **py_scripts** (каталог, необязательный). Если  в main.py включаете какие-то python скрипты, вынесенные в отдельные файлы – помещайте их сюда.

