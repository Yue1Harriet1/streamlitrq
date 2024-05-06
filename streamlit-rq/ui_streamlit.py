import streamlit as st
from . import frontend_utils as utils
from . import task as functions
from streamlit.delta_generator import DeltaGenerator
from typing import (Callable, List, Optional, Tuple)
from datetime import datetime, timedelta
from . import settings
from . import scheduler
from sqlalchemy import engine
import pandas as pd


def get_time_interval_info(unit_col: DeltaGenerator,
                           slider_col: DeltaGenerator
                           ) -> Tuple[Optional[str], Optional[int]]:
    """
    Get execution frequency information from UI inputs.

    Args:
        unit_col: Streamlit column with UI element to select the corresponding
            execution time interval, e.g. 'minutes', 'hours', etc.
        slider_col: Streamlit column with UI element to select the quantity
            of execution time intervals.

    Returns:
        selected time interval and related execution frequency.
    """
    time_unit = unit_col.selectbox("Select Unit", ("Minutes", "Hours", "Days", "Weeks"))
    time_unit_quantity = slider_col.slider(
        f"Every x {time_unit}", min_value=1, max_value=settings.TIME_VALUES[time_unit]
    )

    return time_unit, time_unit_quantity


def select_weekdays(unit_col: DeltaGenerator) -> Optional[List[str]]:
    """
    Select weekdays on which the process must be executed.

    Args:
        unit_col: Streamlit column with UI multi-select element
            to select appropriate weekdays.

    Returns:
        list strings representing selected weekdays.
    """
    return unit_col.multiselect(
        "Select weekdays:",
        options=list(settings.WEEK_DAYS.values()),
        default=list(settings.WEEK_DAYS.values()),
    )

def get_execution_interval_information(execution_frequency: str,
                                       unit_col: DeltaGenerator,
                                       slider_col: DeltaGenerator
                                       ) -> Tuple[Optional[str], Optional[int], Optional[List[str]]]:
    """
    Get command execution interval information, including time interval (e.g. hours, weeks, etc.),
    related quantity and execution weekdays.

    Args:
        execution_frequency: string indicating execution frequency.
        unit_col: Streamlit column with UI element to select the corresponding
            execution time interval, e.g. 'minutes', 'hours', etc.
        slider_col: Streamlit column with UI element to select the quantity
            of execution time intervals.

    Returns:
        tuple with information about time interval, interval quantity and selected weekdays.
    """
    time_unit = time_unit_quantity = weekdays = None

    if execution_frequency == "Interval":
        time_unit, time_unit_quantity = get_time_interval_info(unit_col, slider_col)

    if execution_frequency == "Daily":
        weekdays = select_weekdays(unit_col)

    return time_unit, time_unit_quantity, weekdays

def get_interval_duration(time_unit: str, time_unit_quantity: Optional[int],
                          weekdays: Optional[List[str]]) -> timedelta:
    """
    Get the waiting interval to wait for until the next job execution.

    Args:
        time_unit: (optional) unit of execution time interval, e.g. 'hours', 'days', etc.
        time_unit_quantity: (optional) amount of time interval units.
        weekdays: (optional) list with selected weekdays.

    Returns:
        timedelta: time interval to wait before next schedule.
    """
    try:
        return timedelta(days=1) if weekdays or not time_unit else \
            settings.DATE_TRANSLATION[time_unit] * time_unit_quantity
    except KeyError as exc:
        raise exc

def get_execution_frequency():
	frequency_select_col, unit_select_col, slider_select_col = st.columns(3)
	frequency = frequency_select_col.selectbox("Select Frequency", (settings.IMMEDIATE_FREQUENCY, settings.INTERVAL_FREQUENCY, settings.DAILY_FREQUENCY))
	time_unit, time_unit_quantity, weekdays = get_execution_interval_information(frequency, unit_select_col, slider_select_col)
	interval_duration = get_interval_duration(time_unit, time_unit_quantity, weekdays)
	return(unit_select_col, slider_select_col, interval_duration, weekdays, frequency)


def get_execution_interval_information(execution_frequency: str,
                                       unit_col: DeltaGenerator,
                                       slider_col: DeltaGenerator
                                       ) -> Tuple[Optional[str], Optional[int], Optional[List[str]]]:
    """
    Get command execution interval information, including time interval (e.g. hours, weeks, etc.),
    related quantity and execution weekdays.

    Args:
        execution_frequency: string indicating execution frequency.
        unit_col: Streamlit column with UI element to select the corresponding
            execution time interval, e.g. 'minutes', 'hours', etc.
        slider_col: Streamlit column with UI element to select the quantity
            of execution time intervals.

    Returns:
        tuple with information about time interval, interval quantity and selected weekdays.
    """
    time_unit = time_unit_quantity = weekdays = None

    if execution_frequency == "Interval":
        time_unit, time_unit_quantity = get_time_interval_info(unit_col, slider_col)

    if execution_frequency == "Daily":
        weekdays = select_weekdays(unit_col)

    return time_unit, time_unit_quantity, weekdays

def calculate_execution_start(date_input_col: DeltaGenerator,
                              time_slider_col: DeltaGenerator) -> datetime:
    """
    Calculate the start datetime of command execution.

    Args:
        date_input_col: Streamlit column with UI element to select execution date.
        time_slider_col:  Streamlit column with UI (slider) element to select hr:min of execution.

    Returns:
        datetime object with the start date & time of execution.
    """
    input_date = date_input_col.date_input("Starting date", datetime.now())
    selected_time = time_slider_col.slider(
        "Timepoint",
        min_value=datetime(2020, 1, 1, 00, 00),
        max_value=datetime(2020, 1, 1, 23, 59),
        value=datetime(2020, 1, 1, 12, 00),
        format="HH:mm",
    )

    execution_date = datetime(input_date.year, input_date.month, input_date.day)
    time_difference = selected_time - datetime(2020, 1, 1, 00, 00, 00)

    return execution_date + time_difference

def get_task_execution_start(execution_type: str, execution_frequency: str,
                                weekdays: Optional[List[str]], date_col: DeltaGenerator,
                                slider_col: DeltaGenerator) -> datetime:
    """
    Get the start datetime of command execution.

    Args:
        execution_type: type of execution schedule: is execution "Scheduled" or not.
        execution_frequency: frequency of execution: "Interval" / "Daily"
        weekdays: (optional) list with selected weekdays.
        date_col: Streamlit column with UI element to select execution date.
        slider_col: Streamlit column with UI (slider) element to select hr:min of execution.

    Returns:
        datetime object with the start date & time of execution.
    """
    start = datetime.now()

    if execution_type == "Scheduled":
        start = calculate_execution_start(date_col, slider_col)

        if execution_frequency == "Daily":
            while settings.WEEK_DAYS[start.weekday()] not in weekdays:
                start += timedelta(days=1)

    st.text(f"First execution on {start.strftime(settings.DATE_FORMAT)}.")

    return start


def get_execution_start_date(frequency, weekdays):
	execution_schedule_col, date_input_col, time_slider_col = st.columns(3)
	execution_type = execution_schedule_col.selectbox("Execution", ("Now", "Scheduled"))
	start = get_task_execution_start(execution_type, frequency, weekdays, date_input_col, time_slider_col)
	return(execution_schedule_col, date_input_col, time_slider_col, execution_type, start)


def layout_homepage_define_new_task(process_df, db_engine) -> None:
	"""
	Render and process homepage UI based on streamlit for defining a new task.

	Args:
		process_df: df with current process information.
		db_engine: database engine for saving df into db.
	"""

	with st.expander("# 🎯 Add New Task"):
		st.write("")
		form = st.form(key="annotation")
		func_input = {}
		with form:
			cols = st.columns((1,1))
			user = cols[0].text_input("Username:")
			job_name = cols[1].text_input("Job name", "ABC")
			cols = st.columns(2)
			task_name = cols[0].selectbox("Task:", ["Interpret files"], index=0)
			job_name = task_name
			func_input["filename"] = cols[1].file_uploader("Upload a file")
			cols = st.columns(2)
			data_start = cols[0].date_input("Dataset start from:")
			data_end = cols[1].date_input("Dataset end on:")
			if task_name == "Interpret files": 
				func_input["query"] = st.text_area("Query:")
				func = functions.sleep(5)
			else: comment = st.text_area("Notes:")
			unit_select_col, slider_select_col, interval_duration, weekdays, frequency = get_execution_frequency()
			execution_schedule_col, date_input_col, time_slider_col, execution_type, start = get_execution_start_date(frequency, weekdays)
			cols = st.columns(2)
			submitted = cols[0].form_submit_button(label="Submit")
			refreshed = cols[0].form_submit_button(label="Get updates")
			q, process = scheduler.start_scheduler_process(func, func_input, job_name)


		if submitted:
			#new_task_id = utils.get_start_task_id(process_df)
			new_task_id = 1
			job = scheduler.submit_task(func=func, func_input=func_input, job_name=job_name, task_name="", start=start, interval_duration=interval_duration, weekdays=weekdays, execution_type=execution_type, task_id=new_task_id, db_engine=db_engine, worker_process=process, queue=q, queue_type="rq")
			st.success(f"Submitted task {job_name} with process_id {process.pid} to execute job {job.id}.")

		if refreshed:
			jobs = q.finished_job_registry.get_job_ids()
			jobs = [{'id': job_id, 'time': q.fetch_job(job_id).ended_at} for job_id in jobs]
			jobs = [{'id': job['id'], 'time': job['time'], 'result': q.fetch_job(job['id']).result} for job in jobs]
			st.write(jobs)
			if refreshed: refresh(process_df)

def refresh(df: pd.DataFrame) -> None:
	scheduler.update_process_status_info(df)
	scheduler.update_df_process_last_update_info(df)

def layout_task_tables(process_df, db_engine) -> None:

	with st.expander("# 🔢 Task Updates"):
		process_df = scheduler.get_process_df(db_engine)
		st.table(process_df)
		# In case process df has any processes that are no longer running (but still alive)
		# provide user an option to remove them
		if False in process_df["running"].values:
			if st.button("Remove processes that are not running."):
				running = process_df[process_df["running"]]
				running.to_sql("processes", con=db_engine, if_exists="replace", index=False)
				scheduler.refresh_app()




def homepage(db_engine: engine) -> None:
	global process_df
	process_df = scheduler.get_process_df(db_engine)
	layout_task_tables(process_df, db_engine)

	layout_homepage_define_new_task(process_df, db_engine)
