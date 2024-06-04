import streamlit as st
from . import frontend_utils as utils
from . import task
from . import task as functions
from streamlit.delta_generator import DeltaGenerator
from typing import (Callable, List, Optional, Tuple)
from datetime import datetime, timedelta
from . import settings
from . import scheduler
from sqlalchemy import engine
import pandas as pd
from rq.job import Job
import json
from typing import List



def get_execution_frequency_input(unit_col: DeltaGenerator,
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

def get_execution_frequency_information(execution_frequency: str,
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
		time_unit, time_unit_quantity = get_execution_frequency_input(unit_col, slider_col)

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


def layout_homepage_define_new_task(process_df, db_engine, user_defined_task:List[task.Task]) -> None:
	"""
	Render and process homepage UI based on streamlit for defining a new task.

	Args:
		process_df: df with current process information.
		db_engine: database engine for saving df into db.
	"""  

	tasks_list = {t.name: t.func for t in user_defined_task}
	order, monitor = st.columns(2)

	with order:
		with st.expander("# 🎯 Add New Requests"):
			row = st.columns(4)
			foodgrids = [col.container(height=200) for col in row]
			with foodgrids[0]: st.image("https://assets.epicurious.com/photos/57c5c6d9cf9e9ad43de2d96e/1:1/w_1920,c_limit/the-ultimate-hamburger.jpg")	
			with foodgrids[1]: st.image("https://assets.unileversolutions.com/recipes-v2/243652.jpg")
			with foodgrids[2]: st.image("https://scontent-ord5-1.xx.fbcdn.net/v/t1.18169-9/20621949_181866382354166_2547956009864041122_n.jpg?_nc_cat=101&ccb=1-7&_nc_sid=5f2048&_nc_ohc=2umspTq0CS8Q7kNvgHlehGp&_nc_ht=scontent-ord5-1.xx&oh=00_AYDNexJo9RpY0Twgclf3kwU6-1ddbEsFc5whIgRj_UIDew&oe=6680BCC8")
			with foodgrids[3]: st.image("https://img.taste.com.au/g3bM8rGr/w643-h428-cfill-q90/taste/2016/11/rachel-87711-2.jpeg")
			
			with st.columns(3)[1]: st.image("https://png.pngtree.com/png-clipart/20211116/original/pngtree-cute-hijab-muslim-chef-holding-the-order-here-sign-board-png-image_6933723.png", width=200)
			form = st.form(key="annotation")
			
			with form:
				cols = st.columns((1,1))
				user = cols[0].text_input("Username:")
				job_name = cols[1].text_input("Job name", "ABC")
				cols = st.columns(2)
				task_name = cols[0].selectbox("Task:", tasks_list.keys(), index=0)
				job_name = task_name
				#func_input["filename"] = cols[1].file_uploader("Upload a file")
				cols = st.columns(2)
				#data_start = cols[0].date_input("Dataset start from:")
				#data_end = cols[1].date_input("Dataset end on:")
				#if task_name == "sleep": 
				#	func_input["query"] = st.text_area("Query:")
				#else: comment = st.text_area("Notes:")
				func_inputs = st.text_area("Notes")
				if func_inputs: func_inputs = json.loads(func_inputs)
				func = tasks_list[task_name]

				unit_select_col, slider_select_col, interval_duration, weekdays, frequency = get_execution_frequency()
				execution_schedule_col, date_input_col, time_slider_col, execution_type, start = get_execution_start_date(frequency, weekdays)
				cols = st.columns(2)
				submitted = cols[0].form_submit_button(label="Submit")
				refreshed = cols[1].form_submit_button(label="Get updates")
				conn, q, process = scheduler.start_scheduler_process(job_name)
				
				func_input = {"food_name": task_name, "seconds": 10}

			if submitted:
				#new_task_id = utils.get_start_task_id(process_df)
				new_task_id = 1
				job = scheduler.submit_task(func=func, func_input=func_input, job_name=job_name, task_name="", start=start, interval_duration=interval_duration, weekdays=weekdays, execution_type=execution_type, index=new_task_id, db_engine=db_engine, worker_process=process, queue=q, queue_type="rq")
				st.success(f"Submitted task {job_name} with process_id {process.pid} to execute job {job.id}.")
				st.write(job.get_status())

			if refreshed:
					
				#jobs = [{'id': job_id, 'time': q.fetch_job(job_id).ended_at} for job_id in jobs]
				#jobs = [{'id': j['id'], 'time': j['time'], 'result': q.fetch_job(j['id']).result} for j in jobs]
				#st.write(jobs)
				#st.write(q.fetch_job(job['id']).result)
				
				refresh(process_df, conn)

	with monitor:
		layout_task_tables(process_df, db_engine)


			
def refresh(df:pd.DataFrame, connection, to_wait: int = 0) -> None:
	"""
	(Optionally) wait for a given amount of time (in seconds)
	and trigger Streamlit app refresh.

	Args:
		to_wait: integer indicating amount of seconds to wait.

	Raises:
		RerunException that stops and re-runs the app script.
	"""
	if to_wait:
		empty_slot = st.empty()

		for i in range(to_wait):
			empty_slot.write(f"Refreshing in {to_wait - i} seconds...")
			time.sleep(1)
	st.write(scheduler.update_task_status_info(df, connection))
	if hasattr(st, 'scriptrunner'):
		raise st.scriptrunner.script_runner.RerunException(st.scriptrunner.script_requests.RerunData())
	else: 
		raise st.runtime.scriptrunner.script_runner.RerunException(st.runtime.scriptrunner.script_requests.RerunData())

#def refresh(df: pd.DataFrame, connection) -> None:
#	return(scheduler.update_task_status_info(df, connection))
	#scheduler.update_df_process_last_update_info(df)
@st.experimental_fragment
def layout_task_tables(process_df, db_engine) -> None:
	st.button("Update")

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




def homepage(db_engine: engine, user_tasks) -> None:
	global process_df
	process_df = scheduler.get_process_df(db_engine)

	layout_homepage_define_new_task(process_df, db_engine, user_tasks)
