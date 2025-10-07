import csv
import logging
import importlib
import os.path

from airflow.models import BaseOperator


def get_step_names(context):
    current_step = context['task']
    previous_steps = [task for task in context['task'].upstream_list]
    return {
        "current_step": current_step,
        "previous_steps": previous_steps
    }


class CSVIteratorOperator(BaseOperator):
    def __init__(self, tasks: dict, output_trace: str = "csv_row", **kwargs):
        super().__init__(**kwargs)
        self.tasks = tasks
        self.output_trace = output_trace
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        step_names = get_step_names(context)
        previous_task = step_names["previous_steps"][-1] if step_names["previous_steps"] else None
        if previous_task:
            self.logger.info(f"Previous task ID: {previous_task.task_id}")
            previous_task_xcom_data = context['ti'].xcom_pull(task_ids=previous_task.task_id)
            self.logger.debug(f"XCom data from previous task: {previous_task_xcom_data}")
        else:
            raise Exception("No previous task found, starting from the beginning.")

        try:
            csv_data = None
            if previous_task.output_store in previous_task_xcom_data.keys():
                self.logger.info(f"Loaded CSV data from file path in '{previous_task.output_store}'.")
                csv_data_path = previous_task_xcom_data.get(previous_task.output_store, None)
                if not csv_data_path or not isinstance(csv_data_path, str) or not os.path.isfile(csv_data_path):
                    raise ValueError(f"No file path found in XCom with key: {previous_task.output_store}")
                with open(csv_data_path, 'r') as file:
                    csv_data = file.read()
            else:
                raise Exception(f"Neither 'result' nor '{previous_task.output_store}' found in XCom data from previous task.")

            if not csv_data:
                raise ValueError(
                    f"No CSV data found in XCom with key: {previous_task.output_trace}")

            reader = csv.DictReader(csv_data.splitlines())
            final_result: list = []
            for row_number, row in enumerate(reader, start=1):
                self.logger.info(f"Iterating: row {row_number}: {row}")
                context['ti'].xcom_push(key=f"{self.output_trace}_{row_number}", value=row)
                self.logger.info(f"Row {row_number} pushed to XCom with key: {self.output_trace}_{row_number}")

                # Process sub-tasks dynamically
                previous_output = row  # Start with the current row as input
                context["ti"].xcom_push(key=f"previous_output", value=previous_output)
                for task_id, task_config in self.tasks.items():
                    unique_task_id = f"{task_id}_row_{row_number}"  # Ensure unique task ID
                    operator_type = task_config["type"]
                    operator_module = importlib.import_module(operator_type)
                    operator_class = getattr(operator_module, operator_type)

                    task_config_filtered = {key: value for key, value in task_config.items() if key != "type"}
                    sub_task = operator_class(task_id=unique_task_id,
                                              dag=context['dag'],
                                              **task_config_filtered)

                    self.logger.info(f"Executing sub-task: {unique_task_id}")
                    previous_output = sub_task.execute(context)
                    context['ti'].xcom_push(key=f"previous_output", value=previous_output)

                # Push final result to XCom
                context['ti'].xcom_push(key=f"{self.output_trace}_final_{row_number}", value=previous_output)
                final_result.append(previous_output)
                self.logger.info(
                    f"Final result for row {row_number} pushed to XCom with key: {self.output_trace}_final_{row_number}")
            return final_result
        except Exception as e:
            self.logger.error(f"Error processing CSV file: {e}")
            raise
