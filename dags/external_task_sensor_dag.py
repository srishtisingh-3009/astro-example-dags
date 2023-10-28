from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

with DAG(
    dag_id="external_task_sensor_dag", schedule="*/2 * * * *", start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:
    
    t_start = EmptyOperator(
        task_id='Start',
        doc_md="""Dummy Start Task"""
    )
    t_external_task_sensor1 = ExternalTaskSensor(
        task_id="parent_task_sensor11",
        external_dag_id="DAG1",
        external_task_id="Dag1",
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    t_external_task_sensor2 = ExternalTaskSensor(
        task_id="parent_task_sensor12",
        external_dag_id="DAG1",
        external_task_id="Dag1",
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    t_end = EmptyOperator(
        task_id='End',
        doc_md="""Dummy End Task"""
    )

    t_start >> [t_external_task_sensor1,t_external_task_sensor2] >> t_end
    #task_1 >> t_external_task_sensor1
    #task_1 >> t_external_task_sensor2
    #[t_external_task_sensor1,t_external_task_sensor2] >> t_end
    
    
