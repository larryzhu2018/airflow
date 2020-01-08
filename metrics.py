import collections
import traceback
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search, A
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG

url = "http://dev-iad-cluster-ingest.controltower:9200"
# url = "http://localhost:9201"  # devtest

dag_instance_tag = f"{datetime.now().strftime('%m%d%Y%H%M')}"

def get_helk_tags():
    global url, dag_instance_tag
    print(f"[{datetime.now()}] main_for_load_and_aggregate started")
    client = Elasticsearch(
        url,
        timeout=2000)

    s = Search(using=client, index="counthosts-*")
    s = s.filter("range", ** {"@timestamp": {"gte": "now-30d"}})
    s = s.filter('term', type='hostcounts')
    a = A('terms', field='helk_tag')
    s.aggs.bucket('tag_terms', a)
    res=s.execute()
    buckets = res.aggregations['tag_terms']['buckets']
    lst = []
    for i in range(len(buckets)):
        tag = buckets[i]['key']
        try:
            print(f"[{dag_instance_tag}][{datetime.now()}] main_for_load_and_aggregate checking {tag}")
            s = Search(using=client, index="counthosts-*")
            s = s.filter("range", ** {"@timestamp": {"gte": "now-30d"}})
            s = s.filter('term', type='counthosts')
            s = s.filter('term', helk_tag=tag)
            data = s.execute()
            total_hits = int(data['hits']['total']['value'])
            if total_hits:
                print(f"[{dag_instance_tag}][{datetime.now()}] skipping {tag} already processed")
                continue
            s = Search(using=client, index="clusterlog-*")
            s = s.filter("range", ** {"@timestamp": {"gte": "now-30d"}})
            log_tag = f"end_{tag}"
            # log_tag = "load_all_end"
            s = s.query('match', message=log_tag)
            data = s.execute()
            total_hits = int(data['hits']['total']['value'])
            print(f"[{dag_instance_tag}][{datetime.now()}] main_for_load_and_aggregate {log_tag} total hits {total_hits}")
            if not total_hits:
                print(f"{tag} not done yet no {log_tag}")
                continue
            lst.append(tag)
        except Exception as e:
            traceback.print_exc()
            print(f"[{dag_instance_tag}][{datetime.now()}] main_for_load_and_aggregate failed for {tag} : {str(e)}")
    return lst


def load_and_aggregate(tag):
    global url, dag_instance_tag
    print(f"[{dag_instance_tag}][{datetime.now()}] load_and_aggregate load {tag}")
    client = Elasticsearch(
        url,
        timeout=2000)
    s = Search(using=client, index="counthosts-*")
    s = s.filter("range", ** {"@timestamp": {"gte": "now-30d"}})
    s = s.filter('term', helk_tag=tag)
    s = s.filter('term', type='hostcounts')
    s.execute()
    counts = collections.defaultdict(int)
    counts_by_file_source = collections.defaultdict(int)
    all_hosts = {}
    all_file_sources = set()
    for hit in s.scan():
        host_id = hit['host-identifier']
        file_source = hit['file-source']
        all_file_sources.add(file_source)
        counts[host_id] += 1
        counts_by_file_source[host_id, file_source] += 1
        if host_id not in all_hosts:
            all_hosts[host_id] = hit.to_dict()
        if (host_id, file_source) not in all_hosts:
            all_hosts[host_id, file_source] = hit.to_dict()
    for file_source in all_file_sources:
        print(f"[{dag_instance_tag}][{datetime.now()}] load_and_aggregate uploading counts {tag} for {file_source}")
        post_data = []
        for host_id in counts:
            if (host_id, file_source) not in all_hosts:
                continue
            data = all_hosts[host_id, file_source]
            data['helk_count'] = counts_by_file_source[host_id, file_source]
            data['type'] = f"hosts_{file_source}"
            data['_id'] = f"{host_id}_{file_source}_{tag}"
            data['_index'] = 'counthosts-000001'
            post_data.append(data)
        helpers.bulk(client, post_data)
        print(f"[{dag_instance_tag}][{datetime.now()}] load_and_aggregate uploaded {tag} {len(post_data)} for {file_source}")

    print(f"[{dag_instance_tag}][{datetime.now()}] load_and_aggregate uploading counts {tag}")
    post_data = []
    for host_id in counts:
        data = all_hosts[host_id]
        data['helk_count'] = counts[host_id]
        data['type'] = 'counthosts'
        data['_id'] = f"{host_id}_{tag}"
        data['_index'] = 'counthosts-000001'
        post_data.append(data)
    helpers.bulk(client, post_data)
    print(f"[{dag_instance_tag}][{datetime.now()}] load_and_aggregate uploaded {tag} {len(post_data)}")
    print(f"[{dag_instance_tag}][{datetime.now()}] load_and_aggregate done {tag}")


def load_by_tag_op(**kwargs):
    helk_tag = kwargs['helk_tag']
    load_and_aggregate(helk_tag)

dag = DAG('log_metrics', description='Log Metrics DAG',
          schedule_interval='*/30 * * * *',
          start_date=datetime(2019, 12, 20), catchup=False)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)
tags = get_helk_tags()

chain_end_task = True
for helk_tag in tags:
    if chain_end_task:
        data_task = DummyOperator(
            task_id='data_task',
            dag=dag,
        )
        start_task >> data_task
        chain_end_task = False
    print(f"[{dag_instance_tag}][{datetime.now()}] load_and_aggregate {helk_tag}")
    task = PythonOperator(
        task_id='load_index_for_' + helk_tag,
        python_callable=load_by_tag_op,
        op_kwargs={'helk_tag': helk_tag},
        dag=dag,
    )
    start_task >> data_task >> task >> end_task
if chain_end_task:
    start_task >> end_task
